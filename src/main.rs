// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! gg-log-manager - Greengrass LogManager Generic Type Component
//!
//! Rust implementation of aws.greengrass.LogManager for GG Classic and GG Lite.
//! Tails log files and EMF JSON files, uploads to CloudWatch Logs.

use gg_log_manager::config::{
    derive_log_group_name, load_config, validate_config, ConfigError, LogLevel, LogManagerConfig,
    LogSourceConfig,
};
use gg_log_manager::credentials::resolve_thing_name;
use gg_log_manager::scanner::{
    assemble_multiline, load_checkpoint, read_file_from_offset, recover_offsets, save_checkpoint,
    scan_directory, trim_stale_on_load, CheckpointStore, LogEvent, ScannedFile,
};
use gg_log_manager::uploader::{
    advance_checkpoints, complete_file, effective_interval_secs, evict_stale_entries,
    format_log_stream_name, mtime_to_ms, upload_source_events, CwLogsClient, CwUploadError,
    TTL_24H_MS,
};
use regex::Regex;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Notify;
use tokio::time::Duration;
use tracing::{error, info, warn};

static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);

/// Result of processing a source (or a whole cycle). `StopForAuth` signals a systemic auth
/// failure that should halt the rest of the cycle so the SDK credential provider can refresh
/// before the next pass; `Continue` means carry on normally. Self-documenting alternative to
/// a bare `bool` whose `true`/`false` meaning would not be obvious at the call sites.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CycleOutcome {
    Continue,
    StopForAuth,
}

#[cfg(not(tarpaulin_include))]
#[tokio::main(flavor = "current_thread")]
async fn main() {
    tracing_subscriber::fmt::init();

    let args: Vec<String> = std::env::args().collect();
    let raw_config = resolve_raw_config(&args);
    let work_dir = parse_arg(&args, "--work-dir").unwrap_or_else(|| ".".to_string());

    let config = match load_config(&raw_config) {
        Ok(c) => c,
        Err(e) => {
            error!("Config load error: {e}");
            std::process::exit(1);
        }
    };

    if let Err(e) = validate_config(&config) {
        error!("Config validation error: {e}");
        std::process::exit(1);
    }

    info!(
        components = ?config
            .logs_uploader_configuration
            .component_logs_configuration_map
            .keys()
            .collect::<Vec<_>>(),
        system_logs = config
            .logs_uploader_configuration
            .system_logs_configuration
            .is_some(),
        interval_sec = config.periodic_upload_interval_sec,
        "Configuration loaded"
    );

    // Missing/empty thing name is non-fatal: log and continue with a placeholder.
    let thing_name = resolve_thing_name().unwrap_or_else(|| {
        error!("AWS_IOT_THING_NAME not set or empty, using 'unknown'");
        "unknown".to_string()
    });

    let checkpoint_path = Path::new(&work_dir).join("checkpoint.json");
    let mut store = load_checkpoint(&checkpoint_path, config.deprecated_version_support)
        .unwrap_or_else(|e| {
            error!("Failed to load checkpoint: {e}, starting fresh");
            CheckpointStore::default()
        });
    trim_stale_on_load(&mut store);

    let mut client = CwLogsClient::new().await;

    // Shutdown signal handling (Ctrl-C and, on Unix, SIGTERM).
    let shutdown_notify = Arc::new(Notify::new());
    let shutdown_clone = shutdown_notify.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        initiate_shutdown(&shutdown_clone);
    });
    #[cfg(unix)]
    {
        let shutdown_sigterm = shutdown_notify.clone();
        tokio::spawn(async move {
            let mut sigterm =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("Failed to register SIGTERM handler");
            sigterm.recv().await;
            initiate_shutdown(&shutdown_sigterm);
        });
    }

    // TODO: a LogManager struct owning client/store/thing_name/checkpoint_path would let the
    // process_* helpers carry less state; kept as free functions for now.
    let mut last_persist = Instant::now();
    let persist_interval = Duration::from_secs(config.periodic_upload_interval_sec);

    // Fixed-delay loop: process every source, then sleep the upload interval (so a slow cycle
    // never overlaps the next). The wait is a tokio::select! over the sleep and the shutdown
    // notify, so a shutdown request wakes the loop immediately instead of waiting out the sleep.
    loop {
        if SHUTDOWN_REQUESTED.load(Ordering::SeqCst) {
            break;
        }

        // One timestamp shared across this cycle's checkpoint advancement and stale eviction.
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        // Auth failure is systemic: stop the cycle so credentials can refresh before the next pass.
        if process_all_sources(&config, &mut client, &mut store, &thing_name, now).await
            == CycleOutcome::StopForAuth
        {
            warn!("Upload cycle stopped due to auth error; retrying next cycle");
        }

        // Persist the checkpoint at most once per upload interval.
        if last_persist.elapsed() >= persist_interval {
            if let Err(e) =
                save_checkpoint(&checkpoint_path, &store, config.deprecated_version_support)
            {
                error!("Failed to persist checkpoint: {e}");
            }
            last_persist = Instant::now();
        }

        // TODO: honor per-source uploadIntervalSec here. It is parsed but not yet wired,
        // so every source currently uploads on the global periodicUploadIntervalSec cadence.
        let sleep_secs = effective_interval_secs(None, config.periodic_upload_interval_sec);
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(sleep_secs)) => {}
            _ = shutdown_notify.notified() => { break; }
        }
    }

    // Persist the checkpoint unconditionally on the way out so progress is not lost.
    info!("Shutting down — persisting final checkpoint");
    if let Err(e) = save_checkpoint(&checkpoint_path, &store, config.deprecated_version_support) {
        error!("Failed to persist checkpoint on shutdown: {e}");
    }
    info!("Shutdown complete");
}

/// Resolve the raw config from ordered sources (first non-empty wins):
/// (1) CLI `--config <inline-json-or-path>`, (2) env `GG_LOG_MANAGER_CONFIG`, (3) `{}` default.
// TODO: prepend a higher-priority runtime config source ahead of CLI/env once that
// integration lands; parsing stays agnostic to where the raw config originates.
fn resolve_raw_config(args: &[String]) -> String {
    for source in [config_from_cli(args), config_from_env()] {
        match source {
            Ok(Some(raw)) => return raw,
            Ok(None) => {}
            Err(e) => error!("Config source error: {e}; trying next source"),
        }
    }
    // (3) DEFAULT: empty object — start with no sources configured.
    "{}".to_string()
}

/// (1) CLI source: the `--config` argument value (inline JSON or a file path), if present.
fn config_from_cli(args: &[String]) -> Result<Option<String>, ConfigError> {
    Ok(parse_arg(args, "--config"))
}

/// (2) Env source: the `GG_LOG_MANAGER_CONFIG` variable (inline JSON or a file path), if
/// set and non-empty.
fn config_from_env() -> Result<Option<String>, ConfigError> {
    Ok(std::env::var("GG_LOG_MANAGER_CONFIG")
        .ok()
        .filter(|s| !s.is_empty()))
}

/// Process all configured log sources (component + system) sequentially in one cycle.
#[cfg(not(tarpaulin_include))]
async fn process_all_sources(
    config: &LogManagerConfig,
    client: &mut CwLogsClient,
    store: &mut CheckpointStore,
    thing_name: &str,
    now: u64,
) -> CycleOutcome {
    // Collect all sources as (source, log_group).
    let mut sources: Vec<(&LogSourceConfig, String)> = Vec::new();

    for (name, comp) in &config
        .logs_uploader_configuration
        .component_logs_configuration_map
    {
        let log_group = comp
            .log_group_name
            .as_deref()
            .map(str::to_string)
            .unwrap_or_else(|| derive_log_group_name(name, None));
        sources.push((&comp.source, log_group));
    }

    if let Some(ref sys) = config.logs_uploader_configuration.system_logs_configuration {
        let enabled = sys.upload_to_cloud_watch;
        // TODO: when logFileDirectoryPath/logFileRegex are unset on the system source,
        // runtime-resolve them from the runtime logging configuration, regardless of which
        // source supplies it; until then an unconfigured system source is skipped.
        if enabled && !sys.source.log_file_directory_path.is_empty() {
            let log_group = sys
                .log_group_name
                .as_deref()
                .map(str::to_string)
                .unwrap_or_else(|| {
                    derive_log_group_name("System", Some("GreengrassSystemComponent"))
                });
            sources.push((&sys.source, log_group));
        } else if enabled {
            warn!("systemLogsConfiguration has uploadToCloudWatch enabled but no logFileDirectoryPath configured — skipping");
        }
    }

    // TODO: per-source fault isolation is future work; sources run sequentially, so a slow or
    // failing source delays the rest.
    for (source, log_group) in sources {
        if SHUTDOWN_REQUESTED.load(Ordering::SeqCst) {
            return CycleOutcome::Continue;
        }
        let pattern = match Regex::new(&source.log_file_regex) {
            Ok(r) => r,
            Err(e) => {
                error!(log_group, error = %e, "Invalid file regex, skipping");
                continue;
            }
        };
        if process_source(source, &log_group, &pattern, client, store, thing_name, now).await
            == CycleOutcome::StopForAuth
        {
            // Auth failure — stop the cycle so credentials can refresh before the next pass.
            return CycleOutcome::StopForAuth;
        }
    }
    CycleOutcome::Continue
}

/// Process a single log source: scan → read → batch → upload → checkpoint → disk.
// Uses blocking std::fs; safe only on the current_thread runtime (switch to tokio::fs if that changes).
#[cfg(not(tarpaulin_include))]
async fn process_source(
    source: &LogSourceConfig,
    log_group: &str,
    pattern: &Regex,
    client: &mut CwLogsClient,
    store: &mut CheckpointStore,
    thing_name: &str,
    now: u64,
) -> CycleOutcome {
    let scanned_files = match scan_and_filter_files(source, log_group, pattern, store) {
        Some(files) => files,
        None => return CycleOutcome::Continue,
    };

    let file_events = read_file_events(source, log_group, &scanned_files, store);
    if file_events.is_empty() {
        return CycleOutcome::Continue;
    }

    upload_and_advance_checkpoints(
        source,
        log_group,
        pattern,
        client,
        store,
        thing_name,
        &scanned_files,
        file_events,
        now,
    )
    .await
}

/// Scan the log directory for matching files, dropping already-completed files (older than the
/// last uploaded file and not mid-upload).
fn scan_and_filter_files(
    source: &LogSourceConfig,
    log_group: &str,
    pattern: &Regex,
    store: &CheckpointStore,
) -> Option<Vec<ScannedFile>> {
    let scanned_files = match scan_directory(&source.log_file_directory_path, pattern) {
        Ok(files) => files,
        Err(e) => {
            error!(directory = source.log_file_directory_path, error = %e, "Failed to scan directory");
            return None;
        }
    };

    if scanned_files.is_empty() {
        return None;
    }

    let last_ts = store
        .last_processed_timestamps
        .get(log_group)
        .map(|t| t.last_file_processed_time_stamp)
        .unwrap_or(0);
    let scanned_files: Vec<_> = scanned_files
        .into_iter()
        .filter(|f| {
            mtime_to_ms(f.mtime) > last_ts
                || store
                    .file_processing_info
                    .get(log_group)
                    .is_some_and(|m| m.contains_key(&f.content_hash))
        })
        .collect();

    if scanned_files.is_empty() {
        return None;
    }

    Some(scanned_files)
}

/// Read new content from each scanned file at its checkpointed offset; assemble multi-line entries.
fn read_file_events(
    source: &LogSourceConfig,
    log_group: &str,
    scanned_files: &[ScannedFile],
    store: &mut CheckpointStore,
) -> Vec<(PathBuf, Vec<LogEvent>, u64, String)> {
    let offsets = recover_offsets(store, log_group, scanned_files);
    let multi_regex = source
        .multi_line_start_pattern
        .as_deref()
        .and_then(|p| Regex::new(p).ok());
    // Per-event fallback timestamp for lines with no parseable time — intentionally distinct
    // from the cycle-level `now` used for checkpoint bookkeeping.
    let default_ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;

    let mut file_events: Vec<(PathBuf, Vec<LogEvent>, u64, String)> =
        Vec::with_capacity(offsets.len());

    for (path, offset) in &offsets {
        // Rotation edge case: a fully-read, no-longer-active file is completed here.
        let file_len = match std::fs::metadata(path) {
            Ok(m) => m.len(),
            Err(e) => {
                tracing::debug!(path = %path.display(), error = %e, "Cannot stat file, skipping");
                continue;
            }
        };
        if *offset >= file_len && file_len > 0 {
            let scanned_file = scanned_files.iter().find(|f| f.path == *path);
            let is_active = scanned_file.is_some_and(|f| f.is_active);
            if !is_active {
                if let Some(sf) = scanned_file {
                    // Resolve via `entry().or_default()` (uniform with `advance_checkpoints`) so
                    // the `complete_file` timestamp advance is unconditional.
                    let file_map = store
                        .file_processing_info
                        .entry(log_group.to_string())
                        .or_default();
                    complete_file(
                        file_map,
                        &mut store.last_processed_timestamps,
                        log_group,
                        &sf.content_hash,
                        mtime_to_ms(sf.mtime),
                    );
                    if source.delete_log_file_after_cloud_upload {
                        if let Err(e) = std::fs::remove_file(path) {
                            warn!(path = %path.display(), error = %e, "Failed to delete completed file");
                        } else {
                            info!(path = %path.display(), "Deleted completed file (rotation edge case)");
                        }
                    }
                }
                continue;
            }
        }

        let (events, new_offset) = match read_file_from_offset(path, *offset, default_ts) {
            Ok(result) => result,
            Err(e) => {
                warn!(path = %path.display(), error = %e, "Failed to read file, skipping");
                continue;
            }
        };

        if events.is_empty() {
            continue;
        }

        let assembled = assemble_multiline(events, multi_regex.as_ref());

        let content_hash = scanned_files
            .iter()
            .find(|f| f.path == *path)
            .map(|f| f.content_hash.clone())
            .unwrap_or_default();

        file_events.push((path.clone(), assembled, new_offset, content_hash));
    }

    file_events
}

/// Upload batched events, advance checkpoints for fully-uploaded files, delete completed
/// files when configured, and enforce the disk-space limit.
#[cfg(not(tarpaulin_include))]
#[allow(clippy::too_many_arguments)]
async fn upload_and_advance_checkpoints(
    source: &LogSourceConfig,
    log_group: &str,
    pattern: &Regex,
    client: &mut CwLogsClient,
    store: &mut CheckpointStore,
    thing_name: &str,
    scanned_files: &[ScannedFile],
    file_events: Vec<(PathBuf, Vec<LogEvent>, u64, String)>,
    now: u64,
) -> CycleOutcome {
    let log_stream = format_log_stream_name(thing_name);

    // DEBUG means "no filtering" — all events pass through.
    let min_level = if matches!(source.minimum_log_level, LogLevel::Debug) {
        None
    } else {
        Some(source.minimum_log_level)
    };

    let result =
        match upload_source_events(client, log_group, &log_stream, file_events, min_level).await {
            Ok(result) => result,
            Err(CwUploadError::Auth) => {
                warn!("Auth error — stopping cycle to allow credential refresh");
                return CycleOutcome::StopForAuth;
            }
            Err(e) => {
                warn!(error = %e, "Upload failed for source — will retry next cycle");
                return CycleOutcome::Continue;
            }
        };

    if !result.succeeded.is_empty() {
        let completed =
            advance_checkpoints(store, log_group, &result.succeeded, scanned_files, now);

        if source.delete_log_file_after_cloud_upload {
            for path in &completed {
                if let Err(e) = std::fs::remove_file(path) {
                    warn!(path = %path.display(), error = %e, "Failed to delete completed file");
                } else {
                    info!(path = %path.display(), "Deleted completed file after upload");
                }
            }
        }

        // Disk enforcement runs only after a successful upload and only deletes fully-uploaded files.
        // TODO: if over the disk limit but the upload did not complete, nothing is freed this
        // cycle (only uploaded files are deletable); revisiting this trade-off is future work.
        if let Some(limit_str) = source.disk_space_limit.as_deref() {
            if let Ok(limit_val) = limit_str.parse::<u64>() {
                let limit_bytes = source.disk_space_limit_unit.to_bytes(limit_val);
                let last_ts = store
                    .last_processed_timestamps
                    .get(log_group)
                    .map(|t| t.last_file_processed_time_stamp)
                    .unwrap_or(0);
                let all_eligible: Vec<PathBuf> = scanned_files
                    .iter()
                    .filter(|f| !f.is_active)
                    .filter(|f| mtime_to_ms(f.mtime) <= last_ts)
                    .map(|f| f.path.clone())
                    .collect();
                let deleted = gg_log_manager::disk::free_disk_space(
                    Path::new(&source.log_file_directory_path),
                    pattern,
                    limit_bytes,
                    &all_eligible,
                );
                if !deleted.is_empty() {
                    if let Some(file_map) = store.file_processing_info.get_mut(log_group) {
                        for del_path in &deleted {
                            if let Some(sf) = scanned_files.iter().find(|f| f.path == *del_path) {
                                file_map.remove(&sf.content_hash);
                            }
                        }
                    }
                }
            }
        }
    }

    // Evict stale checkpoint entries each cycle so the checkpoint doesn't grow unbounded as files rotate.
    evict_stale_entries(store, log_group, TTL_24H_MS, now);

    if !result.failed.is_empty() {
        warn!(
            log_group,
            failed_files = result.failed.len(),
            "Some files failed upload — will retry next cycle"
        );
    }

    CycleOutcome::Continue
}

fn parse_arg(args: &[String], flag: &str) -> Option<String> {
    for i in 0..args.len() {
        if args[i] == flag && i + 1 < args.len() {
            return Some(args[i + 1].clone());
        }
    }
    None
}

fn initiate_shutdown(notify: &Notify) {
    if !SHUTDOWN_REQUESTED.swap(true, Ordering::SeqCst) {
        info!("Received shutdown signal");
        notify.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    #[test]
    fn test_parse_arg_with_flag() {
        let args = vec![
            "bin".to_string(),
            "--config".to_string(),
            "/path/config.json".to_string(),
        ];
        assert_eq!(
            parse_arg(&args, "--config"),
            Some("/path/config.json".to_string())
        );
    }

    #[test]
    fn test_parse_arg_missing() {
        let args = vec!["bin".to_string()];
        assert_eq!(parse_arg(&args, "--config"), None);
    }

    #[test]
    fn test_parse_arg_flag_at_end() {
        let args = vec!["bin".to_string(), "--config".to_string()];
        assert_eq!(parse_arg(&args, "--config"), None);
    }

    #[test]
    fn test_parse_arg_work_dir() {
        let args = vec![
            "bin".to_string(),
            "--work-dir".to_string(),
            "/tmp/work".to_string(),
        ];
        assert_eq!(
            parse_arg(&args, "--work-dir"),
            Some("/tmp/work".to_string())
        );
    }

    #[test]
    fn test_config_from_cli_present() {
        let args = vec!["bin".to_string(), "--config".to_string(), "{}".to_string()];
        assert_eq!(config_from_cli(&args).unwrap(), Some("{}".to_string()));
    }

    #[test]
    fn test_config_from_cli_absent() {
        let args = vec!["bin".to_string()];
        assert_eq!(config_from_cli(&args).unwrap(), None);
    }

    #[test]
    #[serial]
    fn test_config_from_env_present_and_absent() {
        std::env::set_var(
            "GG_LOG_MANAGER_CONFIG",
            r#"{"periodicUploadIntervalSec":7}"#,
        );
        assert_eq!(
            config_from_env().unwrap(),
            Some(r#"{"periodicUploadIntervalSec":7}"#.to_string())
        );
        // Empty is treated as absent.
        std::env::set_var("GG_LOG_MANAGER_CONFIG", "");
        assert_eq!(config_from_env().unwrap(), None);
        std::env::remove_var("GG_LOG_MANAGER_CONFIG");
        assert_eq!(config_from_env().unwrap(), None);
    }

    #[test]
    #[serial]
    fn test_resolve_raw_config_cli_wins_over_env() {
        std::env::set_var("GG_LOG_MANAGER_CONFIG", "ENV");
        let args = vec!["bin".to_string(), "--config".to_string(), "CLI".to_string()];
        assert_eq!(resolve_raw_config(&args), "CLI");
        std::env::remove_var("GG_LOG_MANAGER_CONFIG");
    }

    #[test]
    #[serial]
    fn test_resolve_raw_config_env_used_without_cli() {
        std::env::set_var("GG_LOG_MANAGER_CONFIG", "ENV");
        let args = vec!["bin".to_string()];
        assert_eq!(resolve_raw_config(&args), "ENV");
        std::env::remove_var("GG_LOG_MANAGER_CONFIG");
    }

    #[test]
    #[serial]
    fn test_resolve_raw_config_default_when_no_source() {
        std::env::remove_var("GG_LOG_MANAGER_CONFIG");
        let args = vec!["bin".to_string()];
        assert_eq!(resolve_raw_config(&args), "{}");
    }

    #[test]
    fn test_initiate_shutdown_sets_flag() {
        SHUTDOWN_REQUESTED.store(false, Ordering::SeqCst);
        let notify = Notify::new();
        initiate_shutdown(&notify);
        assert!(SHUTDOWN_REQUESTED.load(Ordering::SeqCst));
    }

    #[test]
    fn test_initiate_shutdown_idempotent() {
        SHUTDOWN_REQUESTED.store(false, Ordering::SeqCst);
        let notify = Notify::new();
        initiate_shutdown(&notify);
        initiate_shutdown(&notify); // Second call should be a no-op.
        assert!(SHUTDOWN_REQUESTED.load(Ordering::SeqCst));
    }

    // ---- read_file_events / scan_and_filter_files (client-free, tempfile-based) ----

    use gg_log_manager::config::DiskSpaceLimitUnit;
    use gg_log_manager::scanner::{
        compute_content_hash, FileCheckpoint, LastFileProcessedTimestamp,
    };
    use std::collections::HashMap;
    use std::time::{Duration as StdDuration, UNIX_EPOCH};

    fn mk_source(dir: &Path, regex: &str) -> LogSourceConfig {
        LogSourceConfig {
            log_file_directory_path: dir.to_string_lossy().into_owned(),
            log_file_regex: regex.to_string(),
            minimum_log_level: LogLevel::Info,
            disk_space_limit: None,
            disk_space_limit_unit: DiskSpaceLimitUnit::KB,
            delete_log_file_after_cloud_upload: false,
            multi_line_start_pattern: None,
            upload_interval_sec: None,
        }
    }

    fn mtime_at(ms: u64) -> std::time::SystemTime {
        UNIX_EPOCH + StdDuration::from_millis(ms)
    }

    fn set_file_mtime_ms(path: &Path, ms: u64) {
        filetime::set_file_mtime(
            path,
            filetime::FileTime::from_unix_time(
                (ms / 1000) as i64,
                ((ms % 1000) * 1_000_000) as u32,
            ),
        )
        .unwrap();
    }

    // (1) PRIORITY: a fully-read, rotated (inactive) file is completed end to end — its
    // checkpoint entry is removed and the component timestamp advances. The timestamp map is
    // initially ABSENT for the group, exercising complete_file's or_insert path.
    #[test]
    fn test_read_file_events_completes_rotated_file_advances_ts_when_ts_group_absent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rotated.log");
        std::fs::write(&path, "one line\n").unwrap();
        let file_len = std::fs::metadata(&path).unwrap().len();

        let hash = "hash-rotated".to_string();
        let scanned = vec![ScannedFile {
            path: path.clone(),
            mtime: mtime_at(7000),
            content_hash: hash.clone(),
            is_active: false, // rotated — not the active file
        }];

        // Checkpoint marks the file fully read (start_position == file_len) so the completion
        // branch fires; last_processed_timestamps has NO entry for the group.
        let mut store = CheckpointStore::default();
        let mut fm = HashMap::new();
        fm.insert(
            hash.clone(),
            FileCheckpoint {
                file_hash: hash.clone(),
                start_position: file_len,
                last_modified_time: 7000,
                last_accessed: 0,
            },
        );
        store.file_processing_info.insert("grp".to_string(), fm);
        assert!(!store.last_processed_timestamps.contains_key("grp"));

        let events = read_file_events(
            &mk_source(dir.path(), r".*\.log$"),
            "grp",
            &scanned,
            &mut store,
        );

        // Completed files are not returned as events.
        assert!(events.is_empty());
        // Checkpoint entry removed.
        assert!(!store
            .file_processing_info
            .get("grp")
            .unwrap()
            .contains_key(&hash));
        // Timestamp advance fired even though the group was absent (C6 Site-A guard).
        assert_eq!(
            store
                .last_processed_timestamps
                .get("grp")
                .unwrap()
                .last_file_processed_time_stamp,
            7000
        );
        // File not deleted (delete disabled).
        assert!(path.exists());
    }

    // (2) A fully-read but ACTIVE file is not completed: no events, checkpoint untouched.
    #[test]
    fn test_read_file_events_active_fully_read_skips_completion() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("active.log");
        std::fs::write(&path, "content\n").unwrap();
        let file_len = std::fs::metadata(&path).unwrap().len();

        let hash = "hash-active".to_string();
        let scanned = vec![ScannedFile {
            path: path.clone(),
            mtime: mtime_at(3000),
            content_hash: hash.clone(),
            is_active: true, // active — never completed
        }];

        let mut store = CheckpointStore::default();
        let mut fm = HashMap::new();
        fm.insert(
            hash.clone(),
            FileCheckpoint {
                file_hash: hash.clone(),
                start_position: file_len,
                last_modified_time: 3000,
                last_accessed: 0,
            },
        );
        store.file_processing_info.insert("grp".to_string(), fm);

        let events = read_file_events(
            &mk_source(dir.path(), r".*\.log$"),
            "grp",
            &scanned,
            &mut store,
        );

        assert!(events.is_empty());
        // Active file's checkpoint is retained; no completion timestamp is written.
        assert!(store
            .file_processing_info
            .get("grp")
            .unwrap()
            .contains_key(&hash));
        assert!(!store.last_processed_timestamps.contains_key("grp"));
    }

    // (3) A partial read (offset < file_len) returns the events after the offset.
    #[test]
    fn test_read_file_events_partial_read_returns_events() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("partial.log");
        std::fs::write(&path, "alpha\nbeta\n").unwrap(); // "alpha\n" = 6 bytes
        let file_len = std::fs::metadata(&path).unwrap().len();

        let hash = "hash-partial".to_string();
        let scanned = vec![ScannedFile {
            path: path.clone(),
            mtime: mtime_at(1000),
            content_hash: hash.clone(),
            is_active: true,
        }];

        // Resume from byte 6 (after "alpha\n").
        let mut store = CheckpointStore::default();
        let mut fm = HashMap::new();
        fm.insert(
            hash.clone(),
            FileCheckpoint {
                file_hash: hash.clone(),
                start_position: 6,
                last_modified_time: 1000,
                last_accessed: 0,
            },
        );
        store.file_processing_info.insert("grp".to_string(), fm);

        let events = read_file_events(
            &mk_source(dir.path(), r".*\.log$"),
            "grp",
            &scanned,
            &mut store,
        );

        assert_eq!(events.len(), 1);
        let (ev_path, assembled, new_offset, ev_hash) = &events[0];
        assert_eq!(ev_path, &path);
        assert_eq!(*new_offset, file_len);
        assert_eq!(ev_hash, &hash);
        assert_eq!(assembled.len(), 1);
        assert_eq!(assembled[0].message, "beta");
    }

    // (4) A file that yields only blank lines produces no events and is skipped.
    #[test]
    fn test_read_file_events_empty_events_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blank.log");
        std::fs::write(&path, "\n  \n\n").unwrap();

        let scanned = vec![ScannedFile {
            path: path.clone(),
            mtime: mtime_at(1000),
            content_hash: "hash-blank".to_string(),
            is_active: true,
        }];
        let mut store = CheckpointStore::default();

        let events = read_file_events(
            &mk_source(dir.path(), r".*\.log$"),
            "grp",
            &scanned,
            &mut store,
        );
        assert!(events.is_empty());
    }

    // (5) Continuation lines fold into the preceding date-stamped entry (multiline assembly).
    #[test]
    fn test_read_file_events_multiline_assembly() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("multi.log");
        std::fs::write(
            &path,
            "2024-01-01 start\n    continued line\n2024-01-02 next\n",
        )
        .unwrap();
        let file_len = std::fs::metadata(&path).unwrap().len();

        let scanned = vec![ScannedFile {
            path: path.clone(),
            mtime: mtime_at(1000),
            content_hash: "hash-multi".to_string(),
            is_active: true,
        }];
        let mut store = CheckpointStore::default();

        let events = read_file_events(
            &mk_source(dir.path(), r".*\.log$"),
            "grp",
            &scanned,
            &mut store,
        );

        assert_eq!(events.len(), 1);
        let (_, assembled, new_offset, _) = &events[0];
        // Two entries: the indented line folds into the first date-stamped entry.
        assert_eq!(assembled.len(), 2);
        assert_eq!(*new_offset, file_len);
    }

    // (6) stat-error best-effort: a scanned path that no longer exists on disk is skipped
    // (metadata() errors) rather than panicking.
    #[test]
    fn test_read_file_events_stat_error_is_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("gone.log");
        let scanned = vec![ScannedFile {
            path: missing,
            mtime: mtime_at(1000),
            content_hash: "hash-missing".to_string(),
            is_active: true,
        }];
        let mut store = CheckpointStore::default();

        let events = read_file_events(
            &mk_source(dir.path(), r".*\.log$"),
            "grp",
            &scanned,
            &mut store,
        );
        assert!(events.is_empty());
    }

    #[test]
    fn test_scan_and_filter_empty_dir_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let store = CheckpointStore::default();
        let pattern = Regex::new(r".*\.log$").unwrap();
        let result =
            scan_and_filter_files(&mk_source(dir.path(), r".*\.log$"), "grp", &pattern, &store);
        assert!(result.is_none());
    }

    #[test]
    fn test_scan_and_filter_all_old_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("old.log");
        std::fs::write(&path, "old data\n").unwrap();
        set_file_mtime_ms(&path, 1_000_000);

        let mut store = CheckpointStore::default();
        // Last-processed timestamp is AFTER the file mtime and there is no checkpoint entry.
        store.last_processed_timestamps.insert(
            "grp".to_string(),
            LastFileProcessedTimestamp {
                last_file_processed_time_stamp: 2_000_000,
            },
        );

        let pattern = Regex::new(r".*\.log$").unwrap();
        let result =
            scan_and_filter_files(&mk_source(dir.path(), r".*\.log$"), "grp", &pattern, &store);
        assert!(result.is_none());
    }

    #[test]
    fn test_scan_and_filter_new_file_kept() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("new.log");
        std::fs::write(&path, "fresh\n").unwrap();
        set_file_mtime_ms(&path, 5_000_000);

        // Default last-processed timestamp is 0, so a positive mtime keeps the file.
        let store = CheckpointStore::default();
        let pattern = Regex::new(r".*\.log$").unwrap();
        let result =
            scan_and_filter_files(&mk_source(dir.path(), r".*\.log$"), "grp", &pattern, &store);
        let files = result.expect("new file should be kept");
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].path, path);
    }

    #[test]
    fn test_scan_and_filter_checkpointed_old_file_kept() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("old-cp.log");
        std::fs::write(&path, "resume me\n").unwrap();
        set_file_mtime_ms(&path, 1_000_000);
        let hash = compute_content_hash(&path).unwrap();

        let mut store = CheckpointStore::default();
        // Old by timestamp...
        store.last_processed_timestamps.insert(
            "grp".to_string(),
            LastFileProcessedTimestamp {
                last_file_processed_time_stamp: 2_000_000,
            },
        );
        // ...but an in-progress checkpoint entry exists for its hash → must be kept.
        let mut fm = HashMap::new();
        fm.insert(
            hash.clone(),
            FileCheckpoint {
                file_hash: hash.clone(),
                start_position: 3,
                last_modified_time: 1_000_000,
                last_accessed: 0,
            },
        );
        store.file_processing_info.insert("grp".to_string(), fm);

        let pattern = Regex::new(r".*\.log$").unwrap();
        let result =
            scan_and_filter_files(&mk_source(dir.path(), r".*\.log$"), "grp", &pattern, &store);
        let files = result.expect("checkpointed old file should be kept");
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].content_hash, hash);
    }
}
