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
#[cfg(feature = "gg-ipc")]
use gg_log_manager::ipc_config::{connect_ipc, read_config};
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
use std::sync::{Arc, RwLock};
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

    // Establish the single IPC connection (gg-ipc build) up front and read the startup config
    // from it. The same handle is reused for the configuration-update subscription below, so
    // `Sdk::init()` runs at most once per process.
    #[cfg(feature = "gg-ipc")]
    let ipc_sdk = connect_ipc();
    #[cfg(feature = "gg-ipc")]
    let ipc_value = match ipc_sdk {
        Some(sdk) => read_config(sdk),
        None => None,
    };
    #[cfg(not(feature = "gg-ipc"))]
    let ipc_value: Option<String> = None;

    let raw_config = resolve_raw_config(&args, ipc_value);
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

    // Live config cell: the loop reads a fresh snapshot at the top of each cycle, and the
    // configuration-update subscription (gg-ipc) swaps in a new value between cycles, so config
    // changes are consumed without restarting the component.
    let shared_config: Arc<RwLock<Arc<LogManagerConfig>>> = Arc::new(RwLock::new(Arc::new(config)));

    // Woken when the runtime notifies a configuration update. The subscription callback runs on
    // the SDK's IPC receive thread, which must NOT make IPC calls, so it only signals here; the
    // loop thread (which owns the SDK handle) does the actual re-read. Never fired on the default
    // build (no subscription), where the cell keeps the startup config for the process lifetime.
    let config_changed = Arc::new(Notify::new());

    // Subscribe to the component's own configuration updates and hold the subscription for the
    // process lifetime (its Drop unsubscribes). The callback is leaked to give it a 'static
    // lifetime; it only wakes the loop.
    #[cfg(feature = "gg-ipc")]
    let _config_sub = ipc_sdk.and_then(|sdk| {
        let notify_cb = config_changed.clone();
        let cb: &'static _ = Box::leak(Box::new(move |_component: &str, _key: &[&str]| {
            // A panic unwinding across the extern "C" trampoline would abort the process, so the
            // callback body is guarded even though notifying cannot realistically panic.
            let _ =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| notify_cb.notify_one()));
        }));
        match sdk.subscribe_to_configuration_update(None, &[], cb) {
            Ok(sub) => {
                info!("Subscribed to configuration updates");
                Some(sub)
            }
            Err(e) => {
                warn!(error = %e, "Failed to subscribe to configuration updates; config changes will not apply until restart");
                None
            }
        }
    });

    let mut last_persist = Instant::now();

    // Fixed-delay loop: process every source, then sleep the upload interval (so a slow cycle
    // never overlaps the next). The wait is a tokio::select! over the sleep and the shutdown
    // notify, so a shutdown request wakes the loop immediately instead of waiting out the sleep.
    loop {
        if SHUTDOWN_REQUESTED.load(Ordering::SeqCst) {
            break;
        }

        // Snapshot the current config for this cycle (cheap Arc clone; lock released immediately).
        // A live update swaps the cell between cycles, so the next pass picks it up.
        let config = shared_config
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        // Recomputed each cycle (the only config-derived value cached across cycles) so a live
        // change to periodicUploadIntervalSec takes effect on reload.
        let persist_interval = Duration::from_secs(config.periodic_upload_interval_sec);

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
            _ = config_changed.notified() => {
                // Config-update notification: re-read on THIS (loop) thread — the callback thread
                // may not make IPC calls — then swap the new config in for the next cycle.
                #[cfg(feature = "gg-ipc")]
                let updated = ipc_sdk.and_then(read_config);
                #[cfg(not(feature = "gg-ipc"))]
                let updated: Option<String> = None;
                if let Some(json) = updated {
                    apply_config_update(&shared_config, &json);
                }
            }
        }
    }

    // Persist the checkpoint unconditionally on the way out so progress is not lost.
    info!("Shutting down — persisting final checkpoint");
    // Read the current (possibly hot-reloaded) config from the cell, not the startup value, so
    // the final flush honors the latest deprecatedVersionSupport. Poison-tolerant, matching the loop.
    let deprecated_support = shared_config
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .deprecated_version_support;
    if let Err(e) = save_checkpoint(&checkpoint_path, &store, deprecated_support) {
        error!("Failed to persist checkpoint on shutdown: {e}");
    }
    info!("Shutdown complete");
}

/// Resolve the raw config from ordered sources (first non-empty wins):
/// (1) CLI `--config <inline-json-or-path>`, (2) env `GG_LOG_MANAGER_CONFIG`,
/// (3) IPC (the component's own configuration read from the Greengrass runtime at startup),
/// (4) `{}` default. Parsing stays agnostic to where the raw config originates.
fn resolve_raw_config(args: &[String], ipc_value: Option<String>) -> String {
    for source in [
        config_from_cli(args),
        config_from_env(),
        config_from_ipc(ipc_value),
    ] {
        match source {
            Ok(Some(raw)) => return raw,
            Ok(None) => {}
            Err(e) => error!("Config source error: {e}; trying next source"),
        }
    }
    // (4) DEFAULT: empty object — start with no sources configured.
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

/// (3) IPC source: the startup configuration read from the Greengrass runtime, if any. The
/// value is read once in `main` (single `Sdk::init()`); `None` off-device or when the `gg-ipc`
/// feature is disabled, so on-device deployments that carry no `--config`/env config still
/// resolve one.
fn config_from_ipc(ipc_value: Option<String>) -> Result<Option<String>, ConfigError> {
    Ok(ipc_value.filter(|s| !s.is_empty()))
}

/// Parse and validate a config JSON delivered by a live configuration update and, on success,
/// swap it into the shared cell so the loop picks it up next cycle. Returns `true` when the cell
/// was updated; on a parse or validation failure it logs and keeps the previous config
/// (`false`). This is the testable seam for the live-reload path — the loop supplies the JSON it
/// read on its own thread.
fn apply_config_update(shared: &Arc<RwLock<Arc<LogManagerConfig>>>, json: &str) -> bool {
    match load_config(json) {
        Ok(new_config) => match validate_config(&new_config) {
            Ok(()) => {
                // TODO: when a component is dropped from the config on reload, its checkpoint
                // entries (file_processing_info / last_processed_timestamps for that log group)
                // are left in place. This is pre-existing (such entries already persist across a
                // restart via checkpoint.json) and metadata-only: a removed component is no longer
                // scanned, so nothing re-uploads and no log-file disk space grows. The stale entry
                // is bounded by the process lifetime. Pruning entries for log groups absent from
                // the new config is deferred.
                *shared.write().unwrap_or_else(|e| e.into_inner()) = Arc::new(new_config);
                info!("Configuration reloaded from update");
                true
            }
            Err(e) => {
                warn!(error = %e, "Invalid configuration update ignored; keeping previous config");
                false
            }
        },
        Err(e) => {
            warn!(error = %e, "Failed to parse configuration update; keeping previous config");
            false
        }
    }
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
        assert_eq!(resolve_raw_config(&args, None), "CLI");
        std::env::remove_var("GG_LOG_MANAGER_CONFIG");
    }

    #[test]
    #[serial]
    fn test_resolve_raw_config_env_used_without_cli() {
        std::env::set_var("GG_LOG_MANAGER_CONFIG", "ENV");
        let args = vec!["bin".to_string()];
        assert_eq!(resolve_raw_config(&args, None), "ENV");
        std::env::remove_var("GG_LOG_MANAGER_CONFIG");
    }

    #[test]
    #[serial]
    fn test_resolve_raw_config_default_when_no_source() {
        std::env::remove_var("GG_LOG_MANAGER_CONFIG");
        let args = vec!["bin".to_string()];
        assert_eq!(resolve_raw_config(&args, None), "{}");
    }

    // Default build (no `gg-ipc`): the startup IPC value is `None`, so the IPC source never
    // contributes and the chain behaves as CLI → env → default.
    #[cfg(not(feature = "gg-ipc"))]
    #[test]
    fn test_config_from_ipc_none_without_feature() {
        assert_eq!(config_from_ipc(None).unwrap(), None);
    }

    // A non-empty env value wins over the (last) IPC source even when an IPC value is present.
    #[test]
    #[serial]
    fn test_resolve_raw_config_env_wins_over_ipc_fallback() {
        std::env::set_var("GG_LOG_MANAGER_CONFIG", "ENV");
        let args = vec!["bin".to_string()];
        assert_eq!(resolve_raw_config(&args, Some("IPC".to_string())), "ENV");
        std::env::remove_var("GG_LOG_MANAGER_CONFIG");
    }

    // The empty-string env filter routes past env to the IPC source: CLI absent, env empty, and
    // no IPC value → resolution falls through to the "{}" default.
    #[test]
    #[serial]
    fn test_resolve_raw_config_empty_env_falls_through_to_ipc_fallback() {
        std::env::set_var("GG_LOG_MANAGER_CONFIG", "");
        let args = vec!["bin".to_string()];
        assert_eq!(resolve_raw_config(&args, None), "{}");
        std::env::remove_var("GG_LOG_MANAGER_CONFIG");
    }

    // With CLI and env absent, the startup IPC value is used as the last source before default.
    #[test]
    #[serial]
    fn test_resolve_raw_config_uses_ipc_value_when_cli_env_absent() {
        std::env::remove_var("GG_LOG_MANAGER_CONFIG");
        let args = vec!["bin".to_string()];
        assert_eq!(
            resolve_raw_config(
                &args,
                Some(r#"{"periodicUploadIntervalSec":9}"#.to_string())
            ),
            r#"{"periodicUploadIntervalSec":9}"#
        );
    }

    // apply_config_update swaps the cell on a valid update and keeps the previous config on a bad
    // one. The live subscribe path (the callback firing, the SDK rejecting a get_config on the
    // callback thread, and the end-to-end deploy→swap) is device-only and not unit-testable here.
    #[test]
    fn test_apply_config_update_good_json_swaps_cell() {
        let shared = Arc::new(RwLock::new(Arc::new(load_config("{}").unwrap())));
        let before = shared.read().unwrap().periodic_upload_interval_sec;
        assert!(apply_config_update(
            &shared,
            r#"{"periodicUploadIntervalSec":42}"#
        ));
        let after = shared.read().unwrap().periodic_upload_interval_sec;
        assert_eq!(after, 42);
        assert_ne!(before, after);
    }

    #[test]
    fn test_apply_config_update_bad_json_keeps_previous() {
        let shared = Arc::new(RwLock::new(Arc::new(
            load_config(r#"{"periodicUploadIntervalSec":7}"#).unwrap(),
        )));
        assert!(!apply_config_update(&shared, "not valid json {{{"));
        assert_eq!(shared.read().unwrap().periodic_upload_interval_sec, 7);
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
