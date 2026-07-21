// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! gg-log-manager - Greengrass LogManager Generic Type Component
//!
//! Rust implementation of aws.greengrass.LogManager for GG Classic and GG Lite.
//! Tails log files and EMF JSON files, uploads to CloudWatch Logs.

use gg_log_manager::config::{
    derive_log_group_name, effective_disk_limit_bytes, load_config, validate_config, ConfigError,
    LogLevel, LogManagerConfig, LogSourceConfig, LogsUploaderConfig,
};
use gg_log_manager::credentials::resolve_thing_name;
#[cfg(feature = "gg-ipc")]
use gg_log_manager::ipc_config::{connect_ipc, read_config};
use gg_log_manager::scanner::{
    assemble_multiline, load_checkpoint, read_file_from_offset, recover_offsets, save_checkpoint,
    scan_directory, trim_stale_on_load, CheckpointStore, LogEvent, ScanDirectoryResult,
    ScannedFile,
};
use gg_log_manager::uploader::{
    advance_checkpoints, complete_file, effective_interval_secs, evict_stale_entries,
    format_log_stream_name, mtime_to_ms, upload_source_events, CwLogsClient, CwUploadError,
    TTL_24H_MS,
};
use regex::Regex;
use std::collections::HashSet;
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
        if process_source(
            source,
            &config.logs_uploader_configuration,
            &log_group,
            &pattern,
            client,
            store,
            thing_name,
            now,
        )
        .await
            == CycleOutcome::StopForAuth
        {
            // Auth failure — stop the cycle so credentials can refresh before the next pass.
            return CycleOutcome::StopForAuth;
        }
    }
    CycleOutcome::Continue
}

/// Process a single log source: scan → read → batch → upload → checkpoint, then enforce the
/// disk limit for the source every cycle.
// Uses blocking std::fs; safe only on the current_thread runtime (switch to tokio::fs if that changes).
#[cfg(not(tarpaulin_include))]
#[allow(clippy::too_many_arguments)]
async fn process_source(
    source: &LogSourceConfig,
    uploader_config: &LogsUploaderConfig,
    log_group: &str,
    pattern: &Regex,
    client: &mut CwLogsClient,
    store: &mut CheckpointStore,
    thing_name: &str,
    now: u64,
) -> CycleOutcome {
    // A scan error yields no classification data for this cycle: we cannot tell which on-disk
    // files were dedup-dropped (and thus un-uploaded), so enforcing the disk limit could delete
    // never-uploaded data. Skip enforcement entirely this cycle; it resumes on the next good
    // scan. A successful scan always yields a `ScanOutcome` (possibly with an empty `filtered`
    // set on an idle/all-completed cycle), so enforcement still runs against the directory it
    // enumerates itself.
    let scan_outcome = match scan_and_filter_files(source, log_group, pattern, store) {
        Some(o) => o,
        None => {
            warn!(
                log_group,
                "Scan failed — skipping disk enforcement this cycle (no classification data)"
            );
            return CycleOutcome::Continue;
        }
    };
    let scanned_files = scan_outcome.filtered;
    let dedup_dropped = scan_outcome.dedup_dropped;

    let mut outcome = CycleOutcome::Continue;
    if !scanned_files.is_empty() {
        let file_events = read_file_events(source, log_group, &scanned_files, store);
        if !file_events.is_empty() {
            // Capture the cycle outcome (e.g. StopForAuth) — it is returned after enforcement,
            // so a systemic auth failure still halts the rest of the cycle.
            outcome = upload_and_advance_checkpoints(
                source,
                log_group,
                client,
                store,
                thing_name,
                &scanned_files,
                file_events,
                now,
            )
            .await;
        }
    }

    // Enforce the disk limit last, every cycle — see `enforce_source_disk_limit` for the
    // classification/ordering rationale (it runs on the StopForAuth path and idle cycles too).
    let _ = enforce_source_disk_limit(
        source,
        uploader_config,
        log_group,
        pattern,
        store,
        &scanned_files,
        &dedup_dropped,
    );

    outcome
}

/// Result of scanning + filtering a source's directory for one cycle. Returned on every
/// successful scan (a scan error yields `None`), so the `dedup_dropped` list survives even when
/// `filtered` is empty — the all-completed/idle cycle (path-3) is exactly when disk enforcement
/// still runs and must know which files were skipped as hash-duplicates.
struct ScanOutcome {
    /// Files to read/upload this cycle (new or mid-upload; already-completed files removed).
    filtered: Vec<ScannedFile>,
    /// Paths dropped by content-hash dedup (older files sharing a newer file's first-line hash).
    /// These are never read or uploaded, so disk enforcement treats them as un-uploaded.
    dedup_dropped: Vec<PathBuf>,
}

/// Scan the log directory for matching files, dropping already-completed files (older than the
/// last uploaded file and not mid-upload). Returns `None` only on a scan error (no classification
/// data — the caller skips enforcement); every successful scan returns a `ScanOutcome`, even when
/// the directory is empty or every file is already completed, so the dedup-dropped list is never
/// lost on the very cycles where enforcement runs.
fn scan_and_filter_files(
    source: &LogSourceConfig,
    log_group: &str,
    pattern: &Regex,
    store: &CheckpointStore,
) -> Option<ScanOutcome> {
    let ScanDirectoryResult {
        files: scanned_files,
        dedup_dropped,
    } = match scan_directory(&source.log_file_directory_path, pattern) {
        Ok(res) => res,
        Err(e) => {
            error!(directory = source.log_file_directory_path, error = %e, "Failed to scan directory");
            return None;
        }
    };

    if scanned_files.is_empty() {
        // Empty directory (or all files unhashable): a valid, idle cycle. `dedup_dropped` is
        // empty here but returned uniformly so enforcement still runs.
        return Some(ScanOutcome {
            filtered: Vec::new(),
            dedup_dropped,
        });
    }

    let last_ts = store
        .last_processed_timestamps
        .get(log_group)
        .map(|t| t.last_file_processed_time_stamp)
        .unwrap_or(0);
    let filtered: Vec<_> = scanned_files
        .into_iter()
        .filter(|f| {
            mtime_to_ms(f.mtime) > last_ts
                || store
                    .file_processing_info
                    .get(log_group)
                    .is_some_and(|m| m.contains_key(&f.content_hash))
        })
        .collect();

    // Path-3 (all files filtered out as completed): `filtered` is empty but `dedup_dropped` may
    // be populated — return it so enforcement can protect the dropped files this cycle.
    Some(ScanOutcome {
        filtered,
        dedup_dropped,
    })
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

/// Upload batched events, advance checkpoints for fully-uploaded files, and delete completed
/// files when configured. Disk-space enforcement is handled unconditionally by the caller
/// (`process_source`) every cycle, so it is not done here.
#[cfg(not(tarpaulin_include))]
#[allow(clippy::too_many_arguments)]
async fn upload_and_advance_checkpoints(
    source: &LogSourceConfig,
    log_group: &str,
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

/// Enforce the source's disk limit and drop checkpoint entries for any deleted files. Single
/// home for the disk-enforcement rationale; call sites point here instead of repeating it.
///
/// Runs unconditionally at the end of every `process_source` cycle — idle cycles, upload
/// outages, and the `StopForAuth` path included (local filesystem op, no credentials) — and
/// must run LAST so it observes the freshly advanced `last_processed_timestamps`. The limit
/// and flag are resolved per cycle (live config changes apply next cycle); `None` means the
/// source is unbounded by explicit customer choice and enforcement is skipped. Dedup-dropped
/// files are classified un-uploaded (protected by default), never silently reclaimed by mtime.
fn enforce_source_disk_limit(
    source: &LogSourceConfig,
    uploader_config: &LogsUploaderConfig,
    log_group: &str,
    pattern: &Regex,
    store: &mut CheckpointStore,
    scanned_files: &[ScannedFile],
    dedup_dropped: &[PathBuf],
) -> gg_log_manager::disk::EnforceOutcome {
    let Some(limit_bytes) = effective_disk_limit_bytes(source, uploader_config) else {
        // Unbounded — neither the source nor the component-level default set a limit. This is an
        // explicit customer choice, so do no enforcement (and emit no WARN).
        return gg_log_manager::disk::EnforceOutcome::default();
    };
    let last_ts = store
        .last_processed_timestamps
        .get(log_group)
        .map(|t| t.last_file_processed_time_stamp)
        .unwrap_or(0);

    // Paths still tracked (mid-upload) for this group, matched by content hashes already in the
    // checkpoint — no re-hashing.
    let tracked_paths: HashSet<PathBuf> = store
        .file_processing_info
        .get(log_group)
        .map(|m| {
            scanned_files
                .iter()
                .filter(|f| m.contains_key(&f.content_hash))
                .map(|f| f.path.clone())
                .collect()
        })
        .unwrap_or_default();

    // Dedup-skipped files were never read or uploaded — fold them into the `is_tracked` guard so
    // they classify as un-uploaded (protected by default, sheddable under the opt-in flag). Both
    // sides derive paths from `entry.path()` on the same directory, so membership checks line up.
    let dropped_paths: HashSet<PathBuf> = dedup_dropped.iter().cloned().collect();

    let outcome = gg_log_manager::disk::enforce_disk_limit(
        Path::new(&source.log_file_directory_path),
        pattern,
        limit_bytes,
        last_ts,
        |p| tracked_paths.contains(p) || dropped_paths.contains(p),
        source.delete_unuploaded_files_on_disk_pressure,
    );

    // Drop checkpoint entries for every deleted file (uploaded-safe and un-uploaded).
    if let Some(file_map) = store.file_processing_info.get_mut(log_group) {
        for del_path in outcome
            .uploaded_deleted
            .iter()
            .chain(&outcome.unuploaded_deleted)
        {
            if let Some(sf) = scanned_files.iter().find(|f| f.path == *del_path) {
                file_map.remove(&sf.content_hash);
            }
        }
    }

    outcome
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
            delete_unuploaded_files_on_disk_pressure: false,
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
    fn test_scan_and_filter_empty_dir_returns_empty_outcome() {
        let dir = tempfile::tempdir().unwrap();
        let store = CheckpointStore::default();
        let pattern = Regex::new(r".*\.log$").unwrap();
        let result =
            scan_and_filter_files(&mk_source(dir.path(), r".*\.log$"), "grp", &pattern, &store);
        // Empty dir is a valid (idle) scan: Some with nothing to upload and nothing dropped.
        let outcome = result.expect("empty dir is a successful scan, not an error");
        assert!(outcome.filtered.is_empty());
        assert!(outcome.dedup_dropped.is_empty());
    }

    #[test]
    fn test_scan_and_filter_all_old_returns_empty_filtered() {
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
        // All files filtered out as completed → Some with empty filtered (path-3), no collisions.
        let outcome = result.expect("all-completed is a successful scan, not an error");
        assert!(outcome.filtered.is_empty());
        assert!(outcome.dedup_dropped.is_empty());
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
        let files = result.expect("new file should be kept").filtered;
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
        let files = result
            .expect("checkpointed old file should be kept")
            .filtered;
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].content_hash, hash);
    }

    // ---- enforce_source_disk_limit (synchronous, client-free) ----

    // Idle cycle: scanned_files is empty (the scan returned None), yet a directory over its
    // configured limit is still reclaimed. Older files at/below last_ts are uploaded-safe and
    // deleted by the default pass; the newest (active) file is preserved. This is the headline
    // behavior — enforcement no longer depends on a successful upload having happened.
    #[test]
    fn test_enforce_source_disk_limit_reclaims_on_idle_cycle() {
        let dir = tempfile::tempdir().unwrap();
        let old1 = dir.path().join("old1.log");
        let old2 = dir.path().join("old2.log");
        let active = dir.path().join("active.log");
        std::fs::write(&old1, vec![b'a'; 1000]).unwrap();
        std::fs::write(&old2, vec![b'b'; 1000]).unwrap();
        std::fs::write(&active, vec![b'c'; 1000]).unwrap();
        set_file_mtime_ms(&old1, 1_000);
        set_file_mtime_ms(&old2, 2_000);
        set_file_mtime_ms(&active, 9_000);

        let mut source = mk_source(dir.path(), r".*\.log$");
        source.disk_space_limit = Some("1".to_string()); // 1 KB = 1024 bytes; total is 3000

        // last_ts is after both old files' mtimes → they are uploaded-safe (and untracked).
        let mut store = CheckpointStore::default();
        store.last_processed_timestamps.insert(
            "grp".to_string(),
            LastFileProcessedTimestamp {
                last_file_processed_time_stamp: 5_000,
            },
        );

        let pattern = Regex::new(r".*\.log$").unwrap();
        let _ = enforce_source_disk_limit(
            &source,
            &LogsUploaderConfig::default(),
            "grp",
            &pattern,
            &mut store,
            &[],
            &[],
        );

        assert!(!old1.exists(), "oldest uploaded-safe file reclaimed");
        assert!(!old2.exists(), "second uploaded-safe file reclaimed");
        assert!(active.exists(), "active (newest) file must be preserved");
    }

    // A source with no own diskSpaceLimit is bounded by the component-level defaultDiskSpaceLimit
    // when one is set: older uploaded-safe files are reclaimed at the customer's default bound.
    #[test]
    fn test_enforce_source_disk_limit_component_default_applies_when_source_unset() {
        let dir = tempfile::tempdir().unwrap();
        let old1 = dir.path().join("old1.log");
        let old2 = dir.path().join("old2.log");
        let active = dir.path().join("active.log");
        std::fs::write(&old1, vec![b'a'; 1000]).unwrap();
        std::fs::write(&old2, vec![b'b'; 1000]).unwrap();
        std::fs::write(&active, vec![b'c'; 1000]).unwrap();
        set_file_mtime_ms(&old1, 1_000);
        set_file_mtime_ms(&old2, 2_000);
        set_file_mtime_ms(&active, 9_000);

        // Source sets no limit; the component-level default (1 KB) bounds it. Total is 3000.
        let source = mk_source(dir.path(), r".*\.log$");
        let uploader = LogsUploaderConfig {
            default_disk_space_limit: Some("1".to_string()),
            default_disk_space_limit_unit: DiskSpaceLimitUnit::KB,
            ..Default::default()
        };

        // last_ts after the old files' mtimes → they are uploaded-safe.
        let mut store = CheckpointStore::default();
        store.last_processed_timestamps.insert(
            "grp".to_string(),
            LastFileProcessedTimestamp {
                last_file_processed_time_stamp: 5_000,
            },
        );

        let pattern = Regex::new(r".*\.log$").unwrap();
        let _ =
            enforce_source_disk_limit(&source, &uploader, "grp", &pattern, &mut store, &[], &[]);

        assert!(
            !old1.exists() && !old2.exists(),
            "component default bounds the unconfigured source"
        );
        assert!(active.exists(), "active (newest) file must be preserved");
    }

    // When neither the source nor the component sets a limit, the source is unbounded: no
    // enforcement runs and nothing is deleted, even far over any hypothetical limit.
    #[test]
    fn test_enforce_source_disk_limit_unbounded_when_neither_set() {
        let dir = tempfile::tempdir().unwrap();
        let a = dir.path().join("a.log");
        let b = dir.path().join("b.log");
        std::fs::write(&a, vec![b'a'; 10_000]).unwrap();
        std::fs::write(&b, vec![b'b'; 10_000]).unwrap();
        set_file_mtime_ms(&a, 1_000);
        set_file_mtime_ms(&b, 2_000);

        let source = mk_source(dir.path(), r".*\.log$"); // no diskSpaceLimit
        let uploader = LogsUploaderConfig::default(); // no defaultDiskSpaceLimit

        let mut store = CheckpointStore::default();
        store.last_processed_timestamps.insert(
            "grp".to_string(),
            LastFileProcessedTimestamp {
                last_file_processed_time_stamp: 5_000,
            },
        );

        let pattern = Regex::new(r".*\.log$").unwrap();
        let outcome =
            enforce_source_disk_limit(&source, &uploader, "grp", &pattern, &mut store, &[], &[]);

        assert_eq!(
            outcome,
            gg_log_manager::disk::EnforceOutcome::default(),
            "unbounded source: enforcement is skipped"
        );
        assert!(
            a.exists() && b.exists(),
            "nothing deleted when neither limit is set"
        );
    }

    // A source's own diskSpaceLimit takes precedence over the component default: with a generous
    // source limit (not exceeded) nothing is deleted, even though the tiny component default would
    // have triggered a reclaim.
    #[test]
    fn test_enforce_source_disk_limit_source_limit_overrides_component_default() {
        let dir = tempfile::tempdir().unwrap();
        let a = dir.path().join("a.log");
        let b = dir.path().join("b.log");
        let active = dir.path().join("active.log");
        std::fs::write(&a, vec![b'a'; 1000]).unwrap();
        std::fs::write(&b, vec![b'b'; 1000]).unwrap();
        std::fs::write(&active, vec![b'c'; 1000]).unwrap();
        set_file_mtime_ms(&a, 1_000);
        set_file_mtime_ms(&b, 2_000);
        set_file_mtime_ms(&active, 9_000);

        // Source limit is 1 MB (> 3000 total → not exceeded); the component default is 1 KB
        // (would delete were it applied). Source limit wins → no deletion.
        let mut source = mk_source(dir.path(), r".*\.log$");
        source.disk_space_limit = Some("1".to_string());
        source.disk_space_limit_unit = DiskSpaceLimitUnit::MB;
        let uploader = LogsUploaderConfig {
            default_disk_space_limit: Some("1".to_string()),
            default_disk_space_limit_unit: DiskSpaceLimitUnit::KB,
            ..Default::default()
        };

        let mut store = CheckpointStore::default();
        store.last_processed_timestamps.insert(
            "grp".to_string(),
            LastFileProcessedTimestamp {
                last_file_processed_time_stamp: 5_000,
            },
        );

        let pattern = Regex::new(r".*\.log$").unwrap();
        let outcome =
            enforce_source_disk_limit(&source, &uploader, "grp", &pattern, &mut store, &[], &[]);

        assert_eq!(
            outcome,
            gg_log_manager::disk::EnforceOutcome::default(),
            "source limit not exceeded → nothing deleted (source limit overrides the default)"
        );
        assert!(a.exists() && b.exists() && active.exists());
    }

    // Scoping guard: a dedup-dropped file whose mtime is at/below last_ts would classify as
    // uploaded-safe by mtime alone, but because it is in the dropped set it is routed to the
    // un-uploaded bucket — preserved by default, and shed only under the opt-in flag. This pins
    // that the dropped-set term (not just mtime) drives the classification.
    #[test]
    fn test_enforce_source_disk_limit_dedup_dropped_protected_by_default() {
        let dir = tempfile::tempdir().unwrap();
        let dropped = dir.path().join("dropped.log");
        let active = dir.path().join("active.log");
        std::fs::write(&dropped, vec![b'a'; 1000]).unwrap();
        std::fs::write(&active, vec![b'b'; 1000]).unwrap();
        set_file_mtime_ms(&dropped, 1_000); // older
        set_file_mtime_ms(&active, 9_000); // newest → active

        let mut source = mk_source(dir.path(), r".*\.log$");
        source.disk_space_limit = Some("1".to_string()); // 1 KB; total is 2000 → over limit

        let mut store = CheckpointStore::default();
        // last_ts is after the dropped file's mtime → it WOULD be uploaded-safe absent the fix.
        store.last_processed_timestamps.insert(
            "grp".to_string(),
            LastFileProcessedTimestamp {
                last_file_processed_time_stamp: 5_000,
            },
        );

        let pattern = Regex::new(r".*\.log$").unwrap();
        // scanned_files empty (the dropped file was deduped away, so it never reaches the scan
        // set); the dropped path is supplied via the dedup_dropped list.
        let _ = enforce_source_disk_limit(
            &source,
            &LogsUploaderConfig::default(),
            "grp",
            &pattern,
            &mut store,
            &[],
            std::slice::from_ref(&dropped),
        );

        assert!(
            dropped.exists(),
            "dedup-dropped file must be preserved by default despite mtime <= last_ts"
        );
        assert!(active.exists(), "active (newest) file is always preserved");

        // With the opt-in flag it is shed as un-uploaded (phase 2), not uploaded-safe.
        source.delete_unuploaded_files_on_disk_pressure = true;
        let _ = enforce_source_disk_limit(
            &source,
            &LogsUploaderConfig::default(),
            "grp",
            &pattern,
            &mut store,
            &[],
            std::slice::from_ref(&dropped),
        );
        assert!(
            !dropped.exists(),
            "dedup-dropped file is sheddable under the opt-in flag"
        );
        assert!(active.exists(), "active (newest) file is still preserved");
    }

    // Create a colliding log file: identical first line (so all share one content hash) plus a
    // distinct-length filler after the newline (distinct sizes, still hash-equal).
    fn write_collision_file(
        dir: &Path,
        name: &str,
        filler: u8,
        filler_len: usize,
        mtime_ms: u64,
    ) -> PathBuf {
        let path = dir.join(name);
        let mut content = b"shared first line\n".to_vec();
        content.extend(std::iter::repeat_n(filler, filler_len));
        std::fs::write(&path, &content).unwrap();
        set_file_mtime_ms(&path, mtime_ms);
        path
    }

    // T-A: five files sharing a first line (→ one content hash), so scan dedup keeps the newest
    // (active) and drops the other four. Driven end-to-end through scan_and_filter_files (which
    // produces the real dedup-dropped list) → enforce_source_disk_limit. In DEFAULT mode the four
    // dropped files are classified un-uploaded and preserved across every cycle even though their
    // mtimes are <= last_ts (they would be uploaded-safe by mtime alone); the directory stays
    // over its limit (the documented, WARNed boundedness exception).
    #[test]
    fn collision_workload_default_mode_preserves_unuploaded() {
        let dir = tempfile::tempdir().unwrap();
        let d1 = write_collision_file(dir.path(), "d1.log", b'a', 982, 1_000); // 1000 bytes
        let d2 = write_collision_file(dir.path(), "d2.log", b'b', 1082, 2_000); // 1100
        let d3 = write_collision_file(dir.path(), "d3.log", b'c', 1182, 3_000); // 1200
        let d4 = write_collision_file(dir.path(), "d4.log", b'd', 1282, 4_000); // 1300
        let active = write_collision_file(dir.path(), "active.log", b'e', 1382, 9_000); // 1400
        let dropped_paths = [&d1, &d2, &d3, &d4];

        let mut source = mk_source(dir.path(), r".*\.log$");
        source.disk_space_limit = Some("3".to_string()); // 3 KiB = 3072; total is 6000 → over limit

        let mut store = CheckpointStore::default();
        // last_ts after the four dropped files' mtimes → without the dedup-dropped protection they
        // would classify uploaded-safe and be reclaimed.
        store.last_processed_timestamps.insert(
            "grp".to_string(),
            LastFileProcessedTimestamp {
                last_file_processed_time_stamp: 5_000,
            },
        );

        let pattern = Regex::new(r".*\.log$").unwrap();
        for cycle in 0..3 {
            let outcome = scan_and_filter_files(&source, "grp", &pattern, &store)
                .expect("successful scan yields a ScanOutcome");
            // Real plumbing: dedup dropped the four older colliding files.
            assert_eq!(
                outcome.dedup_dropped.len(),
                4,
                "cycle {cycle}: four older colliding files are dedup-dropped"
            );
            let enforce_outcome = enforce_source_disk_limit(
                &source,
                &LogsUploaderConfig::default(),
                "grp",
                &pattern,
                &mut store,
                &outcome.filtered,
                &outcome.dedup_dropped,
            );
            assert!(
                enforce_outcome.uploaded_deleted.is_empty()
                    && enforce_outcome.unuploaded_deleted.is_empty(),
                "cycle {cycle}: default mode deletes nothing (dropped files are un-uploaded)"
            );
            for p in dropped_paths {
                assert!(
                    p.exists(),
                    "cycle {cycle}: dropped file {p:?} must be preserved"
                );
            }
            assert!(active.exists(), "cycle {cycle}: active file preserved");
        }
    }

    // T-B: same fixture with deleteUnuploadedFilesOnDiskPressure = true. The dedup-dropped files
    // are shed as UN-UPLOADED (phase 2 — reported in unuploaded_deleted, never uploaded_deleted),
    // oldest-first only until under the limit, and the active (newest) file survives.
    #[test]
    fn collision_workload_flag_on_sheds_oldest_and_bounds_disk() {
        let dir = tempfile::tempdir().unwrap();
        let d1 = write_collision_file(dir.path(), "d1.log", b'a', 982, 1_000); // 1000
        let d2 = write_collision_file(dir.path(), "d2.log", b'b', 1082, 2_000); // 1100
        let d3 = write_collision_file(dir.path(), "d3.log", b'c', 1182, 3_000); // 1200
        let d4 = write_collision_file(dir.path(), "d4.log", b'd', 1282, 4_000); // 1300
        let active = write_collision_file(dir.path(), "active.log", b'e', 1382, 9_000); // 1400

        let mut source = mk_source(dir.path(), r".*\.log$");
        source.disk_space_limit = Some("3".to_string()); // 3072; total 6000
        source.delete_unuploaded_files_on_disk_pressure = true;

        let mut store = CheckpointStore::default();
        store.last_processed_timestamps.insert(
            "grp".to_string(),
            LastFileProcessedTimestamp {
                last_file_processed_time_stamp: 5_000,
            },
        );

        let pattern = Regex::new(r".*\.log$").unwrap();
        let outcome = scan_and_filter_files(&source, "grp", &pattern, &store)
            .expect("successful scan yields a ScanOutcome");
        assert_eq!(outcome.dedup_dropped.len(), 4);
        let enforce_outcome = enforce_source_disk_limit(
            &source,
            &LogsUploaderConfig::default(),
            "grp",
            &pattern,
            &mut store,
            &outcome.filtered,
            &outcome.dedup_dropped,
        );

        // Shed as un-uploaded, oldest-first, only until under 3072: 6000 - (1000+1100+1200) = 2700.
        assert_eq!(
            enforce_outcome.unuploaded_deleted,
            vec![d1.clone(), d2.clone(), d3.clone()],
            "oldest un-uploaded dropped files shed first"
        );
        assert!(
            enforce_outcome.uploaded_deleted.is_empty(),
            "dropped files are shed via the un-uploaded path, never uploaded-safe"
        );
        assert!(!d1.exists() && !d2.exists() && !d3.exists());
        assert!(d4.exists(), "only enough shed to get under the limit");
        assert!(active.exists(), "active (newest) file is never deleted");
    }

    // Path-3 (§8.7.1.4): a collision where the SURVIVING (newest) file is itself already
    // completed — its mtime <= last_ts and it has no in-flight checkpoint entry — so
    // scan_and_filter_files returns `filtered` EMPTY while `dedup_dropped` is NON-EMPTY. This is
    // the exact cycle a future re-coupling of enforcement to a non-empty `filtered` set would
    // break: enforcement must still run and must still protect the dropped file. Driven
    // end-to-end (scan_and_filter_files → enforce_source_disk_limit), flag=false.
    #[test]
    fn path3_empty_filtered_with_dropped_preserves_dropped_by_default() {
        let dir = tempfile::tempdir().unwrap();
        // Two colliding files (shared first line → one content hash); older is dedup-dropped,
        // newer survives as active. Sizes total over the limit so enforcement attempts a reclaim.
        let older = write_collision_file(dir.path(), "older.log", b'a', 982, 1_000); // 1000 bytes
        let newer = write_collision_file(dir.path(), "newer.log", b'b', 1082, 2_000); // 1100 bytes

        let mut source = mk_source(dir.path(), r".*\.log$");
        source.disk_space_limit = Some("1".to_string()); // 1 KiB = 1024; total 2100 → over limit

        let mut store = CheckpointStore::default();
        // last_ts is after BOTH files' mtimes (and there is no file_processing_info entry), so the
        // surviving newer file is filtered out as completed → `filtered` is empty (path-3).
        store.last_processed_timestamps.insert(
            "grp".to_string(),
            LastFileProcessedTimestamp {
                last_file_processed_time_stamp: 5_000,
            },
        );

        let pattern = Regex::new(r".*\.log$").unwrap();
        let outcome = scan_and_filter_files(&source, "grp", &pattern, &store)
            .expect("a successful scan yields Some(ScanOutcome), even when everything is filtered");
        assert!(
            outcome.filtered.is_empty(),
            "surviving file is already completed → filtered is empty (path-3)"
        );
        assert_eq!(
            outcome.dedup_dropped,
            vec![older.clone()],
            "the older colliding file is dedup-dropped (non-empty) on this same cycle"
        );

        // Enforcement still runs on path-3 and, in default mode, must protect the dropped file
        // even though its mtime <= last_ts would otherwise class it uploaded-safe.
        let enforce_outcome = enforce_source_disk_limit(
            &source,
            &LogsUploaderConfig::default(),
            "grp",
            &pattern,
            &mut store,
            &outcome.filtered,
            &outcome.dedup_dropped,
        );
        assert!(
            enforce_outcome.uploaded_deleted.is_empty()
                && enforce_outcome.unuploaded_deleted.is_empty(),
            "default mode deletes nothing: the dropped file is un-uploaded, not uploaded-safe"
        );
        assert!(
            older.exists(),
            "dedup-dropped file preserved by default on the path-3 cycle"
        );
        assert!(newer.exists(), "active (newest) file is always preserved");
    }

    // Scan-error cycle: an unreadable directory makes scan_directory return Err, so
    // scan_and_filter_files returns None — the signal on which process_source SKIPS enforcement
    // entirely (no classification data ⇒ no deletions). Verifies the skip trigger fires.
    #[cfg(unix)]
    #[test]
    fn test_scan_and_filter_scan_error_returns_none_to_skip_enforcement() {
        use std::os::unix::fs::PermissionsExt;

        // root ignores permission bits — skip.
        if std::process::Command::new("id")
            .arg("-u")
            .output()
            .map(|o| o.stdout.starts_with(b"0"))
            .unwrap_or(false)
        {
            return;
        }

        let dir = tempfile::tempdir().unwrap();
        // A file exists, so a *successful* scan would find something to act on.
        std::fs::write(dir.path().join("app.log"), vec![b'a'; 5000]).unwrap();
        // Make the directory unreadable so read_dir fails with a non-NotFound error.
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o000)).unwrap();

        let source = mk_source(dir.path(), r".*\.log$");
        let store = CheckpointStore::default();
        let pattern = Regex::new(r".*\.log$").unwrap();
        let result = scan_and_filter_files(&source, "grp", &pattern, &store);

        // Restore permissions so the TempDir can be cleaned up.
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();

        assert!(
            result.is_none(),
            "a scan error yields None so process_source skips disk enforcement this cycle"
        );
    }
}
