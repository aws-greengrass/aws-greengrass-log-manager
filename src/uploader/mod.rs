// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! Upload pipeline - batching, CloudWatch client, retry, scheduling, checkpoint advancement

// The batcher exists solely to prepare events for CloudWatch upload, so it is only
// compiled with the `aws-sdk` feature (its sole consumer is `upload_source_events`).
#[cfg(feature = "aws-sdk")]
mod batcher;
#[cfg(feature = "aws-sdk")]
mod cw_client;

#[cfg(feature = "aws-sdk")]
use crate::config::LogLevel;
use crate::scanner::{
    CheckpointStore, FileCheckpoint, LastFileProcessedTimestamp, LogEvent, ScannedFile,
};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// A sealed batch of log events ready for upload to CloudWatch.
#[derive(Debug)]
pub struct SealedBatch {
    pub log_group: String,
    pub log_stream: String,
    pub events: Vec<LogEvent>,
}

#[cfg(feature = "aws-sdk")]
pub(crate) use cw_client::UploadOutcome;
#[cfg(feature = "aws-sdk")]
pub use cw_client::{CwLogsClient, CwUploadError};

#[cfg(feature = "aws-sdk")]
pub(crate) use batcher::seal_batches;

/// 24 hours in milliseconds — default TTL for checkpoint entries.
pub const TTL_24H_MS: u64 = 24 * 60 * 60 * 1000;

/// Compute the effective upload interval with jitter to prevent a thundering herd.
/// Uses the per-source override when set, otherwise the global interval, plus 0-5s jitter.
#[must_use]
pub fn effective_interval_secs(per_source: Option<u64>, global: u64) -> u64 {
    let base = per_source.unwrap_or(global);
    let jitter = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos() as u64
        % 6;
    base + jitter
}

/// Result of uploading events for a single log source.
// TODO: replace these positional tuples — `(path, new_offset, content_hash)` here and the
// `file_events` `(path, events, new_offset, content_hash)` input — with named structs
// (e.g. `FileUploadSuccess` / `FileUploadInput`) for readability and safer field evolution.
// Deferred: the input tuple currently spans the uploader/main.rs boundary, so the rename is
// cleaner as one atomic pass once the full scan→upload→checkpoint pipeline is in a single tree.
#[derive(Debug)]
#[must_use = "upload results must be checked to advance checkpoints"]
pub struct UploadResult {
    /// Files whose ALL batches succeeded: (path, new_offset, content_hash).
    pub succeeded: Vec<(PathBuf, u64, String)>,
    /// Files that had at least one batch failure.
    pub failed: Vec<PathBuf>,
}

/// Upload events from multiple files for a single log source.
///
/// `seal_batches` merges events across files (sorted by timestamp), so per-file success
/// cannot be tracked at batch granularity. We therefore use an all-or-nothing model for
/// the source: a file's checkpoint advances only if EVERY batch succeeds; if any batch
/// fails, all files for this source are marked failed and retried on the next cycle.
///
/// # Errors
/// Returns [`CwUploadError::Auth`] when credentials are invalid; the orchestrator
/// stops the upload cycle so the credentials can be refreshed.
// TODO: all-or-nothing per source — any batch failure re-sends the whole source on the
// next cycle. Finer per-file/per-stream success tracking (advance only the files whose
// batches succeeded) would cut duplicates on partial failure, but requires carrying a
// file identity through batching; deferred as a behavior change.
#[cfg(feature = "aws-sdk")]
pub async fn upload_source_events(
    client: &mut CwLogsClient,
    log_group: &str,
    log_stream: &str,
    file_events: Vec<(PathBuf, Vec<LogEvent>, u64, String)>, // (path, events, new_offset, hash)
    min_log_level: Option<LogLevel>,
) -> Result<UploadResult, CwUploadError> {
    // Collect file metadata before flattening events for batching.
    let file_info: Vec<(PathBuf, u64, String)> = file_events
        .iter()
        .map(|(p, _, off, hash)| (p.clone(), *off, hash.clone()))
        .collect();

    let all_events: Vec<LogEvent> = file_events
        .into_iter()
        .flat_map(|(_, events, _, _)| events)
        .collect();

    if all_events.is_empty() {
        return Ok(UploadResult {
            succeeded: file_info,
            failed: vec![],
        });
    }

    let now_ms = now_ms() as i64;
    let batches = seal_batches(all_events, log_group, log_stream, min_log_level, now_ms);

    let mut all_ok = true;
    for batch in &batches {
        match client.upload_batch_with_retry(batch).await {
            Ok(UploadOutcome::Success) => {}
            Ok(UploadOutcome::RetriesExhausted) => {
                all_ok = false;
                break;
            }
            // The retry wrapper only ever returns `Err(Auth)`; surface it so the
            // orchestrator can stop the cycle and refresh credentials.
            Err(e) => return Err(e),
        }
    }

    if all_ok {
        Ok(UploadResult {
            succeeded: file_info,
            failed: vec![],
        })
    } else {
        Ok(UploadResult {
            succeeded: vec![],
            failed: file_info.into_iter().map(|(p, _, _)| p).collect(),
        })
    }
}

#[cfg(feature = "aws-sdk")]
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Convert a file mtime to epoch milliseconds. Returns 0 for a pre-1970 time so a
/// no-RTC device never panics or wraps. Shared by the scanner/checkpoint sites.
#[must_use]
pub fn mtime_to_ms(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// A file is fully uploaded when all bytes have been read, it is not the active
/// (currently written-to) file, and it is non-empty.
#[must_use]
fn is_file_fully_uploaded(is_active: bool, file_len: u64, bytes_read: u64) -> bool {
    !is_active && file_len == bytes_read && file_len > 0
}

/// Advance checkpoints after a successful upload. Returns paths of completed files.
///
/// A completed file (`!is_active && file_length == new_offset && len > 0`) is removed
/// from the checkpoint and its component `lastFileProcessedTimeStamp` is advanced
/// monotonically. A partial or active file simply has its `start_position` updated.
pub fn advance_checkpoints(
    store: &mut CheckpointStore,
    log_group_key: &str,
    succeeded: &[(PathBuf, u64, String)], // (path, new_offset, content_hash)
    scanned_files: &[ScannedFile],
    now: u64,
) -> Vec<PathBuf> {
    use std::collections::HashMap;
    let mut completed = Vec::new();

    // Pre-build a path-keyed lookup so per-file resolution is O(1) instead of a linear scan
    // per succeeded file. Paths are unique within a single scan, so a plain map (no
    // first-wins fold) resolves each succeeded `path` to exactly its own `ScannedFile` —
    // resolution is unambiguous. The prior hash-keyed lookup could resolve to a *different*
    // file on a first-line content-hash collision (e.g. empty or identical-first-line
    // files), so `is_active`/`mtime` might describe a file other than the one whose bytes
    // were read, mis-completing an active file.
    let scanned_by_path: HashMap<&Path, &ScannedFile> = scanned_files
        .iter()
        .map(|f| (f.path.as_path(), f))
        .collect();

    let file_map = store
        .file_processing_info
        .entry(log_group_key.to_string())
        .or_default();

    for (path, new_offset, content_hash) in succeeded {
        let scanned = scanned_by_path.get(path.as_path()).copied();
        let is_active = scanned.is_some_and(|f| f.is_active);
        let file_len = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);

        // TODO: The checkpoint is keyed by `content_hash`, so two files that share a
        // `content_hash` still collide on the checkpoint key (one file's checkpoint
        // overwrites/removes the other's). Unique per-file checkpoint identity is deferred.
        if is_file_fully_uploaded(is_active, file_len, *new_offset) {
            file_map.remove(content_hash);
            completed.push(path.clone());

            // Advance the component's last-processed timestamp monotonically.
            let file_mtime_ms = scanned.map(|f| mtime_to_ms(f.mtime)).unwrap_or(0);
            let ts_entry = store
                .last_processed_timestamps
                .entry(log_group_key.to_string())
                .or_insert(LastFileProcessedTimestamp {
                    last_file_processed_time_stamp: 0,
                });
            if file_mtime_ms > ts_entry.last_file_processed_time_stamp {
                ts_entry.last_file_processed_time_stamp = file_mtime_ms;
            }

            tracing::info!(path = %path.display(), "File completed, removed from checkpoint");
        } else {
            file_map.insert(
                content_hash.clone(),
                FileCheckpoint {
                    file_hash: content_hash.clone(),
                    start_position: *new_offset,
                    last_modified_time: scanned.map(|f| mtime_to_ms(f.mtime)).unwrap_or(0),
                    last_accessed: now,
                },
            );
        }
    }

    completed
}

/// Evict checkpoint entries older than `ttl_ms` (24 hours by default).
/// Called after each upload cycle to prevent unbounded checkpoint growth.
pub fn evict_stale_entries(
    store: &mut CheckpointStore,
    log_group_key: &str,
    ttl_ms: u64,
    now: u64,
) {
    if let Some(file_map) = store.file_processing_info.get_mut(log_group_key) {
        let before = file_map.len();
        file_map.retain(|_, cp| now.saturating_sub(cp.last_accessed) < ttl_ms);
        let evicted = before - file_map.len();
        if evicted > 0 {
            tracing::info!(
                log_group_key,
                evicted,
                "Evicted stale checkpoint entries (TTL)"
            );
        }
    }
}

/// Format log stream name matching the component's scheme:
/// `/{yyyy}/{MM}/{dd}/thing/{thingName}` (UTC).
/// Replaces colons with `+` since CW log stream names cannot contain `:`.
#[must_use]
pub fn format_log_stream_name(thing_name: &str) -> String {
    let now = time::OffsetDateTime::now_utc();
    let safe_name = thing_name.replace(':', "+");
    format!(
        "/{}/{:02}/{:02}/thing/{}",
        now.year(),
        now.month() as u8,
        now.day(),
        safe_name
    )
}

/// Check if a timestamp (epoch millis) falls on a different UTC date.
/// Used to detect when a new log stream should be created at midnight.
#[must_use]
pub fn is_different_date(
    timestamp_ms: i64,
    stream_year: i32,
    stream_month: u32,
    stream_day: u32,
) -> bool {
    match time::OffsetDateTime::from_unix_timestamp(timestamp_ms / 1000) {
        Ok(dt) => {
            dt.year() != stream_year
                || dt.month() as u32 != stream_month
                || dt.day() as u32 != stream_day
        }
        Err(_) => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_sealed_batch_struct() {
        let batch = SealedBatch {
            log_group: "group".to_string(),
            log_stream: "stream".to_string(),
            events: vec![],
        };
        assert_eq!(batch.log_group, "group");
        assert_eq!(batch.log_stream, "stream");
        assert!(batch.events.is_empty());
    }

    #[test]
    fn test_format_log_stream_name() {
        let name = format_log_stream_name("eca-store66-device01");
        assert!(name.starts_with('/'));
        assert!(name.ends_with("/thing/eca-store66-device01"));
        let parts: Vec<&str> = name.split('/').collect();
        assert_eq!(parts.len(), 6); // ["", "YYYY", "MM", "DD", "thing", "name"]
        assert_eq!(parts[4], "thing");
    }

    #[test]
    fn test_format_log_stream_name_with_colons() {
        let name = format_log_stream_name("device:with:colons");
        assert!(name.ends_with("/thing/device+with+colons"));
        assert!(!name.contains(':'));
    }

    #[test]
    fn test_utc_date_from_time_crate() {
        let now = time::OffsetDateTime::now_utc();
        let (y, m, d) = (now.year(), now.month() as u32, now.day() as u32);
        assert!(y >= 2024);
        assert!((1..=12).contains(&m));
        assert!((1..=31).contains(&d));
    }

    #[test]
    fn test_is_different_date_same() {
        let now = time::OffsetDateTime::now_utc();
        let (y, m, d) = (now.year(), now.month() as u32, now.day() as u32);
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        assert!(!is_different_date(now_ms, y, m, d));
    }

    #[test]
    fn test_is_different_date_different() {
        assert!(is_different_date(1704067200000, 2023, 12, 31));
        assert!(!is_different_date(1704067200000, 2024, 1, 1));
    }

    #[test]
    fn test_is_different_date_negative_timestamp() {
        assert!(is_different_date(-1, 2024, 1, 1));
        assert!(is_different_date(-1000000, 2024, 1, 1));
    }

    #[test]
    fn test_is_different_date_midnight_boundary() {
        assert!(!is_different_date(1704067199999, 2023, 12, 31));
        assert!(!is_different_date(1704067200000, 2024, 1, 1));
        assert!(is_different_date(1704067199999, 2024, 1, 1));
        assert!(is_different_date(1704067200000, 2023, 12, 31));
    }

    #[test]
    fn test_effective_interval_with_override() {
        let interval = effective_interval_secs(Some(60), 300);
        assert!((60..=65).contains(&interval));
    }

    #[test]
    fn test_effective_interval_global_default() {
        let interval = effective_interval_secs(None, 300);
        assert!((300..=305).contains(&interval));
    }

    #[test]
    fn test_effective_interval_zero() {
        let interval = effective_interval_secs(None, 0);
        assert!((0..=5).contains(&interval));
    }

    #[test]
    fn test_is_file_fully_uploaded() {
        // Fully read, not active, non-empty → complete.
        assert!(is_file_fully_uploaded(false, 100, 100));
        // Active file is never complete even if fully read.
        assert!(!is_file_fully_uploaded(true, 100, 100));
        // Partially read → not complete.
        assert!(!is_file_fully_uploaded(false, 100, 50));
        // Empty file → not complete.
        assert!(!is_file_fully_uploaded(false, 0, 0));
    }

    #[test]
    fn test_advance_checkpoints_partial_file() {
        let mut store = CheckpointStore::default();
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("test.log");
        std::fs::write(&file_path, "hello world\nmore data\n").unwrap();

        let scanned = vec![ScannedFile {
            path: file_path.clone(),
            mtime: SystemTime::now(),
            content_hash: "hash1".to_string(),
            is_active: true,
        }];

        // new_offset=11 but the file is 22 bytes and active → partial update.
        let succeeded = vec![(file_path.clone(), 11u64, "hash1".to_string())];
        let completed = advance_checkpoints(&mut store, "grp", &succeeded, &scanned, 1_000_000);

        assert!(completed.is_empty());
        let cp = store
            .file_processing_info
            .get("grp")
            .unwrap()
            .get("hash1")
            .unwrap();
        assert_eq!(cp.start_position, 11);
    }

    #[test]
    fn test_advance_checkpoints_completed_file() {
        let mut store = CheckpointStore::default();
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("done.log");
        std::fs::write(&file_path, "all content").unwrap(); // 11 bytes

        let scanned = vec![ScannedFile {
            path: file_path.clone(),
            mtime: SystemTime::now(),
            content_hash: "hash2".to_string(),
            is_active: false, // NOT active
        }];

        // new_offset == file_len && !is_active → completed.
        let succeeded = vec![(file_path.clone(), 11u64, "hash2".to_string())];
        let completed = advance_checkpoints(&mut store, "grp", &succeeded, &scanned, 1_000_000);

        assert_eq!(completed, vec![file_path]);
        assert!(store.file_processing_info.get("grp").unwrap().is_empty());
    }

    #[test]
    fn test_advance_checkpoints_active_file_fully_read_not_completed() {
        let mut store = CheckpointStore::default();
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("active.log");
        std::fs::write(&file_path, "data").unwrap(); // 4 bytes

        let scanned = vec![ScannedFile {
            path: file_path.clone(),
            mtime: SystemTime::now(),
            content_hash: "hash3".to_string(),
            is_active: true, // ACTIVE — never completed
        }];

        let succeeded = vec![(file_path.clone(), 4u64, "hash3".to_string())];
        let completed = advance_checkpoints(&mut store, "grp", &succeeded, &scanned, 1_000_000);

        assert!(completed.is_empty()); // Active file is never "completed"
        let cp = store
            .file_processing_info
            .get("grp")
            .unwrap()
            .get("hash3")
            .unwrap();
        assert_eq!(cp.start_position, 4);
    }

    #[test]
    fn test_advance_checkpoints_completes_inactive_file_despite_hash_collision() {
        // Two scanned files share a `content_hash`, but only the inactive (rotated) file is
        // in `succeeded`. Path-keyed resolution finds that file by its own path, sees it is
        // inactive and fully read, completes it, and advances the component timestamp to its
        // mtime. The shared hash belongs to the other (unrelated) file and does not affect
        // the result.
        let mut store = CheckpointStore::default();
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("collision.log");
        std::fs::write(&file_path, "all content").unwrap(); // 11 bytes

        let older = UNIX_EPOCH + std::time::Duration::from_millis(1000);
        let newer = UNIX_EPOCH + std::time::Duration::from_millis(2000);
        // mtime-ascending: inactive (older) first, active (newer) second.
        let scanned = vec![
            ScannedFile {
                path: file_path.clone(),
                mtime: older,
                content_hash: "dup".to_string(),
                is_active: false,
            },
            ScannedFile {
                path: dir.path().join("other.log"),
                mtime: newer,
                content_hash: "dup".to_string(),
                is_active: true,
            },
        ];

        let succeeded = vec![(file_path.clone(), 11u64, "dup".to_string())];
        let completed = advance_checkpoints(&mut store, "grp", &succeeded, &scanned, 5000);

        // Path-keyed resolution finds the inactive file → completed and removed.
        assert_eq!(completed, vec![file_path]);
        assert!(store.file_processing_info.get("grp").unwrap().is_empty());
        // Timestamp advanced to the completed (inactive) file's mtime (1000ms).
        assert_eq!(
            store
                .last_processed_timestamps
                .get("grp")
                .unwrap()
                .last_file_processed_time_stamp,
            1000
        );
    }

    #[test]
    fn test_advance_checkpoints_keeps_active_file_despite_hash_collision() {
        // Two DISTINCT files share a content_hash "H" (synthetic hash collision), but the
        // succeeded batch includes BOTH. Per-file state must be resolved by PATH: the
        // active file B must not be mis-completed just because the inactive file A shares
        // its hash. Fails on the old hash-keyed lookup (B resolves to A → inactive →
        // completed); passes on the path-keyed lookup.
        let mut store = CheckpointStore::default();
        let dir = tempfile::tempdir().unwrap();

        let path_a = dir.path().join("a.log");
        let path_b = dir.path().join("b.log");
        std::fs::write(&path_a, "AAAA").unwrap(); // lenA = 4
        std::fs::write(&path_b, "BBBBBBB").unwrap(); // lenB = 7 (DIFFERENT)
        let len_a = std::fs::metadata(&path_a).unwrap().len();
        let len_b = std::fs::metadata(&path_b).unwrap().len();

        let older = UNIX_EPOCH + std::time::Duration::from_millis(1000);
        let newer = UNIX_EPOCH + std::time::Duration::from_millis(2000);
        let scanned = vec![
            ScannedFile {
                path: path_a.clone(),
                mtime: older,
                content_hash: "H".to_string(),
                is_active: false, // A rotated
            },
            ScannedFile {
                path: path_b.clone(),
                mtime: newer,
                content_hash: "H".to_string(),
                is_active: true, // B still being written
            },
        ];

        // Both fully read (offset == file length).
        let succeeded = vec![
            (path_a.clone(), len_a, "H".to_string()),
            (path_b.clone(), len_b, "H".to_string()),
        ];
        let completed = advance_checkpoints(&mut store, "grp", &succeeded, &scanned, 5000);

        // Regression assertion: A completes, active B must NOT be completed.
        assert!(completed.contains(&path_a));
        assert!(!completed.contains(&path_b));

        // A completes+removes "H", then active B re-inserts "H" at its own offset (len_b).
        assert_eq!(
            store.file_processing_info.get("grp").unwrap()["H"].start_position,
            len_b
        );
    }

    #[test]
    fn test_evict_stale_entries() {
        let mut store = CheckpointStore::default();
        // Fixed reference time so the test never depends on the wall clock.
        let now: u64 = 100 * 60 * 60 * 1000;
        let mut files = HashMap::new();
        // Entry accessed 25 hours ago — should be evicted.
        files.insert(
            "old".to_string(),
            FileCheckpoint {
                file_hash: "old".to_string(),
                start_position: 100,
                last_modified_time: 1000,
                last_accessed: now - 25 * 60 * 60 * 1000,
            },
        );
        // Entry accessed 1 hour ago — should be kept.
        files.insert(
            "fresh".to_string(),
            FileCheckpoint {
                file_hash: "fresh".to_string(),
                start_position: 200,
                last_modified_time: 2000,
                last_accessed: now - 60 * 60 * 1000,
            },
        );
        store.file_processing_info.insert("grp".to_string(), files);

        evict_stale_entries(&mut store, "grp", TTL_24H_MS, now);

        let remaining = store.file_processing_info.get("grp").unwrap();
        assert_eq!(remaining.len(), 1);
        assert!(remaining.contains_key("fresh"));
        assert!(!remaining.contains_key("old"));
    }

    #[test]
    fn test_evict_stale_entries_empty_group() {
        let mut store = CheckpointStore::default();
        // Should not panic on a missing group.
        evict_stale_entries(&mut store, "nonexistent", TTL_24H_MS, 0);
    }
}
