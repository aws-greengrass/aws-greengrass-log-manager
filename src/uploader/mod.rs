// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! Upload pipeline - batching, CloudWatch client, retry, scheduling, checkpoint advancement

mod batcher;
#[cfg(feature = "aws-sdk")]
mod cw_client;

use crate::config::LogLevel;
use crate::scanner::{
    CheckpointStore, FileCheckpoint, LastFileProcessedTimestamp, LogEvent, ScannedFile,
};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

/// A sealed batch of log events ready for upload to CloudWatch.
#[derive(Debug)]
pub struct SealedBatch {
    pub log_group: String,
    pub log_stream: String,
    pub events: Vec<LogEvent>,
}

#[cfg(feature = "aws-sdk")]
pub use cw_client::{CwLogsClient, CwUploadError, UploadOutcome};

pub use batcher::{seal_batches, MAX_EVENT_BYTES};

// --- Scheduling ---

/// Compute effective upload interval with jitter to prevent thundering herd.
/// Uses per-source override if set, otherwise global interval. Adds 0-5s jitter.
pub fn effective_interval_secs(per_source: Option<u64>, global: u64) -> u64 {
    let base = per_source.unwrap_or(global);
    let jitter = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos() as u64
        % 6;
    base + jitter
}

// --- Upload result tracking ---

/// Result of uploading events for a single log source.
pub struct UploadResult {
    /// Files whose ALL batches succeeded: (path, new_offset, content_hash)
    pub succeeded: Vec<(PathBuf, u64, String)>,
    /// Files that had at least one batch failure
    pub failed: Vec<PathBuf>,
}

/// Upload events from multiple files for a single log source.
/// Batches all events together, uploads each batch, tracks per-file success.
///
/// A file is "succeeded" only if ALL batches containing its events succeed.
/// Since `seal_batches` merges events across files (sorted by timestamp), we use
/// a simpler model: if ANY batch fails, ALL files in this source are marked failed.
#[cfg(feature = "aws-sdk")]
pub async fn upload_source_events(
    client: &mut CwLogsClient,
    log_group: &str,
    log_stream: &str,
    file_events: Vec<(PathBuf, Vec<LogEvent>, u64, String)>, // (path, events, new_offset, hash)
    min_log_level: Option<LogLevel>,
) -> UploadResult {
    // Collect file metadata before flattening
    let file_info: Vec<(PathBuf, u64, String)> = file_events
        .iter()
        .map(|(p, _, off, hash)| (p.clone(), *off, hash.clone()))
        .collect();

    // Flatten all events for batching
    let all_events: Vec<LogEvent> = file_events
        .into_iter()
        .flat_map(|(_, events, _, _)| events)
        .collect();

    if all_events.is_empty() {
        return UploadResult {
            succeeded: file_info,
            failed: vec![],
        };
    }

    let now_ms = std::time::SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before Unix epoch")
        .as_millis() as i64;
    let batches = seal_batches(all_events, log_group, log_stream, min_log_level, now_ms);

    // Upload each batch
    let mut all_ok = true;
    for batch in &batches {
        match client.upload_batch_with_retry(batch).await {
            Ok(cw_client::UploadOutcome::Success) => {}
            Ok(cw_client::UploadOutcome::RetriesExhausted) => {
                all_ok = false;
                break;
            }
            Err(e) => {
                tracing::error!(error = %e, "Auth error during upload, stopping source");
                all_ok = false;
                break;
            }
        }
    }

    if all_ok {
        UploadResult {
            succeeded: file_info,
            failed: vec![],
        }
    } else {
        UploadResult {
            succeeded: vec![],
            failed: file_info.into_iter().map(|(p, _, _)| p).collect(),
        }
    }
}

// --- Checkpoint advancement ---

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Advance checkpoints after successful upload. Returns paths of completed files.
///
/// A file is "completed" when: `!is_active && file_length == new_offset`.
/// Completed files are removed from the checkpoint
/// Partial files get their `start_position` updated.
/// A file is fully uploaded when all bytes have been read, it's not the active
/// (currently written-to) file, and it's non-empty.
#[must_use]
fn is_file_fully_uploaded(is_active: bool, file_len: u64, bytes_read: u64) -> bool {
    !is_active && file_len == bytes_read && file_len > 0
}

pub fn advance_checkpoints(
    store: &mut CheckpointStore,
    log_group_key: &str,
    succeeded: &[(PathBuf, u64, String)], // (path, new_offset, content_hash)
    scanned_files: &[ScannedFile],
) -> Vec<PathBuf> {
    let now = now_ms();
    let mut completed = Vec::new();

    let file_map = store
        .file_processing_info
        .entry(log_group_key.to_string())
        .or_default();

    for (path, new_offset, content_hash) in succeeded {
        // Find the scanned file to check is_active and file length
        let scanned = scanned_files
            .iter()
            .find(|f| &f.content_hash == content_hash);
        let is_active = scanned.is_some_and(|f| f.is_active);
        let file_len = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);

        if is_file_fully_uploaded(is_active, file_len, *new_offset) {
            // File fully uploaded and not active → completed
            file_map.remove(content_hash);
            completed.push(path.clone());

            // Update last_processed_timestamps — monotonically advance
            let file_mtime_ms = scanned
                .and_then(|f| f.mtime.duration_since(UNIX_EPOCH).ok())
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
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
            // Partial or active → update offset
            file_map.insert(
                content_hash.clone(),
                FileCheckpoint {
                    file_hash: content_hash.clone(),
                    start_position: *new_offset,
                    last_modified_time: scanned
                        .and_then(|f| f.mtime.duration_since(UNIX_EPOCH).ok())
                        .map(|d| d.as_millis() as u64)
                        .unwrap_or(0),
                    last_accessed: now,
                },
            );
        }
    }

    completed
}

/// Evict checkpoint entries older than TTL (24 hours by default).
/// Called after each upload cycle to prevent unbounded checkpoint growth.
pub fn evict_stale_entries(store: &mut CheckpointStore, log_group_key: &str, ttl_ms: u64) {
    let now = now_ms();
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

// --- Utilities ---

/// Format log stream name:
/// `/{yyyy}/{MM}/{dd}/thing/{thingName}` (UTC)
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

/// 24 hours in milliseconds — default TTL for checkpoint entries.
pub const TTL_24H_MS: u64 = 24 * 60 * 60 * 1000;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scanner::ScannedFile;
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
        assert_eq!(parts.len(), 6);
        assert_eq!(parts[4], "thing");
    }

    #[test]
    fn test_format_log_stream_name_with_colons() {
        let name = format_log_stream_name("device:with:colons");
        assert!(name.ends_with("/thing/device+with+colons"));
        assert!(!name.contains(':'));
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

        // new_offset=11 but file is 22 bytes and active → partial update
        let succeeded = vec![(file_path.clone(), 11u64, "hash1".to_string())];
        let completed = advance_checkpoints(&mut store, "grp", &succeeded, &scanned);

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

        // new_offset == file_len && !is_active → completed
        let succeeded = vec![(file_path.clone(), 11u64, "hash2".to_string())];
        let completed = advance_checkpoints(&mut store, "grp", &succeeded, &scanned);

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
        let completed = advance_checkpoints(&mut store, "grp", &succeeded, &scanned);

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
    fn test_evict_stale_entries() {
        let mut store = CheckpointStore::default();
        let mut files = HashMap::new();
        // Entry accessed 25 hours ago — should be evicted
        files.insert(
            "old".to_string(),
            FileCheckpoint {
                file_hash: "old".to_string(),
                start_position: 100,
                last_modified_time: 1000,
                last_accessed: now_ms() - 25 * 60 * 60 * 1000,
            },
        );
        // Entry accessed 1 hour ago — should be kept
        files.insert(
            "fresh".to_string(),
            FileCheckpoint {
                file_hash: "fresh".to_string(),
                start_position: 200,
                last_modified_time: 2000,
                last_accessed: now_ms() - 60 * 60 * 1000,
            },
        );
        store.file_processing_info.insert("grp".to_string(), files);

        evict_stale_entries(&mut store, "grp", TTL_24H_MS);

        let remaining = store.file_processing_info.get("grp").unwrap();
        assert_eq!(remaining.len(), 1);
        assert!(remaining.contains_key("fresh"));
        assert!(!remaining.contains_key("old"));
    }

    #[test]
    fn test_evict_stale_entries_empty_group() {
        let mut store = CheckpointStore::default();
        // Should not panic on missing group
        evict_stale_entries(&mut store, "nonexistent", TTL_24H_MS);
    }
}
