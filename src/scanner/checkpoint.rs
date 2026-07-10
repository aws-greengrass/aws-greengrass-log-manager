// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! Checkpoint persistence with atomic writes and restart recovery.

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use super::ScannedFile;

/// Per-file checkpoint state. Compatible with Java LogManager v2.3.1+ config.tlog.
/// Java's deprecated `currentProcessingFileName` field is ignored on read, not written.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FileCheckpoint {
    #[serde(rename = "currentProcessingFileHash")]
    pub file_hash: String,
    #[serde(rename = "currentProcessingFileStartPosition")]
    pub start_position: u64,
    #[serde(rename = "currentProcessingFileLastModified")]
    pub last_modified_time: u64,
    /// Epoch millis when this entry was last accessed. Defaults to now on construction.
    #[serde(rename = "lastAccessed")]
    pub last_accessed: u64,
}

impl FileCheckpoint {
    /// Create a new checkpoint entry. `last_accessed` is set to now automatically.
    pub fn new(file_hash: String, start_position: u64, last_modified_time: u64) -> Self {
        Self {
            file_hash,
            start_position,
            last_modified_time,
            last_accessed: now_ms(),
        }
    }
}

/// Timestamp of the last fully processed file for a component.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LastFileProcessedTimestamp {
    #[serde(rename = "lastFileProcessedTimeStamp")]
    pub last_file_processed_time_stamp: u64,
}

/// Deprecated flat checkpoint leaf: one entry per component (not nested by hash), used by
/// LogManager versions before 2.3.1. Read and written only when `deprecated_version_support`
/// is enabled; ignored otherwise.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct DeprecatedFileCheckpoint {
    #[serde(
        rename = "currentProcessingFileName",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub file_name: Option<String>,
    #[serde(rename = "currentProcessingFileHash")]
    pub file_hash: String,
    #[serde(rename = "currentProcessingFileStartPosition")]
    pub start_position: u64,
    #[serde(rename = "currentProcessingFileLastModified")]
    pub last_modified_time: u64,
    #[serde(rename = "lastAccessed", default)]
    pub last_accessed: u64,
}

/// Checkpoint store for all components.
///
/// # Write semantics (for batcher implementer)
/// - Load path: use `putIfAbsent` — don't overwrite existing entries
/// - Upload path: use `put` — overwrite with new startPosition + trigger TTL eviction
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct CheckpointStore {
    /// Current nested format (2.3.1+): `component -> hash -> FileCheckpoint`.
    #[serde(rename = "currentComponentFileProcessingInformationV2", default)]
    pub file_processing_info: HashMap<String, HashMap<String, FileCheckpoint>>,
    /// Deprecated flat format (≤2.3.0): `component -> single FileCheckpoint`. Merged into the
    /// current map on load and regenerated on save when `deprecated_version_support` is on;
    /// omitted from the serialized file when empty.
    #[serde(
        rename = "currentComponentFileProcessingInformation",
        default,
        skip_serializing_if = "HashMap::is_empty"
    )]
    pub(crate) deprecated_file_processing_info: HashMap<String, DeprecatedFileCheckpoint>,
    #[serde(rename = "componentLastFileProcessedTimeStamp", default)]
    pub last_processed_timestamps: HashMap<String, LastFileProcessedTimestamp>,
}

/// Save checkpoint atomically: write to temp file, then rename.
///
/// When `deprecated_version_support` is true, the deprecated flat format is regenerated from
/// the current map (one entry per component, the most-recently-accessed) and written alongside
/// it for backward compatibility; when false, the deprecated format is omitted.
///
/// # Errors
/// Returns `io::Error` if the file cannot be created, written, synced, or renamed.
pub fn save_checkpoint(
    path: &Path,
    store: &CheckpointStore,
    deprecated_version_support: bool,
) -> io::Result<()> {
    let mut store_to_write = store.clone();
    if deprecated_version_support {
        populate_deprecated_from_current(&mut store_to_write);
    } else {
        store_to_write.deprecated_file_processing_info.clear();
    }

    let tmp_path = path.with_extension("tmp");
    let result = (|| -> io::Result<()> {
        let file = fs::File::create(&tmp_path)
            .map_err(|e| io::Error::new(e.kind(), format!("create {}: {e}", tmp_path.display())))?;
        serde_json::to_writer_pretty(&file, &store_to_write)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        file.sync_all()
            .map_err(|e| io::Error::new(e.kind(), format!("sync {}: {e}", tmp_path.display())))?;
        fs::rename(&tmp_path, path)
            .map_err(|e| io::Error::new(e.kind(), format!("rename to {}: {e}", path.display())))?;
        // Fsync parent directory to ensure the rename is durable on power loss
        if let Some(parent) = path.parent() {
            fs::File::open(parent)?.sync_all()?;
        }
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&tmp_path);
    }
    result?;
    tracing::debug!(path = %path.display(), "Checkpoint saved");
    Ok(())
}

/// Regenerate the deprecated flat map from the current map: for each component, pick the
/// entry with the highest `last_accessed` as the single deprecated entry.
fn populate_deprecated_from_current(store: &mut CheckpointStore) {
    store.deprecated_file_processing_info.clear();
    for (component, files) in &store.file_processing_info {
        if let Some(most_recent) = files.values().max_by_key(|cp| cp.last_accessed) {
            store.deprecated_file_processing_info.insert(
                component.clone(),
                DeprecatedFileCheckpoint {
                    file_name: None,
                    file_hash: most_recent.file_hash.clone(),
                    start_position: most_recent.start_position,
                    last_modified_time: most_recent.last_modified_time,
                    last_accessed: most_recent.last_accessed,
                },
            );
        }
    }
}

/// Load checkpoint from disk. Returns empty store if file missing or corrupt.
///
/// When `deprecated_version_support` is true, entries from the deprecated flat format are
/// merged into the current map with put-if-absent semantics (a current entry for the same
/// hash wins); when false, the deprecated format is discarded.
///
/// # Errors
/// Returns `io::Error` on read failures other than `NotFound`.
pub fn load_checkpoint(
    path: &Path,
    deprecated_version_support: bool,
) -> io::Result<CheckpointStore> {
    match fs::read_to_string(path) {
        Ok(content) => match serde_json::from_str::<CheckpointStore>(&content) {
            Ok(mut store) => {
                if deprecated_version_support {
                    merge_deprecated_entries(&mut store);
                } else {
                    store.deprecated_file_processing_info.clear();
                }
                let entry_count: usize = store.file_processing_info.values().map(|m| m.len()).sum();
                tracing::debug!(path = %path.display(), entry_count = entry_count, "Checkpoint loaded");
                Ok(store)
            }
            Err(e) => {
                tracing::warn!("Corrupt checkpoint file, starting fresh: {}", e);
                Ok(CheckpointStore::default())
            }
        },
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(CheckpointStore::default()),
        Err(e) => Err(io::Error::new(
            e.kind(),
            format!("read {}: {e}", path.display()),
        )),
    }
}

/// Merge deprecated flat entries into the current map with put-if-absent semantics (a current
/// entry for the same component + hash is preserved), then clear the deprecated map so no stale
/// state lingers in memory; `save_checkpoint` regenerates it from the current map on every write.
fn merge_deprecated_entries(store: &mut CheckpointStore) {
    for (component, deprecated_entry) in &store.deprecated_file_processing_info {
        let component_map = store
            .file_processing_info
            .entry(component.clone())
            .or_default();
        component_map
            .entry(deprecated_entry.file_hash.clone())
            .or_insert_with(|| FileCheckpoint {
                file_hash: deprecated_entry.file_hash.clone(),
                start_position: deprecated_entry.start_position,
                last_modified_time: deprecated_entry.last_modified_time,
                last_accessed: deprecated_entry.last_accessed,
            });
    }
    store.deprecated_file_processing_info.clear();
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Trim stale entries where lastModifiedTime is at or before the component's
/// lastFileProcessedTimeStamp. Prevents loading already-completed entries from older LM versions.
pub fn trim_stale_on_load(store: &mut CheckpointStore) {
    let ts_snapshot: HashMap<&str, u64> = store
        .last_processed_timestamps
        .iter()
        .map(|(k, v)| (k.as_str(), v.last_file_processed_time_stamp))
        .collect();

    for (component, files) in &mut store.file_processing_info {
        if let Some(&cutoff) = ts_snapshot.get(component.as_str()) {
            let before = files.len();
            files.retain(|_, cp| cp.last_modified_time > cutoff);
            let removed = before - files.len();
            if removed > 0 {
                tracing::info!(
                    component = component.as_str(),
                    removed,
                    "Trimmed stale entries on load"
                );
            }
        }
    }
}

/// Recover file offsets from checkpoint for restart recovery.
/// Returns (file_path, resume_offset) pairs for each scanned file.
/// Updates last_accessed on read (touch-on-read).
/// Evicts entries for files no longer on disk (see DIVERGENCE comment in body).
///
/// The caller must pass ALL files for the component in a single call.
/// One directory per component — the config schema enforces this.
#[must_use = "recovered offsets must be used for file reading"]
pub fn recover_offsets(
    checkpoint: &mut CheckpointStore,
    log_group_key: &str,
    scanned_files: &[ScannedFile],
) -> Vec<(PathBuf, u64)> {
    let current_hashes: HashSet<&str> = scanned_files
        .iter()
        .map(|f| f.content_hash.as_str())
        .collect();
    let now = now_ms();

    let result: Vec<(PathBuf, u64)> = scanned_files
        .iter()
        .map(|file| {
            let offset = checkpoint
                .file_processing_info
                .get_mut(log_group_key)
                .and_then(|m| m.get_mut(&file.content_hash))
                .map(|cp| {
                    cp.last_accessed = now; // touch-on-read
                    tracing::debug!(
                        "Resuming file {} from offset {}",
                        file.path.display(),
                        cp.start_position
                    );
                    cp.start_position
                })
                .unwrap_or_else(|| {
                    tracing::debug!("New file {}, reading from start", file.path.display());
                    0
                });
            (file.path.clone(), offset)
        })
        .collect();

    // DIVERGENCE: Java evicts via TTL on every put() call (triggered by upload
    // callbacks) and explicitly via deleteFileFromGroup() on completed files.
    // We evict on scan because the upload path doesn't exist yet.
    // Move to upload path when batcher lands.
    if let Some(file_map) = checkpoint.file_processing_info.get_mut(log_group_key) {
        file_map.retain(|hash, _| {
            let keep = current_hashes.contains(hash.as_str());
            if !keep {
                tracing::debug!("Evicting checkpoint for hash {}", hash);
            }
            keep
        });
    }

    result
}

/// Remove all checkpoint data for a component. Called when a component is removed from config.
pub fn remove_component(store: &mut CheckpointStore, component: &str) {
    store.file_processing_info.remove(component);
    store.last_processed_timestamps.remove(component);
    tracing::debug!(component, "Removed checkpoint data for component");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scanner::ScannedFile;
    use std::time::SystemTime;
    use tempfile::tempdir;

    #[test]
    fn test_save_and_reload() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("checkpoint.json");

        let mut store = CheckpointStore::default();
        let mut files = HashMap::new();
        files.insert(
            "abc123".to_string(),
            FileCheckpoint {
                file_hash: "abc123".to_string(),
                start_position: 1024,
                last_modified_time: 1700000000000,
                last_accessed: 1700000001000,
            },
        );
        store
            .file_processing_info
            .insert("/aws/greengrass/test".to_string(), files);
        store.last_processed_timestamps.insert(
            "/aws/greengrass/test".to_string(),
            LastFileProcessedTimestamp {
                last_file_processed_time_stamp: 1700000000000,
            },
        );

        save_checkpoint(&path, &store, true).unwrap();
        let loaded = load_checkpoint(&path, true).unwrap();
        assert_eq!(store, loaded);
    }

    // Verifies the serialized JSON uses Java's field names for backward compatibility for checkpointing.
    #[test]
    fn test_json_field_names_match_java() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("checkpoint.json");

        let mut store = CheckpointStore::default();
        let mut files = HashMap::new();
        files.insert(
            "hash123".to_string(),
            FileCheckpoint {
                file_hash: "hash123".to_string(),
                start_position: 512,
                last_modified_time: 1600000000000,
                last_accessed: 1600000001000,
            },
        );
        store
            .file_processing_info
            .insert("test-group".to_string(), files);

        save_checkpoint(&path, &store, true).unwrap();
        let content = fs::read_to_string(&path).unwrap();

        // Verify Java-compatible field names
        assert!(content.contains("currentComponentFileProcessingInformationV2"));
        assert!(content.contains("currentProcessingFileHash"));
        assert!(content.contains("currentProcessingFileStartPosition"));
        assert!(content.contains("currentProcessingFileLastModified"));
        assert!(content.contains("lastAccessed"));
        // Dual-write also emits the deprecated flat top-level key (exact-key check, since the
        // V2 key name contains this string as a prefix).
        let saved: serde_json::Value = serde_json::from_str(&content).unwrap();
        assert!(saved
            .get("currentComponentFileProcessingInformation")
            .is_some());
    }

    #[test]
    fn test_load_java_checkpoint() {
        // Simulate a checkpoint written by Java LogManager
        let dir = tempdir().unwrap();
        let path = dir.path().join("config.tlog");
        let java_checkpoint = r#"{
            "currentComponentFileProcessingInformationV2": {
                "system-health": {
                    "abc123hash": {
                        "currentProcessingFileHash": "abc123hash",
                        "currentProcessingFileStartPosition": 2048,
                        "currentProcessingFileLastModified": 1709913600000,
                        "lastAccessed": 1709913660000
                    }
                }
            },
            "componentLastFileProcessedTimeStamp": {
                "system-health": {
                    "lastFileProcessedTimeStamp": 1709913600000
                }
            }
        }"#;
        fs::write(&path, java_checkpoint).unwrap();

        let store = load_checkpoint(&path, true).unwrap();
        let files = store.file_processing_info.get("system-health").unwrap();
        let entry = files.get("abc123hash").unwrap();
        assert_eq!(entry.file_hash, "abc123hash");
        assert_eq!(entry.start_position, 2048);
        assert_eq!(entry.last_modified_time, 1709913600000);
        assert_eq!(entry.last_accessed, 1709913660000);
    }

    #[test]
    fn test_atomic_write_no_partial() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("checkpoint.json");
        let tmp_path = path.with_extension("tmp");

        let store = CheckpointStore::default();
        save_checkpoint(&path, &store, true).unwrap();

        assert!(!tmp_path.exists());
        assert!(path.exists());
    }

    #[test]
    fn test_missing_file_returns_empty() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("nonexistent.json");
        let store = load_checkpoint(&path, true).unwrap();
        assert!(store.file_processing_info.is_empty());
        assert!(store.last_processed_timestamps.is_empty());
    }

    #[test]
    fn test_corrupt_json_returns_empty() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("corrupt.json");
        fs::write(&path, "{ invalid json }").unwrap();
        let result = load_checkpoint(&path, true).unwrap();
        assert!(result.file_processing_info.is_empty());
        assert!(result.last_processed_timestamps.is_empty());
    }

    #[test]
    fn test_recover_offsets_matching_hash() {
        let mut store = CheckpointStore::default();
        let mut files = HashMap::new();
        files.insert(
            "hash123".to_string(),
            FileCheckpoint {
                file_hash: "hash123".to_string(),
                start_position: 1024,
                last_modified_time: 1700000000000,
                last_accessed: 1700000001000,
            },
        );
        store
            .file_processing_info
            .insert("test-group".to_string(), files);

        let scanned = vec![ScannedFile {
            path: PathBuf::from("/var/log/app.log"),
            mtime: SystemTime::now(),
            content_hash: "hash123".to_string(),
            is_active: true,
        }];

        let result = recover_offsets(&mut store, "test-group", &scanned);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, PathBuf::from("/var/log/app.log"));
        assert_eq!(result[0].1, 1024);
    }

    #[test]
    fn test_recover_offsets_new_file() {
        let mut store = CheckpointStore::default();

        let scanned = vec![ScannedFile {
            path: PathBuf::from("/var/log/new.log"),
            mtime: SystemTime::now(),
            content_hash: "newhash".to_string(),
            is_active: true,
        }];

        let result = recover_offsets(&mut store, "test-group", &scanned);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].1, 0);
    }

    #[test]
    fn test_recover_offsets_removes_stale() {
        let mut store = CheckpointStore::default();
        let mut files = HashMap::new();
        files.insert(
            "stale_hash".to_string(),
            FileCheckpoint {
                file_hash: "stale_hash".to_string(),
                start_position: 500,
                last_modified_time: 1700000000000,
                last_accessed: 1700000001000,
            },
        );
        files.insert(
            "current_hash".to_string(),
            FileCheckpoint {
                file_hash: "current_hash".to_string(),
                start_position: 200,
                last_modified_time: 1700000000000,
                last_accessed: 1700000001000,
            },
        );
        store
            .file_processing_info
            .insert("test-group".to_string(), files);

        let scanned = vec![ScannedFile {
            path: PathBuf::from("/var/log/current.log"),
            mtime: SystemTime::now(),
            content_hash: "current_hash".to_string(),
            is_active: true,
        }];

        let _ = recover_offsets(&mut store, "test-group", &scanned);

        let remaining = store.file_processing_info.get("test-group").unwrap();
        assert_eq!(remaining.len(), 1);
        assert!(remaining.contains_key("current_hash"));
        assert!(!remaining.contains_key("stale_hash"));
    }

    #[test]
    fn test_trim_stale_on_load() {
        let mut store = CheckpointStore::default();
        let mut files = HashMap::new();
        // Entry with lastModifiedTime BEFORE lastProcessedTimestamp — should be trimmed
        files.insert(
            "old_hash".to_string(),
            FileCheckpoint {
                file_hash: "old_hash".to_string(),
                start_position: 100,
                last_modified_time: 1000,
                last_accessed: 2000,
            },
        );
        // Entry with lastModifiedTime AFTER lastProcessedTimestamp — should be kept
        files.insert(
            "new_hash".to_string(),
            FileCheckpoint {
                file_hash: "new_hash".to_string(),
                start_position: 200,
                last_modified_time: 3000,
                last_accessed: 4000,
            },
        );
        store.file_processing_info.insert("comp".to_string(), files);
        store.last_processed_timestamps.insert(
            "comp".to_string(),
            LastFileProcessedTimestamp {
                last_file_processed_time_stamp: 2000,
            },
        );

        trim_stale_on_load(&mut store);

        let remaining = store.file_processing_info.get("comp").unwrap();
        assert_eq!(remaining.len(), 1);
        assert!(remaining.contains_key("new_hash"));
        assert!(!remaining.contains_key("old_hash"));
    }

    #[test]
    fn test_touch_on_read_refreshes_old_entries() {
        let mut store = CheckpointStore::default();
        let mut files = HashMap::new();
        // Entry with old last_accessed — touch-on-read will refresh it
        let old_accessed = now_ms() - 90_000_000; // 25 hours ago
        files.insert(
            "stale_ttl".to_string(),
            FileCheckpoint {
                file_hash: "stale_ttl".to_string(),
                start_position: 100,
                last_modified_time: 1000,
                last_accessed: old_accessed,
            },
        );
        // Fresh entry — should be kept
        files.insert(
            "fresh_hash".to_string(),
            FileCheckpoint {
                file_hash: "fresh_hash".to_string(),
                start_position: 200,
                last_modified_time: 2000,
                last_accessed: now_ms(),
            },
        );
        store
            .file_processing_info
            .insert("group".to_string(), files);

        // Both files are on disk, but stale_ttl has old last_accessed
        let scanned = vec![
            ScannedFile {
                path: PathBuf::from("/var/log/stale.log"),
                mtime: SystemTime::now(),
                content_hash: "stale_ttl".to_string(),
                is_active: false,
            },
            ScannedFile {
                path: PathBuf::from("/var/log/fresh.log"),
                mtime: SystemTime::now(),
                content_hash: "fresh_hash".to_string(),
                is_active: true,
            },
        ];

        let _ = recover_offsets(&mut store, "group", &scanned);

        // Both files are on disk — both survive. Touch-on-read refreshes last_accessed.
        let remaining = store.file_processing_info.get("group").unwrap();
        assert_eq!(remaining.len(), 2);
        // Verify old entry's last_accessed was refreshed
        assert!(remaining.get("stale_ttl").unwrap().last_accessed > old_accessed);
    }

    #[test]
    fn test_touch_on_read_updates_last_accessed() {
        let mut store = CheckpointStore::default();
        let mut files = HashMap::new();
        let old_accessed = 1000; // very old
        files.insert(
            "hash1".to_string(),
            FileCheckpoint {
                file_hash: "hash1".to_string(),
                start_position: 500,
                last_modified_time: 2000,
                last_accessed: old_accessed,
            },
        );
        store
            .file_processing_info
            .insert("group".to_string(), files);

        let scanned = vec![ScannedFile {
            path: PathBuf::from("/var/log/app.log"),
            mtime: SystemTime::now(),
            content_hash: "hash1".to_string(),
            is_active: true,
        }];

        let before = now_ms();
        let _ = recover_offsets(&mut store, "group", &scanned);
        let after = now_ms();

        let entry = store
            .file_processing_info
            .get("group")
            .unwrap()
            .get("hash1")
            .unwrap();
        // last_accessed should be updated to approximately now
        assert!(entry.last_accessed >= before);
        assert!(entry.last_accessed <= after);
    }

    #[test]
    fn test_remove_component() {
        let mut store = CheckpointStore::default();
        let mut files = HashMap::new();
        files.insert(
            "h1".to_string(),
            FileCheckpoint::new("h1".into(), 100, 1000),
        );
        store
            .file_processing_info
            .insert("comp-a".to_string(), files);
        store.last_processed_timestamps.insert(
            "comp-a".to_string(),
            LastFileProcessedTimestamp {
                last_file_processed_time_stamp: 1000,
            },
        );

        remove_component(&mut store, "comp-a");

        assert!(!store.file_processing_info.contains_key("comp-a"));
        assert!(!store.last_processed_timestamps.contains_key("comp-a"));
    }

    #[test]
    fn test_save_checkpoint_cleans_tmp_on_error() {
        // Write to a nonexistent directory — create will fail
        let path = Path::new("/nonexistent/dir/checkpoint.json");
        let tmp_path = path.with_extension("tmp");
        let store = CheckpointStore::default();
        let result = save_checkpoint(path, &store, true);
        assert!(result.is_err());
        assert!(!tmp_path.exists());
    }

    #[test]
    fn test_load_deprecated_checkpoint() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("checkpoint.json");
        // Deprecated flat format: component -> single entry with the hash as a leaf field.
        let v1_json = r#"{
            "currentComponentFileProcessingInformation": {
                "my-component": {
                    "currentProcessingFileName": "/var/log/app.log",
                    "currentProcessingFileHash": "v1hash123",
                    "currentProcessingFileStartPosition": 4096,
                    "currentProcessingFileLastModified": 1700000000000,
                    "lastAccessed": 1700000001000
                }
            }
        }"#;
        fs::write(&path, v1_json).unwrap();

        let store = load_checkpoint(&path, true).unwrap();
        // Deprecated entry is merged into the current map.
        let files = store.file_processing_info.get("my-component").unwrap();
        let entry = files.get("v1hash123").unwrap();
        assert_eq!(entry.file_hash, "v1hash123");
        assert_eq!(entry.start_position, 4096);
        assert_eq!(entry.last_modified_time, 1700000000000);
        assert_eq!(entry.last_accessed, 1700000001000);
    }

    #[test]
    fn test_load_deprecated_does_not_overwrite_current() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("checkpoint.json");
        // Both formats present for the same component + hash — the current entry wins.
        let json = r#"{
            "currentComponentFileProcessingInformationV2": {
                "my-component": {
                    "v1hash123": {
                        "currentProcessingFileHash": "v1hash123",
                        "currentProcessingFileStartPosition": 8192,
                        "currentProcessingFileLastModified": 1700000002000,
                        "lastAccessed": 1700000003000
                    }
                }
            },
            "currentComponentFileProcessingInformation": {
                "my-component": {
                    "currentProcessingFileHash": "v1hash123",
                    "currentProcessingFileStartPosition": 4096,
                    "currentProcessingFileLastModified": 1700000000000,
                    "lastAccessed": 1700000001000
                }
            }
        }"#;
        fs::write(&path, json).unwrap();

        let store = load_checkpoint(&path, true).unwrap();
        let files = store.file_processing_info.get("my-component").unwrap();
        let entry = files.get("v1hash123").unwrap();
        // put-if-absent: the current value is preserved.
        assert_eq!(entry.start_position, 8192);
    }

    #[test]
    fn test_save_writes_both_deprecated_and_current() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("checkpoint.json");

        let mut store = CheckpointStore::default();
        let mut files = HashMap::new();
        files.insert(
            "hashA".to_string(),
            FileCheckpoint {
                file_hash: "hashA".to_string(),
                start_position: 100,
                last_modified_time: 1000,
                last_accessed: 5000, // most recently accessed
            },
        );
        files.insert(
            "hashB".to_string(),
            FileCheckpoint {
                file_hash: "hashB".to_string(),
                start_position: 200,
                last_modified_time: 2000,
                last_accessed: 3000,
            },
        );
        store.file_processing_info.insert("comp".to_string(), files);

        save_checkpoint(&path, &store, true).unwrap();
        let content = fs::read_to_string(&path).unwrap();

        let saved: serde_json::Value = serde_json::from_str(&content).unwrap();
        assert!(saved
            .get("currentComponentFileProcessingInformationV2")
            .is_some());
        // Deprecated format holds the single most-recently-accessed entry (hashA).
        let deprecated = &saved["currentComponentFileProcessingInformation"]["comp"];
        assert_eq!(deprecated["currentProcessingFileHash"], "hashA");
        assert_eq!(deprecated["currentProcessingFileStartPosition"], 100);
    }

    #[test]
    fn test_load_deprecated_skipped_when_deprecated_support_false() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("checkpoint.json");
        let v1_json = r#"{
            "currentComponentFileProcessingInformation": {
                "my-component": {
                    "currentProcessingFileName": "/var/log/app.log",
                    "currentProcessingFileHash": "v1hash123",
                    "currentProcessingFileStartPosition": 4096,
                    "currentProcessingFileLastModified": 1700000000000,
                    "lastAccessed": 1700000001000
                }
            }
        }"#;
        fs::write(&path, v1_json).unwrap();

        let store = load_checkpoint(&path, false).unwrap();
        // Deprecated data is neither merged nor retained.
        assert!(store.file_processing_info.is_empty());
        assert!(store.deprecated_file_processing_info.is_empty());
    }

    #[test]
    fn test_save_skips_deprecated_when_deprecated_support_false() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("checkpoint.json");

        let mut store = CheckpointStore::default();
        let mut files = HashMap::new();
        files.insert(
            "hashA".to_string(),
            FileCheckpoint {
                file_hash: "hashA".to_string(),
                start_position: 100,
                last_modified_time: 1000,
                last_accessed: 5000,
            },
        );
        store.file_processing_info.insert("comp".to_string(), files);

        save_checkpoint(&path, &store, false).unwrap();
        let content = fs::read_to_string(&path).unwrap();

        // Current format present, deprecated format absent (the trailing quote distinguishes
        // it from the V2 key, which shares this prefix).
        assert!(content.contains("currentComponentFileProcessingInformationV2"));
        assert!(!content.contains("currentComponentFileProcessingInformation\""));
    }
}
