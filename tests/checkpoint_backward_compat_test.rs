// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! Backward-compatibility tests: reading a deprecated flat-format checkpoint written by an
//! older LogManager and migrating it into the current nested format through the same
//! load → trim sequence the binary runs at startup.

use gg_log_manager::scanner::{load_checkpoint, trim_stale_on_load};
use std::io::Write;
use tempfile::tempdir;

/// A deprecated flat-format checkpoint is read, merged into the current map, and the migrated
/// entry survives the stale-trim step, so the resume offset is recovered. Reading the old
/// format must never panic. The entry's last-modified time is AFTER the component's
/// last-processed timestamp, so `trim_stale_on_load` keeps it (a non-vacuous trim: the trim
/// runs and the entry is retained because of that relationship).
#[test]
fn test_migrate_deprecated_checkpoint_survives_load_and_trim() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("config.tlog");

    let deprecated_tlog = r#"{
        "currentComponentFileProcessingInformation": {
            "my-component": {
                "currentProcessingFileName": "/var/log/my-component/app.log",
                "currentProcessingFileHash": "deadbeefhash",
                "currentProcessingFileStartPosition": 4096,
                "currentProcessingFileLastModified": 1700000000000,
                "lastAccessed": 1700000005000
            }
        },
        "componentLastFileProcessedTimeStamp": {
            "my-component": {
                "lastFileProcessedTimeStamp": 1600000000000
            }
        }
    }"#;
    let mut f = std::fs::File::create(&path).unwrap();
    f.write_all(deprecated_tlog.as_bytes()).unwrap();

    // Same sequence main() runs at startup: load (with deprecated support) then trim.
    let mut store = load_checkpoint(&path, true).unwrap();
    trim_stale_on_load(&mut store);

    // The deprecated entry migrated into the current nested map and survived the trim.
    let files = store
        .file_processing_info
        .get("my-component")
        .expect("component present after migration");
    let entry = files
        .get("deadbeefhash")
        .expect("deprecated entry migrated and not trimmed");
    assert_eq!(entry.start_position, 4096);
    assert_eq!(entry.last_modified_time, 1700000000000);
}

/// A stale deprecated entry (last-modified at or before the component's last-processed
/// timestamp) is trimmed away after migration — confirming the load → trim path is live and
/// the survival above is not vacuous.
#[test]
fn test_migrate_deprecated_checkpoint_trimmed_when_stale() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("config.tlog");

    let deprecated_tlog = r#"{
        "currentComponentFileProcessingInformation": {
            "my-component": {
                "currentProcessingFileHash": "stalehash",
                "currentProcessingFileStartPosition": 4096,
                "currentProcessingFileLastModified": 1600000000000,
                "lastAccessed": 1700000005000
            }
        },
        "componentLastFileProcessedTimeStamp": {
            "my-component": {
                "lastFileProcessedTimeStamp": 1700000000000
            }
        }
    }"#;
    let mut f = std::fs::File::create(&path).unwrap();
    f.write_all(deprecated_tlog.as_bytes()).unwrap();

    let mut store = load_checkpoint(&path, true).unwrap();
    trim_stale_on_load(&mut store);

    // Migrated but stale (mtime <= last-processed) → trimmed.
    let files = store.file_processing_info.get("my-component").unwrap();
    assert!(!files.contains_key("stalehash"));
}

/// With deprecated support disabled, a flat-format checkpoint is ignored (not migrated) and
/// still does not panic.
#[test]
fn test_deprecated_checkpoint_ignored_when_support_disabled() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("config.tlog");

    let deprecated_tlog = r#"{
        "currentComponentFileProcessingInformation": {
            "my-component": {
                "currentProcessingFileHash": "deadbeefhash",
                "currentProcessingFileStartPosition": 4096,
                "currentProcessingFileLastModified": 1700000000000,
                "lastAccessed": 1700000005000
            }
        }
    }"#;
    let mut f = std::fs::File::create(&path).unwrap();
    f.write_all(deprecated_tlog.as_bytes()).unwrap();

    let store = load_checkpoint(&path, false).unwrap();
    assert!(store.file_processing_info.is_empty());
}
