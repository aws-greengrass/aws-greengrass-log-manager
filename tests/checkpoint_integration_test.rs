// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for checkpoint persistence and restart recovery

use gg_log_manager::scanner::{
    load_checkpoint, recover_offsets, save_checkpoint, scan_directory, CheckpointStore,
    FileCheckpoint,
};
use regex::Regex;
use std::io::Write;
use tempfile::tempdir;

#[test]
fn test_scan_checkpoint_restart_resume() {
    let dir = tempdir().unwrap();
    let checkpoint_path = dir.path().join("checkpoint.json");

    // Write 3 log files with known content
    for name in &["a.log", "b.log", "c.log"] {
        let mut f = std::fs::File::create(dir.path().join(name)).unwrap();
        writeln!(f, "log line from {name}").unwrap();
    }

    // First scan
    let pattern = Regex::new(r".*\.log$").unwrap();
    let files = scan_directory(dir.path().to_str().unwrap(), &pattern)
        .unwrap()
        .files;
    assert_eq!(files.len(), 3);

    // Simulate partial upload: checkpoint 2 files with offsets
    let mut store = CheckpointStore::default();
    let mut file_map = std::collections::HashMap::new();
    file_map.insert(
        files[0].content_hash.clone(),
        FileCheckpoint::new(files[0].content_hash.clone(), 10, 1000),
    );
    file_map.insert(
        files[1].content_hash.clone(),
        FileCheckpoint::new(files[1].content_hash.clone(), 20, 2000),
    );
    store
        .file_processing_info
        .insert("test-group".to_string(), file_map);

    // Save checkpoint (simulating shutdown)
    save_checkpoint(&checkpoint_path, &store, true).unwrap();

    // Load checkpoint (simulating restart)
    let mut loaded = load_checkpoint(&checkpoint_path, true).unwrap();

    // Re-scan and recover offsets
    let files_after_restart = scan_directory(dir.path().to_str().unwrap(), &pattern)
        .unwrap()
        .files;
    let offsets = recover_offsets(&mut loaded, "test-group", &files_after_restart);

    // Assert: 2 files resume from checkpointed offsets, 1 starts at 0
    assert_eq!(offsets.len(), 3);
    let offset_map: std::collections::HashMap<_, _> = offsets.into_iter().collect();
    assert_eq!(offset_map[&files[0].path], 10);
    assert_eq!(offset_map[&files[1].path], 20);
    assert_eq!(offset_map[&files[2].path], 0);
}

#[test]
fn test_checkpoint_stale_entry_removed_after_file_deleted() {
    let dir = tempdir().unwrap();
    let checkpoint_path = dir.path().join("checkpoint.json");

    // Write 2 log files
    for name in &["keep.log", "delete_me.log"] {
        let mut f = std::fs::File::create(dir.path().join(name)).unwrap();
        writeln!(f, "content of {name}").unwrap();
    }

    // Scan and checkpoint both
    let pattern = Regex::new(r".*\.log$").unwrap();
    let files = scan_directory(dir.path().to_str().unwrap(), &pattern)
        .unwrap()
        .files;
    assert_eq!(files.len(), 2);

    let mut store = CheckpointStore::default();
    let mut file_map = std::collections::HashMap::new();
    for file in &files {
        file_map.insert(
            file.content_hash.clone(),
            FileCheckpoint::new(file.content_hash.clone(), 100, 1000),
        );
    }
    store
        .file_processing_info
        .insert("test-group".to_string(), file_map);
    save_checkpoint(&checkpoint_path, &store, true).unwrap();

    // Delete one file
    std::fs::remove_file(dir.path().join("delete_me.log")).unwrap();

    // Re-scan (now only 1 file) and recover
    let mut loaded = load_checkpoint(&checkpoint_path, true).unwrap();
    let files_after = scan_directory(dir.path().to_str().unwrap(), &pattern)
        .unwrap()
        .files;
    assert_eq!(files_after.len(), 1);

    let offsets = recover_offsets(&mut loaded, "test-group", &files_after);

    // Remaining file resumes at checkpointed offset
    assert_eq!(offsets.len(), 1);
    assert_eq!(offsets[0].1, 100);

    // Deleted file's checkpoint entry is evicted
    let remaining = loaded.file_processing_info.get("test-group").unwrap();
    assert_eq!(remaining.len(), 1);
}
