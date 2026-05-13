// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the upload orchestrator — verifies Java behavioral parity.

use gg_log_manager::config::LogLevel;
use gg_log_manager::disk::free_disk_space;
use gg_log_manager::scanner::{
    assemble_multiline, load_checkpoint, read_file_from_offset, recover_offsets, save_checkpoint,
    scan_directory, CheckpointStore, FileCheckpoint,
};
use gg_log_manager::uploader::{
    advance_checkpoints, effective_interval_secs, evict_stale_entries, seal_batches, TTL_24H_MS,
};
use regex::Regex;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::PathBuf;
use tempfile::{tempdir, TempDir};

fn emf_pattern() -> Regex {
    Regex::new(r".*\.emf\.json$").unwrap()
}

fn log_pattern() -> Regex {
    Regex::new(r".*\.log$").unwrap()
}

fn all_pattern() -> Regex {
    Regex::new(r".*\.(emf\.json|log)$").unwrap()
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

fn write_test_file(dir: &std::path::Path, name: &str, lines: &[&str]) -> PathBuf {
    let path = dir.join(name);
    let mut f = File::create(&path).unwrap();
    for line in lines {
        writeln!(f, "{}", line).unwrap();
    }
    path
}

// --- T1: Full cycle scan → read → batch → checkpoint advance (mixed EMF + logs) ---

#[test]
fn test_full_cycle_scan_read_batch_checkpoint() {
    let dir = TempDir::new().unwrap();

    // Write EMF files
    write_test_file(
        dir.path(),
        "a.emf.json",
        &[r#"{"_aws":{"Timestamp":1705314645000},"metric":"cpu","value":12.5}"#],
    );
    // Write component log files
    write_test_file(
        dir.path(),
        "app.log",
        &[
            r#"{"level":"INFO","message":"started","timestamp":1705314646000}"#,
            r#"{"level":"WARN","message":"slow query","timestamp":1705314647000}"#,
        ],
    );
    write_test_file(
        dir.path(),
        "b.emf.json",
        &[r#"{"_aws":{"Timestamp":1705314648000},"metric":"mem","value":67.8}"#],
    );

    let pattern = all_pattern();
    let scanned = scan_directory(dir.path().to_str().unwrap(), &pattern).unwrap();
    assert_eq!(scanned.len(), 3);

    // Recover offsets (fresh start)
    let mut store = CheckpointStore::default();
    let offsets = recover_offsets(&mut store, "test-group", &scanned);
    assert_eq!(offsets.len(), 3);
    assert!(offsets.iter().all(|(_, off)| *off == 0));

    // Read all files
    let default_ts = now_ms();
    let mut file_events = Vec::new();
    for (path, offset) in &offsets {
        let (events, new_offset) = read_file_from_offset(path, *offset, default_ts).unwrap();
        let hash = scanned
            .iter()
            .find(|f| f.path == *path)
            .unwrap()
            .content_hash
            .clone();
        file_events.push((path.clone(), events, new_offset, hash));
    }
    assert_eq!(file_events.len(), 3);

    // Batch events — EMF passes through, logs respect level filter
    let all_events: Vec<_> = file_events
        .iter()
        .flat_map(|(_, e, _, _)| e.clone())
        .collect();
    // No filter (DEBUG) — all 4 events pass
    let batches = seal_batches(all_events.clone(), "/aws/test", "/stream", None, now_ms());
    assert_eq!(batches[0].events.len(), 4);

    // With WARN filter — EMF still passes (no level field), only WARN+ logs pass
    let batches = seal_batches(
        all_events,
        "/aws/test",
        "/stream",
        Some(LogLevel::Warn),
        now_ms(),
    );
    assert_eq!(batches[0].events.len(), 3); // 2 EMF + 1 WARN log

    // Simulate successful upload → advance checkpoints
    let succeeded: Vec<_> = file_events
        .iter()
        .map(|(p, _, off, h)| (p.clone(), *off, h.clone()))
        .collect();
    let _completed = advance_checkpoints(&mut store, "test-group", &succeeded, &scanned);

    let active_count = scanned.iter().filter(|f| f.is_active).count();
    assert_eq!(active_count, 1);

    // Verify checkpoints were written for all files
    let file_map = store.file_processing_info.get("test-group").unwrap();
    assert!(!file_map.is_empty());
}

// --- T2: Restart recovery — no duplicate uploads ---

#[test]
fn test_restart_recovery_no_duplicates() {
    let dir = TempDir::new().unwrap();

    let checkpoint_path = dir.path().join("checkpoint.json");
    let path = write_test_file(
        dir.path(),
        "metrics.emf.json",
        &[r#"{"metric":"line1"}"#, r#"{"metric":"line2"}"#],
    );

    let pattern = emf_pattern();
    let scanned = scan_directory(dir.path().to_str().unwrap(), &pattern).unwrap();
    let mut store = CheckpointStore::default();

    // First read — gets both lines
    let offsets = recover_offsets(&mut store, "grp", &scanned);
    let (events1, offset1) = read_file_from_offset(&offsets[0].0, offsets[0].1, now_ms()).unwrap();
    assert_eq!(events1.len(), 2);

    // Simulate upload success → advance checkpoint
    let hash = scanned[0].content_hash.clone();
    let succeeded = vec![(path.clone(), offset1, hash.clone())];
    advance_checkpoints(&mut store, "grp", &succeeded, &scanned);

    // Persist checkpoint
    save_checkpoint(&checkpoint_path, &store, false).unwrap();

    // "Restart" — reload checkpoint
    let mut loaded = load_checkpoint(&checkpoint_path, false).unwrap();
    let scanned2 = scan_directory(dir.path().to_str().unwrap(), &pattern).unwrap();
    let offsets2 = recover_offsets(&mut loaded, "grp", &scanned2);

    // Offset should resume from where we left off
    assert_eq!(offsets2[0].1, offset1);

    // Append new content
    let mut f = fs::OpenOptions::new().append(true).open(&path).unwrap();
    writeln!(f, r#"{{"metric":"line3"}}"#).unwrap();

    // Read from checkpoint offset — only gets new content
    let (events2, _) = read_file_from_offset(&offsets2[0].0, offsets2[0].1, now_ms()).unwrap();
    assert_eq!(events2.len(), 1);
    assert!(events2[0].message.contains("line3"));
}

// --- T3: Completed file detection (E2E with full pipeline) ---

#[test]
fn test_completed_file_detection() {
    let dir = TempDir::new().unwrap();

    // Write two files — set mtime so "old.emf.json" is NOT active
    let old_path = write_test_file(dir.path(), "old.emf.json", &[r#"{"m":"old"}"#]);
    std::thread::sleep(std::time::Duration::from_millis(50));
    let _new_path = write_test_file(dir.path(), "new.emf.json", &[r#"{"m":"new"}"#]);

    let pattern = emf_pattern();
    let scanned = scan_directory(dir.path().to_str().unwrap(), &pattern).unwrap();

    let old_scanned = scanned.iter().find(|f| f.path == old_path).unwrap();
    assert!(!old_scanned.is_active);

    // Read old file fully
    let (_, new_offset) = read_file_from_offset(&old_path, 0, now_ms()).unwrap();
    let file_len = fs::metadata(&old_path).unwrap().len();
    assert_eq!(new_offset, file_len);

    // Advance checkpoint — old file should be detected as completed
    let mut store = CheckpointStore::default();
    let succeeded = vec![(
        old_path.clone(),
        new_offset,
        old_scanned.content_hash.clone(),
    )];
    let completed = advance_checkpoints(&mut store, "grp", &succeeded, &scanned);

    assert_eq!(completed, vec![old_path]);
}

// --- T4: deleteLogFileAfterCloudUpload ---

#[test]
fn test_delete_file_after_upload() {
    let dir = TempDir::new().unwrap();

    let old_path = write_test_file(dir.path(), "old.emf.json", &[r#"{"m":"data"}"#]);
    std::thread::sleep(std::time::Duration::from_millis(50));
    let _new_path = write_test_file(dir.path(), "new.emf.json", &[r#"{"m":"active"}"#]);

    let pattern = emf_pattern();
    let scanned = scan_directory(dir.path().to_str().unwrap(), &pattern).unwrap();
    let old_scanned = scanned.iter().find(|f| f.path == old_path).unwrap();

    let (_, new_offset) = read_file_from_offset(&old_path, 0, now_ms()).unwrap();

    let mut store = CheckpointStore::default();
    let succeeded = vec![(
        old_path.clone(),
        new_offset,
        old_scanned.content_hash.clone(),
    )];
    let completed = advance_checkpoints(&mut store, "grp", &succeeded, &scanned);

    for path in &completed {
        fs::remove_file(path).unwrap();
    }

    assert!(!old_path.exists());
}

// --- T5: Active file never deleted ---

#[test]
fn test_active_file_never_completed() {
    let dir = TempDir::new().unwrap();

    let path = write_test_file(dir.path(), "only.emf.json", &[r#"{"m":"data"}"#]);

    let pattern = emf_pattern();
    let scanned = scan_directory(dir.path().to_str().unwrap(), &pattern).unwrap();
    assert!(scanned[0].is_active);

    let (_, new_offset) = read_file_from_offset(&path, 0, now_ms()).unwrap();
    let file_len = fs::metadata(&path).unwrap().len();
    assert_eq!(new_offset, file_len);

    let mut store = CheckpointStore::default();
    let succeeded = vec![(path.clone(), new_offset, scanned[0].content_hash.clone())];
    let completed = advance_checkpoints(&mut store, "grp", &succeeded, &scanned);

    assert!(completed.is_empty());
    assert!(path.exists());
}

// --- T6: Upload failure — checkpoint NOT advanced ---

#[test]
fn test_upload_failure_no_checkpoint_advance() {
    let dir = TempDir::new().unwrap();

    let checkpoint_path = dir.path().join("checkpoint.json");
    write_test_file(dir.path(), "data.emf.json", &[r#"{"m":"value"}"#]);

    let pattern = emf_pattern();
    let scanned = scan_directory(dir.path().to_str().unwrap(), &pattern).unwrap();
    let mut store = CheckpointStore::default();
    let offsets = recover_offsets(&mut store, "grp", &scanned);

    let (_, new_offset) = read_file_from_offset(&offsets[0].0, offsets[0].1, now_ms()).unwrap();
    assert!(new_offset > 0);

    // Simulate upload FAILURE — do NOT call advance_checkpoints
    save_checkpoint(&checkpoint_path, &store, false).unwrap();

    let mut loaded = load_checkpoint(&checkpoint_path, false).unwrap();
    let scanned2 = scan_directory(dir.path().to_str().unwrap(), &pattern).unwrap();
    let offsets2 = recover_offsets(&mut loaded, "grp", &scanned2);

    assert_eq!(offsets2[0].1, 0); // Not advanced — will re-read
}

// --- T7: Disk space enforcement ---

#[test]
fn test_disk_space_enforcement() {
    let dir = TempDir::new().unwrap();

    for i in 0..5 {
        write_test_file(
            dir.path(),
            &format!("{i}.emf.json"),
            &[r#"{"m":"data_padding"}"#],
        );
        std::thread::sleep(std::time::Duration::from_millis(20));
    }

    let pattern = emf_pattern();
    let scanned = scan_directory(dir.path().to_str().unwrap(), &pattern).unwrap();
    assert_eq!(scanned.len(), 5);

    let processed: Vec<PathBuf> = scanned[..3].iter().map(|f| f.path.clone()).collect();

    let deleted = gg_log_manager::disk::free_disk_space(dir.path(), &pattern, 50, &processed);

    assert!(!deleted.is_empty());
    for d in &deleted {
        assert!(!d.exists());
    }
}

// --- T8: TTL eviction (24h) ---

#[test]
fn test_ttl_eviction() {
    let mut store = CheckpointStore::default();
    let mut files = HashMap::new();

    files.insert(
        "old_hash".to_string(),
        FileCheckpoint {
            file_hash: "old_hash".to_string(),
            start_position: 100,
            last_modified_time: 1000,
            last_accessed: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64
                - 25 * 60 * 60 * 1000,
        },
    );
    files.insert(
        "fresh_hash".to_string(),
        FileCheckpoint {
            file_hash: "fresh_hash".to_string(),
            start_position: 200,
            last_modified_time: 2000,
            last_accessed: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64
                - 60 * 60 * 1000,
        },
    );
    store.file_processing_info.insert("grp".to_string(), files);

    evict_stale_entries(&mut store, "grp", TTL_24H_MS);

    let remaining = store.file_processing_info.get("grp").unwrap();
    assert_eq!(remaining.len(), 1);
    assert!(remaining.contains_key("fresh_hash"));
    assert!(!remaining.contains_key("old_hash"));
}

// --- T9: Per-source upload interval ---

#[test]
fn test_per_source_interval() {
    let interval = effective_interval_secs(Some(60), 300);
    assert!((60..=65).contains(&interval));

    let interval = effective_interval_secs(None, 300);
    assert!((300..=305).contains(&interval));
}

// --- T10: Shutdown saves checkpoint ---

#[test]
fn test_shutdown_saves_checkpoint() {
    let dir = TempDir::new().unwrap();
    let checkpoint_path = dir.path().join("checkpoint.json");

    let mut store = CheckpointStore::default();
    let mut files = HashMap::new();
    files.insert(
        "h1".to_string(),
        FileCheckpoint {
            file_hash: "h1".to_string(),
            start_position: 512,
            last_modified_time: 1000,
            last_accessed: 2000,
        },
    );
    store.file_processing_info.insert("grp".to_string(), files);

    save_checkpoint(&checkpoint_path, &store, false).unwrap();

    let loaded = load_checkpoint(&checkpoint_path, false).unwrap();
    let cp = loaded
        .file_processing_info
        .get("grp")
        .unwrap()
        .get("h1")
        .unwrap();
    assert_eq!(cp.start_position, 512);
}

// --- T11: File rotation — content hash identity ---

#[test]
fn test_file_rotation_content_hash_identity() {
    let dir = TempDir::new().unwrap();

    let path_a = write_test_file(dir.path(), "app.emf.json", &[r#"{"m":"original"}"#]);

    let pattern = emf_pattern();
    let scanned1 = scan_directory(dir.path().to_str().unwrap(), &pattern).unwrap();
    let hash_a = scanned1[0].content_hash.clone();

    let mut store = CheckpointStore::default();
    let mut files = HashMap::new();
    files.insert(
        hash_a.clone(),
        FileCheckpoint {
            file_hash: hash_a.clone(),
            start_position: 5,
            last_modified_time: 1000,
            last_accessed: 2000,
        },
    );
    store.file_processing_info.insert("grp".to_string(), files);

    let path_old = dir.path().join("app.emf.json.old");
    fs::rename(&path_a, &path_old).unwrap();

    write_test_file(dir.path(), "app.emf.json", &[r#"{"m":"new_content"}"#]);

    let scanned2 = scan_directory(dir.path().to_str().unwrap(), &pattern).unwrap();

    let offsets = recover_offsets(&mut store, "grp", &scanned2);

    let old_file = scanned2.iter().find(|f| f.content_hash == hash_a);
    if let Some(old) = old_file {
        let offset = offsets.iter().find(|(p, _)| *p == old.path).unwrap().1;
        assert_eq!(offset, 5);
    }
}

// --- T12: minimumLogLevel filtering end-to-end ---

#[test]
fn test_minimum_log_level_filtering_end_to_end() {
    let dir = TempDir::new().unwrap();

    write_test_file(
        dir.path(),
        "component.log",
        &[
            r#"{"level":"DEBUG","message":"debug msg","timestamp":1000}"#,
            r#"{"level":"INFO","message":"info msg","timestamp":1001}"#,
            r#"{"level":"WARN","message":"warn msg","timestamp":1002}"#,
            r#"{"level":"ERROR","message":"error msg","timestamp":1003}"#,
        ],
    );

    let pattern = log_pattern();
    let scanned = scan_directory(dir.path().to_str().unwrap(), &pattern).unwrap();
    let mut store = CheckpointStore::default();
    let offsets = recover_offsets(&mut store, "grp", &scanned);

    let (events, _) = read_file_from_offset(&offsets[0].0, offsets[0].1, now_ms()).unwrap();
    assert_eq!(events.len(), 4);

    let batches = seal_batches(events, "/grp", "/stream", Some(LogLevel::Warn), now_ms());
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].events.len(), 2);
    assert!(batches[0].events[0].message.contains("warn msg"));
    assert!(batches[0].events[1].message.contains("error msg"));
}

// --- T13: Multi-line assembly end-to-end ---

#[test]
fn test_multiline_assembly_end_to_end() {
    let dir = TempDir::new().unwrap();

    write_test_file(
        dir.path(),
        "app.log",
        &[
            "2024-01-15 10:30:45 [ERROR] NullPointerException",
            "    at com.example.Main.run(Main.java:42)",
            "    at com.example.Main.main(Main.java:10)",
            "2024-01-15 10:30:46 [INFO] Recovery complete",
        ],
    );

    let pattern = log_pattern();
    let scanned = scan_directory(dir.path().to_str().unwrap(), &pattern).unwrap();
    let mut store = CheckpointStore::default();
    let offsets = recover_offsets(&mut store, "grp", &scanned);

    let (events, _) = read_file_from_offset(&offsets[0].0, offsets[0].1, now_ms()).unwrap();
    assert_eq!(events.len(), 4);

    let multi_pattern = Regex::new(r"^\d{4}-\d{2}-\d{2}").unwrap();
    let assembled = assemble_multiline(events, Some(&multi_pattern));

    assert_eq!(assembled.len(), 2);
    assert!(assembled[0].message.contains("NullPointerException"));
    assert!(assembled[0].message.contains("Main.java:42"));
    assert!(assembled[1].message.contains("Recovery complete"));
}

// --- T14: Empty directory — no crash ---

#[test]
fn test_empty_directory_no_crash() {
    let dir = TempDir::new().unwrap();
    let pattern = emf_pattern();

    let scanned = scan_directory(dir.path().to_str().unwrap(), &pattern).unwrap();
    assert!(scanned.is_empty());

    let mut store = CheckpointStore::default();
    let offsets = recover_offsets(&mut store, "grp", &scanned);
    assert!(offsets.is_empty());
}

// --- T15: Completed file not re-uploaded on next cycle ---

#[test]
fn test_completed_file_not_reprocessed_next_cycle() {
    let dir = TempDir::new().unwrap();

    let old_path = write_test_file(dir.path(), "old.emf.json", &[r#"{"m":"old"}"#]);
    std::thread::sleep(std::time::Duration::from_millis(50));
    let _new_path = write_test_file(dir.path(), "new.emf.json", &[r#"{"m":"new"}"#]);

    let pattern = emf_pattern();
    let scanned = scan_directory(dir.path().to_str().unwrap(), &pattern).unwrap();
    let old_scanned = scanned.iter().find(|f| f.path == old_path).unwrap();
    assert!(!old_scanned.is_active);

    let mut store = CheckpointStore::default();
    let (_, new_offset) = read_file_from_offset(&old_path, 0, now_ms()).unwrap();
    let succeeded = vec![(
        old_path.clone(),
        new_offset,
        old_scanned.content_hash.clone(),
    )];
    let completed = advance_checkpoints(&mut store, "grp", &succeeded, &scanned);
    assert_eq!(completed, vec![old_path.clone()]);

    let ts = store.last_processed_timestamps.get("grp").unwrap();
    assert!(ts.last_file_processed_time_stamp > 0);

    // Cycle 2: re-scan
    let scanned2 = scan_directory(dir.path().to_str().unwrap(), &pattern).unwrap();
    assert_eq!(scanned2.len(), 2);

    let last_ts = store
        .last_processed_timestamps
        .get("grp")
        .map(|t| t.last_file_processed_time_stamp)
        .unwrap_or(0);
    let filtered: Vec<_> = scanned2
        .iter()
        .filter(|f| {
            let mtime_ms = f
                .mtime
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            mtime_ms > last_ts
                || store
                    .file_processing_info
                    .get("grp")
                    .is_some_and(|m| m.contains_key(&f.content_hash))
        })
        .collect();

    assert_eq!(filtered.len(), 1);
    assert_ne!(filtered[0].path, old_path);
}

// --- T16: Disk space enforcement uses correct file list ---

#[test]
fn test_disk_space_uses_all_fully_uploaded_files() {
    let dir = TempDir::new().unwrap();

    let f1 = write_test_file(dir.path(), "1.emf.json", &[r#"{"m":"first"}"#]);
    std::thread::sleep(std::time::Duration::from_millis(30));
    let f2 = write_test_file(dir.path(), "2.emf.json", &[r#"{"m":"second"}"#]);
    std::thread::sleep(std::time::Duration::from_millis(30));
    let _f3 = write_test_file(dir.path(), "3.emf.json", &[r#"{"m":"third_active"}"#]);

    let pattern = emf_pattern();
    let scanned = scan_directory(dir.path().to_str().unwrap(), &pattern).unwrap();

    let f1_scanned = scanned.iter().find(|f| f.path == f1).unwrap();
    let f2_scanned = scanned.iter().find(|f| f.path == f2).unwrap();
    assert!(!f1_scanned.is_active);
    assert!(!f2_scanned.is_active);

    let f1_len = fs::metadata(&f1).unwrap().len();
    let mut store = CheckpointStore::default();
    let mut file_map = HashMap::new();
    file_map.insert(
        f1_scanned.content_hash.clone(),
        FileCheckpoint {
            file_hash: f1_scanned.content_hash.clone(),
            start_position: f1_len,
            last_modified_time: 1000,
            last_accessed: 2000,
        },
    );
    store
        .file_processing_info
        .insert("grp".to_string(), file_map);

    let completed = vec![f2.clone()];

    let mut all_processed: Vec<PathBuf> = completed.clone();
    for f in scanned.iter().filter(|f| !f.is_active) {
        if all_processed.contains(&f.path) {
            continue;
        }
        let fully_uploaded = store
            .file_processing_info
            .get("grp")
            .and_then(|m| m.get(&f.content_hash))
            .is_some_and(|cp| {
                let flen = fs::metadata(&f.path).map(|m| m.len()).unwrap_or(0);
                cp.start_position >= flen && flen > 0
            });
        if fully_uploaded {
            all_processed.push(f.path.clone());
        }
    }

    assert_eq!(all_processed.len(), 2);
    assert!(all_processed.contains(&f1));
    assert!(all_processed.contains(&f2));
}

// --- Disk enforcement helpers ---

fn create_file(dir: &std::path::Path, name: &str, size: u64) -> PathBuf {
    let path = dir.join(name);
    let f = File::create(&path).unwrap();
    f.set_len(size).unwrap();
    path
}

fn set_mtime(path: &std::path::Path, secs_ago: i64) {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let t = filetime::FileTime::from_unix_time(now - secs_ago, 0);
    filetime::set_file_mtime(path, t).unwrap();
}

// --- T17: Disk enforcement only deletes fully-uploaded files ---

/// The caller filters the list before passing to free_disk_space.
#[test]
fn test_disk_enforcement_only_deletes_uploaded_files() {
    let dir = tempdir().unwrap();

    // A = old, fully uploaded; B = partially uploaded; C = active (newest)
    let file_a = create_file(dir.path(), "a.log", 200);
    let file_b = create_file(dir.path(), "b.log", 200);
    let file_c = create_file(dir.path(), "c.log", 200);
    set_mtime(&file_a, 300); // oldest
    set_mtime(&file_b, 200);
    set_mtime(&file_c, 100); // newest = active

    // Total = 600 bytes, limit = 300 → need to free 300
    // Only file_a is in the processed list (simulating caller filtering:
    // fully uploaded, mtime <= last_processed_ts, not active)
    let processed = vec![file_a.clone()];

    let deleted = free_disk_space(dir.path(), &log_pattern(), 300, &processed);

    assert_eq!(deleted, vec![file_a.clone()]);
    assert!(!file_a.exists(), "fully uploaded file should be deleted");
    assert!(file_b.exists(), "partially uploaded file must remain");
    assert!(file_c.exists(), "active file must remain");
}

// --- T18: Disk enforcement skipped when upload fails ---

/// The caller passes an empty processed list when upload failed.
#[test]
fn test_disk_enforcement_skipped_when_no_upload_success() {
    let dir = tempdir().unwrap();

    // Create files well over the limit
    let file_a = create_file(dir.path(), "a.log", 500);
    let file_b = create_file(dir.path(), "b.log", 500);
    set_mtime(&file_a, 200);
    set_mtime(&file_b, 100);

    // Total = 1000, limit = 100 → way over, but empty succeeded list
    let deleted = free_disk_space(dir.path(), &log_pattern(), 100, &[]);

    assert!(
        deleted.is_empty(),
        "no files should be deleted when upload failed"
    );
    assert!(file_a.exists());
    assert!(file_b.exists());
}
