// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for per-cycle disk enforcement over the real scan → read path.
//!
//! These drive the actual scanner and checkpoint types into `enforce_disk_limit`, covering
//! the reclaim path that the (now unconditional, every-cycle) disk enforcement enables — i.e.
//! reclaiming disk on a cycle where nothing was uploaded. The CloudWatch upload leg itself is
//! covered by the uploader unit tests and `sdk_retry_test.rs`; here nothing is uploaded (so no
//! checkpoint advances), which is exactly the outage/idle case.

use gg_log_manager::disk::enforce_disk_limit;
use gg_log_manager::scanner::{
    compute_content_hash, read_file_from_offset, recover_offsets, scan_directory, CheckpointStore,
    LogEvent, ScanDirectoryResult, ScannedFile,
};
use regex::Regex;
use std::collections::HashSet;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use tempfile::TempDir;

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// Write a file of exactly `size` bytes filled with `byte` (distinct fill bytes give distinct
/// content hashes).
fn write_sized(dir: &std::path::Path, name: &str, byte: u8, size: usize) -> PathBuf {
    let path = dir.join(name);
    let mut f = File::create(&path).unwrap();
    f.write_all(&vec![byte; size]).unwrap();
    path
}

fn set_mtime_ms(path: &std::path::Path, ms: u64) {
    filetime::set_file_mtime(
        path,
        filetime::FileTime::from_unix_time((ms / 1000) as i64, ((ms % 1000) * 1_000_000) as u32),
    )
    .unwrap();
}

/// Build the tracked-paths set exactly as `enforce_source_disk_limit` does: scanned files whose
/// content hash currently has a checkpoint entry for the group.
fn tracked_paths(
    store: &CheckpointStore,
    group: &str,
    scanned: &[ScannedFile],
) -> HashSet<PathBuf> {
    store
        .file_processing_info
        .get(group)
        .map(|m| {
            scanned
                .iter()
                .filter(|f| m.contains_key(&f.content_hash))
                .map(|f| f.path.clone())
                .collect()
        })
        .unwrap_or_default()
}

/// Full scan → read → enforce path with no upload (the outage / empty-`succeeded` case): a
/// directory over its limit is still reclaimed, oldest-first, and the active (newest) file is
/// preserved. With `last_ts = 0` every non-active file is un-uploaded, so this exercises the
/// opt-in `deleteUnuploadedFilesOnDiskPressure` phase.
#[test]
fn enforcement_reclaims_over_limit_dir_without_upload() {
    let dir = TempDir::new().unwrap();
    let old1 = write_sized(dir.path(), "old1.log", b'a', 1000);
    let old2 = write_sized(dir.path(), "old2.log", b'b', 1000);
    let active = write_sized(dir.path(), "active.log", b'c', 100);
    set_mtime_ms(&old1, 1_000);
    set_mtime_ms(&old2, 2_000);
    set_mtime_ms(&active, 9_000);

    let pattern = Regex::new(r".*\.log$").unwrap();

    // Scan → read (mirrors scan_and_filter_files + read_file_events); no CW upload occurs, so
    // nothing is "succeeded" and no checkpoint advances — the outage scenario.
    let scanned = scan_directory(dir.path().to_str().unwrap(), &pattern)
        .unwrap()
        .files;
    assert!(!scanned.is_empty(), "scan must find files");
    let mut store = CheckpointStore::default();
    let offsets = recover_offsets(&mut store, "grp", &scanned);
    for (path, offset) in &offsets {
        let _read: (Vec<LogEvent>, u64) = read_file_from_offset(path, *offset, now_ms()).unwrap();
    }

    let tracked = tracked_paths(&store, "grp", &scanned);
    // total = 2100 bytes, limit = 1500 → reclaim the oldest un-uploaded file only.
    let outcome = enforce_disk_limit(dir.path(), &pattern, 1500, 0, |p| tracked.contains(p), true);

    assert!(
        outcome.unuploaded_deleted.contains(&old1),
        "oldest file reclaimed first even though nothing was uploaded"
    );
    assert!(!old1.exists());
    assert!(old2.exists(), "only enough deleted to get under the limit");
    assert!(
        active.exists(),
        "active (newest) file must never be deleted"
    );
}

/// Content-hash dedup drops older files that share a first line with a newer file. Disk
/// enforcement must classify those dropped files as UN-UPLOADED and never reclaim them by mtime
/// alone: the pipeline never reads or uploads a deduped-away file, so treating it as
/// uploaded-safe could delete data that was never sent. `enforce_source_disk_limit` routes the
/// scan's dedup-dropped set into the enforcement guard, so a dropped file stays protected in
/// default mode even when its mtime is `<= last_ts` (i.e. when it would otherwise look
/// uploaded-safe). It is reclaimable only when the source opts into
/// `deleteUnuploadedFilesOnDiskPressure`, where it is shed as un-uploaded. This pins the
/// (best-effort) contract that, by default, only files known to be uploaded are reclaimed. The
/// active (newest) file is always preserved.
#[test]
fn dedup_collision_older_file_is_protected_by_default() {
    let dir = TempDir::new().unwrap();

    // Identical first line → identical content hash; distinct fill after the newline keeps the
    // files byte-distinct but hash-equal.
    let older = dir.path().join("older.log");
    let newer = dir.path().join("newer.log");
    {
        let mut f = File::create(&older).unwrap();
        f.write_all(b"shared first line\n").unwrap();
        f.write_all(&vec![b'a'; 982]).unwrap(); // 1000 bytes total
    }
    {
        let mut f = File::create(&newer).unwrap();
        f.write_all(b"shared first line\n").unwrap();
        f.write_all(&vec![b'b'; 982]).unwrap();
    }
    set_mtime_ms(&older, 1_000);
    set_mtime_ms(&newer, 2_000);
    assert_eq!(
        compute_content_hash(&older).unwrap(),
        compute_content_hash(&newer).unwrap(),
        "same first line must collide on content hash"
    );

    let pattern = Regex::new(r".*\.log$").unwrap();
    // Real plumbing: the scanner keeps the newest of the colliding pair and reports the older one
    // as dedup-dropped (no hand-built dropped set).
    let ScanDirectoryResult {
        files: scanned,
        dedup_dropped: dropped,
    } = scan_directory(dir.path().to_str().unwrap(), &pattern).unwrap();
    assert_eq!(scanned.len(), 1, "collision deduped to the newest file");
    assert_eq!(scanned[0].path, newer);
    assert_eq!(
        dropped,
        vec![older.clone()],
        "older colliding file is reported dedup-dropped"
    );

    // No checkpoint entries → tracked set is empty; the dropped set carries the older file.
    let store = CheckpointStore::default();
    let tracked = tracked_paths(&store, "grp", &scanned);
    let dropped_set: HashSet<PathBuf> = dropped.iter().cloned().collect();

    // total = 2000, limit = 1500, last_ts = 1500 (>= older mtime). Default pass. By mtime alone
    // the older file would be uploaded-safe; routing the dedup-dropped set into the guard (as
    // enforce_source_disk_limit does) reclassifies it as un-uploaded, so it is protected.
    let outcome = enforce_disk_limit(
        dir.path(),
        &pattern,
        1500,
        1_500,
        |p| tracked.contains(p) || dropped_set.contains(p),
        false,
    );

    assert!(
        outcome.uploaded_deleted.is_empty() && outcome.unuploaded_deleted.is_empty(),
        "dedup-dropped file is protected in default mode (not reclaimed as uploaded-safe)"
    );
    assert!(
        older.exists(),
        "deduped-away, never-uploaded file must not be reclaimed by default"
    );
    assert!(newer.exists(), "newest (active) file is always preserved");
}
