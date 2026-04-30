// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! Disk space management — per-component log directory size limits.
//! Deletes oldest already-processed (uploaded) files when total exceeds diskSpaceLimit.

use regex::Regex;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

/// Calculate total size of regular files matching `pattern` in `dir` (top-level only).
/// Skips symlinks and subdirectories.
fn dir_size(dir: &Path, pattern: &Regex) -> u64 {
    match fs::read_dir(dir) {
        Ok(entries) => entries
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().is_ok_and(|ft| ft.is_file()))
            .filter(|e| pattern.is_match(&e.file_name().to_string_lossy()))
            .filter_map(|e| e.metadata().ok())
            .map(|m| m.len())
            .sum(),
        Err(e) => {
            tracing::warn!(dir = %dir.display(), error = %e, "Cannot read directory for disk management");
            0
        }
    }
}

/// Free disk space by deleting oldest processed files until directory size ≤ limit.
///
/// Only deletes files in `processed_files` — caller must ensure these are safe to delete
/// (fully uploaded, not actively written to). This function trusts the list it receives.
///
/// Returns paths of successfully deleted files.
pub(crate) fn free_disk_space(
    dir: &Path,
    pattern: &Regex,
    limit_bytes: u64,
    processed_files: &[PathBuf],
) -> Vec<PathBuf> {
    if processed_files.is_empty() {
        return Vec::new();
    }

    let total_size = dir_size(dir, pattern);

    if total_size <= limit_bytes {
        return Vec::new();
    }

    // Cache metadata upfront to avoid double reads (once for sort, once for size).
    let mut file_info: Vec<_> = processed_files
        .iter()
        .filter_map(|p| {
            fs::metadata(p)
                .ok()
                .map(|m| (p.clone(), m.len(), m.modified().unwrap_or(SystemTime::UNIX_EPOCH)))
        })
        .collect();
    file_info.sort_by_key(|(_, _, mtime)| *mtime); // oldest first

    let mut bytes_to_free = total_size.saturating_sub(limit_bytes);
    let mut deleted = Vec::with_capacity(file_info.len());
    let mut actually_freed: u64 = 0;

    for (path, size, _) in file_info {
        if bytes_to_free == 0 {
            break;
        }
        if let Err(e) = fs::remove_file(&path) {
            tracing::warn!(path = %path.display(), error = %e, "Failed to delete processed file, skipping");
            continue;
        }
        tracing::info!(path = %path.display(), freed_bytes = size, "Deleted processed log file");
        deleted.push(path);
        actually_freed += size;
        bytes_to_free = bytes_to_free.saturating_sub(size);
    }

    if !deleted.is_empty() {
        tracing::info!(total_freed = actually_freed, files_deleted = deleted.len(), "Disk space freed");
    }

    deleted
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use tempfile::TempDir;

    fn create_file(dir: &Path, name: &str, size: u64) -> PathBuf {
        let path = dir.join(name);
        let f = File::create(&path).expect("failed to create test file");
        f.set_len(size).expect("failed to set file length");
        path
    }

    fn set_mtime(path: &Path, secs_ago: i64) {
        let now = i64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock before UNIX_EPOCH")
                .as_secs(),
        )
        .expect("timestamp overflow");
        let t = filetime::FileTime::from_unix_time(now - secs_ago, 0);
        filetime::set_file_mtime(path, t).expect("failed to set mtime");
    }

    fn log_pattern() -> Regex {
        Regex::new(r"\.log$").expect("invalid test regex")
    }

    #[test]
    fn no_op_when_under_limit() {
        let tmp = TempDir::new().unwrap();
        let f1 = create_file(tmp.path(), "a.log", 100);
        let f2 = create_file(tmp.path(), "b.log", 100);
        let processed = vec![f1, f2];

        let deleted = free_disk_space(tmp.path(), &log_pattern(), 500, &processed);
        assert!(deleted.is_empty());
        assert!(processed.iter().all(|p| p.exists()));
    }

    #[test]
    fn deletes_oldest_first_until_under_limit() {
        let tmp = TempDir::new().unwrap();
        let old = create_file(tmp.path(), "old.log", 100);
        let mid = create_file(tmp.path(), "mid.log", 100);
        let new = create_file(tmp.path(), "new.log", 100);
        set_mtime(&old, 300);
        set_mtime(&mid, 200);
        set_mtime(&new, 100);

        let processed = vec![old.clone(), mid.clone(), new.clone()];
        // total=300, limit=250 → need to free 50 → delete oldest (100 bytes)
        let deleted = free_disk_space(tmp.path(), &log_pattern(), 250, &processed);
        assert_eq!(deleted, vec![old.clone()]);
        assert!(!old.exists());
        assert!(mid.exists());
        assert!(new.exists());
    }

    #[test]
    fn only_deletes_processed_files() {
        let tmp = TempDir::new().unwrap();
        let active = create_file(tmp.path(), "active.log", 200);
        let processed = create_file(tmp.path(), "done.log", 200);
        set_mtime(&processed, 100);

        // total=400, limit=250 → need 150 freed, but only processed is deletable
        let deleted = free_disk_space(tmp.path(), &log_pattern(), 250, &[processed.clone()]);
        assert_eq!(deleted, vec![processed]);
        assert!(active.exists(), "active file must not be deleted");
    }

    #[test]
    fn handles_missing_files_gracefully() {
        let tmp = TempDir::new().unwrap();
        let exists = create_file(tmp.path(), "exists.log", 200);
        let ghost = tmp.path().join("ghost.log");
        set_mtime(&exists, 50);

        // total=200, limit=50 → need to free 150
        let deleted = free_disk_space(tmp.path(), &log_pattern(), 50, &[ghost, exists.clone()]);
        assert_eq!(deleted, vec![exists]);
    }

    #[test]
    fn empty_processed_files_is_noop() {
        let tmp = TempDir::new().unwrap();
        create_file(tmp.path(), "a.log", 1000);

        let deleted = free_disk_space(tmp.path(), &log_pattern(), 10, &[]);
        assert!(deleted.is_empty());
    }

    #[test]
    fn all_files_deleted_when_way_over_limit() {
        let tmp = TempDir::new().unwrap();
        let f1 = create_file(tmp.path(), "a.log", 500);
        let f2 = create_file(tmp.path(), "b.log", 500);
        let f3 = create_file(tmp.path(), "c.log", 500);
        set_mtime(&f1, 300);
        set_mtime(&f2, 200);
        set_mtime(&f3, 100);

        // total=1500, limit=100 → need 1400 freed → all 3 deleted
        let deleted = free_disk_space(tmp.path(), &log_pattern(), 100, &[f1.clone(), f2.clone(), f3.clone()]);
        assert_eq!(deleted.len(), 3);
        assert!(!f1.exists());
        assert!(!f2.exists());
        assert!(!f3.exists());
    }

    #[test]
    fn zero_limit_deletes_all_processed() {
        let tmp = TempDir::new().unwrap();
        let f1 = create_file(tmp.path(), "a.log", 100);
        let f2 = create_file(tmp.path(), "b.log", 200);
        set_mtime(&f1, 200);
        set_mtime(&f2, 100);

        let deleted = free_disk_space(tmp.path(), &log_pattern(), 0, &[f1.clone(), f2.clone()]);
        assert_eq!(deleted.len(), 2);
        assert!(!f1.exists());
        assert!(!f2.exists());
    }

    #[cfg(unix)]
    #[test]
    fn skips_undeletable_files() {
        use std::os::unix::fs::PermissionsExt;

        // Skip if running as root (root ignores permission bits)
        if std::process::Command::new("id")
            .arg("-u")
            .output()
            .map(|o| o.stdout.starts_with(b"0"))
            .unwrap_or(false)
        {
            return;
        }

        let tmp = TempDir::new().unwrap();
        let f1 = create_file(tmp.path(), "a.log", 200);
        let f2 = create_file(tmp.path(), "b.log", 200);
        set_mtime(&f1, 200);
        set_mtime(&f2, 100);

        // Make directory read-only so files can't be deleted
        let dir_perms = fs::Permissions::from_mode(0o555);
        fs::set_permissions(tmp.path(), dir_perms).unwrap();

        // total=400, limit=0 → wants to delete all, but can't
        let deleted = free_disk_space(tmp.path(), &log_pattern(), 0, &[f1.clone(), f2.clone()]);
        assert!(deleted.is_empty());
        assert!(f1.exists());
        assert!(f2.exists());

        // Restore permissions for cleanup
        let restore_perms = fs::Permissions::from_mode(0o755);
        fs::set_permissions(tmp.path(), restore_perms).unwrap();
    }

    #[test]
    fn pattern_filter_excludes_non_matching_files() {
        let tmp = TempDir::new().unwrap();
        // Non-log file inflates directory but shouldn't count toward limit
        create_file(tmp.path(), "config.json", 1000);
        let log_file = create_file(tmp.path(), "app.log", 100);
        set_mtime(&log_file, 100);

        // dir has 1100 bytes total, but only 100 bytes of .log files → under limit
        let deleted = free_disk_space(tmp.path(), &log_pattern(), 200, &[log_file.clone()]);
        assert!(deleted.is_empty());
        assert!(log_file.exists());
    }
}
