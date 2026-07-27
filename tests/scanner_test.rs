// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for file scanning with rotation detection

use gg_log_manager::scanner::{compute_content_hash, scan_directory};
use regex::Regex;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;
use std::time::Duration;
use tempfile::TempDir;

fn create_file_with_content(dir: &Path, name: &str, content: &[u8]) {
    let path = dir.join(name);
    let mut file = File::create(&path).unwrap();
    file.write_all(content).unwrap();
}

fn set_mtime_offset(path: &Path, offset_secs: i64) {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let target = if offset_secs < 0 {
        now - Duration::from_secs((-offset_secs) as u64)
    } else {
        now + Duration::from_secs(offset_secs as u64)
    };
    let mtime = filetime::FileTime::from_unix_time(target.as_secs() as i64, 0);
    filetime::set_file_mtime(path, mtime).unwrap();
}

#[test]
fn test_scan_with_rotation_active_file_detection() {
    let dir = TempDir::new().unwrap();
    let pattern = Regex::new(r".*\.emf\.json$").unwrap();

    // Create 3 files with different mtimes
    // a.emf.json: 100 bytes, mtime T-120s (oldest)
    create_file_with_content(dir.path(), "a.emf.json", &[b'a'; 100]);
    set_mtime_offset(&dir.path().join("a.emf.json"), -120);

    // b.emf.json: 100 bytes, mtime T-60s (middle)
    create_file_with_content(dir.path(), "b.emf.json", &[b'b'; 100]);
    set_mtime_offset(&dir.path().join("b.emf.json"), -60);

    // c.emf.json: 50 bytes, mtime T-0s (newest/active)
    create_file_with_content(dir.path(), "c.emf.json", &[b'c'; 50]);
    // No offset needed - it's the newest

    let files = scan_directory(dir.path().to_str().unwrap(), &pattern)
        .unwrap()
        .files;

    assert_eq!(files.len(), 3);

    // Files should be sorted by mtime ascending
    assert!(files[0].path.ends_with("a.emf.json"));
    assert!(files[1].path.ends_with("b.emf.json"));
    assert!(files[2].path.ends_with("c.emf.json"));

    // Only the newest file (c) should be active
    assert!(!files[0].is_active, "a.emf.json should not be active");
    assert!(!files[1].is_active, "b.emf.json should not be active");
    assert!(files[2].is_active, "c.emf.json should be active");
}

#[test]
fn test_scan_rotation_content_hash_survives_rename() {
    let dir = TempDir::new().unwrap();
    let _pattern = Regex::new(r".*\.emf\.json$|.*\.old$").unwrap();

    // Create file b.emf.json with specific content
    let content_b = b"unique content for file b that we will track";
    create_file_with_content(dir.path(), "b.emf.json", content_b);

    // Get the content hash before rename
    let hash_before = compute_content_hash(&dir.path().join("b.emf.json")).unwrap();

    // Simulate rotation: rename b.emf.json to b.old
    fs::rename(dir.path().join("b.emf.json"), dir.path().join("b.old")).unwrap();

    // Get the content hash after rename
    let hash_after = compute_content_hash(&dir.path().join("b.old")).unwrap();

    // Content hash should be the same after rename
    assert_eq!(
        hash_before, hash_after,
        "Content hash should survive file rename"
    );
}

#[test]
fn test_scan_rotation_renamed_file_still_scannable() {
    let dir = TempDir::new().unwrap();

    // Create initial files
    create_file_with_content(dir.path(), "a.emf.json", &[b'a'; 100]);
    set_mtime_offset(&dir.path().join("a.emf.json"), -120);

    create_file_with_content(dir.path(), "b.emf.json", &[b'b'; 100]);
    set_mtime_offset(&dir.path().join("b.emf.json"), -60);

    create_file_with_content(dir.path(), "c.emf.json", &[b'c'; 50]);

    // First scan with pattern matching .emf.json
    let pattern1 = Regex::new(r".*\.emf\.json$").unwrap();
    let files_before = scan_directory(dir.path().to_str().unwrap(), &pattern1)
        .unwrap()
        .files;
    assert_eq!(files_before.len(), 3);

    // Get hash of b before rename
    let b_hash = files_before
        .iter()
        .find(|f| f.path.ends_with("b.emf.json"))
        .unwrap()
        .content_hash
        .clone();

    // Simulate rotation: rename b.emf.json to b.old
    fs::rename(dir.path().join("b.emf.json"), dir.path().join("b.old")).unwrap();

    // Re-scan with pattern that includes .old files
    let pattern2 = Regex::new(r".*\.emf\.json$|.*\.old$").unwrap();
    let files_after = scan_directory(dir.path().to_str().unwrap(), &pattern2)
        .unwrap()
        .files;

    // Should still find 3 files (a.emf.json, b.old, c.emf.json)
    assert_eq!(files_after.len(), 3);

    // Find the renamed file and verify its hash matches
    let b_old = files_after
        .iter()
        .find(|f| f.path.ends_with("b.old"))
        .expect("Should find b.old after rename");

    assert_eq!(
        b_old.content_hash, b_hash,
        "Content hash should match after rename"
    );
}

#[test]
fn test_scan_content_hash_is_sha256() {
    let dir = TempDir::new().unwrap();
    create_file_with_content(dir.path(), "test.emf.json", b"test content");

    let pattern = Regex::new(r".*\.emf\.json$").unwrap();
    let files = scan_directory(dir.path().to_str().unwrap(), &pattern)
        .unwrap()
        .files;

    assert_eq!(files.len(), 1);
    // SHA-256 produces 64 hex characters
    assert_eq!(files[0].content_hash.len(), 44);
    // Should be valid Base64
    assert!(files[0]
        .content_hash
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '+' || c == '/' || c == '='));
}

#[test]
fn test_scan_empty_directory() {
    let dir = TempDir::new().unwrap();
    let pattern = Regex::new(r".*\.emf\.json$").unwrap();

    let files = scan_directory(dir.path().to_str().unwrap(), &pattern)
        .unwrap()
        .files;
    assert!(files.is_empty());
}

#[test]
fn test_scan_single_file_is_active() {
    let dir = TempDir::new().unwrap();
    create_file_with_content(dir.path(), "only.emf.json", b"content");

    let pattern = Regex::new(r".*\.emf\.json$").unwrap();
    let files = scan_directory(dir.path().to_str().unwrap(), &pattern)
        .unwrap()
        .files;

    assert_eq!(files.len(), 1);
    assert!(files[0].is_active, "Single file should be marked as active");
}

#[test]
fn test_scan_filters_by_regex() {
    let dir = TempDir::new().unwrap();
    create_file_with_content(dir.path(), "match.emf.json", b"content");
    create_file_with_content(dir.path(), "nomatch.txt", b"content");
    create_file_with_content(dir.path(), "another.log", b"content");

    let pattern = Regex::new(r".*\.emf\.json$").unwrap();
    let files = scan_directory(dir.path().to_str().unwrap(), &pattern)
        .unwrap()
        .files;

    assert_eq!(files.len(), 1);
    assert!(files[0].path.ends_with("match.emf.json"));
}

#[test]
fn test_scan_directory_skips_symlinks() {
    let dir = TempDir::new().unwrap();
    // Create a real file
    let real_path = dir.path().join("real.log");
    std::fs::write(&real_path, "content").unwrap();
    // Create a symlink
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(&real_path, dir.path().join("link.log")).unwrap();
    }
    let pattern = regex::Regex::new(r".*\.log$").unwrap();
    let result = gg_log_manager::scanner::scan_directory(dir.path().to_str().unwrap(), &pattern)
        .unwrap()
        .files;
    // Should only find the real file, not the symlink
    assert_eq!(result.len(), 1);
    assert!(result[0].path.ends_with("real.log"));
}
