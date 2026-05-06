// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! File scanner subsystem - directory scanning, file reading, rotation detection

mod checkpoint;
mod multiline;
mod reader;

pub use checkpoint::{
    load_checkpoint, recover_offsets, remove_component, save_checkpoint, trim_stale_on_load,
    CheckpointStore, FileCheckpoint,
};
pub use multiline::assemble_multiline;
pub use reader::{compute_content_hash, read_file_from_offset, LogEvent};

use regex::Regex;
use std::fs;
use std::path::PathBuf;
use std::time::SystemTime;

/// Represents a scanned log file with metadata
#[derive(Debug, Clone)]
pub struct ScannedFile {
    pub path: PathBuf,
    pub mtime: SystemTime,
    pub content_hash: String,
    pub is_active: bool,
}

/// Scan a directory for log files matching the given regex pattern.
/// Returns files sorted by mtime ascending, with the newest marked as active.
/// TODO: Optimize to skip hashing files that haven't changed since last scan (cache by path+mtime)
pub fn scan_directory(directory: &str, pattern: &Regex) -> std::io::Result<Vec<ScannedFile>> {
    tracing::info!(directory = %directory, "Starting directory scan");
    let dir_entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(e) => {
            tracing::debug!(directory = %directory, error = %e, "Unable to read the directory");
            return Ok(vec![]);
        }
    };
    let mut files: Vec<(PathBuf, SystemTime)> = dir_entries
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| {
            let path = entry.path();
            let file_type = entry.file_type().ok()?;
            if file_type.is_symlink() {
                tracing::warn!(path = %path.display(), "Skipping symlink");
                return None;
            }
            if file_type.is_file() {
                let name = path.file_name()?.to_str()?;
                if pattern.is_match(name) {
                    let mtime = entry.metadata().ok()?.modified().ok()?;
                    tracing::debug!(file = %path.display(), "Found matching file");
                    return Some((path, mtime));
                }
            }
            None
        })
        .collect();

    files.sort_by_key(|(_, mtime)| *mtime);

    let mut result: Vec<ScannedFile> = files
        .into_iter()
        .filter_map(|(path, mtime)| match compute_content_hash(&path) {
            Ok(content_hash) => Some(ScannedFile {
                path,
                mtime,
                content_hash,
                is_active: false,
            }),
            Err(e) => {
                tracing::debug!(file = %path.display(), error = %e, "Skipping file during scan");
                None
            }
        })
        .collect();

    // The last file by mtime (already sorted) is the active file
    if let Some(last) = result.last_mut() {
        last.is_active = true;
    }

    tracing::info!(file_count = result.len(), "Directory scan complete");
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use std::thread::sleep;
    use std::time::Duration;

    fn create_test_file(dir: &std::path::Path, name: &str, content: &[u8]) -> PathBuf {
        let path = dir.join(name);
        let mut file = File::create(&path).unwrap();
        file.write_all(content).unwrap();
        path
    }

    #[test]
    fn test_scan_directory_empty() {
        let dir = tempfile::tempdir().unwrap();
        let pattern = Regex::new(r".*\.log$").unwrap();
        let result = scan_directory(dir.path().to_str().unwrap(), &pattern).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_scan_directory_filters_by_regex() {
        let dir = tempfile::tempdir().unwrap();
        create_test_file(dir.path(), "app.log", b"log content");
        create_test_file(dir.path(), "app.txt", b"txt content");
        create_test_file(dir.path(), "other.log", b"other log");

        let pattern = Regex::new(r".*\.log$").unwrap();
        let result = scan_directory(dir.path().to_str().unwrap(), &pattern).unwrap();

        assert_eq!(result.len(), 2);
        assert!(result.iter().all(|f| f.path.extension().unwrap() == "log"));
    }

    #[test]
    fn test_scan_directory_sorted_by_mtime_newest_is_active() {
        let dir = tempfile::tempdir().unwrap();

        create_test_file(dir.path(), "old.log", b"old");
        sleep(Duration::from_millis(50));
        create_test_file(dir.path(), "mid.log", b"mid");
        sleep(Duration::from_millis(50));
        create_test_file(dir.path(), "new.log", b"new");

        let pattern = Regex::new(r".*\.log$").unwrap();
        let result = scan_directory(dir.path().to_str().unwrap(), &pattern).unwrap();

        assert_eq!(result.len(), 3);
        // Sorted by mtime ascending
        assert!(result[0].mtime <= result[1].mtime);
        assert!(result[1].mtime <= result[2].mtime);
        // Only newest is active
        assert!(!result[0].is_active);
        assert!(!result[1].is_active);
        assert!(result[2].is_active);
    }

    #[test]
    fn test_scan_directory_single_file_is_active() {
        let dir = tempfile::tempdir().unwrap();
        create_test_file(dir.path(), "only.log", b"content");

        let pattern = Regex::new(r".*\.log$").unwrap();
        let result = scan_directory(dir.path().to_str().unwrap(), &pattern).unwrap();

        assert_eq!(result.len(), 1);
        assert!(result[0].is_active);
    }

    #[test]
    fn test_scan_directory_content_hash() {
        let dir = tempfile::tempdir().unwrap();
        create_test_file(dir.path(), "test.log", b"test content");

        let pattern = Regex::new(r".*\.log$").unwrap();
        let result = scan_directory(dir.path().to_str().unwrap(), &pattern).unwrap();

        assert_eq!(result.len(), 1);
        assert!(!result[0].content_hash.is_empty());
        assert_eq!(result[0].content_hash.len(), 44); // Base64 of SHA-256 is 44 chars
    }

    #[test]
    fn test_scan_directory_nonexistent_returns_empty() {
        let pattern = Regex::new(r".*\.log$").unwrap();
        let result = scan_directory("/nonexistent/path/12345", &pattern).unwrap();
        assert!(result.is_empty());
    }
}
