// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for deprecated checkpoint format backward compatibility.

use gg_log_manager::scanner::{
    compute_content_hash, load_checkpoint, read_file_from_offset, recover_offsets, save_checkpoint,
    scan_directory,
};
use regex::Regex;
use std::fs::{self, File};
use std::io::Write;
use std::path::PathBuf;
use tempfile::TempDir;

fn log_pattern() -> Regex {
    Regex::new(r".*\.log$").unwrap()
}

fn write_test_file(dir: &std::path::Path, name: &str, lines: &[&str]) -> PathBuf {
    let path = dir.join(name);
    let mut f = File::create(&path).unwrap();
    for line in lines {
        writeln!(f, "{}", line).unwrap();
    }
    path
}

/// Full pipeline: deprecated checkpoint → load → recover_offsets → correct resume offset.

#[test]
fn test_deprecated_checkpoint_resumes_from_correct_offset() {
    let dir = TempDir::new().unwrap();
    let checkpoint_path = dir.path().join("checkpoint.json");

    // Create a log file with known content
    let log_path = write_test_file(
        dir.path(),
        "app.log",
        &[
            "line1: first log entry here",
            "line2: second log entry here",
            "line3: third log entry here",
        ],
    );

    // Scan to get the content hash that the scanner would compute
    let pattern = log_pattern();
    let scanned = scan_directory(dir.path().to_str().unwrap(), &pattern).unwrap();
    assert_eq!(scanned.len(), 1);
    let file_hash = scanned[0].content_hash.clone();

    // Determine a midpoint offset (simulate partial upload)
    let file_len = fs::metadata(&log_path).unwrap().len();
    let partial_offset = file_len / 2;
    assert!(partial_offset > 0);

    // Write a deprecated-format checkpoint that says this file was read up to partial_offset
    let deprecated_json = format!(
        r#"{{
            "currentComponentFileProcessingInformation": {{
                "my-component": {{
                    "currentProcessingFileName": "{}",
                    "currentProcessingFileHash": "{}",
                    "currentProcessingFileStartPosition": {},
                    "currentProcessingFileLastModified": 1700000000000,
                    "lastAccessed": 1700000001000
                }}
            }}
        }}"#,
        log_path.display(),
        file_hash,
        partial_offset
    );
    fs::write(&checkpoint_path, deprecated_json).unwrap();

    // Load with deprecated version support — deprecated entry merges into current format
    let mut store = load_checkpoint(&checkpoint_path, true).unwrap();
    let files = store.file_processing_info.get("my-component").unwrap();
    let entry = files.get(&file_hash).unwrap();
    assert_eq!(entry.start_position, partial_offset);

    // Recover offsets using the loaded checkpoint — this is the full pipeline
    let offsets = recover_offsets(&mut store, "my-component", &scanned);
    assert_eq!(offsets.len(), 1);
    assert_eq!(
        offsets[0].1, partial_offset,
        "Offset must resume from deprecated checkpoint position, not 0"
    );

    // Read from the recovered offset — only gets remaining content
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let (events, new_offset) = read_file_from_offset(&offsets[0].0, offsets[0].1, now_ms).unwrap();

    // Should NOT have all 3 lines — only the tail after partial_offset
    assert!(events.len() < 3, "Should not re-read from beginning");
    assert_eq!(new_offset, file_len, "Should read to end of file");
}

/// Save roundtrip: load deprecated → modify → save → verify both format keys exist → reload.
#[test]
fn test_deprecated_checkpoint_save_roundtrip() {
    let dir = TempDir::new().unwrap();
    let checkpoint_path = dir.path().join("checkpoint.json");

    // Create a log file and get its hash
    let log_path = write_test_file(dir.path(), "service.log", &["startup message logged"]);
    let file_hash = compute_content_hash(&log_path).unwrap();

    // Write deprecated-format checkpoint
    let deprecated_json = format!(
        r#"{{
            "currentComponentFileProcessingInformation": {{
                "svc-component": {{
                    "currentProcessingFileName": "{}",
                    "currentProcessingFileHash": "{}",
                    "currentProcessingFileStartPosition": 10,
                    "currentProcessingFileLastModified": 1700000000000,
                    "lastAccessed": 1700000001000
                }}
            }}
        }}"#,
        log_path.display(),
        file_hash
    );
    fs::write(&checkpoint_path, deprecated_json).unwrap();

    // Load deprecated checkpoint
    let mut store = load_checkpoint(&checkpoint_path, true).unwrap();

    // Advance the offset (simulating upload progress)
    let files = store.file_processing_info.get_mut("svc-component").unwrap();
    files.get_mut(&file_hash).unwrap().start_position = 42;

    // Save with deprecated version support
    save_checkpoint(&checkpoint_path, &store, true).unwrap();

    // Read raw JSON and verify BOTH keys exist
    let content = fs::read_to_string(&checkpoint_path).unwrap();
    assert!(
        content.contains("currentComponentFileProcessingInformationV2"),
        "Current format key must be present after save"
    );
    assert!(
        content.contains("\"currentComponentFileProcessingInformation\""),
        "Deprecated format key must be present for backward compat"
    );

    // Reload and verify data survived the roundtrip
    let reloaded = load_checkpoint(&checkpoint_path, true).unwrap();
    let entry = reloaded
        .file_processing_info
        .get("svc-component")
        .unwrap()
        .get(&file_hash)
        .unwrap();
    assert_eq!(
        entry.start_position, 42,
        "Advanced offset must survive save/load roundtrip"
    );
}
