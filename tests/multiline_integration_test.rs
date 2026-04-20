// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for multiline log assembly

use gg_log_manager::scanner::{assemble_multiline, read_file_from_offset};
use regex::Regex;
use std::io::Write;
use tempfile::NamedTempFile;

#[test]
fn test_multiline_with_scanner() {
    let mut file = NamedTempFile::new().unwrap();
    writeln!(file, "2024-01-15T10:00:00 ERROR NullPointerException").unwrap();
    writeln!(file, "    at com.example.App.main(App.java:42)").unwrap();
    writeln!(file, "    at java.lang.Thread.run(Thread.java:750)").unwrap();
    writeln!(file, "2024-01-15T10:00:01 INFO Recovery complete").unwrap();

    let (lines, _) = read_file_from_offset(file.path(), 0, 1000).unwrap();

    let pattern = Regex::new(r"^\d{4}-\d{2}-\d{2}").unwrap();
    let events = assemble_multiline(lines, Some(&pattern));

    assert_eq!(events.len(), 2);
    assert!(events[0].message.contains("NullPointerException"));
    assert!(events[0].message.contains("App.java:42"));
    assert!(events[0].message.contains("Thread.java:750"));
    assert_eq!(
        events[1].message,
        "2024-01-15T10:00:01 INFO Recovery complete"
    );
}

#[test]
fn test_emf_single_line_passthrough() {
    let mut file = NamedTempFile::new().unwrap();
    writeln!(
        file,
        r#"{{"_aws":{{"Timestamp":1705314645000}},"metric":1}}"#
    )
    .unwrap();
    writeln!(
        file,
        r#"{{"_aws":{{"Timestamp":1705314646000}},"metric":2}}"#
    )
    .unwrap();

    let (lines, _) = read_file_from_offset(file.path(), 0, 1000).unwrap();

    let events = assemble_multiline(lines, None);

    assert_eq!(events.len(), 2);
    assert!(events[0].message.contains(r#""metric":1"#));
    assert!(events[1].message.contains(r#""metric":2"#));
    // Timestamp comes from LogEvent.timestamp (set by reader's default_timestamp), not from assemble_multiline
    assert_eq!(events[0].timestamp, 1000);
    assert_eq!(events[1].timestamp, 1000);
}
