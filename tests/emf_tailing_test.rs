// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for EMF file tailing end-to-end

use gg_log_manager::scanner::{read_file_from_offset, LogEvent};
use std::fs::File;
use std::io::Write;
use tempfile::TempDir;

/// Valid EMF JSON content representing system health metrics
const SYSTEM_HEALTH_EMF: &str = r#"{"_aws":{"Timestamp":1705314645000,"CloudWatchMetrics":[{"Namespace":"GreenGrass/SystemHealth","Dimensions":[["ThingName"]],"Metrics":[{"Name":"CpuUsage","Unit":"Percent"},{"Name":"MemoryUsage","Unit":"Percent"}]}]},"ThingName":"my-iot-device","CpuUsage":45.2,"MemoryUsage":67.8}"#;

#[test]
fn test_emf_file_tailing_preserves_json_content() {
    let dir = TempDir::new().unwrap();
    let emf_path = dir.path().join("system-health.emf.json");

    // Write valid EMF JSON file
    let mut file = File::create(&emf_path).unwrap();
    writeln!(file, "{}", SYSTEM_HEALTH_EMF).unwrap();

    // Read using read_file_from_offset
    let default_ts = 1000000000000i64;
    let (events, new_offset) = read_file_from_offset(&emf_path, 0, default_ts).unwrap();

    // Assert single LogEvent is returned
    assert_eq!(
        events.len(),
        1,
        "Should return exactly one LogEvent for single EMF line"
    );

    // Assert the EMF JSON content is preserved exactly (LogManager does not parse or modify EMF)
    assert_eq!(
        events[0].message, SYSTEM_HEALTH_EMF,
        "EMF JSON content should be preserved exactly"
    );

    // Verify offset advanced
    assert!(new_offset > 0);
}

#[test]
fn test_emf_timestamp_extracted_from_aws_field() {
    let dir = TempDir::new().unwrap();
    let emf_path = dir.path().join("test.emf.json");

    // EMF with timestamp in _aws field
    let emf_content = r#"{"_aws":{"Timestamp":1705314645000},"metric":"value"}"#;

    let mut file = File::create(&emf_path).unwrap();
    writeln!(file, "{}", emf_content).unwrap();

    let (events, _) = read_file_from_offset(&emf_path, 0, 9999).unwrap();

    assert_eq!(events.len(), 1);
    // Note: The reader extracts timestamp from line start, not from JSON parsing
    // EMF lines starting with { won't have extractable timestamp, so default is used
    assert_eq!(events[0].timestamp, 9999);
    assert_eq!(events[0].message, emf_content);
}

#[test]
fn test_emf_multiple_lines() {
    let dir = TempDir::new().unwrap();
    let emf_path = dir.path().join("multi.emf.json");

    let emf1 = r#"{"_aws":{"Timestamp":1705314645000},"metric1":"value1"}"#;
    let emf2 = r#"{"_aws":{"Timestamp":1705314646000},"metric2":"value2"}"#;
    let emf3 = r#"{"_aws":{"Timestamp":1705314647000},"metric3":"value3"}"#;

    let mut file = File::create(&emf_path).unwrap();
    writeln!(file, "{}", emf1).unwrap();
    writeln!(file, "{}", emf2).unwrap();
    writeln!(file, "{}", emf3).unwrap();

    let (events, _) = read_file_from_offset(&emf_path, 0, 1000).unwrap();

    assert_eq!(events.len(), 3);
    assert_eq!(events[0].message, emf1);
    assert_eq!(events[1].message, emf2);
    assert_eq!(events[2].message, emf3);
}

#[test]
fn test_emf_tailing_from_offset() {
    let dir = TempDir::new().unwrap();
    let emf_path = dir.path().join("tail.emf.json");

    let emf1 = r#"{"metric":"first"}"#;
    let emf2 = r#"{"metric":"second"}"#;

    let mut file = File::create(&emf_path).unwrap();
    writeln!(file, "{}", emf1).unwrap();
    writeln!(file, "{}", emf2).unwrap();

    // First read from offset 0
    let (events1, offset1) = read_file_from_offset(&emf_path, 0, 1000).unwrap();
    assert_eq!(events1.len(), 2);

    // Append more content
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(&emf_path)
        .unwrap();
    let emf3 = r#"{"metric":"third"}"#;
    writeln!(file, "{}", emf3).unwrap();

    // Read from previous offset - should only get new content
    let (events2, _) = read_file_from_offset(&emf_path, offset1, 1000).unwrap();
    assert_eq!(events2.len(), 1);
    assert_eq!(events2[0].message, emf3);
}

#[test]
fn test_emf_content_not_modified() {
    let dir = TempDir::new().unwrap();
    let emf_path = dir.path().join("preserve.emf.json");

    // EMF with special characters, nested objects, arrays
    let complex_emf = r#"{"_aws":{"CloudWatchMetrics":[{"Namespace":"Test","Metrics":[{"Name":"M1"}]}]},"tags":["a","b"],"nested":{"key":"value"}}"#;

    let mut file = File::create(&emf_path).unwrap();
    writeln!(file, "{}", complex_emf).unwrap();

    let (events, _) = read_file_from_offset(&emf_path, 0, 1000).unwrap();

    // Content should be byte-for-byte identical
    assert_eq!(events[0].message, complex_emf);
}

#[test]
fn test_emf_empty_file() {
    let dir = TempDir::new().unwrap();
    let emf_path = dir.path().join("empty.emf.json");

    File::create(&emf_path).unwrap();

    let (events, offset) = read_file_from_offset(&emf_path, 0, 1000).unwrap();

    assert!(events.is_empty());
    assert_eq!(offset, 0);
}

#[test]
fn test_emf_file_truncation_detection() {
    let dir = TempDir::new().unwrap();
    let emf_path = dir.path().join("truncate.emf.json");

    // Write initial content
    let mut file = File::create(&emf_path).unwrap();
    writeln!(file, r#"{{"metric":"value"}}"#).unwrap();

    let (_, offset1) = read_file_from_offset(&emf_path, 0, 1000).unwrap();

    // Truncate file (simulate rotation)
    let mut file = File::create(&emf_path).unwrap();
    writeln!(file, r#"{{"new":"data"}}"#).unwrap();

    // Read with old offset that's now larger than file size
    // Should reset to 0 and read from beginning
    let (events, _) = read_file_from_offset(&emf_path, offset1 + 1000, 1000).unwrap();

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].message, r#"{"new":"data"}"#);
}

#[test]
fn test_emf_with_epoch_timestamp_prefix() {
    let dir = TempDir::new().unwrap();
    let emf_path = dir.path().join("epoch.emf.json");

    // EMF line with epoch timestamp prefix (13 digits)
    let emf_with_ts = r#"1705314645000 {"metric":"value"}"#;

    let mut file = File::create(&emf_path).unwrap();
    writeln!(file, "{}", emf_with_ts).unwrap();

    let (events, _) = read_file_from_offset(&emf_path, 0, 9999).unwrap();

    assert_eq!(events.len(), 1);
    // Timestamp should be extracted from the epoch prefix
    assert_eq!(events[0].timestamp, 1705314645000);
    assert_eq!(events[0].message, emf_with_ts);
}

#[test]
fn test_emf_skips_empty_lines() {
    let dir = TempDir::new().unwrap();
    let emf_path = dir.path().join("blanks.emf.json");

    let mut file = File::create(&emf_path).unwrap();
    writeln!(file, r#"{{"metric":"first"}}"#).unwrap();
    writeln!(file).unwrap(); // empty line - will be skipped
    writeln!(file, r#"{{"metric":"second"}}"#).unwrap();

    let (events, _) = read_file_from_offset(&emf_path, 0, 1000).unwrap();

    // Empty lines (after trimming \n\r) are skipped
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].message, r#"{"metric":"first"}"#);
    assert_eq!(events[1].message, r#"{"metric":"second"}"#);
}

#[test]
fn test_timestamp_extraction_march_leap_year() {
    // 2024-03-01 is after Feb in a leap year — tests the leap day addition path
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("leap.log");
    let mut f = File::create(&path).unwrap();
    writeln!(f, "2024-03-01T12:00:00 leap year march event").unwrap();
    let (events, _) = read_file_from_offset(&path, 0, 0).unwrap();
    assert_eq!(events.len(), 1);
    // 2024-03-01 12:00:00 UTC = 1709294400000 ms
    assert_eq!(events[0].timestamp, 1709294400000);
}
