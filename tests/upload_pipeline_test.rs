// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

#![cfg(feature = "aws-sdk")]

//! E2E tests exercising the full upload pipeline with a mocked CW client.
//! Uses StaticReplayClient to provide canned HTTP responses.

use aws_sdk_cloudwatchlogs::{
    config::BehaviorVersion, config::Credentials, config::Region, Client,
};
use aws_smithy_http_client::test_util::{ReplayEvent, StaticReplayClient};
use aws_smithy_types::body::SdkBody;
use gg_log_manager::scanner::{
    read_file_from_offset, recover_offsets, scan_directory, CheckpointStore, LogEvent,
};
use gg_log_manager::uploader::{advance_checkpoints, upload_source_events, CwLogsClient};
use regex::Regex;
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

fn write_test_file(dir: &std::path::Path, name: &str, lines: &[&str]) -> PathBuf {
    let path = dir.join(name);
    let mut f = File::create(&path).unwrap();
    for line in lines {
        writeln!(f, "{}", line).unwrap();
    }
    path
}

fn dummy_request() -> http::Request<SdkBody> {
    http::Request::builder()
        .uri("https://logs.us-east-1.amazonaws.com/")
        .body(SdkBody::empty())
        .unwrap()
}

fn success_response() -> http::Response<SdkBody> {
    http::Response::builder()
        .status(200)
        .body(SdkBody::from(r#"{}"#))
        .unwrap()
}

fn error_response() -> http::Response<SdkBody> {
    http::Response::builder()
        .status(500)
        .body(SdkBody::from(
            r#"{"__type":"ServiceUnavailableException","message":"service down"}"#,
        ))
        .unwrap()
}

fn make_cw_client(replay_client: StaticReplayClient) -> CwLogsClient {
    let config = aws_sdk_cloudwatchlogs::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .credentials_provider(Credentials::new("test", "test", None, None, "test"))
        .region(Region::new("us-east-1"))
        .retry_config(
            aws_sdk_cloudwatchlogs::config::retry::RetryConfig::standard().with_max_attempts(1),
        )
        .http_client(replay_client)
        .build();
    let client = Client::from_conf(config.clone());
    CwLogsClient::new_with_client(client, config)
}

/// Full E2E: scan → read → upload (mocked success) → checkpoint advance
#[tokio::test]
async fn test_e2e_upload_success_advances_checkpoint() {
    let dir = TempDir::new().unwrap();

    // Write two EMF files with different mtimes
    let old_path = write_test_file(
        dir.path(),
        "old.emf.json",
        &[r#"{"_aws":{"Timestamp":1705314645000},"metric":"cpu","value":12.5}"#],
    );
    std::thread::sleep(std::time::Duration::from_millis(50));
    let _new_path = write_test_file(
        dir.path(),
        "new.emf.json",
        &[r#"{"_aws":{"Timestamp":1705314646000},"metric":"mem","value":67.8}"#],
    );

    // Scan and read
    let pattern = Regex::new(r".*\.emf\.json$").unwrap();
    let scanned = scan_directory(dir.path().to_str().unwrap(), &pattern).unwrap();
    let mut store = CheckpointStore::default();
    let offsets = recover_offsets(&mut store, "grp", &scanned);

    let mut file_events: Vec<(PathBuf, Vec<LogEvent>, u64, String)> = Vec::new();
    for (path, offset) in &offsets {
        let (events, new_offset) = read_file_from_offset(path, *offset, now_ms()).unwrap();
        let hash = scanned
            .iter()
            .find(|f| f.path == *path)
            .unwrap()
            .content_hash
            .clone();
        file_events.push((path.clone(), events, new_offset, hash));
    }

    // Mock CW: create_log_group + create_log_stream + put_log_events all succeed
    let replay_client = StaticReplayClient::new(vec![
        ReplayEvent::new(dummy_request(), success_response()), // create_log_group
        ReplayEvent::new(dummy_request(), success_response()), // create_log_stream
        ReplayEvent::new(dummy_request(), success_response()), // put_log_events
    ]);
    let mut cw_client = make_cw_client(replay_client);

    // Upload
    let result =
        upload_source_events(&mut cw_client, "/test/grp", "/stream", file_events, None).await;
    assert!(!result.succeeded.is_empty(), "Upload should succeed");
    assert!(result.failed.is_empty());

    // Advance checkpoints
    let completed = advance_checkpoints(&mut store, "grp", &result.succeeded, &scanned);

    // Old file (non-active) should be completed
    assert!(completed.contains(&old_path));
}

/// Upload failure: checkpoint should NOT advance
#[tokio::test]
async fn test_e2e_upload_failure_no_checkpoint_advance() {
    let dir = TempDir::new().unwrap();

    write_test_file(
        dir.path(),
        "data.emf.json",
        &[r#"{"_aws":{"Timestamp":1705314645000},"m":1}"#],
    );

    let pattern = Regex::new(r".*\.emf\.json$").unwrap();
    let scanned = scan_directory(dir.path().to_str().unwrap(), &pattern).unwrap();
    let mut store = CheckpointStore::default();
    let offsets = recover_offsets(&mut store, "grp", &scanned);

    let mut file_events: Vec<(PathBuf, Vec<LogEvent>, u64, String)> = Vec::new();
    for (path, offset) in &offsets {
        let (events, new_offset) = read_file_from_offset(path, *offset, now_ms()).unwrap();
        let hash = scanned
            .iter()
            .find(|f| f.path == *path)
            .unwrap()
            .content_hash
            .clone();
        file_events.push((path.clone(), events, new_offset, hash));
    }

    // Mock CW: create_log_group succeeds, create_log_stream succeeds, put_log_events fails
    let replay_client = StaticReplayClient::new(vec![
        ReplayEvent::new(dummy_request(), success_response()), // create_log_group
        ReplayEvent::new(dummy_request(), success_response()), // create_log_stream
        ReplayEvent::new(dummy_request(), error_response()),   // put_log_events fails
    ]);
    let mut cw_client = make_cw_client(replay_client);

    let result =
        upload_source_events(&mut cw_client, "/test/grp", "/stream", file_events, None).await;

    // All files should be in failed
    assert!(result.succeeded.is_empty());
    assert!(!result.failed.is_empty());
}
