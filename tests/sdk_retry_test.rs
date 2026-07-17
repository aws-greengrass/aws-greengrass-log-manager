// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests verifying AWS SDK retry behavior and our CwLogsClient error handling.
//! Uses mock HTTP to prove retry/no-retry semantics without hitting real CloudWatch.

use aws_sdk_cloudwatchlogs::{
    config::BehaviorVersion, config::Credentials, config::Region, Client,
};
use aws_smithy_http_client::test_util::{ReplayEvent, StaticReplayClient};
use aws_smithy_types::body::SdkBody;

fn throttle_response() -> http::Response<SdkBody> {
    http::Response::builder()
        .status(429)
        .body(SdkBody::from(
            r#"{"__type":"ThrottlingException","message":"Rate exceeded"}"#,
        ))
        .unwrap()
}

fn success_response() -> http::Response<SdkBody> {
    http::Response::builder()
        .status(200)
        .body(SdkBody::from(r#"{"nextSequenceToken":null}"#))
        .unwrap()
}

fn dummy_request() -> http::Request<SdkBody> {
    http::Request::builder()
        .uri("https://logs.us-east-1.amazonaws.com/")
        .body(SdkBody::empty())
        .unwrap()
}

fn make_client(replay_client: StaticReplayClient) -> Client {
    let retry_config =
        aws_sdk_cloudwatchlogs::config::retry::RetryConfig::standard().with_max_attempts(5);
    Client::from_conf(
        aws_sdk_cloudwatchlogs::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .credentials_provider(Credentials::new("test", "test", None, None, "test"))
            .region(Region::new("us-east-1"))
            .retry_config(retry_config)
            .http_client(replay_client)
            .build(),
    )
}

#[tokio::test]
async fn sdk_retries_on_throttling() {
    // 2 throttle responses, then 1 success — SDK should retry through the 429s
    let replay_client = StaticReplayClient::new(vec![
        ReplayEvent::new(dummy_request(), throttle_response()),
        ReplayEvent::new(dummy_request(), throttle_response()),
        ReplayEvent::new(dummy_request(), success_response()),
    ]);

    let client = make_client(replay_client.clone());

    let result = client
        .put_log_events()
        .log_group_name("test-group")
        .log_stream_name("test-stream")
        .send()
        .await;

    assert!(
        result.is_ok(),
        "Expected success after SDK retries, got: {result:?}"
    );

    let actual_requests: Vec<_> = replay_client.actual_requests().collect();
    assert_eq!(
        actual_requests.len(),
        3,
        "Expected 3 requests (2 throttled + 1 success), got {}",
        actual_requests.len()
    );
}

#[tokio::test]
async fn sdk_does_not_retry_invalid_parameter() {
    // 400 InvalidParameterException — SDK should NOT retry client errors
    let replay_client = StaticReplayClient::new(vec![ReplayEvent::new(
        dummy_request(),
        http::Response::builder()
            .status(400)
            .body(SdkBody::from(
                r#"{"__type":"InvalidParameterException","message":"Invalid"}"#,
            ))
            .unwrap(),
    )]);

    let client = make_client(replay_client.clone());

    let result = client
        .put_log_events()
        .log_group_name("test-group")
        .log_stream_name("test-stream")
        .send()
        .await;

    assert!(
        result.is_err(),
        "Expected error for InvalidParameterException"
    );

    let actual_requests: Vec<_> = replay_client.actual_requests().collect();
    assert_eq!(
        actual_requests.len(),
        1,
        "Expected exactly 1 request (no retry on 400), got {}",
        actual_requests.len()
    );
}

#[tokio::test]
async fn data_already_accepted_is_success() {
    // DataAlreadyAcceptedException is a 400 from CW — our cw_client.rs treats it as Ok(())
    let replay_client = StaticReplayClient::new(vec![
        ReplayEvent::new(
            dummy_request(),
            http::Response::builder()
                .status(400)
                .body(SdkBody::from(
                    r#"{"__type":"DataAlreadyAcceptedException","message":"already accepted","expectedSequenceToken":"token"}"#,
                ))
                .unwrap(),
        ),
    ]);

    let client = make_client(replay_client.clone());

    let result = client
        .put_log_events()
        .log_group_name("test-group")
        .log_stream_name("test-stream")
        .send()
        .await;

    // SDK returns this as a typed error — our CwLogsClient maps it to Ok(())
    assert!(
        result.is_err(),
        "SDK surfaces DataAlreadyAcceptedException as error"
    );
    let err = result.unwrap_err();
    let service_err = err.into_service_error();
    assert!(
        matches!(
            service_err,
            aws_sdk_cloudwatchlogs::operation::put_log_events::PutLogEventsError::DataAlreadyAcceptedException(_)
        ),
        "Expected DataAlreadyAcceptedException, got: {service_err:?}"
    );
}

#[tokio::test]
async fn resource_not_found_is_retriable() {
    // ResourceNotFoundException — our cw_client.rs maps it to CwUploadError::Retryable
    let replay_client = StaticReplayClient::new(vec![ReplayEvent::new(
        dummy_request(),
        http::Response::builder()
            .status(400)
            .body(SdkBody::from(
                r#"{"__type":"ResourceNotFoundException","message":"log group not found"}"#,
            ))
            .unwrap(),
    )]);

    let client = make_client(replay_client.clone());

    let result = client
        .put_log_events()
        .log_group_name("test-group")
        .log_stream_name("test-stream")
        .send()
        .await;

    assert!(
        result.is_err(),
        "SDK surfaces ResourceNotFoundException as error"
    );
    let err = result.unwrap_err();
    let service_err = err.into_service_error();
    assert!(
        matches!(
            service_err,
            aws_sdk_cloudwatchlogs::operation::put_log_events::PutLogEventsError::ResourceNotFoundException(_)
        ),
        "Expected ResourceNotFoundException, got: {service_err:?}"
    );
}
