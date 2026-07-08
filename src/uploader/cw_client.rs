// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! CloudWatch Logs API client

use super::SealedBatch;
use aws_sdk_cloudwatchlogs::{
    error::SdkError, operation::put_log_events::PutLogEventsError, types::InputLogEvent, Client,
};
use std::collections::HashSet;

/// Classification of a CloudWatch upload failure.
///
/// The AWS SDK already retries transient faults (throttling, 5xx, timeouts)
/// internally via [`RetryConfig`], so the only fault worth a manual retry
/// is a missing log group/stream — everything else is either fatal for this cycle or
/// an auth problem the same credentials cannot fix.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CwUploadError {
    /// Authentication/authorization failure. Not retryable with the same credentials —
    /// kept distinct so the caller can trigger a TES credential refresh.
    #[error("authentication error")]
    Auth,
    /// A fault the SDK does not retry but a single immediate re-attempt may clear —
    /// notably `ResourceNotFoundException` (recreate the group/stream) or a raw
    /// transport error surfacing after the SDK's own retries are exhausted.
    #[error("retryable: {0}")]
    Retryable(String),
    /// A non-retryable failure for this cycle: invalid input, a hard resource/account
    /// quota such as `LimitExceeded` that retrying cannot clear, or a transient class
    /// the SDK already retried and exhausted (e.g. throttling/5xx).
    #[error("{0}")]
    Fatal(String),
}

/// Outcome of an upload attempt.
#[derive(Debug)]
#[must_use = "upload outcome must be handled"]
pub(crate) enum UploadOutcome {
    /// All events uploaded successfully.
    Success,
    /// A retryable fault persisted after the single re-attempt — the batch will be
    /// retried on the next upload cycle.
    RetriesExhausted,
}

pub struct CwLogsClient {
    client: Client,
    created_groups: HashSet<String>,
    created_streams: HashSet<String>,
}

impl CwLogsClient {
    pub async fn new() -> Self {
        let retry_config = aws_config::retry::RetryConfig::standard().with_max_attempts(5);
        let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .retry_config(retry_config)
            .load()
            .await;
        let config = aws_sdk_cloudwatchlogs::Config::new(&sdk_config);
        let client = Client::from_conf(config);
        Self {
            client,
            created_groups: HashSet::new(),
            created_streams: HashSet::new(),
        }
    }

    #[cfg(test)]
    fn new_with_client(client: Client) -> Self {
        Self {
            client,
            created_groups: HashSet::new(),
            created_streams: HashSet::new(),
        }
    }

    pub(crate) async fn upload_batch(&mut self, batch: &SealedBatch) -> Result<(), CwUploadError> {
        if batch.events.is_empty() {
            return Ok(());
        }
        self.ensure_log_group(&batch.log_group).await?;
        self.ensure_log_stream(&batch.log_group, &batch.log_stream)
            .await?;
        self.put_log_events(batch).await
    }

    /// Upload a batch, retrying exactly once for a retryable fault.
    ///
    /// The AWS SDK already retries throttling/5xx/timeouts internally
    /// ([`RetryConfig`] with `max_attempts(5)`), so there is deliberately no app-level
    /// sleep/backoff loop here. The single re-attempt exists for the one case the SDK
    /// cannot handle: a missing log group/stream. On `ResourceNotFoundException`,
    /// [`Self::put_log_events`] clears the resource cache, so re-running `upload_batch`
    /// recreates the group/stream and re-sends once when the target was deleted
    /// between the cache check and the upload. Any other persistent failure is
    /// deferred to the next cycle.
    ///
    /// # Errors
    /// Returns [`CwUploadError::Auth`] when credentials are invalid (non-retryable);
    /// the caller stops processing the source so credentials can be refreshed.
    pub(crate) async fn upload_batch_with_retry(
        &mut self,
        batch: &SealedBatch,
    ) -> Result<UploadOutcome, CwUploadError> {
        match self.upload_batch(batch).await {
            Ok(()) => return Ok(UploadOutcome::Success),
            Err(CwUploadError::Auth) => return Err(CwUploadError::Auth),
            Err(CwUploadError::Fatal(msg)) => {
                tracing::error!(log_group = %batch.log_group, error = %msg, "Non-retryable upload error");
                return Ok(UploadOutcome::RetriesExhausted);
            }
            Err(CwUploadError::Retryable(msg)) => {
                tracing::warn!(log_group = %batch.log_group, error = %msg, "Retryable error, recreating resources and retrying once");
            }
        }

        // Single re-attempt, no sleep. Persistent failures wait for the next upload cycle.
        match self.upload_batch(batch).await {
            Ok(()) => Ok(UploadOutcome::Success),
            Err(CwUploadError::Auth) => Err(CwUploadError::Auth),
            Err(e) => {
                tracing::error!(log_group = %batch.log_group, error = %e, "Upload failed after recreate retry");
                Ok(UploadOutcome::RetriesExhausted)
            }
        }
    }

    async fn ensure_log_group(&mut self, log_group: &str) -> Result<(), CwUploadError> {
        if self.created_groups.contains(log_group) {
            return Ok(());
        }
        match self
            .client
            .create_log_group()
            .log_group_name(log_group)
            .send()
            .await
        {
            Ok(_) => {
                tracing::info!(log_group = %log_group, "Created log group");
                self.created_groups.insert(log_group.to_string());
                Ok(())
            }
            Err(SdkError::ServiceError(e)) if is_group_exists(e.err()) => {
                self.created_groups.insert(log_group.to_string());
                Ok(())
            }
            Err(SdkError::ServiceError(e)) if is_limit_exceeded_group(e.err()) => Err(
                CwUploadError::Fatal("LimitExceededException on create_log_group".to_string()),
            ),
            Err(SdkError::ServiceError(e)) if is_auth_error(e.err()) => Err(CwUploadError::Auth),
            Err(e) => Err(CwUploadError::Retryable(e.to_string())),
        }
    }

    async fn ensure_log_stream(
        &mut self,
        log_group: &str,
        log_stream: &str,
    ) -> Result<(), CwUploadError> {
        let key = format!("{}:{}", log_group, log_stream);
        if self.created_streams.contains(&key) {
            return Ok(());
        }
        match self
            .client
            .create_log_stream()
            .log_group_name(log_group)
            .log_stream_name(log_stream)
            .send()
            .await
        {
            Ok(_) => {
                tracing::info!(log_group = %log_group, log_stream = %log_stream, "Created log stream");
                self.created_streams.insert(key);
                Ok(())
            }
            Err(SdkError::ServiceError(e)) if is_stream_exists(e.err()) => {
                self.created_streams.insert(key);
                Ok(())
            }
            Err(SdkError::ServiceError(e)) if is_auth_error(e.err()) => Err(CwUploadError::Auth),
            Err(e) => Err(CwUploadError::Retryable(e.to_string())),
        }
    }

    async fn put_log_events(&mut self, batch: &SealedBatch) -> Result<(), CwUploadError> {
        let events: Result<Vec<InputLogEvent>, _> = batch
            .events
            .iter()
            .map(|e| {
                InputLogEvent::builder()
                    .timestamp(e.timestamp)
                    .message(&e.message)
                    .build()
            })
            .collect();
        let events =
            events.map_err(|e| CwUploadError::Fatal(format!("Failed to build log event: {e}")))?;

        // EMF metrics are auto-extracted by CloudWatch when log events contain the _aws key.
        // No sequence tokens needed — deprecated since late 2023, and we create new streams daily.
        let req = self
            .client
            .put_log_events()
            .log_group_name(&batch.log_group)
            .log_stream_name(&batch.log_stream)
            .set_log_events(Some(events));

        match req.send().await {
            Ok(output) => {
                if let Some(rejected) = output.rejected_log_events_info() {
                    tracing::error!(log_group = %batch.log_group, "Log events rejected by CloudWatch (data loss): {:?}", rejected);
                }
                tracing::debug!(log_group = %batch.log_group, log_stream = %batch.log_stream, event_count = batch.events.len(), "Upload successful");
                Ok(())
            }
            Err(SdkError::ServiceError(e)) => match e.into_err() {
                PutLogEventsError::DataAlreadyAcceptedException(_) => {
                    tracing::debug!(log_group = %batch.log_group, "Data already accepted");
                    Ok(())
                }
                PutLogEventsError::UnrecognizedClientException(_) => Err(CwUploadError::Auth),
                PutLogEventsError::ServiceUnavailableException(_) => Err(CwUploadError::Fatal(
                    "ServiceUnavailable after SDK retries exhausted".to_string(),
                )),
                PutLogEventsError::ResourceNotFoundException(_) => {
                    // Clear cache so the single re-attempt recreates the group/stream.
                    self.created_groups.remove(&batch.log_group);
                    let key = format!("{}:{}", batch.log_group, batch.log_stream);
                    self.created_streams.remove(&key);
                    Err(CwUploadError::Retryable("ResourceNotFound".to_string()))
                }
                PutLogEventsError::InvalidParameterException(ex) => {
                    Err(CwUploadError::Fatal(ex.to_string()))
                }
                other => {
                    use aws_sdk_cloudwatchlogs::error::ProvideErrorMetadata;
                    if other.code() == Some("ThrottlingException") {
                        Err(CwUploadError::Fatal(
                            "Throttled after SDK retries exhausted".to_string(),
                        ))
                    } else {
                        Err(CwUploadError::Fatal(other.to_string()))
                    }
                }
            },
            Err(e) => Err(CwUploadError::Retryable(e.to_string())),
        }
    }

    #[cfg(test)]
    pub(crate) fn mark_group_created(&mut self, group: &str) {
        self.created_groups.insert(group.to_string());
    }

    #[cfg(test)]
    pub(crate) fn mark_stream_created(&mut self, group: &str, stream: &str) {
        self.created_streams.insert(format!("{}:{}", group, stream));
    }

    #[cfg(test)]
    pub(crate) fn created_groups(&self) -> &HashSet<String> {
        &self.created_groups
    }

    #[cfg(test)]
    pub(crate) fn created_streams(&self) -> &HashSet<String> {
        &self.created_streams
    }
}

fn is_group_exists(
    err: &aws_sdk_cloudwatchlogs::operation::create_log_group::CreateLogGroupError,
) -> bool {
    matches!(err, aws_sdk_cloudwatchlogs::operation::create_log_group::CreateLogGroupError::ResourceAlreadyExistsException(_))
}

fn is_limit_exceeded_group(
    err: &aws_sdk_cloudwatchlogs::operation::create_log_group::CreateLogGroupError,
) -> bool {
    matches!(err, aws_sdk_cloudwatchlogs::operation::create_log_group::CreateLogGroupError::LimitExceededException(_))
}

fn is_stream_exists(
    err: &aws_sdk_cloudwatchlogs::operation::create_log_stream::CreateLogStreamError,
) -> bool {
    matches!(err, aws_sdk_cloudwatchlogs::operation::create_log_stream::CreateLogStreamError::ResourceAlreadyExistsException(_))
}

/// Auth errors are not retryable — same credentials will produce the same failure.
fn is_auth_error<E: aws_sdk_cloudwatchlogs::error::ProvideErrorMetadata>(err: &E) -> bool {
    matches!(
        err.code(),
        Some("UnrecognizedClientException")
            | Some("ExpiredTokenException")
            | Some("AccessDeniedException")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_client() -> CwLogsClient {
        let config = aws_sdk_cloudwatchlogs::Config::builder()
            .behavior_version(aws_sdk_cloudwatchlogs::config::BehaviorVersion::latest())
            .build();
        let client = Client::from_conf(config);
        CwLogsClient::new_with_client(client)
    }

    fn batch_with_event() -> SealedBatch {
        SealedBatch {
            log_group: "test-group".to_string(),
            log_stream: "test-stream".to_string(),
            events: vec![crate::scanner::LogEvent {
                timestamp: 1000,
                message: "hello".to_string(),
            }],
        }
    }

    #[test]
    fn test_cw_upload_error_display() {
        assert_eq!(CwUploadError::Auth.to_string(), "authentication error");
        assert_eq!(
            CwUploadError::Retryable("timeout".into()).to_string(),
            "retryable: timeout"
        );
        assert_eq!(CwUploadError::Fatal("bad".into()).to_string(), "bad");
    }

    #[test]
    fn test_cw_upload_error_variants() {
        let auth = CwUploadError::Auth;
        assert!(matches!(auth, CwUploadError::Auth));

        let retryable = CwUploadError::Retryable("connection error".to_string());
        if let CwUploadError::Retryable(msg) = retryable {
            assert_eq!(msg, "connection error");
        } else {
            panic!("Expected Retryable variant");
        }

        let fatal = CwUploadError::Fatal("test error".to_string());
        if let CwUploadError::Fatal(msg) = fatal {
            assert_eq!(msg, "test error");
        } else {
            panic!("Expected Fatal variant");
        }
    }

    #[test]
    fn test_upload_outcome_variants() {
        assert!(matches!(UploadOutcome::Success, UploadOutcome::Success));
        assert!(matches!(
            UploadOutcome::RetriesExhausted,
            UploadOutcome::RetriesExhausted
        ));
    }

    #[test]
    fn test_mark_group_created_idempotency() {
        let mut cw = make_test_client();
        assert!(!cw.created_groups().contains("test-group"));
        cw.mark_group_created("test-group");
        assert!(cw.created_groups().contains("test-group"));
        cw.mark_group_created("test-group");
        assert_eq!(cw.created_groups().len(), 1);
    }

    #[test]
    fn test_mark_stream_created_idempotency() {
        let mut cw = make_test_client();
        let key = "grp:stream";
        assert!(!cw.created_streams().contains(key));
        cw.mark_stream_created("grp", "stream");
        assert!(cw.created_streams().contains(key));
        cw.mark_stream_created("grp", "stream");
        assert_eq!(cw.created_streams().len(), 1);
    }

    fn ok_response() -> http::Response<aws_smithy_types::body::SdkBody> {
        http::Response::builder()
            .status(200)
            .body(aws_smithy_types::body::SdkBody::from("{}"))
            .unwrap()
    }

    fn error_response(
        status: u16,
        body: &'static str,
    ) -> http::Response<aws_smithy_types::body::SdkBody> {
        http::Response::builder()
            .status(status)
            .body(aws_smithy_types::body::SdkBody::from(body))
            .unwrap()
    }

    fn dummy_request() -> http::Request<aws_smithy_types::body::SdkBody> {
        http::Request::builder()
            .uri("https://logs.us-east-1.amazonaws.com/")
            .body(aws_smithy_types::body::SdkBody::empty())
            .unwrap()
    }

    fn replay_client(
        responses: Vec<http::Response<aws_smithy_types::body::SdkBody>>,
    ) -> aws_smithy_http_client::test_util::StaticReplayClient {
        use aws_smithy_http_client::test_util::{ReplayEvent, StaticReplayClient};
        StaticReplayClient::new(
            responses
                .into_iter()
                .map(|resp| ReplayEvent::new(dummy_request(), resp))
                .collect(),
        )
    }

    fn client_with_responses(
        responses: Vec<http::Response<aws_smithy_types::body::SdkBody>>,
    ) -> (
        CwLogsClient,
        aws_smithy_http_client::test_util::StaticReplayClient,
    ) {
        let replay = replay_client(responses);
        let config = aws_sdk_cloudwatchlogs::Config::builder()
            .behavior_version(aws_sdk_cloudwatchlogs::config::BehaviorVersion::latest())
            .credentials_provider(aws_sdk_cloudwatchlogs::config::Credentials::new(
                "test", "test", None, None, "test",
            ))
            .region(aws_sdk_cloudwatchlogs::config::Region::new("us-east-1"))
            .retry_config(
                aws_sdk_cloudwatchlogs::config::retry::RetryConfig::standard().with_max_attempts(1),
            )
            .http_client(replay.clone())
            .build();
        let client = Client::from_conf(config);
        (CwLogsClient::new_with_client(client), replay)
    }

    #[tokio::test]
    async fn test_resource_not_found_clears_cache() {
        let (mut cw, _replay) = client_with_responses(vec![
            ok_response(), // CreateLogGroup
            ok_response(), // CreateLogStream
            error_response(
                400,
                r#"{"__type":"ResourceNotFoundException","message":"The specified log group does not exist."}"#,
            ),
        ]);

        let batch = batch_with_event();
        let err = cw.upload_batch(&batch).await.unwrap_err();
        assert!(
            matches!(&err, CwUploadError::Retryable(msg) if msg.contains("ResourceNotFound")),
            "Expected Retryable(ResourceNotFound), got: {err}"
        );
        assert!(
            !cw.created_groups().contains("test-group"),
            "Log group should be cleared from cache after ResourceNotFoundException"
        );
        assert!(
            !cw.created_streams().contains("test-group:test-stream"),
            "Log stream should be cleared from cache after ResourceNotFoundException"
        );
    }

    #[tokio::test]
    async fn test_with_retry_recreates_and_succeeds_on_resource_not_found() {
        // First attempt: create group/stream OK, put → ResourceNotFound (clears cache).
        // Single re-attempt: re-create group/stream, put → OK.
        let (mut cw, _replay) = client_with_responses(vec![
            ok_response(), // CreateLogGroup (attempt 1)
            ok_response(), // CreateLogStream (attempt 1)
            error_response(
                400,
                r#"{"__type":"ResourceNotFoundException","message":"missing"}"#,
            ), // PutLogEvents (attempt 1)
            ok_response(), // CreateLogGroup (attempt 2)
            ok_response(), // CreateLogStream (attempt 2)
            ok_response(), // PutLogEvents (attempt 2)
        ]);

        let batch = batch_with_event();
        let outcome = cw.upload_batch_with_retry(&batch).await.unwrap();
        assert!(matches!(outcome, UploadOutcome::Success));
    }

    #[tokio::test]
    async fn test_with_retry_fatal_does_not_retry() {
        // InvalidParameter is fatal — no re-attempt; only the first 3 calls are consumed.
        let (mut cw, replay) = client_with_responses(vec![
            ok_response(), // CreateLogGroup
            ok_response(), // CreateLogStream
            error_response(
                400,
                r#"{"__type":"InvalidParameterException","message":"bad"}"#,
            ), // PutLogEvents
        ]);

        let batch = batch_with_event();
        let outcome = cw.upload_batch_with_retry(&batch).await.unwrap();
        assert!(matches!(outcome, UploadOutcome::RetriesExhausted));
        // No second attempt: exactly the 3 seeded requests were issued.
        assert_eq!(replay.actual_requests().count(), 3);
    }

    #[tokio::test]
    async fn test_with_retry_auth_propagates() {
        // CreateLogGroup → AccessDenied surfaces as Auth, propagated to the caller.
        let (mut cw, _replay) = client_with_responses(vec![error_response(
            400,
            r#"{"__type":"AccessDeniedException","message":"denied"}"#,
        )]);

        let batch = batch_with_event();
        let err = cw.upload_batch_with_retry(&batch).await.unwrap_err();
        assert!(matches!(err, CwUploadError::Auth));
    }

    // Direct tests for the source-level orchestration in `upload_source_events`.

    fn event_at(ts: i64, msg: &str) -> crate::scanner::LogEvent {
        crate::scanner::LogEvent {
            timestamp: ts,
            message: msg.to_string(),
        }
    }

    fn now_ms_i64() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    }

    #[tokio::test]
    async fn test_upload_source_events_empty_events() {
        // No events → nothing uploaded; the file is reported succeeded so its
        // checkpoint can still advance. The client is never called.
        let (mut cw, _replay) = client_with_responses(vec![]);
        let path = std::path::PathBuf::from("/tmp/empty.log");
        let file_events = vec![(path.clone(), Vec::new(), 0u64, "hash-empty".to_string())];

        let result =
            crate::uploader::upload_source_events(&mut cw, "grp", "stream", file_events, None)
                .await
                .unwrap();

        assert_eq!(result.succeeded.len(), 1);
        assert_eq!(result.succeeded[0].0, path);
        assert!(result.failed.is_empty());
    }

    #[tokio::test]
    async fn test_upload_source_events_all_succeed() {
        // Single batch: CreateLogGroup + CreateLogStream + PutLogEvents all succeed.
        let (mut cw, _replay) = client_with_responses(vec![
            ok_response(), // CreateLogGroup
            ok_response(), // CreateLogStream
            ok_response(), // PutLogEvents
        ]);
        let path = std::path::PathBuf::from("/tmp/ok.log");
        let file_events = vec![(
            path.clone(),
            vec![event_at(now_ms_i64(), "hello")],
            5u64,
            "hash-ok".to_string(),
        )];

        let result = crate::uploader::upload_source_events(
            &mut cw,
            "test-group",
            "test-stream",
            file_events,
            None,
        )
        .await
        .unwrap();

        assert_eq!(result.succeeded.len(), 1);
        assert_eq!(result.succeeded[0].0, path);
        assert!(result.failed.is_empty());
    }

    #[tokio::test]
    async fn test_upload_source_events_multibatch_partial_failure() {
        // >10_000 events → 2 batches. Batch 1 succeeds; batch 2's PutLogEvents returns a
        // fatal InvalidParameter (→ RetriesExhausted). All-or-nothing per source: the whole
        // source is marked failed and no checkpoint advances, so it is re-read next cycle.
        let now = now_ms_i64();
        let events: Vec<crate::scanner::LogEvent> =
            (0..10_001).map(|i| event_at(now + i, "x")).collect();
        // Batch 1 issues CreateLogGroup + CreateLogStream + PutLogEvents; batch 2 reuses the
        // cached group/stream and only issues PutLogEvents (which fails).
        let (mut cw, _replay) = client_with_responses(vec![
            ok_response(), // CreateLogGroup (batch 1)
            ok_response(), // CreateLogStream (batch 1)
            ok_response(), // PutLogEvents (batch 1)
            error_response(
                400,
                r#"{"__type":"InvalidParameterException","message":"bad"}"#,
            ), // PutLogEvents (batch 2)
        ]);
        let path = std::path::PathBuf::from("/tmp/source.log");
        let file_events = vec![(path.clone(), events, 100u64, "hash-multi".to_string())];

        let result = crate::uploader::upload_source_events(
            &mut cw,
            "test-group",
            "test-stream",
            file_events,
            None,
        )
        .await
        .unwrap();

        assert!(result.succeeded.is_empty());
        assert_eq!(result.failed, vec![path]);
    }

    #[tokio::test]
    async fn test_upload_source_events_auth_bubbles_up() {
        // CreateLogGroup → AccessDenied surfaces as Auth and propagates out of the source
        // orchestration so the caller can stop the cycle and refresh credentials.
        let (mut cw, _replay) = client_with_responses(vec![error_response(
            400,
            r#"{"__type":"AccessDeniedException","message":"denied"}"#,
        )]);
        let path = std::path::PathBuf::from("/tmp/auth.log");
        let file_events = vec![(
            path,
            vec![event_at(now_ms_i64(), "hello")],
            5u64,
            "hash-auth".to_string(),
        )];

        let err = crate::uploader::upload_source_events(
            &mut cw,
            "test-group",
            "test-stream",
            file_events,
            None,
        )
        .await
        .unwrap_err();

        assert!(matches!(err, CwUploadError::Auth));
    }
}
