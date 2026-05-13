// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! CloudWatch Logs API client

use super::SealedBatch;
use aws_sdk_cloudwatchlogs::{
    error::SdkError, operation::put_log_events::PutLogEventsError, types::InputLogEvent, Client,
};
use std::collections::HashSet;
use std::time::Duration;
use tokio::time::sleep;

/// Backoff delays in seconds for app-level retries.
const BACKOFF_DELAYS: [u64; 3] = [2, 4, 8];
/// Max app-level retries — derived from BACKOFF_DELAYS length.
const MAX_APP_RETRIES: usize = BACKOFF_DELAYS.len();

#[derive(Debug, thiserror::Error)]
pub enum CwUploadError {
    #[error("authentication error")]
    AuthError,
    #[error("retriable: {0}")]
    Retriable(String),
    #[error("{0}")]
    Other(String),
}

/// Outcome of an upload attempt with retry.
pub enum UploadOutcome {
    /// All events uploaded successfully.
    Success,
    /// Retries exhausted — retriable error persisted.
    RetriesExhausted,
}

pub struct CwLogsClient {
    client: Client,
    config: aws_sdk_cloudwatchlogs::Config,
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
        let client = Client::from_conf(config.clone());
        Self {
            client,
            config,
            created_groups: HashSet::new(),
            created_streams: HashSet::new(),
        }
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn new_with_client(client: Client, config: aws_sdk_cloudwatchlogs::Config) -> Self {
        Self {
            client,
            config,
            created_groups: HashSet::new(),
            created_streams: HashSet::new(),
        }
    }

    /// Recreate the SDK client (e.g., after persistent network errors).
    /// Caches are preserved — if a resource was deleted externally, the next
    /// put_log_events will get ResourceNotFoundException which clears the cache.
    pub fn recreate_client(&mut self) {
        self.client = Client::from_conf(self.config.clone());
    }

    pub async fn upload_batch(&mut self, batch: &SealedBatch) -> Result<(), CwUploadError> {
        if batch.events.is_empty() {
            return Ok(());
        }
        self.ensure_log_group(&batch.log_group).await?;
        self.ensure_log_stream(&batch.log_group, &batch.log_stream)
            .await?;
        self.put_log_events(batch).await
    }

    /// Upload a batch with app-level retry for retriable errors.
    /// SDK already retries 5 times internally for transient HTTP errors.
    ///
    /// # Errors
    /// Returns `Err(CwUploadError::AuthError)` if credentials are invalid (non-retriable).
    pub async fn upload_batch_with_retry(
        &mut self,
        batch: &SealedBatch,
    ) -> Result<UploadOutcome, CwUploadError> {
        for (attempt, delay) in BACKOFF_DELAYS.iter().enumerate() {
            match self.upload_batch(batch).await {
                Ok(()) => return Ok(UploadOutcome::Success),
                Err(CwUploadError::AuthError) => {
                    return Err(CwUploadError::AuthError);
                }
                Err(CwUploadError::Other(msg)) => {
                    tracing::error!(log_group = %batch.log_group, error = %msg, "Non-retriable upload error");
                    return Ok(UploadOutcome::RetriesExhausted);
                }
                Err(CwUploadError::Retriable(msg)) => {
                    if attempt >= MAX_APP_RETRIES - 1 {
                        tracing::error!(log_group = %batch.log_group, attempts = MAX_APP_RETRIES, error = %msg, "Upload failed after all retries");
                        return Ok(UploadOutcome::RetriesExhausted);
                    }
                    tracing::warn!(log_group = %batch.log_group, attempt = attempt + 1, delay_s = delay, error = %msg, "Retriable error, backing off");
                    sleep(Duration::from_secs(*delay)).await;
                }
            }
        }
        Ok(UploadOutcome::RetriesExhausted)
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
                CwUploadError::Retriable("LimitExceededException on create_log_group".to_string()),
            ),
            Err(SdkError::ServiceError(e)) if is_auth_error(e.err()) => {
                Err(CwUploadError::AuthError)
            }
            Err(e) => Err(CwUploadError::Retriable(e.to_string())),
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
            Err(SdkError::ServiceError(e)) if is_limit_exceeded_stream(e.err()) => Err(
                CwUploadError::Retriable("LimitExceededException on create_log_stream".to_string()),
            ),
            Err(SdkError::ServiceError(e)) if is_auth_error(e.err()) => {
                Err(CwUploadError::AuthError)
            }
            Err(e) => Err(CwUploadError::Retriable(e.to_string())),
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
            events.map_err(|e| CwUploadError::Other(format!("Failed to build log event: {e}")))?;

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
                PutLogEventsError::UnrecognizedClientException(_) => Err(CwUploadError::AuthError),
                PutLogEventsError::ServiceUnavailableException(_) => Err(CwUploadError::Other(
                    "ServiceUnavailable after SDK retries exhausted".to_string(),
                )),
                PutLogEventsError::ResourceNotFoundException(_) => {
                    // Clear cache so next retry re-creates the group/stream
                    self.created_groups.remove(&batch.log_group);
                    let key = format!("{}:{}", batch.log_group, batch.log_stream);
                    self.created_streams.remove(&key);
                    Err(CwUploadError::Retriable("ResourceNotFound".to_string()))
                }
                PutLogEventsError::InvalidParameterException(ex) => {
                    Err(CwUploadError::Other(ex.to_string()))
                }
                other => {
                    use aws_sdk_cloudwatchlogs::error::ProvideErrorMetadata;
                    if other.code() == Some("ThrottlingException") {
                        Err(CwUploadError::Other(
                            "Throttled after SDK retries exhausted".to_string(),
                        ))
                    } else {
                        Err(CwUploadError::Other(other.to_string()))
                    }
                }
            },
            Err(e) => Err(CwUploadError::Retriable(e.to_string())),
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

/// String-based match because `CreateLogStreamError` does not have a typed
/// `LimitExceededException` variant in SDK v1.20 (unlike `CreateLogGroupError`).
fn is_limit_exceeded_stream(
    err: &aws_sdk_cloudwatchlogs::operation::create_log_stream::CreateLogStreamError,
) -> bool {
    use aws_sdk_cloudwatchlogs::error::ProvideErrorMetadata;
    err.code() == Some("LimitExceededException")
}

/// Auth errors are not retriable — same credentials will produce the same failure.
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
        let client = Client::from_conf(config.clone());
        CwLogsClient::new_with_client(client, config)
    }

    #[test]
    fn test_cw_upload_error_display() {
        assert_eq!(CwUploadError::AuthError.to_string(), "authentication error");
        assert_eq!(
            CwUploadError::Retriable("timeout".into()).to_string(),
            "retriable: timeout"
        );
        assert_eq!(CwUploadError::Other("bad".into()).to_string(), "bad");
    }

    #[test]
    fn test_cw_upload_error_variants() {
        let auth = CwUploadError::AuthError;
        assert!(matches!(auth, CwUploadError::AuthError));

        let retriable = CwUploadError::Retriable("connection error".to_string());
        if let CwUploadError::Retriable(msg) = retriable {
            assert_eq!(msg, "connection error");
        } else {
            panic!("Expected Retriable variant");
        }

        let other = CwUploadError::Other("test error".to_string());
        if let CwUploadError::Other(msg) = other {
            assert_eq!(msg, "test error");
        } else {
            panic!("Expected Other variant");
        }
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

    #[test]
    fn test_limit_exceeded_on_group_create() {
        let err =
            CwUploadError::Retriable("LimitExceededException on create_log_group".to_string());
        if let CwUploadError::Retriable(msg) = err {
            assert!(msg.contains("LimitExceededException"));
            assert!(msg.contains("create_log_group"));
        } else {
            panic!("Expected Retriable variant");
        }
    }

    #[test]
    fn test_limit_exceeded_on_stream_create() {
        let err =
            CwUploadError::Retriable("LimitExceededException on create_log_stream".to_string());
        if let CwUploadError::Retriable(msg) = err {
            assert!(msg.contains("LimitExceededException"));
            assert!(msg.contains("create_log_stream"));
        } else {
            panic!("Expected Retriable variant");
        }
    }

    #[test]
    fn test_client_can_be_recreated() {
        let mut cw = make_test_client();
        cw.mark_group_created("test-group");
        cw.mark_stream_created("test-group", "test-stream");
        cw.recreate_client();
        assert!(cw.created_groups().contains("test-group"));
        assert!(cw.created_streams().contains("test-group:test-stream"));
    }

    #[tokio::test]
    async fn test_resource_not_found_clears_cache() {
        use aws_smithy_http_client::test_util::{ReplayEvent, StaticReplayClient};
        use aws_smithy_types::body::SdkBody;

        fn ok_response() -> http::Response<SdkBody> {
            http::Response::builder()
                .status(200)
                .body(SdkBody::from("{}"))
                .unwrap()
        }
        fn resource_not_found_response() -> http::Response<SdkBody> {
            http::Response::builder()
                .status(400)
                .body(SdkBody::from(
                    r#"{"__type":"ResourceNotFoundException","message":"The specified log group does not exist."}"#,
                ))
                .unwrap()
        }
        fn dummy_request() -> http::Request<SdkBody> {
            http::Request::builder()
                .uri("https://logs.us-east-1.amazonaws.com/")
                .body(SdkBody::empty())
                .unwrap()
        }

        let replay_client = StaticReplayClient::new(vec![
            // CreateLogGroup → 200 OK
            ReplayEvent::new(dummy_request(), ok_response()),
            // CreateLogStream → 200 OK
            ReplayEvent::new(dummy_request(), ok_response()),
            // PutLogEvents → ResourceNotFoundException
            ReplayEvent::new(dummy_request(), resource_not_found_response()),
        ]);

        let config = aws_sdk_cloudwatchlogs::Config::builder()
            .behavior_version(aws_sdk_cloudwatchlogs::config::BehaviorVersion::latest())
            .credentials_provider(aws_sdk_cloudwatchlogs::config::Credentials::new(
                "test", "test", None, None, "test",
            ))
            .region(aws_sdk_cloudwatchlogs::config::Region::new("us-east-1"))
            .retry_config(
                aws_sdk_cloudwatchlogs::config::retry::RetryConfig::standard().with_max_attempts(1),
            )
            .http_client(replay_client)
            .build();
        let client = Client::from_conf(config.clone());
        let mut cw = CwLogsClient::new_with_client(client, config);

        let batch = SealedBatch {
            log_group: "test-group".to_string(),
            log_stream: "test-stream".to_string(),
            events: vec![crate::scanner::LogEvent {
                timestamp: 1000,
                message: "hello".to_string(),
            }],
        };

        let result = cw.upload_batch(&batch).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(&err, CwUploadError::Retriable(msg) if msg.contains("ResourceNotFound")),
            "Expected Retriable(ResourceNotFound), got: {err}"
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
}
