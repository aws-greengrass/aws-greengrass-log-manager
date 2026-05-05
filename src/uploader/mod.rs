// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! Upload pipeline - batching, CloudWatch client, scheduling

mod batcher;
#[cfg(feature = "aws-sdk")]
mod cw_client;

use crate::scanner::LogEvent;

/// A sealed batch of log events ready for upload to CloudWatch.
#[derive(Debug)]
pub(crate) struct SealedBatch {
    pub(crate) log_group: String,
    pub(crate) log_stream: String,
    pub(crate) events: Vec<LogEvent>,
}

#[cfg(feature = "aws-sdk")]
pub(crate) use cw_client::{CwLogsClient, CwUploadError};

/// Format log stream name matching Java LogManager:
/// `/{yyyy}/{MM}/{dd}/thing/{thingName}` (UTC)
/// Replaces colons with `+` since CW log stream names cannot contain `:`.
pub(crate) fn format_log_stream_name(thing_name: &str) -> String {
    let now = time::OffsetDateTime::now_utc();
    let safe_name = thing_name.replace(':', "+");
    format!(
        "/{}/{:02}/{:02}/thing/{}",
        now.year(),
        now.month() as u8,
        now.day(),
        safe_name
    )
}

/// Check if a timestamp (epoch millis) falls on a different UTC date.
/// Used to detect when a new log stream should be created at midnight.
#[allow(dead_code, reason = "used by upload orchestrator in follow-up PR")]
pub(crate) fn is_different_date(
    timestamp_ms: i64,
    stream_year: i32,
    stream_month: u32,
    stream_day: u32,
) -> bool {
    match time::OffsetDateTime::from_unix_timestamp(timestamp_ms / 1000) {
        Ok(dt) => {
            dt.year() != stream_year
                || dt.month() as u32 != stream_month
                || dt.day() as u32 != stream_day
        }
        Err(_) => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sealed_batch_struct() {
        let batch = SealedBatch {
            log_group: "group".to_string(),
            log_stream: "stream".to_string(),
            events: vec![],
        };
        assert_eq!(batch.log_group, "group");
        assert_eq!(batch.log_stream, "stream");
        assert!(batch.events.is_empty());
    }

    #[test]
    fn test_format_log_stream_name() {
        let name = format_log_stream_name("eca-store66-device01");
        assert!(name.starts_with('/'));
        assert!(name.ends_with("/thing/eca-store66-device01"));
        let parts: Vec<&str> = name.split('/').collect();
        assert_eq!(parts.len(), 6); // ["", "YYYY", "MM", "DD", "thing", "name"]
        assert_eq!(parts[4], "thing");
    }

    #[test]
    fn test_format_log_stream_name_with_colons() {
        let name = format_log_stream_name("device:with:colons");
        assert!(name.ends_with("/thing/device+with+colons"));
        assert!(!name.contains(':'));
    }

    #[test]
    fn test_utc_date_from_time_crate() {
        let now = time::OffsetDateTime::now_utc();
        let (y, m, d) = (now.year(), now.month() as u32, now.day() as u32);
        assert!(y >= 2024);
        assert!((1..=12).contains(&m));
        assert!((1..=31).contains(&d));
    }

    #[test]
    fn test_is_different_date_same() {
        let now = time::OffsetDateTime::now_utc();
        let (y, m, d) = (now.year(), now.month() as u32, now.day() as u32);
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        assert!(!is_different_date(now_ms, y, m, d));
    }

    #[test]
    fn test_is_different_date_different() {
        assert!(is_different_date(1704067200000, 2023, 12, 31));
        assert!(!is_different_date(1704067200000, 2024, 1, 1));
    }

    #[test]
    fn test_is_different_date_negative_timestamp() {
        assert!(is_different_date(-1, 2024, 1, 1));
        assert!(is_different_date(-1000000, 2024, 1, 1));
    }

    #[test]
    fn test_is_different_date_midnight_boundary() {
        assert!(!is_different_date(1704067199999, 2023, 12, 31));
        assert!(!is_different_date(1704067200000, 2024, 1, 1));
        assert!(is_different_date(1704067199999, 2024, 1, 1));
        assert!(is_different_date(1704067200000, 2023, 12, 31));
    }
}
