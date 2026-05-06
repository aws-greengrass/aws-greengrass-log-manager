// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! Log event batching for PutLogEvents.
//!
//! Accumulates [`LogEvent`]s into [`SealedBatch`]es respecting CloudWatch Logs API limits.
//! Matches Java `CloudWatchAttemptLogsProcessor.addNewLogEvent` behavior:
//! - 1 MB max batch payload (including 26+8 byte per-event overhead)
//! - 10,000 max events per batch
//! - 23-hour max time span within a batch
//! - 14-day age filter (events older than 14 days are dropped)
//! - 2-hour future filter (CW rejects events >2h ahead; Java relies on server-side rejection)
//! - Oversized events (>262,110 bytes) are chunked, not truncated
//!
//! # Known divergences from Java
//!
//! **Dropped events don't advance checkpoint.** Java returns consumed byte count for
//! dropped events (14-day filter) so the caller advances the file offset past them.
//! This batcher silently drops them. The orchestrator must filter old events before
//! batching, or accept re-reading them each cycle. Tracked under Asana 1.8.
//!
//! **Log level filtering matches Java behavior.** JSON-structured `GreengrassLogMessage`
//! lines are deserialized to extract the `level` field. Text-format lines pass through
//! without level filtering (matching Java's `addNewLogEvent` path).

use super::SealedBatch;
use crate::config::LogLevel;
use crate::scanner::LogEvent;

/// CW per-event framing overhead (bytes).
const EVENT_STORAGE_OVERHEAD: usize = 26;
/// Timestamp field size (bytes).
const TIMESTAMP_BYTES: usize = 8;
/// CW PutLogEvents max payload: 1 MB.
const MAX_BATCH_SIZE: usize = 1_048_576;
/// CW max message bytes per event: 256KB - 8 - 26 = 262,110.
pub(crate) const MAX_EVENT_BYTES: usize = 256 * 1024 - TIMESTAMP_BYTES - EVENT_STORAGE_OVERHEAD;
/// CW max events per PutLogEvents call.
const MAX_NUM_OF_LOG_EVENTS: usize = 10_000;
/// Max time span between earliest and latest event in a batch (23 hours in ms).
const MAX_TIME_SPAN_MS: i64 = 23 * 60 * 60 * 1000;
/// Events older than 14 days are rejected by CW.
const FOURTEEN_DAYS_MS: i64 = 14 * 24 * 60 * 60 * 1000;
/// Events more than 2 hours in the future are rejected by CW.
const TWO_HOURS_FUTURE_MS: i64 = 2 * 60 * 60 * 1000;

/// Per-event effective size for batch accounting.
fn event_wire_size(message_bytes: usize) -> usize {
    message_bytes + TIMESTAMP_BYTES + EVENT_STORAGE_OVERHEAD
}

/// Seal log events into batches ready for CloudWatch upload.
///
/// Events are sorted by timestamp, filtered by age and log level,
/// oversized events are chunked, and batches are sealed at CW API limits.
#[must_use]
pub(crate) fn seal_batches(
    mut events: Vec<LogEvent>,
    log_group: &str,
    log_stream: &str,
    min_log_level: Option<LogLevel>,
    now_ms: i64,
) -> Vec<SealedBatch> {
    // CW requires events sorted by timestamp. Must use stable sort to preserve
    // insertion order for events with equal timestamps.
    events.sort_by_key(|e| e.timestamp);

    let cutoff = now_ms - FOURTEEN_DAYS_MS;
    let max_future = now_ms + TWO_HOURS_FUTURE_MS;

    let mut batches: Vec<SealedBatch> = Vec::new();
    let mut current_events: Vec<LogEvent> = Vec::new();
    let mut current_size: usize = 0;
    let mut earliest_ts: Option<i64> = None;

    let seal_current = |events: &mut Vec<LogEvent>,
                        size: &mut usize,
                        earliest: &mut Option<i64>,
                        batches: &mut Vec<SealedBatch>| {
        if !events.is_empty() {
            batches.push(SealedBatch {
                log_group: log_group.to_string(),
                log_stream: log_stream.to_string(),
                events: std::mem::take(events),
            });
            *size = 0;
            *earliest = None;
        }
    };

    for event in events {
        // Drop events outside CW allowed range
        if event.timestamp < cutoff || event.timestamp > max_future {
            tracing::debug!(
                timestamp = event.timestamp,
                cutoff = cutoff,
                max_future = max_future,
                "Dropping event outside CW allowed range"
            );
            continue;
        }

        // minimumLogLevel filtering for GG structured log format
        if let Some(min_level) = min_log_level {
            if should_filter_by_level(&event.message, min_level) {
                tracing::debug!(
                    min_level = ?min_level,
                    "Filtering event below minimum log level"
                );
                continue;
            }
        }

        // Chunk oversized messages
        let chunks = chunk_message(&event.message, event.timestamp);

        for chunk in chunks {
            if chunk.message.is_empty() {
                continue;
            }

            let wire_size = event_wire_size(chunk.message.len());

            // Check 23-hour span
            let earliest = earliest_ts.unwrap_or(chunk.timestamp);
            if chunk.timestamp - earliest > MAX_TIME_SPAN_MS {
                seal_current(
                    &mut current_events,
                    &mut current_size,
                    &mut earliest_ts,
                    &mut batches,
                );
            }

            // Check event count limit
            if current_events.len() >= MAX_NUM_OF_LOG_EVENTS {
                seal_current(
                    &mut current_events,
                    &mut current_size,
                    &mut earliest_ts,
                    &mut batches,
                );
            }

            // Check batch size limit
            if current_size + wire_size > MAX_BATCH_SIZE {
                seal_current(
                    &mut current_events,
                    &mut current_size,
                    &mut earliest_ts,
                    &mut batches,
                );
            }

            if earliest_ts.is_none() {
                earliest_ts = Some(chunk.timestamp);
            }
            current_size += wire_size;
            current_events.push(chunk);
        }
    }

    seal_current(
        &mut current_events,
        &mut current_size,
        &mut earliest_ts,
        &mut batches,
    );

    batches
}

/// Split a message into chunks of at most MAX_EVENT_BYTES bytes.
/// Each chunk gets the same timestamp (matching Java behavior).
fn chunk_message(message: &str, timestamp: i64) -> Vec<LogEvent> {
    let bytes = message.as_bytes();
    if bytes.len() <= MAX_EVENT_BYTES {
        return vec![LogEvent {
            timestamp,
            message: message.to_string(),
        }];
    }

    let num_chunks = (bytes.len() + MAX_EVENT_BYTES - 1) / MAX_EVENT_BYTES;
    tracing::debug!(
        size = bytes.len(),
        chunks = num_chunks,
        "Chunking oversized event into multiple CW events"
    );

    let mut chunks = Vec::with_capacity(num_chunks);
    let mut pos = 0;
    while pos < bytes.len() {
        let end = (pos + MAX_EVENT_BYTES).min(bytes.len());
        // Find a valid UTF-8 char boundary
        let mut boundary = end;
        while boundary > pos && !message.is_char_boundary(boundary) {
            boundary -= 1;
        }
        if boundary == pos {
            // Safety: can't happen with valid UTF-8 (max 4 bytes < MAX_EVENT_BYTES),
            // but guard against infinite loop.
            tracing::error!(
                pos = pos,
                total_bytes = bytes.len(),
                "Chunking failed: boundary walked back to pos, dropping remaining bytes"
            );
            break;
        }
        let chunk_str = &message[pos..boundary];
        if !chunk_str.is_empty() {
            chunks.push(LogEvent {
                timestamp,
                message: chunk_str.to_string(),
            });
        }
        pos = boundary;
    }
    chunks
}

/// Check if an event should be filtered based on minimumLogLevel.
/// Matches Java `tryGetStructuredLogMessage` + `checkAndAddNewLogEvent`:
/// - Lines starting with `{` are deserialized as JSON to extract the `level` field
/// - If level < min_level, the event is filtered
/// - Non-JSON lines (text logs, EMF) always pass through (Java behavior)
fn should_filter_by_level(message: &str, min_level: LogLevel) -> bool {
    // Java fast path: only attempt JSON parse if line starts with '{'
    if !message.starts_with('{') {
        return false;
    }
    if let Ok(msg) = serde_json::from_str::<GgLogMessage>(message) {
        if let Some(ref level) = msg.level {
            if let Some(event_level) = parse_log_level(level) {
                return level_ordinal(event_level) < level_ordinal(min_level);
            }
        }
    }
    false
}

/// Minimal struct for extracting level from Greengrass structured JSON logs.
/// Matches Java's `GreengrassLogMessage` — only the `level` field is needed for filtering.
#[derive(serde::Deserialize)]
struct GgLogMessage {
    level: Option<String>,
}

fn parse_log_level(s: &str) -> Option<LogLevel> {
    match s.trim() {
        "DEBUG" => Some(LogLevel::Debug),
        "INFO" => Some(LogLevel::Info),
        "WARN" => Some(LogLevel::Warn),
        "ERROR" => Some(LogLevel::Error),
        _ => None,
    }
}

fn level_ordinal(level: LogLevel) -> u8 {
    match level {
        LogLevel::Debug => 0,
        LogLevel::Info => 1,
        LogLevel::Warn => 2,
        LogLevel::Error => 3,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(ts: i64, msg: &str) -> LogEvent {
        LogEvent {
            timestamp: ts,
            message: msg.to_string(),
        }
    }

    fn now_ms() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    }

    #[test]
    fn test_single_event_single_batch() {
        let now = now_ms();
        let batches = seal_batches(vec![ev(now, "hello")], "/grp", "/stream", None, now);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].events.len(), 1);
        assert_eq!(batches[0].events[0].message, "hello");
        assert_eq!(batches[0].log_group, "/grp");
    }

    #[test]
    fn test_event_count_limit() {
        let now = now_ms();
        let events: Vec<LogEvent> = (0..10_001).map(|i| ev(now + i, "x")).collect();
        let batches = seal_batches(events, "/grp", "/s", None, now);
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].events.len(), 10_000);
        assert_eq!(batches[1].events.len(), 1);
    }

    #[test]
    fn test_batch_size_limit() {
        let now = now_ms();
        // Each event: 100 bytes msg + 8 + 26 = 134 bytes wire size
        // 1,048,576 / 134 = 7825.9 → 7825 events per batch
        let msg = "x".repeat(100);
        let events: Vec<LogEvent> = (0..8000).map(|i| ev(now + i, &msg)).collect();
        let batches = seal_batches(events, "/grp", "/s", None, now);
        assert!(batches.len() >= 2);
        // First batch should have ~7825 events
        let first_size: usize = batches[0]
            .events
            .iter()
            .map(|e| event_wire_size(e.message.len()))
            .sum();
        assert!(first_size <= MAX_BATCH_SIZE);
    }

    #[test]
    fn test_time_span_limit() {
        let now = now_ms();
        let hour_ms = 60 * 60 * 1000;
        // 23h + 1min gap exceeds the 23-hour span limit
        let events = vec![
            ev(now - 24 * hour_ms, "early"),
            ev(now - 24 * hour_ms + 23 * hour_ms + 60_000, "late"),
        ];
        let batches = seal_batches(events, "/grp", "/s", None, now);
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].events[0].message, "early");
        assert_eq!(batches[1].events[0].message, "late");
    }

    #[test]
    fn test_oversized_event_chunked() {
        let now = now_ms();
        let big_msg = "a".repeat(MAX_EVENT_BYTES * 2 + 100);
        let batches = seal_batches(vec![ev(now, &big_msg)], "/grp", "/s", None, now);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].events.len(), 3); // 2 full chunks + 1 remainder
        assert_eq!(batches[0].events[0].message.len(), MAX_EVENT_BYTES);
        assert_eq!(batches[0].events[1].message.len(), MAX_EVENT_BYTES);
        assert_eq!(batches[0].events[2].message.len(), 100);
        // All chunks have same timestamp
        assert!(batches[0].events.iter().all(|e| e.timestamp == now));
    }

    #[test]
    fn test_event_exactly_max_length() {
        let now = now_ms();
        let msg = "b".repeat(MAX_EVENT_BYTES);
        let batches = seal_batches(vec![ev(now, &msg)], "/grp", "/s", None, now);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].events.len(), 1);
        assert_eq!(batches[0].events[0].message.len(), MAX_EVENT_BYTES);
    }

    #[test]
    fn test_14_day_old_event_dropped() {
        let now = now_ms();
        let old = now - 15 * 24 * 60 * 60 * 1000; // 15 days ago
        let batches = seal_batches(vec![ev(old, "old")], "/grp", "/s", None, now);
        assert!(batches.is_empty());
    }

    #[test]
    fn test_13_day_old_event_kept() {
        let now = now_ms();
        let recent = now - 13 * 24 * 60 * 60 * 1000; // 13 days ago
        let batches = seal_batches(vec![ev(recent, "recent")], "/grp", "/s", None, now);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].events[0].message, "recent");
    }

    #[test]
    fn test_future_event_dropped() {
        let now = now_ms();
        let future = now + 3 * 60 * 60 * 1000; // 3 hours in future
        let batches = seal_batches(vec![ev(future, "future")], "/grp", "/s", None, now);
        assert!(batches.is_empty());
    }

    #[test]
    fn test_near_future_event_kept() {
        let now = now_ms();
        let near_future = now + 60 * 60 * 1000; // 1 hour in future
        let batches = seal_batches(vec![ev(near_future, "soon")], "/grp", "/s", None, now);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].events[0].message, "soon");
    }

    #[test]
    fn test_empty_message_skipped() {
        let now = now_ms();
        let batches = seal_batches(vec![ev(now, "")], "/grp", "/s", None, now);
        assert!(batches.is_empty());
    }

    #[test]
    fn test_events_sorted_by_timestamp() {
        let now = now_ms();
        let events = vec![ev(now + 2000, "c"), ev(now, "a"), ev(now + 1000, "b")];
        let batches = seal_batches(events, "/grp", "/s", None, now);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].events[0].message, "a");
        assert_eq!(batches[0].events[1].message, "b");
        assert_eq!(batches[0].events[2].message, "c");
    }

    #[test]
    fn test_mixed_old_oversized_normal() {
        let now = now_ms();
        let old = now - 15 * 24 * 60 * 60 * 1000;
        let big_msg = "x".repeat(MAX_EVENT_BYTES + 50);
        let events = vec![ev(old, "dropped"), ev(now, "normal"), ev(now + 1, &big_msg)];
        let batches = seal_batches(events, "/grp", "/s", None, now);
        assert_eq!(batches.len(), 1);
        // "dropped" is gone, "normal" + 2 chunks of big_msg
        assert_eq!(batches[0].events.len(), 3);
        assert_eq!(batches[0].events[0].message, "normal");
    }

    #[test]
    fn test_minimum_log_level_filtering() {
        let now = now_ms();
        let events = vec![
            // JSON-structured GG log — DEBUG filtered when min=WARN
            ev(
                now,
                r#"{"level":"DEBUG","message":"debug msg","timestamp":1000}"#,
            ),
            // JSON-structured GG log — INFO filtered when min=WARN
            ev(
                now + 1,
                r#"{"level":"INFO","message":"info msg","timestamp":1001}"#,
            ),
            // JSON-structured GG log — WARN passes
            ev(
                now + 2,
                r#"{"level":"WARN","message":"warn msg","timestamp":1002}"#,
            ),
            // JSON-structured GG log — ERROR passes
            ev(
                now + 3,
                r#"{"level":"ERROR","message":"error msg","timestamp":1003}"#,
            ),
            // Text-format log — always passes (Java doesn't filter text logs)
            ev(now + 4, "2024-01-15 10:30:45 [DEBUG] text debug msg"),
            // EMF JSON — always passes (no "level" field)
            ev(now + 5, r#"{"_aws":{"Timestamp":1234},"metric":1}"#),
        ];
        let batches = seal_batches(events, "/grp", "/s", Some(LogLevel::Warn), now);
        assert_eq!(batches.len(), 1);
        // JSON DEBUG and INFO filtered, WARN + ERROR + text DEBUG + EMF kept
        assert_eq!(batches[0].events.len(), 4);
        assert!(batches[0].events[0].message.contains("warn msg"));
        assert!(batches[0].events[1].message.contains("error msg"));
        assert!(batches[0].events[2].message.contains("text debug msg"));
        assert!(batches[0].events[3].message.contains("_aws"));
    }

    #[test]
    fn test_chunk_multibyte_boundary() {
        let now = now_ms();
        // Fill to near boundary then add a 4-byte emoji
        let base = "x".repeat(MAX_EVENT_BYTES - 2);
        let msg = format!("{base}🎉more");
        let batches = seal_batches(vec![ev(now, &msg)], "/grp", "/s", None, now);
        // Should chunk at a valid char boundary
        for batch in &batches {
            for event in &batch.events {
                assert!(event.message.is_char_boundary(event.message.len()));
            }
        }
    }

    #[test]
    fn test_empty_input() {
        let batches = seal_batches(vec![], "/grp", "/s", None, now_ms());
        assert!(batches.is_empty());
    }

    #[test]
    fn test_chunk_message_small() {
        let chunks = chunk_message("hello", 1000);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].message, "hello");
    }

    #[test]
    fn test_chunk_message_exact_boundary() {
        let msg = "a".repeat(MAX_EVENT_BYTES);
        let chunks = chunk_message(&msg, 1000);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].message.len(), MAX_EVENT_BYTES);
    }

    #[test]
    fn test_chunk_message_oversized() {
        let msg = "b".repeat(MAX_EVENT_BYTES * 3);
        let chunks = chunk_message(&msg, 1000);
        assert_eq!(chunks.len(), 3);
        assert!(chunks.iter().all(|c| c.timestamp == 1000));
    }

    #[test]
    fn test_level_filtering_non_json_passes() {
        // Text-format logs always pass through (Java behavior)
        assert!(!should_filter_by_level("just plain text", LogLevel::Error));
        assert!(!should_filter_by_level(
            "2024-01-15 10:30:45 [DEBUG] text log",
            LogLevel::Error
        ));
        assert!(!should_filter_by_level("short", LogLevel::Error));
    }

    // --- Boundary tests (exact-at-limit) ---

    #[test]
    fn test_14_day_boundary_exact_kept() {
        let now = now_ms();
        // Exactly 14 days ago — no clock drift since now_ms is injected
        let exactly_14d = now - 14 * 24 * 60 * 60 * 1000;
        let batches = seal_batches(vec![ev(exactly_14d, "boundary")], "/grp", "/s", None, now);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].events[0].message, "boundary");
    }

    #[test]
    fn test_14_day_boundary_one_ms_over_dropped() {
        let now = now_ms();
        let just_over = now - 14 * 24 * 60 * 60 * 1000 - 1;
        let batches = seal_batches(vec![ev(just_over, "gone")], "/grp", "/s", None, now);
        assert!(batches.is_empty());
    }

    #[test]
    fn test_2h_future_boundary_exact_kept() {
        let now = now_ms();
        // Exactly 2 hours in future — strict > means this is NOT dropped
        let exactly_2h = now + 2 * 60 * 60 * 1000;
        let batches = seal_batches(vec![ev(exactly_2h, "boundary")], "/grp", "/s", None, now);
        assert_eq!(batches.len(), 1);
    }

    #[test]
    fn test_2h_future_boundary_one_ms_over_dropped() {
        let now = now_ms();
        // Exactly 1ms over 2h — dropped
        let just_over = now + 2 * 60 * 60 * 1000 + 1;
        let batches = seal_batches(vec![ev(just_over, "gone")], "/grp", "/s", None, now);
        assert!(batches.is_empty());
    }

    #[test]
    fn test_23h_span_exact_same_batch() {
        let now = now_ms();
        let hour_ms = 60 * 60 * 1000;
        // Exactly 23h span — strict > means these stay in SAME batch
        let events = vec![
            ev(now - 24 * hour_ms, "early"),
            ev(now - 24 * hour_ms + 23 * hour_ms, "late"),
        ];
        let batches = seal_batches(events, "/grp", "/s", None, now);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].events.len(), 2);
    }

    #[test]
    fn test_exactly_10k_events_single_batch() {
        let now = now_ms();
        let events: Vec<LogEvent> = (0..10_000).map(|i| ev(now + i, "x")).collect();
        let batches = seal_batches(events, "/grp", "/s", None, now);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].events.len(), 10_000);
    }

    #[test]
    fn test_batch_overflow_not_dropped() {
        let now = now_ms();
        // Each 600KB message gets chunked into ~3 x 262KB chunks before batch size checks.
        // Total chunks exceed 1MB, forcing a batch split. "overflow" must not be dropped.
        let msg = "x".repeat(600_000);
        let events = vec![ev(now, &msg), ev(now + 1, &msg), ev(now + 2, "overflow")];
        let batches = seal_batches(events, "/grp", "/s", None, now);
        assert!(batches.len() >= 2);
        let all_msgs: Vec<&str> = batches
            .iter()
            .flat_map(|b| b.events.iter().map(|e| e.message.as_str()))
            .collect();
        assert!(all_msgs.contains(&"overflow"));
    }

    #[test]
    fn test_single_event_larger_than_max_batch() {
        let now = now_ms();
        // 4MB message — larger than MAX_BATCH_SIZE (1MB)
        let msg = "z".repeat(4 * 1024 * 1024);
        let batches = seal_batches(vec![ev(now, &msg)], "/grp", "/s", None, now);
        // Should produce multiple batches, each within 1MB
        assert!(batches.len() >= 4);
        for batch in &batches {
            let total: usize = batch
                .events
                .iter()
                .map(|e| event_wire_size(e.message.len()))
                .sum();
            assert!(total <= MAX_BATCH_SIZE);
        }
    }

    #[test]
    fn test_timestamp_zero_dropped() {
        // timestamp=0 is before the 14-day cutoff
        let batches = seal_batches(vec![ev(0, "epoch")], "/grp", "/s", None, now_ms());
        assert!(batches.is_empty());
    }

    #[test]
    fn test_negative_timestamp_dropped() {
        let batches = seal_batches(vec![ev(-1000, "negative")], "/grp", "/s", None, now_ms());
        assert!(batches.is_empty());
    }

    #[test]
    fn test_debug_passes_at_min_debug() {
        let now = now_ms();
        let events = vec![ev(
            now,
            r#"{"level":"DEBUG","message":"debug msg","timestamp":1000}"#,
        )];
        let batches = seal_batches(events, "/grp", "/s", Some(LogLevel::Debug), now);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].events.len(), 1);
    }
}
