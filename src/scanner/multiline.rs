// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! Multi-line log assembly

use super::reader::{extract_timestamp, LogEvent};
use super::MAX_EVENT_SIZE;
use regex::Regex;
use std::sync::LazyLock;
use std::time::SystemTime;

/// Sentinel value indicating no timestamp was parsed for a LogEvent.
const NO_TIMESTAMP: i64 = 0;

/// Default multiline start pattern for log lines starting with a date or JSON.
/// Matches lines starting with a date (optionally in brackets/parens) or JSON object.
static DEFAULT_MULTILINE_PATTERN: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[\[(]?\d{4}-\d\d-\d\d|^\{").unwrap());

/// Assemble lines into LogEvents based on multi-line start pattern.
/// When start_pattern is None, the default pattern is applied.
/// When start_pattern is Some, that pattern is used.
/// Lines are buffered until a new match, then emitted concatenated (no separator).
///
/// # Examples
///
/// ```
/// use gg_log_manager::scanner::{assemble_multiline, LogEvent};
/// use regex::Regex;
///
/// let lines = vec![
///     LogEvent { timestamp: 1000, message: "2024-01-15 ERROR something".into() },
///     LogEvent { timestamp: 0, message: "  stack trace".into() },
/// ];
/// let pattern = Regex::new(r"^\d{4}-\d{2}-\d{2}").unwrap();
/// let events = assemble_multiline(lines, Some(&pattern));
/// assert_eq!(events.len(), 1);
/// ```
#[must_use = "assembled events must be consumed"]
pub fn assemble_multiline(lines: Vec<LogEvent>, start_pattern: Option<&Regex>) -> Vec<LogEvent> {
    let pattern = start_pattern.unwrap_or(&DEFAULT_MULTILINE_PATTERN);
    let mut events = Vec::with_capacity(lines.len());
    let mut buffer: Vec<String> = Vec::new();
    let mut first_timestamp: Option<i64> = None;

    for event in lines {
        if pattern.is_match(&event.message) && !buffer.is_empty() {
            events.push(emit_buffered(&buffer, first_timestamp));
            buffer.clear();
            first_timestamp = None;
        }
        if !event.message.trim().is_empty() {
            if buffer.is_empty() {
                first_timestamp = Some(event.timestamp);
            }
            buffer.push(event.message);
        }
    }
    if !buffer.is_empty() {
        events.push(emit_buffered(&buffer, first_timestamp));
    }
    tracing::debug!(event_count = events.len(), "Multiline assembly complete");
    events
}

fn emit_buffered(buffer: &[String], first_event_timestamp: Option<i64>) -> LogEvent {
    debug_assert!(!buffer.is_empty(), "emit_buffered called with empty buffer");
    let timestamp = first_event_timestamp
        .and_then(|ts| if ts != NO_TIMESTAMP { Some(ts) } else { None })
        .or_else(|| extract_timestamp(&buffer[0]))
        .unwrap_or_else(|| {
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(NO_TIMESTAMP)
        });
    // Join without separator to match Java's StringBuilder.append(partialLogLine) behavior.
    let joined = buffer.join("");
    let message = if joined.len() <= MAX_EVENT_SIZE {
        joined
    } else {
        let first_line = &buffer[0][..80.min(buffer[0].len())];
        tracing::warn!(
            original_size = joined.len(),
            max_size = MAX_EVENT_SIZE,
            first_line,
            "Truncating oversized multiline event"
        );
        let mut end = MAX_EVENT_SIZE;
        while !joined.is_char_boundary(end) {
            end -= 1;
        }
        joined[..end].to_string()
    };
    LogEvent { timestamp, message }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(ts: i64, msg: &str) -> LogEvent {
        LogEvent {
            timestamp: ts,
            message: msg.into(),
        }
    }

    #[test]
    fn test_default_pattern_groups_non_matching_lines() {
        // With None pattern, the default Java pattern is applied.
        // Lines without date/JSON prefix are grouped as continuations.
        let lines = vec![ev(1000, "line1"), ev(1000, "line2"), ev(1000, "line3")];
        let events = assemble_multiline(lines, None);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].message, "line1line2line3");
    }

    #[test]
    fn test_default_pattern_splits_on_date() {
        let lines = vec![
            ev(1000, "2024-01-15 first event"),
            ev(1000, "  continuation"),
            ev(1000, "2024-01-16 second event"),
        ];
        let events = assemble_multiline(lines, None);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].message, "2024-01-15 first event  continuation");
        assert_eq!(events[1].message, "2024-01-16 second event");
    }

    #[test]
    fn test_default_pattern_splits_on_json() {
        let lines = vec![ev(1000, r#"{"key":"val1"}"#), ev(1000, r#"{"key":"val2"}"#)];
        let events = assemble_multiline(lines, None);
        assert_eq!(events.len(), 2);
    }

    #[test]
    fn test_multiline_assembly() {
        let pattern = Regex::new(r"^\d{4}-\d{2}-\d{2}").unwrap();
        let lines = vec![
            ev(1705276800000, "2024-01-15 ERROR something"),
            ev(0, "  stack trace line 1"),
            ev(0, "  stack trace line 2"),
            ev(1705363200000, "2024-01-16 INFO next event"),
        ];
        let events = assemble_multiline(lines, Some(&pattern));
        assert_eq!(events.len(), 2);
        assert_eq!(
            events[0].message,
            "2024-01-15 ERROR something  stack trace line 1  stack trace line 2"
        );
        assert_eq!(events[1].message, "2024-01-16 INFO next event");
    }

    #[test]
    fn test_eof_flush() {
        let pattern = Regex::new(r"^START").unwrap();
        let lines = vec![ev(1000, "START event1"), ev(0, "continuation")];
        let events = assemble_multiline(lines, Some(&pattern));
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].message, "START event1continuation");
    }

    #[test]
    fn test_empty_input() {
        let events = assemble_multiline(vec![], None);
        assert!(events.is_empty());

        let pattern = Regex::new(r"^START").unwrap();
        let events = assemble_multiline(vec![], Some(&pattern));
        assert!(events.is_empty());
    }

    #[test]
    fn test_empty_lines_skipped() {
        let lines = vec![
            ev(1000, "line1"),
            ev(0, ""),
            ev(0, "   "),
            ev(1000, "line2"),
        ];
        let events = assemble_multiline(lines, None);
        // Default pattern groups non-matching lines; empty lines are filtered
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].message, "line1line2");
    }

    #[test]
    fn test_timestamp_extraction_in_multiline() {
        let pattern = Regex::new(r"^\d{13}").unwrap();
        let lines = vec![
            ev(1705314645000, "1705314645000 first event"),
            ev(0, "continuation"),
            ev(1705314646000, "1705314646000 second event"),
        ];
        let events = assemble_multiline(lines, Some(&pattern));
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].timestamp, 1705314645000);
        assert_eq!(events[1].timestamp, 1705314646000);
    }

    #[test]
    fn test_timestamp_fallback_to_system_time() {
        let lines = vec![ev(0, "no timestamp here")];
        let before = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let events = assemble_multiline(lines, None);
        let after = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        // Timestamp should be current time, not the default
        assert!(events[0].timestamp >= before && events[0].timestamp <= after);
    }

    #[test]
    fn test_orphan_continuation_lines() {
        let pattern = Regex::new(r"^START").unwrap();
        let lines = vec![
            ev(0, "  orphan continuation 1"),
            ev(0, "  orphan continuation 2"),
            ev(1000, "START actual event"),
        ];
        let events = assemble_multiline(lines, Some(&pattern));
        assert_eq!(events.len(), 2);
        assert_eq!(
            events[0].message,
            "  orphan continuation 1  orphan continuation 2"
        );
        assert_eq!(events[1].message, "START actual event");
    }

    #[test]
    fn test_only_orphan_lines() {
        let pattern = Regex::new(r"^START").unwrap();
        let lines = vec![ev(0, "  orphan line 1"), ev(0, "  orphan line 2")];
        let events = assemble_multiline(lines, Some(&pattern));
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].message, "  orphan line 1  orphan line 2");
    }

    #[test]
    fn test_oversized_multiline_truncation() {
        let pattern = Regex::new(r"^START").unwrap();
        let large_line = "x".repeat(200_000);
        let lines = vec![
            ev(1000, "START event"),
            ev(0, &large_line),
            ev(0, &large_line),
        ];
        let events = assemble_multiline(lines, Some(&pattern));
        assert_eq!(events.len(), 1);
        assert!(events[0].message.len() <= MAX_EVENT_SIZE);
    }

    #[test]
    fn test_regex_pathological_pattern_no_crash() {
        let pattern = Regex::new(r"^(a+)+$").unwrap();
        let lines = vec![ev(1000, "aaaaaaaaaaaaaaaaab")];
        let events = assemble_multiline(lines, Some(&pattern));
        assert!(!events.is_empty());
    }

    #[test]
    fn test_orphan_lines_flushed_on_pattern_change() {
        let pattern1 = Regex::new(r"^START").unwrap();
        let lines1 = vec![ev(0, "  orphan line"), ev(1000, "START event")];
        let events1 = assemble_multiline(lines1, Some(&pattern1));
        assert_eq!(events1.len(), 2);

        let pattern2 = Regex::new(r"^BEGIN").unwrap();
        let lines2 = vec![ev(0, "  leftover orphan"), ev(2000, "BEGIN new event")];
        let events2 = assemble_multiline(lines2, Some(&pattern2));
        assert_eq!(events2.len(), 2);
        assert_eq!(events2[0].message, "  leftover orphan");
        assert_eq!(events2[1].message, "BEGIN new event");
    }
}
