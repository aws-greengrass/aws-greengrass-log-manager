// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! File reader with offset tracking and content hashing

use super::MAX_EVENT_SIZE;
use base64::{engine::general_purpose::STANDARD, Engine};
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

/// A parsed log event with timestamp and message
#[derive(Debug, Clone, PartialEq)]
pub struct LogEvent {
    pub timestamp: i64,
    pub message: String,
}

/// Compute SHA-256 content hash for file identity across rotations.
/// Returns Base64-encoded digest, compatible with Java LogManager checkpoints.
///
/// Hashes the first line (up to newline) or first 1024 bytes if no newline found.
pub fn compute_content_hash(path: &Path) -> std::io::Result<String> {
    let mut file = File::open(path)?;
    let mut buffer = [0u8; 1024];
    let bytes_read = file.read(&mut buffer)?;
    let data = &buffer[..bytes_read];
    // Match Java: decode bytes as UTF-8 string, then hash the string bytes
    let text = String::from_utf8_lossy(data);
    let hash_input = match text.find('\n') {
        Some(pos) => &text[..=pos],
        None => &text,
    };
    let mut hasher = Sha256::new();
    hasher.update(hash_input.as_bytes());
    Ok(STANDARD.encode(hasher.finalize()))
}

/// Read a file from the given byte offset, parsing lines as LogEvents.
/// Returns (events, new_offset). If file size < offset, resets to 0.
/// Invalid UTF-8 bytes are replaced with U+FFFD.
///
/// Note: reads remaining file content into memory. This is acceptable because each call
/// only reads the delta since the last checkpoint (typically a few KB per upload cycle).
/// For first-read of a large unchecked file, memory usage equals file size minus offset.
pub fn read_file_from_offset(
    path: &Path,
    offset: u64,
    default_timestamp: i64,
) -> std::io::Result<(Vec<LogEvent>, u64)> {
    let mut file = File::open(path)?;
    let file_size = file.metadata()?.len();

    let start_offset = if file_size < offset {
        tracing::warn!(path = %path.display(), file_size = file_size, offset = offset, "File truncated, resetting to start");
        0
    } else {
        offset
    };

    tracing::debug!(path = %path.display(), offset = start_offset, "Reading file from offset");
    file.seek(SeekFrom::Start(start_offset))?;

    let mut raw = Vec::new();
    file.read_to_end(&mut raw)?;
    let text = String::from_utf8_lossy(&raw);
    let new_offset = start_offset + raw.len() as u64;

    let events = text
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            let timestamp = extract_timestamp(line).unwrap_or(default_timestamp);
            let message = truncate_message(line);
            LogEvent { timestamp, message }
        })
        .collect();

    tracing::debug!(path = %path.display(), new_offset = new_offset, "File read complete");
    Ok((events, new_offset))
}

pub(crate) fn extract_timestamp(line: &str) -> Option<i64> {
    let bytes = line.as_bytes();
    // Epoch millis: exactly 13 digits at start (not part of a longer number), within realistic range
    if bytes.len() >= 13
        && bytes[0..13].iter().all(|b| b.is_ascii_digit())
        && (bytes.len() == 13 || !bytes[13].is_ascii_digit())
    {
        if let Ok(ts) = line[0..13].parse::<i64>() {
            if (1_000_000_000_000..=4_102_444_800_000).contains(&ts) {
                return Some(ts);
            }
        }
        return None;
    }
    // ISO-8601: YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD HH:MM:SS
    if bytes.len() >= 19
        && bytes[0..4].iter().all(|b| b.is_ascii_digit())
        && bytes[4] == b'-'
        && bytes[5..7].iter().all(|b| b.is_ascii_digit())
        && bytes[7] == b'-'
        && bytes[8..10].iter().all(|b| b.is_ascii_digit())
        && (bytes[10] == b'T' || bytes[10] == b' ')
        && bytes[11..13].iter().all(|b| b.is_ascii_digit())
        && bytes[13] == b':'
        && bytes[14..16].iter().all(|b| b.is_ascii_digit())
        && bytes[16] == b':'
        && bytes[17..19].iter().all(|b| b.is_ascii_digit())
    {
        return parse_iso8601(&line[0..19]);
    }
    None
}

pub(crate) fn parse_iso8601(s: &str) -> Option<i64> {
    let b = s.as_bytes();
    let year: i32 = std::str::from_utf8(&b[0..4]).ok()?.parse().ok()?;
    let month: u32 = std::str::from_utf8(&b[5..7]).ok()?.parse().ok()?;
    let day: u32 = std::str::from_utf8(&b[8..10]).ok()?.parse().ok()?;
    let hour: u32 = std::str::from_utf8(&b[11..13]).ok()?.parse().ok()?;
    let min: u32 = std::str::from_utf8(&b[14..16]).ok()?.parse().ok()?;
    let sec: u32 = std::str::from_utf8(&b[17..19]).ok()?.parse().ok()?;
    // Simple epoch calculation (assumes UTC, no leap seconds)
    let days = days_since_epoch(year, month, day)?;
    Some(
        (days as i64) * 86400000
            + (hour as i64) * 3600000
            + (min as i64) * 60000
            + (sec as i64) * 1000,
    )
}

fn days_since_epoch(year: i32, month: u32, day: u32) -> Option<i64> {
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    // Days from 1970-01-01
    let y = year as i64;
    let m = month as i64;
    let d = day as i64;
    // Days in years since 1970
    let mut days = (y - 1970) * 365;
    // Add leap years
    days += (y - 1969) / 4 - (y - 1901) / 100 + (y - 1601) / 400;
    // Days in months
    let month_days = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334];
    days += month_days[(m - 1) as usize] as i64;
    // Add leap day if after Feb in leap year
    if m > 2 && (y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)) {
        days += 1;
    }
    days += d - 1;
    Some(days)
}

fn truncate_message(s: &str) -> String {
    if s.len() <= MAX_EVENT_SIZE {
        s.to_string()
    } else {
        let mut end = MAX_EVENT_SIZE;
        while !s.is_char_boundary(end) {
            end -= 1;
        }
        s[..end].to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_read_from_offset_zero() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "line1").unwrap();
        writeln!(file, "line2").unwrap();

        let (events, offset) = read_file_from_offset(file.path(), 0, 1000).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].message, "line1");
        assert_eq!(events[1].message, "line2");
        assert_eq!(offset, 12); // "line1\nline2\n"
    }

    #[test]
    fn test_read_from_mid_file_offset() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "line1").unwrap();
        writeln!(file, "line2").unwrap();

        let (events, offset) = read_file_from_offset(file.path(), 6, 1000).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].message, "line2");
        assert_eq!(offset, 12);
    }

    #[test]
    fn test_truncation_detection() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "short").unwrap();

        // Offset larger than file size should reset to 0
        let (events, offset) = read_file_from_offset(file.path(), 1000, 1000).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].message, "short");
        assert_eq!(offset, 6);
    }

    #[test]
    fn test_timestamp_extraction_epoch_millis() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "1234567890123 message").unwrap();

        let (events, _) = read_file_from_offset(file.path(), 0, 9999).unwrap();
        assert_eq!(events[0].timestamp, 1234567890123);
    }

    #[test]
    fn test_timestamp_extraction_iso8601() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "2024-01-15T10:30:45 message").unwrap();

        let (events, _) = read_file_from_offset(file.path(), 0, 9999).unwrap();
        // 2024-01-15 10:30:45 UTC in millis
        assert_eq!(events[0].timestamp, 1705314645000);
    }

    #[test]
    fn test_timestamp_extraction_default() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "no timestamp here").unwrap();

        let (events, _) = read_file_from_offset(file.path(), 0, 5555).unwrap();
        assert_eq!(events[0].timestamp, 5555);
    }

    #[test]
    fn test_event_size_truncation() {
        let mut file = NamedTempFile::new().unwrap();
        let large_msg = "x".repeat(300_000);
        writeln!(file, "{}", large_msg).unwrap();

        let (events, _) = read_file_from_offset(file.path(), 0, 1000).unwrap();
        assert_eq!(events[0].message.len(), MAX_EVENT_SIZE);
    }

    #[test]
    fn test_empty_lines_skipped() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "line1").unwrap();
        writeln!(file).unwrap();
        writeln!(file, "line2").unwrap();

        let (events, _) = read_file_from_offset(file.path(), 0, 1000).unwrap();
        assert_eq!(events.len(), 2);
    }

    #[test]
    fn test_compute_content_hash_with_newline() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "first line").unwrap();
        writeln!(file, "second line").unwrap();

        let hash = compute_content_hash(file.path()).unwrap();
        // Hash should be of "first line\n" (first line including newline)
        assert_eq!(hash.len(), 44); // Base64 of SHA-256 (32 bytes) = 44 chars

        // Create another file with same first line
        let mut file2 = NamedTempFile::new().unwrap();
        writeln!(file2, "first line").unwrap();
        writeln!(file2, "different second line").unwrap();

        let hash2 = compute_content_hash(file2.path()).unwrap();
        assert_eq!(hash, hash2); // Same first line = same hash
    }

    #[test]
    fn test_compute_content_hash_no_newline() {
        let mut file = NamedTempFile::new().unwrap();
        write!(file, "no newline content").unwrap();

        let hash = compute_content_hash(file.path()).unwrap();
        assert_eq!(hash.len(), 44);
    }

    #[test]
    fn test_extract_timestamp_13_digits_followed_by_digit() {
        // 13 digits followed by another digit should NOT be extracted
        // (it's a 14+ digit number, not epoch millis)
        let result = extract_timestamp("12345678901234 message");
        assert!(result.is_none());
    }

    #[test]
    fn test_extract_timestamp_exactly_13_digits() {
        // Exactly 13 digits followed by non-digit should be extracted
        let result = extract_timestamp("1705314645000 message");
        assert_eq!(result, Some(1705314645000));
    }

    #[test]
    fn test_extract_timestamp_13_digits_at_end() {
        // 13 digits at end of string (no following char)
        let result = extract_timestamp("1705314645000");
        assert_eq!(result, Some(1705314645000));
    }

    #[test]
    fn test_extract_timestamp_out_of_range() {
        // Too small (before year 2001)
        let result = extract_timestamp("0000000000001 message");
        assert!(result.is_none());

        // Too large (after year 2100)
        let result = extract_timestamp("9999999999999 message");
        assert!(result.is_none());
    }

    #[test]
    fn test_read_file_with_invalid_utf8() {
        let mut file = NamedTempFile::new().unwrap();
        // Write valid UTF-8, then invalid bytes, then more valid UTF-8
        file.write_all(b"valid line\n").unwrap();
        file.write_all(&[0xFF, 0xFE]).unwrap(); // Invalid UTF-8
        file.write_all(b"after invalid\n").unwrap();

        let (events, _) = read_file_from_offset(file.path(), 0, 1000).unwrap();
        // First line is valid
        assert_eq!(events[0].message, "valid line");
        // Invalid bytes become replacement chars, rest of content preserved
        assert!(events[1].message.contains('\u{FFFD}'));
        assert!(events[1].message.contains("after invalid"));
    }

    #[test]
    fn test_parse_iso8601_invalid_month() {
        // Month 13 is invalid
        let result = parse_iso8601("2024-13-15T10:30:45");
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_iso8601_invalid_day() {
        // Day 32 is invalid
        let result = parse_iso8601("2024-01-32T10:30:45");
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_iso8601_leap_year() {
        // Feb 29 2024 (leap year)
        let result = parse_iso8601("2024-02-29T00:00:00");
        assert!(result.is_some());
        // Feb 29 2023 (not leap year) - still parses, validation is basic
        let result2 = parse_iso8601("2023-02-29T00:00:00");
        assert!(result2.is_some());
    }

    #[test]
    fn test_content_hash_matches_java_logfile_hash() {
        // Java LogFile.hashString() uses:
        //   1. Read up to 1024 bytes
        //   2. If newline found, take bytes up to and including newline
        //   3. Convert to String (UTF-8)
        //   4. SHA-256 the string bytes
        //   5. Base64-encode the digest
        //
        // Rust compute_content_hash() now matches this exactly.

        use sha2::{Digest, Sha256};

        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "first line").unwrap();
        writeln!(file, "second line").unwrap();

        let rust_hash = compute_content_hash(file.path()).unwrap();

        // Compute what Java would produce
        let input = "first line\n";
        let digest = Sha256::digest(input.as_bytes());
        let java_hash = STANDARD.encode(digest);

        assert_eq!(
            rust_hash, java_hash,
            "Rust and Java hashes must match for checkpoint compatibility"
        );
        assert_eq!(rust_hash.len(), 44, "Base64 encoding is 44 chars");
    }

    #[test]
    fn test_truncate_message_multibyte_char_boundary() {
        // Create a string with multibyte UTF-8 characters that would be split
        // at a non-char-boundary if we just truncated at MAX_EVENT_SIZE
        let emoji = "🎉"; // 4 bytes
        let base = "x".repeat(MAX_EVENT_SIZE - 2);
        let large_msg = format!("{}{}", base, emoji);

        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "{}", large_msg).unwrap();

        let (events, _) = read_file_from_offset(file.path(), 0, 1000).unwrap();
        // Should truncate at a valid char boundary
        assert!(events[0].message.len() <= MAX_EVENT_SIZE);
        assert!(events[0].message.is_char_boundary(events[0].message.len()));
    }
}
