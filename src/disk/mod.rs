// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! Disk space management — per-source log directory size limits.
//!
//! `enforce_disk_limit` runs every scan cycle: it enumerates the directory,
//! classifies each file (active / uploaded-safe / un-uploaded), and deletes the
//! oldest eligible files until under `diskSpaceLimit`. Uploaded-safe files are
//! reclaimed first; un-uploaded files only when the caller opts in. The newest
//! (active) file is never deleted, and each file is re-stat'd immediately before
//! unlink so one that has since grown into the active file is left alone.

use regex::Regex;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

/// Files deleted by [`enforce_disk_limit`], split by upload status.
#[derive(Debug, Default, PartialEq, Eq)]
#[must_use = "disk-enforcement outcome records which files were deleted"]
pub struct EnforceOutcome {
    /// Fully-uploaded ("uploaded-safe") files that were deleted.
    pub uploaded_deleted: Vec<PathBuf>,
    /// Un-uploaded files deleted under disk pressure (only when `allow_unuploaded`).
    pub unuploaded_deleted: Vec<PathBuf>,
}

/// A matching regular file found during directory enumeration.
#[derive(Debug)]
struct DiskEntry {
    path: PathBuf,
    size: u64,
    mtime: SystemTime,
}

/// Enumerate top-level regular files in `dir` matching `pattern`.
/// Skips symlinks and subdirectories (mirrors the scanner).
fn enumerate_files(dir: &Path, pattern: &Regex) -> Vec<DiskEntry> {
    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) => {
            tracing::warn!(dir = %dir.display(), error = %e, "Cannot read directory for disk management");
            return Vec::new();
        }
    };
    entries
        .filter_map(Result::ok)
        .filter_map(|e| {
            if !e.file_type().ok()?.is_file() {
                return None;
            }
            if !pattern.is_match(&e.file_name().to_string_lossy()) {
                return None;
            }
            let meta = e.metadata().ok()?;
            Some(DiskEntry {
                path: e.path(),
                size: meta.len(),
                mtime: meta.modified().unwrap_or(SystemTime::UNIX_EPOCH),
            })
        })
        .collect()
}

fn mtime_ms(t: SystemTime) -> u64 {
    t.duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Re-stat immediately before unlink (TOCTOU guard) and delete.
/// Returns freed bytes on success, or `None` if the file has become the
/// newest/active file, vanished, or could not be removed.
fn restat_and_unlink(path: &Path, newest: SystemTime) -> Option<u64> {
    let meta = fs::metadata(path).ok()?;
    if meta.modified().unwrap_or(SystemTime::UNIX_EPOCH) >= newest {
        // Grew into the newest/active file since enumeration — do not delete.
        tracing::debug!(path = %path.display(), "Skipping delete: file is now newest/active");
        return None;
    }
    match fs::remove_file(path) {
        Ok(()) => Some(meta.len()),
        Err(e) => {
            tracing::warn!(path = %path.display(), error = %e, "Failed to delete file, skipping");
            None
        }
    }
}

// Real-filesystem-usage-based enforcement (statvfs) is not supported: enforcement is a
// per-source quota on each source's own directory, not a check of actual device free space.
// It therefore does not protect against whole-device disk exhaustion (e.g. another process
// filling the disk), and this component can only ever delete its own files.

// TODO: apply Bytes/EpochMs (and Duration for sec/ms values) newtypes crate-wide instead of
// bare u64s. The epoch-ms fields serialize into the backward-compatible checkpoint format, so
// this needs #[serde(transparent)] plus a wire-format round-trip test against the legacy layout.
/// Enforce `limit_bytes` on `dir` by deleting the oldest eligible files.
///
/// Enumerates `dir` directly and classifies each matching file: newest mtime = **active**
/// (never deleted); `mtime <= last_ts_ms && !is_tracked` = **uploaded-safe**; the rest =
/// **un-uploaded**. Deletes uploaded-safe oldest-first until under limit, then (only if
/// `allow_unuploaded`) un-uploaded oldest-first, WARN each. Files are re-stat'd before unlink.
pub fn enforce_disk_limit(
    dir: &Path,
    pattern: &Regex,
    limit_bytes: u64,
    last_ts_ms: u64,
    is_tracked: impl Fn(&Path) -> bool,
    allow_unuploaded: bool,
) -> EnforceOutcome {
    let mut outcome = EnforceOutcome::default();

    let entries = enumerate_files(dir, pattern);
    let total: u64 = entries.iter().map(|e| e.size).sum();
    if total <= limit_bytes {
        return outcome;
    }

    // Active file(s) = newest mtime; never deleted (ties treated as active for safety).
    let newest = entries
        .iter()
        .map(|e| e.mtime)
        .max()
        .unwrap_or(SystemTime::UNIX_EPOCH);

    let mut uploaded_safe: Vec<&DiskEntry> = Vec::new();
    let mut unuploaded: Vec<&DiskEntry> = Vec::new();
    for e in &entries {
        if e.mtime >= newest {
            continue; // active — never delete
        }
        if mtime_ms(e.mtime) <= last_ts_ms && !is_tracked(&e.path) {
            uploaded_safe.push(e);
        } else {
            unuploaded.push(e);
        }
    }
    uploaded_safe.sort_by_key(|e| e.mtime); // oldest first
    unuploaded.sort_by_key(|e| e.mtime);

    let mut remaining = total;

    // Phase 1: reclaim uploaded-safe files first.
    for e in uploaded_safe {
        if remaining <= limit_bytes {
            break;
        }
        if let Some(freed) = restat_and_unlink(&e.path, newest) {
            tracing::info!(path = %e.path.display(), freed_bytes = freed, "Deleted uploaded log file under disk pressure");
            remaining = remaining.saturating_sub(freed);
            outcome.uploaded_deleted.push(e.path.clone());
        }
    }

    // Phase 2: reclaim un-uploaded files only when opted in and still over.
    if allow_unuploaded {
        for e in unuploaded {
            if remaining <= limit_bytes {
                break;
            }
            if let Some(freed) = restat_and_unlink(&e.path, newest) {
                tracing::warn!(
                    path = %e.path.display(),
                    freed_bytes = freed,
                    reason = "disk pressure, not yet uploaded",
                    "Deleted un-uploaded log file under disk pressure"
                );
                remaining = remaining.saturating_sub(freed);
                outcome.unuploaded_deleted.push(e.path.clone());
            }
        }
    }

    if !outcome.unuploaded_deleted.is_empty() {
        tracing::warn!(
            count = outcome.unuploaded_deleted.len(),
            dir = %dir.display(),
            "Deleted un-uploaded files under disk pressure"
        );
    } else if outcome.uploaded_deleted.is_empty() {
        // Over limit but nothing eligible to reclaim (e.g. only the active file, or
        // all remaining files are un-uploaded and un-uploaded shedding is disabled).
        tracing::warn!(
            dir = %dir.display(),
            total_bytes = total,
            limit_bytes,
            "Over disk limit but no eligible files to delete"
        );
    }

    outcome
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use tempfile::TempDir;

    fn create_file(dir: &Path, name: &str, size: u64) -> PathBuf {
        let path = dir.join(name);
        let f = File::create(&path).expect("failed to create test file");
        f.set_len(size).expect("failed to set file length");
        path
    }

    fn set_mtime(path: &Path, secs_ago: i64) {
        let now = i64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock before UNIX_EPOCH")
                .as_secs(),
        )
        .expect("timestamp overflow");
        let t = filetime::FileTime::from_unix_time(now - secs_ago, 0);
        filetime::set_file_mtime(path, t).expect("failed to set mtime");
    }

    fn log_pattern() -> Regex {
        Regex::new(r"\.log$").expect("invalid test regex")
    }

    /// Epoch-millis for `secs` seconds ago — matches how `set_mtime` sets mtimes.
    fn ts_ms_secs_ago(secs: i64) -> u64 {
        let now = i64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock before UNIX_EPOCH")
                .as_secs(),
        )
        .expect("timestamp overflow");
        u64::try_from((now - secs) * 1000).expect("negative timestamp")
    }

    fn untracked(_: &Path) -> bool {
        false
    }

    // T1 (default): over limit, allow_unuploaded=false → deletes only uploaded-safe
    // oldest-first; never the active file; never un-uploaded files.
    #[test]
    fn enforce_deletes_only_uploaded_safe_oldest_first() {
        let tmp = TempDir::new().unwrap();
        let active = create_file(tmp.path(), "active.log", 100);
        let unup = create_file(tmp.path(), "unup.log", 100);
        let up_old = create_file(tmp.path(), "up_old.log", 100);
        let up_mid = create_file(tmp.path(), "up_mid.log", 100);
        set_mtime(&active, 10);
        set_mtime(&unup, 50);
        set_mtime(&up_mid, 200);
        set_mtime(&up_old, 300);
        let last_ts = ts_ms_secs_ago(150);

        // total=400, limit=250 → reclaim the two uploaded-safe files.
        let outcome =
            enforce_disk_limit(tmp.path(), &log_pattern(), 250, last_ts, untracked, false);

        assert_eq!(
            outcome.uploaded_deleted,
            vec![up_old.clone(), up_mid.clone()]
        );
        assert!(outcome.unuploaded_deleted.is_empty());
        assert!(!up_old.exists() && !up_mid.exists());
        assert!(active.exists(), "active file must never be deleted");
        assert!(
            unup.exists(),
            "un-uploaded file must not be deleted by default"
        );
    }

    // T2 (opt-in): still over after uploaded-safe removed → deletes oldest un-uploaded.
    // Asserts a file with mtime > last_ts (un-uploaded) is deleted — the differentiator.
    #[test]
    fn enforce_deletes_unuploaded_when_still_over_and_opted_in() {
        let tmp = TempDir::new().unwrap();
        let active = create_file(tmp.path(), "active.log", 100);
        let unup = create_file(tmp.path(), "unup.log", 100);
        let up_old = create_file(tmp.path(), "up_old.log", 100);
        let up_mid = create_file(tmp.path(), "up_mid.log", 100);
        set_mtime(&active, 10);
        set_mtime(&unup, 50);
        set_mtime(&up_mid, 200);
        set_mtime(&up_old, 300);
        let last_ts = ts_ms_secs_ago(150);

        // total=400, limit=150 → reclaim both uploaded-safe, then the oldest un-uploaded.
        let outcome = enforce_disk_limit(tmp.path(), &log_pattern(), 150, last_ts, untracked, true);

        assert_eq!(outcome.uploaded_deleted, vec![up_old, up_mid]);
        assert_eq!(outcome.unuploaded_deleted, vec![unup.clone()]);
        assert!(
            !unup.exists(),
            "un-uploaded (mtime>last_ts) file should be deleted when opted in"
        );
        assert!(active.exists(), "active file must never be deleted");
    }

    // T3: a single active file alone exceeds the limit → nothing deleted, no thrash/panic.
    #[test]
    fn enforce_single_active_file_over_limit_deletes_nothing() {
        let tmp = TempDir::new().unwrap();
        let active = create_file(tmp.path(), "active.log", 1000);
        set_mtime(&active, 10);

        let outcome = enforce_disk_limit(tmp.path(), &log_pattern(), 100, 0, untracked, true);

        assert!(outcome.uploaded_deleted.is_empty());
        assert!(outcome.unuploaded_deleted.is_empty());
        assert!(active.exists());
    }

    // T4 (negative): under limit → no-op regardless of classification.
    #[test]
    fn enforce_noop_when_under_limit() {
        let tmp = TempDir::new().unwrap();
        let a = create_file(tmp.path(), "a.log", 100);
        let b = create_file(tmp.path(), "b.log", 100);
        set_mtime(&a, 200);
        set_mtime(&b, 100);

        let outcome = enforce_disk_limit(
            tmp.path(),
            &log_pattern(),
            1000,
            ts_ms_secs_ago(50),
            untracked,
            true,
        );

        assert_eq!(outcome, EnforceOutcome::default());
        assert!(a.exists() && b.exists());
    }

    // A file with mtime <= last_ts but still tracked in the checkpoint is classified
    // un-uploaded (in-progress), so the default (uploaded-safe only) pass must not delete it.
    #[test]
    fn enforce_tracked_file_is_not_uploaded_safe() {
        let tmp = TempDir::new().unwrap();
        let active = create_file(tmp.path(), "active.log", 100);
        let tracked = create_file(tmp.path(), "tracked.log", 100);
        let untracked_old = create_file(tmp.path(), "untracked.log", 100);
        set_mtime(&active, 10);
        set_mtime(&untracked_old, 250);
        set_mtime(&tracked, 300);
        let last_ts = ts_ms_secs_ago(150);

        let tracked_path = tracked.clone();
        // total=300, limit=150, default pass. Only the untracked old file is uploaded-safe.
        let outcome = enforce_disk_limit(
            tmp.path(),
            &log_pattern(),
            150,
            last_ts,
            move |p| p == tracked_path.as_path(),
            false,
        );

        assert_eq!(outcome.uploaded_deleted, vec![untracked_old.clone()]);
        assert!(outcome.unuploaded_deleted.is_empty());
        assert!(!untracked_old.exists());
        assert!(
            tracked.exists(),
            "tracked (in-progress) file must not be deleted"
        );
        assert!(active.exists());
    }

    // Missing directory is a safe no-op (nothing to enumerate).
    #[test]
    fn enforce_missing_directory_is_noop() {
        let missing = Path::new("/nonexistent/path/enforce/12345");
        let outcome = enforce_disk_limit(missing, &log_pattern(), 0, 0, untracked, true);
        assert_eq!(outcome, EnforceOutcome::default());
    }

    // Files matching the pattern total under the limit even though a non-matching file
    // inflates the directory → no-op (only matching files count toward the limit).
    #[test]
    fn enforce_pattern_filter_excludes_non_matching_files() {
        let tmp = TempDir::new().unwrap();
        // Non-log file inflates the directory but must not count toward the limit.
        create_file(tmp.path(), "config.json", 1000);
        let log_file = create_file(tmp.path(), "app.log", 100);
        set_mtime(&log_file, 100);

        // Only 100 bytes of .log files → under the 200-byte limit → nothing deleted.
        let outcome = enforce_disk_limit(
            tmp.path(),
            &log_pattern(),
            200,
            ts_ms_secs_ago(50),
            untracked,
            true,
        );

        assert_eq!(outcome, EnforceOutcome::default());
        assert!(log_file.exists());
    }

    // Undeletable files (read-only directory) are skipped without panicking; the outcome
    // reports nothing deleted because the unlink failed.
    #[cfg(unix)]
    #[test]
    fn enforce_skips_undeletable_files() {
        use std::os::unix::fs::PermissionsExt;

        // Skip if running as root (root ignores permission bits).
        if std::process::Command::new("id")
            .arg("-u")
            .output()
            .map(|o| o.stdout.starts_with(b"0"))
            .unwrap_or(false)
        {
            return;
        }

        let tmp = TempDir::new().unwrap();
        let active = create_file(tmp.path(), "active.log", 200);
        let old = create_file(tmp.path(), "old.log", 200);
        set_mtime(&active, 10);
        set_mtime(&old, 200);

        // Make the directory read-only so files cannot be unlinked.
        fs::set_permissions(tmp.path(), fs::Permissions::from_mode(0o555)).unwrap();

        // total=400, limit=100, opted in → wants to delete `old`, but cannot.
        let outcome = enforce_disk_limit(
            tmp.path(),
            &log_pattern(),
            100,
            ts_ms_secs_ago(50),
            untracked,
            true,
        );

        assert!(outcome.uploaded_deleted.is_empty());
        assert!(outcome.unuploaded_deleted.is_empty());
        assert!(old.exists());
        assert!(active.exists());

        // Restore permissions so the TempDir can be cleaned up.
        fs::set_permissions(tmp.path(), fs::Permissions::from_mode(0o755)).unwrap();
    }
}
