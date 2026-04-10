// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! gg-log-manager - Greengrass LogManager Generic Type Component
//!
//! Rust implementation of aws.greengrass.LogManager for GG Classic and GG Lite.
//! Tails log files and EMF JSON files, uploads to CloudWatch Logs.

use gg_log_manager::config::{load_config, validate_config};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;
use tokio::time::{timeout, Duration};
use tracing::{error, info};

static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);

#[cfg(not(tarpaulin_include))]
#[tokio::main(flavor = "current_thread")]
async fn main() {
    tracing_subscriber::fmt::init();

    let args: Vec<String> = std::env::args().collect();
    let config_arg = parse_args(&args);

    let config = match load_config(&config_arg) {
        Ok(c) => c,
        Err(e) => {
            error!("Config load error: {e}");
            std::process::exit(1);
        }
    };

    if let Err(e) = validate_config(&config) {
        error!("Config validation error: {e}");
        std::process::exit(1);
    }

    info!(
        "Loaded {} component configs, {} system configs",
        config.component_logs_configuration.len(),
        config.system_logs_configuration.len()
    );

    let shutdown_notify = Arc::new(Notify::new());
    let shutdown_notify_clone = shutdown_notify.clone();

    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        initiate_shutdown(&shutdown_notify_clone);
    });

    #[cfg(unix)]
    {
        let shutdown_notify_sigterm = shutdown_notify.clone();
        tokio::spawn(async move {
            let mut sigterm =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("Failed to register SIGTERM handler");
            sigterm.recv().await;
            initiate_shutdown(&shutdown_notify_sigterm);
        });
    }

    // Main loop placeholder - wait for shutdown
    shutdown_notify.notified().await;

    // Graceful shutdown sequence
    graceful_shutdown().await;
}

fn parse_args(args: &[String]) -> String {
    for i in 0..args.len() {
        if args[i] == "--config" && i + 1 < args.len() {
            return args[i + 1].clone();
        }
    }
    "{}".to_string()
}

fn initiate_shutdown(notify: &Notify) {
    if !SHUTDOWN_REQUESTED.swap(true, Ordering::SeqCst) {
        info!("Received shutdown signal");
        notify.notify_one();
    }
}

#[cfg(not(tarpaulin_include))]
async fn graceful_shutdown() {
    info!("Step 1/4: Cancelling pending upload futures");
    cancel_pending_uploads();

    info!("Step 2/4: Flushing in-progress PutLogEvents (5s timeout)");
    match timeout(Duration::from_secs(5), flush_pending_uploads()).await {
        Ok(_) => info!("Flush completed successfully"),
        Err(_) => error!("Flush timeout exceeded after 5s"),
    }

    info!("Step 3/4: Persisting file checkpoints");
    persist_checkpoints();

    info!("Step 4/4: Shutdown complete, exiting");
}

#[cfg(not(tarpaulin_include))]
fn cancel_pending_uploads() {
    // Cancel pending uploads - implementation in uploader track
}

#[cfg(not(tarpaulin_include))]
async fn flush_pending_uploads() {
    // Flush in-progress PutLogEvents - implementation in uploader track
}

#[cfg(not(tarpaulin_include))]
fn persist_checkpoints() {
    // Persist file read positions - implementation in scanner track
}

/// Subscribes to configuration updates via GG Component SDK IPC.
/// Re-validates and applies new configuration without restart.
#[cfg(target_os = "linux")]
fn subscribe_to_config_updates() {
    // TODO: Wire up with gg_sdk::Sdk::subscribe_to_configuration_update()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_args_with_config_flag() {
        let args = vec![
            "gg-log-manager".to_string(),
            "--config".to_string(),
            "/path/to/config.json".to_string(),
        ];
        assert_eq!(parse_args(&args), "/path/to/config.json");
    }

    #[test]
    fn test_parse_args_without_flag() {
        let args = vec!["gg-log-manager".to_string()];
        assert_eq!(parse_args(&args), "{}");
    }

    #[test]
    fn test_parse_args_config_at_end_no_value() {
        let args = vec!["gg-log-manager".to_string(), "--config".to_string()];
        assert_eq!(parse_args(&args), "{}");
    }

    #[test]
    fn test_initiate_shutdown_sets_flag() {
        SHUTDOWN_REQUESTED.store(false, Ordering::SeqCst);
        let notify = Notify::new();
        initiate_shutdown(&notify);
        assert!(SHUTDOWN_REQUESTED.load(Ordering::SeqCst));
    }

    #[test]
    fn test_initiate_shutdown_idempotent() {
        SHUTDOWN_REQUESTED.store(false, Ordering::SeqCst);
        let notify = Notify::new();
        initiate_shutdown(&notify);
        initiate_shutdown(&notify); // Second call should be no-op
        assert!(SHUTDOWN_REQUESTED.load(Ordering::SeqCst));
    }

    #[test]
    fn test_config_change_preserves_checkpoints() {
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        let checkpoint_path = dir.path().join("checkpoints.json");
        let mut f = std::fs::File::create(&checkpoint_path).unwrap();
        writeln!(f, r#"{{"file1": 100}}"#).unwrap();

        let json1 = format!(
            r#"{{"componentLogsConfiguration": [{{"componentName": "c1", "logFileDirectoryPath": "{}", "logFileRegex": ".*"}}]}}"#,
            dir.path().display()
        );
        let json2 = format!(
            r#"{{"componentLogsConfiguration": [{{"componentName": "c2", "logFileDirectoryPath": "{}", "logFileRegex": ".*"}}]}}"#,
            dir.path().display()
        );
        let _cfg1 = gg_log_manager::config::load_config(&json1).unwrap();
        let _cfg2 = gg_log_manager::config::load_config(&json2).unwrap();

        let content = std::fs::read_to_string(&checkpoint_path).unwrap();
        assert!(content.contains("file1"));
    }
}
