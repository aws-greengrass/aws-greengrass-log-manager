// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! Configuration schema structs matching Java LogManager JSON schema

use serde::{Deserialize, Serialize};
use std::fmt;

pub const DEFAULT_UPLOAD_INTERVAL_SEC: u64 = 300;

/// Custom error type for configuration operations.
#[derive(Debug)]
pub enum ConfigError {
    Io(std::io::Error),
    Parse(String),
    Validation(String),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "IO error: {e}"),
            Self::Parse(msg) => write!(f, "Parse error: {msg}"),
            Self::Validation(msg) => write!(f, "Validation error: {msg}"),
        }
    }
}

impl std::error::Error for ConfigError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for ConfigError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "UPPERCASE")]
pub enum LogLevel {
    Debug,
    #[default]
    Info,
    Warn,
    Error,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum DiskSpaceLimitUnit {
    #[default]
    KB,
    MB,
    GB,
}

impl DiskSpaceLimitUnit {
    pub fn to_bytes(self, limit: u64) -> u64 {
        match self {
            Self::KB => limit * 1024,
            Self::MB => limit * 1024 * 1024,
            Self::GB => limit * 1024 * 1024 * 1024,
        }
    }
}

/// Shared fields between component and system log configs.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogSourceConfig {
    pub log_file_directory_path: String,
    pub log_file_regex: String,
    #[serde(default)]
    pub minimum_log_level: LogLevel,
    #[serde(default)]
    pub disk_space_limit: Option<String>,
    #[serde(default)]
    pub disk_space_limit_unit: DiskSpaceLimitUnit,
    #[serde(default)]
    pub delete_log_file_after_cloud_upload: bool,
    pub multi_line_start_pattern: Option<String>,
    pub upload_interval_sec: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogManagerConfig {
    #[serde(default)]
    pub component_logs_configuration: Vec<ComponentLogConfig>,
    #[serde(default)]
    pub system_logs_configuration: Vec<SystemLogConfig>,
    #[serde(default = "default_periodic_interval")]
    pub periodic_upload_interval_sec: u64,
}

fn default_periodic_interval() -> u64 {
    DEFAULT_UPLOAD_INTERVAL_SEC
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ComponentLogConfig {
    pub component_name: String,
    #[serde(flatten)]
    pub source: LogSourceConfig,
    pub log_group_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SystemLogConfig {
    #[serde(flatten)]
    pub source: LogSourceConfig,
    pub log_group_name: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_values() {
        let config: LogManagerConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(
            config.periodic_upload_interval_sec,
            DEFAULT_UPLOAD_INTERVAL_SEC
        );
        assert!(config.component_logs_configuration.is_empty());
        assert!(config.system_logs_configuration.is_empty());
    }

    #[test]
    fn test_component_defaults() {
        let json = r#"{"componentName":"test","logFileDirectoryPath":"/tmp","logFileRegex":".*"}"#;
        let comp: ComponentLogConfig = serde_json::from_str(json).unwrap();
        assert_eq!(comp.source.disk_space_limit, None);
        assert_eq!(comp.source.disk_space_limit_unit, DiskSpaceLimitUnit::KB);
        assert_eq!(comp.source.minimum_log_level, LogLevel::Info);
    }

    #[test]
    fn test_serde_roundtrip() {
        let config = LogManagerConfig {
            periodic_upload_interval_sec: 600,
            component_logs_configuration: vec![ComponentLogConfig {
                component_name: "test".into(),
                source: LogSourceConfig {
                    log_file_directory_path: "/var/log".into(),
                    log_file_regex: ".*\\.log".into(),
                    minimum_log_level: LogLevel::Debug,
                    disk_space_limit: Some("50".into()),
                    disk_space_limit_unit: DiskSpaceLimitUnit::GB,
                    delete_log_file_after_cloud_upload: true,
                    multi_line_start_pattern: Some("^\\d".into()),
                    upload_interval_sec: Some(120),
                },
                log_group_name: Some("/test/logs".into()),
            }],
            system_logs_configuration: vec![],
        };
        let json = serde_json::to_string(&config).unwrap();
        let parsed: LogManagerConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.periodic_upload_interval_sec, 600);
        assert_eq!(parsed.component_logs_configuration.len(), 1);
        let comp = &parsed.component_logs_configuration[0];
        assert_eq!(comp.component_name, "test");
        assert_eq!(comp.source.disk_space_limit, Some("50".into()));
        assert_eq!(comp.source.upload_interval_sec, Some(120));
    }

    #[test]
    fn test_disk_space_limit_unit_to_bytes() {
        assert_eq!(DiskSpaceLimitUnit::KB.to_bytes(1), 1024);
        assert_eq!(DiskSpaceLimitUnit::MB.to_bytes(25), 25 * 1024 * 1024);
        assert_eq!(DiskSpaceLimitUnit::GB.to_bytes(1), 1024 * 1024 * 1024);
    }

    #[test]
    fn test_config_error_display() {
        let io_err = ConfigError::Io(std::io::Error::new(std::io::ErrorKind::NotFound, "gone"));
        assert!(io_err.to_string().contains("IO error"));
        let parse_err = ConfigError::Parse("bad json".into());
        assert!(parse_err.to_string().contains("Parse error"));
        let val_err = ConfigError::Validation("bad dir".into());
        assert!(val_err.to_string().contains("Validation error"));
    }

    #[test]
    fn test_log_level_serde() {
        let json = r#""DEBUG""#;
        let level: LogLevel = serde_json::from_str(json).unwrap();
        assert_eq!(level, LogLevel::Debug);
        assert_eq!(
            serde_json::to_string(&LogLevel::Error).unwrap(),
            r#""ERROR""#
        );
    }

    #[test]
    fn test_disk_unit_serde() {
        let json = r#""GB""#;
        let unit: DiskSpaceLimitUnit = serde_json::from_str(json).unwrap();
        assert_eq!(unit, DiskSpaceLimitUnit::GB);
    }

    /// Verify that the JSON wire format is unchanged — existing Java configs must parse.
    #[test]
    fn test_wire_format_compatibility() {
        let java_json = r#"{
            "componentLogsConfiguration": [{
                "componentName": "MyApp",
                "logFileDirectoryPath": "/var/log",
                "logFileRegex": ".*\\.log",
                "minimumLogLevel": "WARN",
                "diskSpaceLimit": "100",
                "diskSpaceLimitUnit": "GB",
                "deleteLogFileAfterCloudUpload": true,
                "multiLineStartPattern": "^\\d",
                "uploadIntervalSec": 60
            }],
            "systemLogsConfiguration": [{
                "logFileDirectoryPath": "/var/log/sys",
                "logFileRegex": "syslog.*",
                "logGroupName": "/aws/greengrass/system",
                "minimumLogLevel": "ERROR",
                "diskSpaceLimit": "50",
                "diskSpaceLimitUnit": "MB"
            }],
            "periodicUploadIntervalSec": 120
        }"#;
        let config: LogManagerConfig = serde_json::from_str(java_json).unwrap();
        assert_eq!(
            config.component_logs_configuration[0].component_name,
            "MyApp"
        );
        assert_eq!(
            config.component_logs_configuration[0]
                .source
                .minimum_log_level,
            LogLevel::Warn
        );
        assert_eq!(
            config.component_logs_configuration[0]
                .source
                .disk_space_limit_unit,
            DiskSpaceLimitUnit::GB
        );
        assert_eq!(
            config.system_logs_configuration[0].log_group_name,
            "/aws/greengrass/system"
        );
        assert_eq!(
            config.system_logs_configuration[0].source.minimum_log_level,
            LogLevel::Error
        );
    }
}
