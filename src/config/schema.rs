// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! Configuration schema structs matching Java LogManager JSON schema

use serde::{Deserialize, Deserializer, Serialize};
use std::collections::HashMap;
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
    #[serde(default)]
    pub log_file_directory_path: String,
    #[serde(default)]
    pub log_file_regex: String,
    #[serde(default)]
    pub minimum_log_level: LogLevel,
    #[serde(default, deserialize_with = "deserialize_optional_string_or_number")]
    pub disk_space_limit: Option<String>,
    #[serde(default)]
    pub disk_space_limit_unit: DiskSpaceLimitUnit,
    #[serde(default, deserialize_with = "deserialize_bool_or_string")]
    pub delete_log_file_after_cloud_upload: bool,
    pub multi_line_start_pattern: Option<String>,
    /// Per-source upload interval override. Not yet wired — all sources use the global
    /// periodicUploadIntervalSec. Per-source timing requires tracking last-upload-time per source.
    pub upload_interval_sec: Option<u64>,
}

/// The inner config object representing the `logsUploaderConfiguration` subtree.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct LogsUploaderConfig {
    /// Map format (v2.2.0+): keyed by component name.
    /// Also accepts legacy list format for backward compat with <v2.2.0 configs.
    #[serde(
        default,
        alias = "componentLogsConfiguration",
        deserialize_with = "deserialize_component_logs"
    )]
    pub component_logs_configuration_map: HashMap<String, ComponentSourceConfig>,
    #[serde(default)]
    pub system_logs_configuration: Option<SystemLogSourceConfig>,
}

/// Top-level config struct deserialized from the full recipe configuration tree.
/// Supports both formats:
/// - **Nested** (official docs): `{"logsUploaderConfiguration": {...}, "periodicUploadIntervalSec": 300}`
/// - **Flat** (recipe interpolation): `{"componentLogsConfigurationMap": {...}, "periodicUploadIntervalSec": 300}`
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LogManagerConfig {
    pub logs_uploader_configuration: LogsUploaderConfig,
    pub periodic_upload_interval_sec: u64,
    /// Whether to support deprecated V1 checkpoint format (Java ≤ 2.3.0). Default: true.
    pub deprecated_version_support: bool,
}

impl<'de> Deserialize<'de> for LogManagerConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        let obj = value
            .as_object()
            .ok_or_else(|| serde::de::Error::custom("expected object"))?;

        let periodic = obj
            .get("periodicUploadIntervalSec")
            .and_then(|v| {
                v.as_u64()
                    .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
            })
            .unwrap_or(DEFAULT_UPLOAD_INTERVAL_SEC);

        let uploader_config = if let Some(inner) = obj.get("logsUploaderConfiguration") {
            serde_json::from_value(inner.clone()).map_err(serde::de::Error::custom)?
        } else {
            serde_json::from_value(value.clone()).map_err(serde::de::Error::custom)?
        };

        let deprecated_version_support = obj
            .get("deprecatedVersionSupport")
            .and_then(|v| {
                v.as_bool()
                    .or_else(|| v.as_str().map(|s| s.eq_ignore_ascii_case("true")))
            })
            .unwrap_or(true);

        Ok(LogManagerConfig {
            logs_uploader_configuration: uploader_config,
            periodic_upload_interval_sec: periodic,
            deprecated_version_support,
        })
    }
}

/// Component source config — the value in the componentLogsConfigurationMap.
/// Does NOT contain `componentName` — that comes from the map key.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ComponentSourceConfig {
    #[serde(flatten)]
    pub source: LogSourceConfig,
    pub log_group_name: Option<String>,
}

/// System logs configuration — single object (not a list).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SystemLogSourceConfig {
    #[serde(flatten)]
    pub source: LogSourceConfig,
    #[serde(default, deserialize_with = "deserialize_bool_or_string")]
    pub upload_to_cloud_watch: bool,
    pub log_group_name: Option<String>,
}

/// Legacy list entry for componentLogsConfiguration (<v2.2.0 format).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LegacyComponentLogConfig {
    component_name: String,
    #[serde(flatten)]
    source: LogSourceConfig,
    log_group_name: Option<String>,
}

/// Accepts both JSON boolean (`true`) and string (`"true"`/`"false"`).
/// GG Nucleus stores config values as Object — users may provide either format.
fn deserialize_bool_or_string<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    match &value {
        serde_json::Value::Bool(b) => Ok(*b),
        serde_json::Value::String(s) => Ok(s.eq_ignore_ascii_case("true")),
        _ => Ok(false),
    }
}

/// Accepts JSON string (`"25"`) or number (`25`), returns as Option<String>.
fn deserialize_optional_string_or_number<'de, D>(
    deserializer: D,
) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    match &value {
        serde_json::Value::Null => Ok(None),
        serde_json::Value::String(s) if s.is_empty() => Ok(None),
        serde_json::Value::String(s) => Ok(Some(s.clone())),
        serde_json::Value::Number(n) => Ok(Some(n.to_string())),
        _ => Ok(None),
    }
}

/// Deserializes componentLogsConfigurationMap from either:
/// - A map (new format): `{"componentName": {...}}`
/// - A list (legacy format): `[{"componentName": "x", ...}]`
///
/// Also handles the legacy field name `componentLogsConfiguration`.
fn deserialize_component_logs<'de, D>(
    deserializer: D,
) -> Result<HashMap<String, ComponentSourceConfig>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum MapOrList {
        Map(HashMap<String, ComponentSourceConfig>),
        List(Vec<LegacyComponentLogConfig>),
    }

    match MapOrList::deserialize(deserializer)? {
        MapOrList::Map(map) => Ok(map),
        MapOrList::List(list) => {
            let mut map = HashMap::new();
            for entry in list {
                map.insert(
                    entry.component_name,
                    ComponentSourceConfig {
                        source: entry.source,
                        log_group_name: entry.log_group_name,
                    },
                );
            }
            Ok(map)
        }
    }
}

// Keep these type aliases for backward compat with existing code that references them.
// They map to the new types.
pub type ComponentLogConfig = ComponentSourceConfig;
pub type SystemLogConfig = SystemLogSourceConfig;

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
        assert!(config
            .logs_uploader_configuration
            .component_logs_configuration_map
            .is_empty());
        assert!(config
            .logs_uploader_configuration
            .system_logs_configuration
            .is_none());
    }

    #[test]
    fn test_component_defaults() {
        let json = r#"{"logFileDirectoryPath":"/tmp","logFileRegex":".*"}"#;
        let comp: ComponentSourceConfig = serde_json::from_str(json).unwrap();
        assert_eq!(comp.source.disk_space_limit, None);
        assert_eq!(comp.source.disk_space_limit_unit, DiskSpaceLimitUnit::KB);
        assert_eq!(comp.source.minimum_log_level, LogLevel::Info);
    }

    #[test]
    fn test_serde_roundtrip() {
        let mut map = HashMap::new();
        map.insert(
            "test".to_string(),
            ComponentSourceConfig {
                source: LogSourceConfig {
                    log_file_directory_path: "/var/log".into(),
                    log_file_regex: ".*\\.log".into(),
                    minimum_log_level: LogLevel::Info,
                    disk_space_limit: Some("50".into()),
                    disk_space_limit_unit: DiskSpaceLimitUnit::GB,
                    delete_log_file_after_cloud_upload: true,
                    multi_line_start_pattern: Some("^\\d".into()),
                    upload_interval_sec: Some(120),
                },
                log_group_name: Some("/test/logs".into()),
            },
        );
        let config = LogManagerConfig {
            periodic_upload_interval_sec: 600,
            logs_uploader_configuration: LogsUploaderConfig {
                component_logs_configuration_map: map,
                system_logs_configuration: None,
            },
            deprecated_version_support: true,
        };
        let json = serde_json::to_string(&config).unwrap();
        let parsed: LogManagerConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.periodic_upload_interval_sec, 600);
        assert_eq!(
            parsed
                .logs_uploader_configuration
                .component_logs_configuration_map
                .len(),
            1
        );
        let comp = parsed
            .logs_uploader_configuration
            .component_logs_configuration_map
            .get("test")
            .unwrap();
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

    /// Verify that the new map format parses correctly.
    #[test]
    fn test_map_format() {
        let json = r#"{
            "componentLogsConfigurationMap": {
                "MyApp": {
                    "logFileDirectoryPath": "/var/log",
                    "logFileRegex": ".*\\.log",
                    "minimumLogLevel": "WARN",
                    "diskSpaceLimit": "100",
                    "diskSpaceLimitUnit": "GB",
                    "deleteLogFileAfterCloudUpload": "true",
                    "multiLineStartPattern": "^\\d",
                    "uploadIntervalSec": 60
                }
            },
            "systemLogsConfiguration": {
                "logFileDirectoryPath": "/var/log/sys",
                "logFileRegex": "syslog.*",
                "logGroupName": "/aws/greengrass/system",
                "minimumLogLevel": "ERROR",
                "diskSpaceLimit": "50",
                "diskSpaceLimitUnit": "MB"
            },
            "periodicUploadIntervalSec": 120
        }"#;
        let config: LogManagerConfig = serde_json::from_str(json).unwrap();
        let comp = config
            .logs_uploader_configuration
            .component_logs_configuration_map
            .get("MyApp")
            .unwrap();
        assert_eq!(comp.source.minimum_log_level, LogLevel::Warn);
        assert_eq!(comp.source.disk_space_limit_unit, DiskSpaceLimitUnit::GB);
        let sys = config
            .logs_uploader_configuration
            .system_logs_configuration
            .as_ref()
            .unwrap();
        assert_eq!(
            sys.log_group_name.as_deref(),
            Some("/aws/greengrass/system")
        );
        assert_eq!(sys.source.minimum_log_level, LogLevel::Error);
    }

    /// Verify that the legacy list format still parses (backward compat with <v2.2.0).
    #[test]
    fn test_legacy_list_format_compatibility() {
        let java_json = r#"{
            "componentLogsConfiguration": [{
                "componentName": "MyApp",
                "logFileDirectoryPath": "/var/log",
                "logFileRegex": ".*\\.log",
                "minimumLogLevel": "WARN",
                "diskSpaceLimit": "100",
                "diskSpaceLimitUnit": "GB",
                "deleteLogFileAfterCloudUpload": "true",
                "multiLineStartPattern": "^\\d",
                "uploadIntervalSec": 60
            }],
            "periodicUploadIntervalSec": 120
        }"#;
        let config: LogManagerConfig = serde_json::from_str(java_json).unwrap();
        let comp = config
            .logs_uploader_configuration
            .component_logs_configuration_map
            .get("MyApp")
            .unwrap();
        assert_eq!(comp.source.minimum_log_level, LogLevel::Warn);
        assert_eq!(comp.source.disk_space_limit_unit, DiskSpaceLimitUnit::GB);
    }

    /// Verify that the nested format parses correctly.
    #[test]
    fn test_nested_format() {
        let json = r#"{
            "logsUploaderConfiguration": {
                "componentLogsConfigurationMap": {
                    "MyApp": {
                        "logFileDirectoryPath": "/var/log",
                        "logFileRegex": ".*\\.log"
                    }
                }
            },
            "periodicUploadIntervalSec": 120
        }"#;
        let config: LogManagerConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.periodic_upload_interval_sec, 120);
        assert!(config
            .logs_uploader_configuration
            .component_logs_configuration_map
            .contains_key("MyApp"));
    }

    #[test]
    fn test_default_log_level_is_info() {
        assert_eq!(LogLevel::default(), LogLevel::Info);
    }

    #[test]
    fn test_periodic_interval_as_string() {
        let json = r#"{"periodicUploadIntervalSec": "60"}"#;
        let config: LogManagerConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.periodic_upload_interval_sec, 60);
    }
}
