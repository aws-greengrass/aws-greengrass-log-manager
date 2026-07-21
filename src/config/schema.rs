// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! Configuration schema structs for the aws.greengrass.LogManager component.

use serde::{Deserialize, Deserializer, Serialize};
use std::collections::HashMap;

pub const DEFAULT_UPLOAD_INTERVAL_SEC: u64 = 300;

/// Custom error type for configuration operations.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ConfigError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON parse error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Parse error: {0}")]
    Parse(String),
    #[error("Validation error: {0}")]
    Validation(String),
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
    /// Empty means "not provided" — the documented default is `{/greengrass/v2}/logs`.
    /// Runtime derivation (regex → `^<componentName>\w*.log`, directory → root log dir)
    /// is applied when the scan pipeline consumes these values.
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
    /// Opt-in: when still over `diskSpaceLimit` after reclaiming fully-uploaded files, also
    /// delete the oldest un-uploaded (non-active) files. Default false, so the default
    /// behavior only ever reclaims already-uploaded files.
    #[serde(default, deserialize_with = "deserialize_bool_or_string")]
    pub delete_unuploaded_files_on_disk_pressure: bool,
    pub multi_line_start_pattern: Option<String>,
    /// Per-source upload interval override. Not yet honored — every source currently
    /// uploads on the global `periodicUploadIntervalSec` cadence.
    // TODO: wire per-source uploadIntervalSec
    pub upload_interval_sec: Option<u64>,
}

/// The inner config object representing the `logsUploaderConfiguration` subtree.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct LogsUploaderConfig {
    /// Map format (v2.2.0+): keyed by component name. Also accepts the legacy list
    /// format (and the legacy `componentLogsConfiguration` field name) for backward
    /// compatibility with configurations written for component versions before 2.2.0.
    #[serde(
        default,
        alias = "componentLogsConfiguration",
        deserialize_with = "deserialize_component_logs"
    )]
    pub component_logs_configuration_map: HashMap<String, ComponentSourceConfig>,
    #[serde(default)]
    pub system_logs_configuration: Option<SystemLogSourceConfig>,
    /// Optional fallback applied to any source that does not set its own `diskSpaceLimit`.
    /// When neither is set, the source is not bounded. Uses the same string-or-number form
    /// and unit field as the per-source `diskSpaceLimit`/`diskSpaceLimitUnit`.
    #[serde(default, deserialize_with = "deserialize_optional_string_or_number")]
    pub default_disk_space_limit: Option<String>,
    #[serde(default)]
    pub default_disk_space_limit_unit: DiskSpaceLimitUnit,
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
    /// Write the deprecated flat checkpoint format alongside the current nested one, and read
    /// it on load, for compatibility with older LogManager versions. Defaults to `true`.
    pub deprecated_version_support: bool,
}

impl<'de> Deserialize<'de> for LogManagerConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let mut value = serde_json::Value::deserialize(deserializer)?;
        let obj = value
            .as_object()
            .ok_or_else(|| serde::de::Error::custom("expected object"))?;

        // Absent → silent default. Present but unparseable or not > 0 → warn + default,
        // matching the aws.greengrass.LogManager behavior ("Must be a number greater than 0").
        let periodic = match obj.get("periodicUploadIntervalSec") {
            None => DEFAULT_UPLOAD_INTERVAL_SEC,
            Some(v) => match v
                .as_u64()
                .or_else(|| v.as_str().and_then(|s| s.trim().parse::<u64>().ok()))
            {
                Some(n) if n > 0 => n,
                _ => {
                    tracing::warn!(
                        value = %v, default_secs = DEFAULT_UPLOAD_INTERVAL_SEC,
                        "Invalid periodicUploadIntervalSec (must be a number greater than 0); using default"
                    );
                    DEFAULT_UPLOAD_INTERVAL_SEC
                }
            },
        };

        // Absent → default true (keep writing the deprecated format for downgrade safety).
        // Accept a JSON bool or a string ("true"/"false"), like the other bool fields, since
        // the Nucleus passes config values as strings; anything unparseable falls back to true.
        let deprecated_version_support = match obj.get("deprecatedVersionSupport") {
            None => true,
            Some(v) => v
                .as_bool()
                .or_else(|| match v.as_str().map(str::trim) {
                    Some(s) if s.eq_ignore_ascii_case("true") => Some(true),
                    Some(s) if s.eq_ignore_ascii_case("false") => Some(false),
                    _ => None,
                })
                .unwrap_or(true),
        };

        // Accept the nested `logsUploaderConfiguration` wrapper (official schema) or a
        // flat top-level object (recipe interpolation flattens the subtree).
        let inner = value
            .as_object_mut()
            .and_then(|o| o.remove("logsUploaderConfiguration"));
        let uploader_config = match inner {
            Some(inner) => serde_json::from_value(inner).map_err(serde::de::Error::custom)?,
            None => serde_json::from_value(value).map_err(serde::de::Error::custom)?,
        };

        Ok(LogManagerConfig {
            logs_uploader_configuration: uploader_config,
            periodic_upload_interval_sec: periodic,
            deprecated_version_support,
        })
    }
}

/// Component source config — the value in the `componentLogsConfigurationMap`.
/// Does NOT contain `componentName`; that is supplied by the map key.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ComponentSourceConfig {
    #[serde(flatten)]
    pub source: LogSourceConfig,
    pub log_group_name: Option<String>,
}

/// System logs configuration — a single object (not a list).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SystemLogSourceConfig {
    #[serde(flatten)]
    pub source: LogSourceConfig,
    #[serde(default, deserialize_with = "deserialize_bool_or_string")]
    pub upload_to_cloud_watch: bool,
    pub log_group_name: Option<String>,
}

/// Legacy list entry for `componentLogsConfiguration` (the pre-2.2.0 list format).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LegacyComponentLogConfig {
    component_name: String,
    #[serde(flatten)]
    source: LogSourceConfig,
    log_group_name: Option<String>,
}

/// Accepts a JSON boolean (`true`) or a string (`"true"`/`"false"`). The Greengrass
/// Nucleus passes config values as strings, so users may supply either form.
fn deserialize_bool_or_string<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    match &value {
        serde_json::Value::Bool(b) => Ok(*b),
        serde_json::Value::String(s) => {
            let trimmed = s.trim();
            if trimmed.eq_ignore_ascii_case("true") {
                Ok(true)
            } else if trimmed.eq_ignore_ascii_case("false") || trimmed.is_empty() {
                Ok(false)
            } else {
                tracing::warn!(value = %s, "Unrecognized boolean string, defaulting to false");
                Ok(false)
            }
        }
        _ => {
            tracing::warn!(
                ?value,
                "Unexpected type for boolean field, defaulting to false"
            );
            Ok(false)
        }
    }
}

/// Accepts a JSON string (`"25"`) or number (`25`), returned as `Option<String>`. The
/// Greengrass Nucleus passes config values as strings, so users may supply either form.
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

/// Deserializes `componentLogsConfigurationMap` from either:
/// - a map (current format): `{"componentName": {...}}`
/// - a list (legacy format): `[{"componentName": "x", ...}]`
///
/// The legacy field name `componentLogsConfiguration` is handled via the field alias.
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
    fn test_delete_unuploaded_flag_default_and_string() {
        // Defaults to false when omitted.
        let json = r#"{"logFileDirectoryPath":"/tmp","logFileRegex":".*"}"#;
        let comp: ComponentSourceConfig = serde_json::from_str(json).unwrap();
        assert!(!comp.source.delete_unuploaded_files_on_disk_pressure);

        // Accepts the Greengrass string form "true" (config values arrive as strings).
        let json = r#"{"logFileDirectoryPath":"/tmp","logFileRegex":".*","deleteUnuploadedFilesOnDiskPressure":"true"}"#;
        let comp: ComponentSourceConfig = serde_json::from_str(json).unwrap();
        assert!(comp.source.delete_unuploaded_files_on_disk_pressure);
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
                    delete_unuploaded_files_on_disk_pressure: false,
                    multi_line_start_pattern: Some("^\\d".into()),
                    upload_interval_sec: Some(120),
                },
                log_group_name: Some("/test/logs".into()),
            },
        );
        let config = LogManagerConfig {
            periodic_upload_interval_sec: 600,
            deprecated_version_support: true,
            logs_uploader_configuration: LogsUploaderConfig {
                component_logs_configuration_map: map,
                system_logs_configuration: None,
                default_disk_space_limit: Some("128".into()),
                default_disk_space_limit_unit: DiskSpaceLimitUnit::MB,
            },
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
        assert_eq!(
            parsed.logs_uploader_configuration.default_disk_space_limit,
            Some("128".into())
        );
        assert_eq!(
            parsed
                .logs_uploader_configuration
                .default_disk_space_limit_unit,
            DiskSpaceLimitUnit::MB
        );
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

    /// Verify that the componentLogsConfigurationMap format parses correctly.
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
        let legacy_json = r#"{
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
        let config: LogManagerConfig = serde_json::from_str(legacy_json).unwrap();
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
    fn test_default_disk_space_limit_keys_parse() {
        // Component-level default (nested form) parses like the per-source keys.
        let json = r#"{
            "logsUploaderConfiguration": {
                "defaultDiskSpaceLimit": "512",
                "defaultDiskSpaceLimitUnit": "MB"
            }
        }"#;
        let config: LogManagerConfig = serde_json::from_str(json).unwrap();
        assert_eq!(
            config.logs_uploader_configuration.default_disk_space_limit,
            Some("512".into())
        );
        assert_eq!(
            config
                .logs_uploader_configuration
                .default_disk_space_limit_unit,
            DiskSpaceLimitUnit::MB
        );

        // JSON number form is accepted too (config values may arrive as numbers).
        let json = r#"{
            "logsUploaderConfiguration": {
                "defaultDiskSpaceLimit": 512
            }
        }"#;
        let config: LogManagerConfig = serde_json::from_str(json).unwrap();
        assert_eq!(
            config.logs_uploader_configuration.default_disk_space_limit,
            Some("512".into())
        );

        // Omitted → None (unbounded) with the default unit.
        let config: LogManagerConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(
            config.logs_uploader_configuration.default_disk_space_limit,
            None
        );
        assert_eq!(
            config
                .logs_uploader_configuration
                .default_disk_space_limit_unit,
            DiskSpaceLimitUnit::KB
        );
    }

    #[test]
    fn test_deprecated_version_support_defaults_true() {
        let config: LogManagerConfig = serde_json::from_str("{}").unwrap();
        assert!(config.deprecated_version_support);
    }

    #[test]
    fn test_deprecated_version_support_present_false() {
        let config: LogManagerConfig =
            serde_json::from_str(r#"{"deprecatedVersionSupport": false}"#).unwrap();
        assert!(!config.deprecated_version_support);
    }

    #[test]
    fn test_deprecated_version_support_string_false() {
        let config: LogManagerConfig =
            serde_json::from_str(r#"{"deprecatedVersionSupport": "false"}"#).unwrap();
        assert!(!config.deprecated_version_support);
    }

    #[test]
    fn test_deprecated_version_support_string_true() {
        let config: LogManagerConfig =
            serde_json::from_str(r#"{"deprecatedVersionSupport": "true"}"#).unwrap();
        assert!(config.deprecated_version_support);
    }

    #[test]
    fn test_deprecated_version_support_unparseable_defaults_true() {
        let config: LogManagerConfig =
            serde_json::from_str(r#"{"deprecatedVersionSupport": "maybe"}"#).unwrap();
        assert!(config.deprecated_version_support);
    }

    #[test]
    fn test_periodic_interval_as_string() {
        let json = r#"{"periodicUploadIntervalSec": "60"}"#;
        let config: LogManagerConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.periodic_upload_interval_sec, 60);
    }

    #[test]
    fn test_bool_or_string_tolerant_parsing() {
        #[derive(Deserialize)]
        struct W(#[serde(deserialize_with = "deserialize_bool_or_string")] bool);
        let f = |j: &str| serde_json::from_str::<W>(j).unwrap().0;
        // Native bools
        assert!(f("true"));
        assert!(!f("false"));
        // Case-insensitive strings
        assert!(f(r#""true""#));
        assert!(f(r#""TRUE""#));
        assert!(!f(r#""false""#));
        assert!(!f(r#""FALSE""#));
        // Trim whitespace
        assert!(f(r#"" true ""#));
        assert!(!f(r#"" false ""#));
        // Unrecognized strings → false (with warn)
        assert!(!f(r#""yes""#));
        assert!(!f(r#""1""#));
        assert!(!f(r#""""#)); // empty → false

        // Unexpected types → false (with warn)
        assert!(!f("1"));
        assert!(!f("null"));
        assert!(!f("[1]"));
    }
}
