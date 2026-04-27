// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! Configuration management for gg-log-manager

mod schema;

pub use schema::{
    ComponentLogConfig, ConfigError, DiskSpaceLimitUnit, LogLevel, LogManagerConfig,
    LogSourceConfig, SystemLogConfig, DEFAULT_UPLOAD_INTERVAL_SEC,
};

use regex::Regex;
use std::path::Path;

/// Load config from CLI arg — inline JSON or file path.
#[must_use = "config result must be handled"]
pub fn load_config(config_arg: &str) -> Result<LogManagerConfig, ConfigError> {
    let json = if config_arg.trim_start().starts_with('{') {
        config_arg.to_string()
    } else {
        std::fs::read_to_string(config_arg)?
    };
    let config = parse_config(&json)?;
    tracing::info!("Configuration loaded successfully");
    Ok(config)
}

#[must_use = "config result must be handled"]
pub fn parse_config(json: &str) -> Result<LogManagerConfig, ConfigError> {
    serde_json::from_str(json).map_err(|e| ConfigError::Parse(format!("{e}")))
}

/// Validate a single log source's directory, regex, and disk limit.
fn validate_log_source(source: &LogSourceConfig) -> Result<(), ConfigError> {
    validate_directory(&source.log_file_directory_path)?;
    validate_regex(&source.log_file_regex)?;
    if let Some(ref limit) = source.disk_space_limit {
        validate_disk_limit(limit)?;
    }
    Ok(())
}

#[must_use = "validation result must be handled"]
pub fn validate_config(config: &LogManagerConfig) -> Result<(), ConfigError> {
    config
        .component_logs_configuration
        .iter()
        .try_for_each(|c| {
            tracing::debug!(component = %c.component_name, dir = %c.source.log_file_directory_path, "Validating component config");
            validate_log_source(&c.source)
        })?;
    config.system_logs_configuration.iter().try_for_each(|s| {
        tracing::debug!(dir = %s.source.log_file_directory_path, "Validating system log config");
        validate_log_source(&s.source)
    })?;
    tracing::info!("Configuration validation passed");
    Ok(())
}

fn validate_directory(path: impl AsRef<Path>) -> Result<(), ConfigError> {
    let p = path.as_ref();
    if !p.is_dir() {
        return Err(ConfigError::Validation(format!(
            "Directory does not exist: {}",
            p.display()
        )));
    }
    Ok(())
}

fn validate_regex(pattern: &str) -> Result<(), ConfigError> {
    Regex::new(pattern)
        .map_err(|e| ConfigError::Validation(format!("Invalid regex '{pattern}': {e}")))?;
    Ok(())
}

fn validate_disk_limit(limit: &str) -> Result<(), ConfigError> {
    let parsed: u64 = limit.parse().map_err(|_| {
        ConfigError::Validation(format!(
            "diskSpaceLimit must be a positive number, got: {limit}"
        ))
    })?;
    if parsed == 0 {
        return Err(ConfigError::Validation(
            "diskSpaceLimit must be positive".into(),
        ));
    }
    Ok(())
}

/// Parse diskSpaceLimit string to u64. Returns None if limit is not configured.
#[must_use = "parse result must be handled"]
pub fn parse_disk_space_limit(limit: Option<&str>) -> Result<Option<u64>, ConfigError> {
    match limit {
        Some(l) => l
            .parse()
            .map(Some)
            .map_err(|_| ConfigError::Parse(format!("Invalid diskSpaceLimit: {l}"))),
        None => Ok(None),
    }
}

/// Derive log group name: /aws/greengrass/{componentType}/{region}/{componentName}
pub fn derive_log_group_name(component_name: &str, component_type: Option<&str>) -> String {
    let comp_type = component_type.unwrap_or("UserComponent");
    let region = std::env::var("AWS_DEFAULT_REGION").unwrap_or_else(|_| "us-east-1".to_string());
    format!("/aws/greengrass/{comp_type}/{region}/{component_name}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    #[test]
    fn test_parse_config_empty() {
        let config = parse_config("{}").unwrap();
        assert!(config.component_logs_configuration.is_empty());
        assert!(config.system_logs_configuration.is_empty());
        assert_eq!(
            config.periodic_upload_interval_sec,
            DEFAULT_UPLOAD_INTERVAL_SEC
        );
    }

    #[test]
    fn test_validate_directory_exists() {
        let dir = tempfile::tempdir().unwrap();
        assert!(validate_directory(dir.path()).is_ok());
    }

    #[test]
    fn test_validate_directory_not_exists() {
        let result = validate_directory("/nonexistent/path/12345");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("does not exist"));
    }

    #[test]
    fn test_validate_regex_valid() {
        assert!(validate_regex(r".*\.log$").is_ok());
        assert!(validate_regex(r"^\d{4}-\d{2}-\d{2}").is_ok());
    }

    #[test]
    fn test_validate_regex_invalid() {
        assert!(validate_regex(r"[invalid").is_err());
    }

    #[test]
    fn test_validate_disk_limit_valid() {
        assert!(validate_disk_limit("100").is_ok());
        assert!(validate_disk_limit("25").is_ok());
        assert!(validate_disk_limit("1").is_ok());
    }

    #[test]
    fn test_validate_disk_limit_zero() {
        let result = validate_disk_limit("0");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("positive"));
    }

    #[test]
    fn test_validate_disk_limit_non_numeric() {
        assert!(validate_disk_limit("abc").is_err());
    }

    #[test]
    fn test_validate_config_valid() {
        let dir = tempfile::tempdir().unwrap();
        let json = format!(
            r#"{{
                "componentLogsConfiguration": [{{
                    "componentName": "test",
                    "logFileDirectoryPath": "{}",
                    "logFileRegex": ".*\\.log$"
                }}]
            }}"#,
            dir.path().to_str().unwrap()
        );
        let config = parse_config(&json).unwrap();
        assert!(validate_config(&config).is_ok());
    }

    #[test]
    fn test_validate_config_system_logs() {
        let dir = tempfile::tempdir().unwrap();
        let json = format!(
            r#"{{
                "systemLogsConfiguration": [{{
                    "logFileDirectoryPath": "{}",
                    "logFileRegex": ".*\\.log$",
                    "logGroupName": "/aws/greengrass/system"
                }}]
            }}"#,
            dir.path().to_str().unwrap()
        );
        let config = parse_config(&json).unwrap();
        assert!(validate_config(&config).is_ok());
    }

    #[test]
    #[serial]
    fn test_derive_log_group_name_with_region() {
        std::env::set_var("AWS_DEFAULT_REGION", "eu-west-1");
        let name = derive_log_group_name("my-component", Some("GreengrassSystemComponent"));
        assert_eq!(
            name,
            "/aws/greengrass/GreengrassSystemComponent/eu-west-1/my-component"
        );
        std::env::remove_var("AWS_DEFAULT_REGION");
    }

    #[test]
    #[serial]
    fn test_derive_log_group_name_default_region() {
        std::env::remove_var("AWS_DEFAULT_REGION");
        let name = derive_log_group_name("my-component", None);
        assert_eq!(name, "/aws/greengrass/UserComponent/us-east-1/my-component");
    }

    #[test]
    fn test_parse_disk_space_limit() {
        assert_eq!(parse_disk_space_limit(Some("100")).unwrap(), Some(100));
        assert_eq!(parse_disk_space_limit(Some("0")).unwrap(), Some(0));
        assert!(parse_disk_space_limit(Some("abc")).is_err());
        assert_eq!(parse_disk_space_limit(None).unwrap(), None);
    }

    #[test]
    fn test_load_config_inline_json() {
        let config = load_config("{}").unwrap();
        assert!(config.component_logs_configuration.is_empty());
    }

    #[test]
    fn test_load_config_file_not_found() {
        let result = load_config("/nonexistent/config.json");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("IO error"));
    }
}
