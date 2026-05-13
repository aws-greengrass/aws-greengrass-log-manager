// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! Config parsing tests

use gg_log_manager::config::{
    derive_log_group_name, load_config, parse_config, parse_disk_space_limit, validate_config,
    DiskSpaceLimitUnit, LogLevel, DEFAULT_UPLOAD_INTERVAL_SEC,
};
use serial_test::serial;
use std::io::Write;

const TEST_CONFIG_JSON: &str = r#"{
    "periodicUploadIntervalSec": 300,
    "componentLogsConfigurationMap": {
        "com.example.MyApp": {
            "logFileDirectoryPath": "/tmp",
            "logFileRegex": "app\\.log.*",
            "diskSpaceLimit": "25",
            "diskSpaceLimitUnit": "MB",
            "deleteLogFileAfterCloudUpload": "false"
        },
        "aws.greengrass.Nucleus": {
            "logFileDirectoryPath": "/tmp",
            "logFileRegex": "greengrass\\.log.*",
            "logGroupName": "/custom/nucleus/logs",
            "minimumLogLevel": "DEBUG",
            "diskSpaceLimit": "100",
            "diskSpaceLimitUnit": "MB",
            "uploadIntervalSec": 60
        },
        "com.example.EMFApp": {
            "logFileDirectoryPath": "/tmp",
            "logFileRegex": "emf-.*\\.json",
            "diskSpaceLimit": "10",
            "multiLineStartPattern": "^\\{"
        }
    },
    "systemLogsConfiguration": {
        "logFileDirectoryPath": "/tmp",
        "logFileRegex": "syslog.*",
        "logGroupName": "/aws/greengrass/system/syslog",
        "diskSpaceLimit": "50",
        "diskSpaceLimitUnit": "GB",
        "minimumLogLevel": "WARN",
        "uploadIntervalSec": 120
    }
}"#;

#[test]
fn test_parse_config_fields() {
    let config = parse_config(TEST_CONFIG_JSON).expect("Should parse valid config");

    assert_eq!(config.periodic_upload_interval_sec, 300);
    assert_eq!(
        config
            .logs_uploader_configuration
            .component_logs_configuration_map
            .len(),
        3
    );
    assert!(config
        .logs_uploader_configuration
        .system_logs_configuration
        .is_some());
}

#[test]
fn test_component_config_fields() {
    let config = parse_config(TEST_CONFIG_JSON).unwrap();
    let comp = config
        .logs_uploader_configuration
        .component_logs_configuration_map
        .get("com.example.MyApp")
        .unwrap();

    assert_eq!(comp.source.log_file_directory_path, "/tmp");
    assert_eq!(comp.source.log_file_regex, "app\\.log.*");
    assert_eq!(comp.source.disk_space_limit, Some("25".into()));
    assert_eq!(comp.source.disk_space_limit_unit, DiskSpaceLimitUnit::MB);
    assert!(!comp.source.delete_log_file_after_cloud_upload);
    assert!(comp.log_group_name.is_none());
}

#[test]
fn test_upload_interval_override() {
    let config = parse_config(TEST_CONFIG_JSON).unwrap();
    let nucleus = config
        .logs_uploader_configuration
        .component_logs_configuration_map
        .get("aws.greengrass.Nucleus")
        .unwrap();

    assert_eq!(nucleus.source.upload_interval_sec, Some(60));
    assert_eq!(
        nucleus.log_group_name,
        Some("/custom/nucleus/logs".to_string())
    );
    assert_eq!(nucleus.source.minimum_log_level, LogLevel::Debug);
}

#[test]
fn test_defaults_applied() {
    let config = parse_config(TEST_CONFIG_JSON).unwrap();
    let emf = config
        .logs_uploader_configuration
        .component_logs_configuration_map
        .get("com.example.EMFApp")
        .unwrap();

    assert_eq!(emf.source.minimum_log_level, LogLevel::Info);
    assert_eq!(emf.source.disk_space_limit_unit, DiskSpaceLimitUnit::KB);
    assert!(!emf.source.delete_log_file_after_cloud_upload);
    assert_eq!(
        emf.source.multi_line_start_pattern,
        Some("^\\{".to_string())
    );
}

#[test]
fn test_system_config_fields() {
    let config = parse_config(TEST_CONFIG_JSON).unwrap();
    let sys = config
        .logs_uploader_configuration
        .system_logs_configuration
        .as_ref()
        .unwrap();

    assert_eq!(
        sys.log_group_name.as_deref(),
        Some("/aws/greengrass/system/syslog")
    );
    assert_eq!(sys.source.disk_space_limit, Some("50".into()));
    assert_eq!(sys.source.disk_space_limit_unit, DiskSpaceLimitUnit::GB);
    assert_eq!(sys.source.minimum_log_level, LogLevel::Warn);
    assert_eq!(sys.source.upload_interval_sec, Some(120));
}

#[test]
#[serial]
fn test_derive_log_group_name() {
    std::env::set_var("AWS_DEFAULT_REGION", "us-west-2");
    let name = derive_log_group_name("MyComponent", Some("GreengrassSystemComponent"));
    assert_eq!(
        name,
        "/aws/greengrass/GreengrassSystemComponent/us-west-2/MyComponent"
    );

    let name_default = derive_log_group_name("MyComponent", None);
    assert_eq!(
        name_default,
        "/aws/greengrass/UserComponent/us-west-2/MyComponent"
    );
    std::env::remove_var("AWS_DEFAULT_REGION");
}

#[test]
fn test_default_periodic_interval() {
    let config = parse_config("{}").unwrap();
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
fn test_disk_space_limit_string_parsing() {
    assert_eq!(parse_disk_space_limit(Some("100")).unwrap(), Some(100));
    assert!(parse_disk_space_limit(Some("0")).is_err());
    assert!(parse_disk_space_limit(Some("abc")).is_err());
    assert!(parse_disk_space_limit(Some("-5")).is_err());
    assert_eq!(parse_disk_space_limit(None).unwrap(), None);
    assert_eq!(parse_disk_space_limit(Some("")).unwrap(), None);
}

#[test]
fn test_load_config_inline_json() {
    let config = load_config(r#"{"periodicUploadIntervalSec": 120}"#).unwrap();
    assert_eq!(config.periodic_upload_interval_sec, 120);
}

#[test]
fn test_load_config_from_file() {
    let dir = std::env::temp_dir();
    let path = dir.join("test_config.json");
    let mut file = std::fs::File::create(&path).unwrap();
    writeln!(file, r#"{{"periodicUploadIntervalSec": 180}}"#).unwrap();

    let config = load_config(path.to_str().unwrap()).unwrap();
    assert_eq!(config.periodic_upload_interval_sec, 180);

    std::fs::remove_file(path).ok();
}

#[test]
fn test_load_config_file_not_found() {
    let result = load_config("/nonexistent/path/config.json");
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("IO error"));
}

#[test]
fn test_validate_config_invalid_disk_limit() {
    let json = r#"{
        "componentLogsConfigurationMap": {
            "test": {
                "logFileDirectoryPath": "/tmp",
                "logFileRegex": ".*",
                "diskSpaceLimit": "abc"
            }
        }
    }"#;
    let config = parse_config(json).unwrap();
    let result = validate_config(&config);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("diskSpaceLimit"));
}

#[test]
fn test_validate_config_invalid_regex() {
    let json = r#"{
        "componentLogsConfigurationMap": {
            "test": {
                "logFileDirectoryPath": "/tmp",
                "logFileRegex": "[invalid",
                "diskSpaceLimit": "100"
            }
        }
    }"#;
    let config = parse_config(json).unwrap();
    let result = validate_config(&config);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Invalid regex"));
}

#[test]
fn test_validate_config_zero_disk_limit() {
    let json = r#"{
        "componentLogsConfigurationMap": {
            "test": {
                "logFileDirectoryPath": "/tmp",
                "logFileRegex": ".*",
                "diskSpaceLimit": "0"
            }
        }
    }"#;
    let config = parse_config(json).unwrap();
    let result = validate_config(&config);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("positive"));
}

/// Test backward compat: legacy list format still parses
#[test]
fn test_legacy_list_format() {
    let json = r#"{
        "componentLogsConfigurationMap": [
            {
                "componentName": "MyApp",
                "logFileDirectoryPath": "/tmp",
                "logFileRegex": ".*\\.log"
            }
        ]
    }"#;
    let config = parse_config(json).unwrap();
    assert!(config
        .logs_uploader_configuration
        .component_logs_configuration_map
        .contains_key("MyApp"));
}
