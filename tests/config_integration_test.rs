// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for config parsing with full LLD J.5 config

use gg_log_manager::config::{parse_config, DiskSpaceLimitUnit, LogLevel, DEFAULT_UPLOAD_INTERVAL_SEC};
use serial_test::serial;

/// Full LLD J.5 config with 3 componentLogsConfiguration entries and 1 systemLogsConfiguration
const LLD_J5_CONFIG: &str = r#"{
    "periodicUploadIntervalSec": 300,
    "componentLogsConfiguration": [
        {
            "componentName": "system-health",
            "logFileDirectoryPath": "/greengrass/v2/logs/emf",
            "logFileRegex": "system-health-.*\\.emf\\.json",
            "diskSpaceLimit": "50",
            "diskSpaceLimitUnit": "MB",
            "deleteLogFileAfterCloudUpload": true,
            "multiLineStartPattern": "^\\{",
            "uploadIntervalSec": 60
        },
        {
            "componentName": "docker-health",
            "logFileDirectoryPath": "/greengrass/v2/logs/emf",
            "logFileRegex": "docker-health-.*\\.emf\\.json",
            "logGroupName": "/aws/greengrass/custom/docker-health",
            "diskSpaceLimit": "25",
            "diskSpaceLimitUnit": "MB",
            "deleteLogFileAfterCloudUpload": false,
            "minimumLogLevel": "DEBUG"
        },
        {
            "componentName": "DeviceBridge",
            "logFileDirectoryPath": "/greengrass/v2/logs/bridge",
            "logFileRegex": "bridge-.*\\.log",
            "diskSpaceLimit": "100",
            "diskSpaceLimitUnit": "MB"
        }
    ],
    "systemLogsConfiguration": [
        {
            "logFileDirectoryPath": "/greengrass/v2/logs/insights",
            "logFileRegex": "insights-.*\\.json",
            "logGroupName": "/aws/greengrass/system/insights",
            "diskSpaceLimit": "200",
            "diskSpaceLimitUnit": "MB",
            "deleteLogFileAfterCloudUpload": true,
            "minimumLogLevel": "WARN",
            "uploadIntervalSec": 120
        }
    ]
}"#;

#[test]
fn test_lld_j5_config_parses_all_entries() {
    let config = parse_config(LLD_J5_CONFIG).expect("Should parse LLD J.5 config");

    assert_eq!(config.component_logs_configuration.len(), 3);
    assert_eq!(config.system_logs_configuration.len(), 1);
}

#[test]
fn test_lld_j5_global_periodic_interval() {
    let config = parse_config(LLD_J5_CONFIG).unwrap();
    assert_eq!(config.periodic_upload_interval_sec, DEFAULT_UPLOAD_INTERVAL_SEC);
}

#[test]
fn test_lld_j5_system_health_component() {
    let config = parse_config(LLD_J5_CONFIG).unwrap();
    let system_health = &config.component_logs_configuration[0];

    assert_eq!(system_health.component_name, "system-health");
    assert_eq!(
        system_health.source.log_file_directory_path,
        "/greengrass/v2/logs/emf"
    );
    assert_eq!(
        system_health.source.log_file_regex,
        "system-health-.*\\.emf\\.json"
    );
    assert_eq!(system_health.source.disk_space_limit, "50");
    assert_eq!(system_health.source.disk_space_limit_unit, DiskSpaceLimitUnit::MB);
    assert!(system_health.source.delete_log_file_after_cloud_upload);
    assert_eq!(
        system_health.source.multi_line_start_pattern,
        Some("^\\{".to_string())
    );
    assert_eq!(system_health.source.upload_interval_sec, Some(60));
    assert!(system_health.log_group_name.is_none());
}

#[test]
fn test_lld_j5_upload_interval_override() {
    let config = parse_config(LLD_J5_CONFIG).unwrap();
    let system_health = &config.component_logs_configuration[0];

    assert_eq!(system_health.source.upload_interval_sec, Some(60));
    assert_eq!(config.periodic_upload_interval_sec, DEFAULT_UPLOAD_INTERVAL_SEC);
}

#[test]
fn test_lld_j5_docker_health_component() {
    let config = parse_config(LLD_J5_CONFIG).unwrap();
    let docker_health = &config.component_logs_configuration[1];

    assert_eq!(docker_health.component_name, "docker-health");
    assert_eq!(
        docker_health.source.log_file_directory_path,
        "/greengrass/v2/logs/emf"
    );
    assert_eq!(
        docker_health.source.log_file_regex,
        "docker-health-.*\\.emf\\.json"
    );
    assert_eq!(
        docker_health.log_group_name,
        Some("/aws/greengrass/custom/docker-health".to_string())
    );
    assert_eq!(docker_health.source.disk_space_limit, "25");
    assert_eq!(docker_health.source.disk_space_limit_unit, DiskSpaceLimitUnit::MB);
    assert!(!docker_health.source.delete_log_file_after_cloud_upload);
    assert_eq!(docker_health.source.minimum_log_level, LogLevel::Debug);
    assert!(docker_health.source.upload_interval_sec.is_none());
}

#[test]
fn test_lld_j5_device_bridge_component_defaults() {
    let config = parse_config(LLD_J5_CONFIG).unwrap();
    let device_bridge = &config.component_logs_configuration[2];

    assert_eq!(device_bridge.component_name, "DeviceBridge");
    assert_eq!(
        device_bridge.source.log_file_directory_path,
        "/greengrass/v2/logs/bridge"
    );
    assert_eq!(device_bridge.source.log_file_regex, "bridge-.*\\.log");
    assert_eq!(device_bridge.source.disk_space_limit, "100");
    assert_eq!(device_bridge.source.disk_space_limit_unit, DiskSpaceLimitUnit::MB);
    assert!(!device_bridge.source.delete_log_file_after_cloud_upload);
    assert_eq!(device_bridge.source.minimum_log_level, LogLevel::Info);
    assert!(device_bridge.log_group_name.is_none());
    assert!(device_bridge.source.multi_line_start_pattern.is_none());
    assert!(device_bridge.source.upload_interval_sec.is_none());
}

#[test]
fn test_lld_j5_insights_system_config() {
    let config = parse_config(LLD_J5_CONFIG).unwrap();
    let insights = &config.system_logs_configuration[0];

    assert_eq!(
        insights.source.log_file_directory_path,
        "/greengrass/v2/logs/insights"
    );
    assert_eq!(insights.source.log_file_regex, "insights-.*\\.json");
    assert_eq!(insights.log_group_name, "/aws/greengrass/system/insights");
    assert_eq!(insights.source.disk_space_limit, "200");
    assert_eq!(insights.source.disk_space_limit_unit, DiskSpaceLimitUnit::MB);
    assert!(insights.source.delete_log_file_after_cloud_upload);
    assert_eq!(insights.source.minimum_log_level, LogLevel::Warn);
    assert_eq!(insights.source.upload_interval_sec, Some(120));
}

#[test]
#[serial]
fn test_auto_derived_log_group_name() {
    use gg_log_manager::config::derive_log_group_name;

    std::env::set_var("AWS_DEFAULT_REGION", "us-east-1");

    let derived = derive_log_group_name("system-health", Some("UserComponent"));
    assert_eq!(
        derived,
        "/aws/greengrass/UserComponent/us-east-1/system-health"
    );

    let derived_default = derive_log_group_name("DeviceBridge", None);
    assert_eq!(
        derived_default,
        "/aws/greengrass/UserComponent/us-east-1/DeviceBridge"
    );

    std::env::remove_var("AWS_DEFAULT_REGION");
}

#[test]
fn test_missing_optional_fields_get_defaults() {
    let minimal_json = r#"{
        "componentLogsConfiguration": [{
            "componentName": "minimal",
            "logFileDirectoryPath": "/tmp",
            "logFileRegex": ".*\\.log",
            "diskSpaceLimit": "10"
        }]
    }"#;

    let config = parse_config(minimal_json).unwrap();
    let comp = &config.component_logs_configuration[0];

    assert_eq!(comp.source.minimum_log_level, LogLevel::Info);
    assert_eq!(comp.source.disk_space_limit_unit, DiskSpaceLimitUnit::MB);
    assert!(!comp.source.delete_log_file_after_cloud_upload);
    assert!(comp.log_group_name.is_none());
    assert!(comp.source.multi_line_start_pattern.is_none());
    assert!(comp.source.upload_interval_sec.is_none());

    assert_eq!(config.periodic_upload_interval_sec, DEFAULT_UPLOAD_INTERVAL_SEC);
}
