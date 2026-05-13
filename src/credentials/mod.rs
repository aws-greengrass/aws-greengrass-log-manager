// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! ThingName resolution for Greengrass.
//!
//! Credentials are handled by the AWS SDK default chain (`aws_config::load_defaults`),
//! which picks up TES-provided credentials automatically when running as a GG component.
//! Both Classic and Lite set `AWS_IOT_THING_NAME` for components.

/// Resolve ThingName from `$AWS_IOT_THING_NAME` environment variable.
/// Both GG Classic and GG Lite set this for component processes.
pub fn resolve_thing_name() -> Option<String> {
    match std::env::var("AWS_IOT_THING_NAME") {
        Ok(name) if !name.is_empty() => {
            tracing::info!(thing_name = %name, "Resolved thing name");
            Some(name)
        }
        Ok(_) => {
            tracing::error!("AWS_IOT_THING_NAME is set but empty");
            None
        }
        Err(_) => {
            tracing::error!("AWS_IOT_THING_NAME not set");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    #[test]
    #[serial]
    fn test_resolve_thing_name_present() {
        std::env::set_var("AWS_IOT_THING_NAME", "test-device-01");
        assert_eq!(resolve_thing_name(), Some("test-device-01".to_string()));
        std::env::remove_var("AWS_IOT_THING_NAME");
    }

    #[test]
    #[serial]
    fn test_resolve_thing_name_missing() {
        std::env::remove_var("AWS_IOT_THING_NAME");
        assert_eq!(resolve_thing_name(), None);
    }

    #[test]
    #[serial]
    fn test_resolve_thing_name_empty() {
        std::env::set_var("AWS_IOT_THING_NAME", "");
        assert_eq!(resolve_thing_name(), None);
        std::env::remove_var("AWS_IOT_THING_NAME");
    }
}
