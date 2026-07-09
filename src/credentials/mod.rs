// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! ThingName resolution for Greengrass.
//!
//! Credentials are handled by the AWS SDK default chain (`aws_config::load_defaults`),
//! which picks up TES-provided credentials automatically when running as a Greengrass
//! component. Both Classic and Lite set `AWS_IOT_THING_NAME` for component processes.

/// Resolve the thing name from the `AWS_IOT_THING_NAME` environment variable.
///
/// Returns `None` (after logging an error) when the variable is unset or empty, so the
/// caller can fall back to a placeholder. Both GG Classic and GG Lite set this for
/// component processes.
#[must_use]
pub fn resolve_thing_name() -> Option<String> {
    resolve_thing_name_from(|key| std::env::var(key))
}

/// Resolve the thing name using an injected environment lookup.
///
/// Split out from [`resolve_thing_name`] so tests can supply a closure instead of
/// mutating process-global environment state.
fn resolve_thing_name_from(
    get_var: impl Fn(&str) -> Result<String, std::env::VarError>,
) -> Option<String> {
    match get_var("AWS_IOT_THING_NAME") {
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

    #[test]
    fn test_resolve_thing_name_present() {
        assert_eq!(
            resolve_thing_name_from(|_: &str| Ok("test-device-01".to_string())),
            Some("test-device-01".to_string())
        );
    }

    #[test]
    fn test_resolve_thing_name_missing() {
        assert_eq!(
            resolve_thing_name_from(|_: &str| Err(std::env::VarError::NotPresent)),
            None
        );
    }

    #[test]
    fn test_resolve_thing_name_empty() {
        assert_eq!(resolve_thing_name_from(|_: &str| Ok(String::new())), None);
    }
}
