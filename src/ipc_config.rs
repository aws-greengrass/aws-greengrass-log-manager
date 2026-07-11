// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! IPC configuration loading via the `aws-greengrass-component-sdk` crate
//! (imported as `gg_sdk`).
//!
//! Loads the component's own merged configuration from the Greengrass runtime
//! over IPC (`GetConfiguration`), converting the SDK's `Object` into a
//! `serde_json::Value` for the existing JSON-based config loader. Works on both
//! Greengrass Classic and Greengrass Lite, which speak the same component IPC
//! protocol.

/// Connect to the Greengrass runtime over IPC, returning a reusable handle.
///
/// Returns `None` when IPC is unavailable (e.g. not running under a Greengrass
/// runtime), so the caller can fall through to the next config source.
///
/// This MUST be called at most once per process: `gg_sdk::Sdk::init()` panics if
/// called more than once. The returned handle is a cheap `Copy` value and should
/// be reused for both the startup config read and the configuration-update
/// subscription rather than reconnecting.
#[cfg(feature = "gg-ipc")]
pub fn connect_ipc() -> Option<gg_sdk::Sdk> {
    use gg_sdk::Sdk;

    // Fast-fail: skip IPC unless the runtime has provided the component IPC socket.
    if std::env::var("AWS_GG_NUCLEUS_DOMAIN_SOCKET_FILEPATH_FOR_COMPONENT").is_err() {
        return None;
    }

    tracing::info!("Connecting to Greengrass IPC");
    let sdk = Sdk::init();
    if let Err(e) = sdk.connect() {
        tracing::warn!(error = %e, "GG IPC connect failed, using defaults");
        return None;
    }
    Some(sdk)
}

/// Read the component's own merged configuration over an already-connected IPC
/// handle (`GetConfiguration`), converting it to JSON for the config loader.
///
/// Returns `None` when the configuration is empty or the read fails, so the
/// caller falls back to the previous/next config source. Callable repeatedly on
/// the same handle (e.g. once at startup and again on each configuration-update
/// notification) — but only from a thread that owns the handle, never from a
/// subscription callback (the SDK rejects IPC calls made on the callback thread).
#[cfg(feature = "gg-ipc")]
pub fn read_config(sdk: gg_sdk::Sdk) -> Option<String> {
    use core::mem::MaybeUninit;

    let mut buf = [MaybeUninit::uninit(); 16384];
    match sdk.get_config(&[], None, &mut buf) {
        Ok(obj) => {
            let json_value = object_to_json(&obj);
            if json_value.is_null() || json_value.as_object().is_some_and(|m| m.is_empty()) {
                tracing::warn!("IPC GetConfiguration returned empty config, using defaults");
                return None;
            }
            match serde_json::to_string(&json_value) {
                Ok(json) => {
                    tracing::info!("Configuration loaded from Greengrass IPC");
                    Some(json)
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Failed to serialize IPC config to JSON");
                    None
                }
            }
        }
        Err(e) => {
            tracing::warn!(error = %e, "GetConfiguration IPC failed, using defaults");
            None
        }
    }
}

#[cfg(feature = "gg-ipc")]
fn object_to_json(obj: &gg_sdk::Object) -> serde_json::Value {
    use gg_sdk::UnpackedObject;
    match obj.unpack() {
        UnpackedObject::Null => serde_json::Value::Null,
        UnpackedObject::Bool(b) => serde_json::Value::Bool(b),
        UnpackedObject::I64(i) => serde_json::json!(i),
        UnpackedObject::F64(f) => serde_json::json!(f),
        UnpackedObject::Buf(s) => serde_json::Value::String(s.to_string()),
        UnpackedObject::List(list) => {
            serde_json::Value::Array(list.iter().map(object_to_json).collect())
        }
        UnpackedObject::Map(map) => {
            let obj = map
                .iter()
                .map(|kv| (kv.key().to_string(), object_to_json(kv.val())))
                .collect();
            serde_json::Value::Object(obj)
        }
    }
}
