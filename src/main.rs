// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! gg-log-manager: Greengrass LogManager Generic Type Component.

// Modules used in subsequent PRs
#[allow(unused_imports)]
use gg_log_manager::{config, credentials, disk, scanner, uploader};

fn main() {
    tracing_subscriber::fmt::init();
    tracing::info!("gg-log-manager starting");
}
