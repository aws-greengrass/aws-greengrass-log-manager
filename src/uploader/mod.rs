// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! Upload pipeline - batching, CloudWatch client, retry logic, scheduling

mod batcher;
mod cw_client;
mod retry;
