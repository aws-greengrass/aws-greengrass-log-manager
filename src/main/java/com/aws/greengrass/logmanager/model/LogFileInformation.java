/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.logmanager.model;

import lombok.Builder;
import lombok.Getter;
import lombok.Value;

import java.io.File;

@Builder
@Value
@Getter
public class LogFileInformation {
    private File file;
    private long startPosition;
}
