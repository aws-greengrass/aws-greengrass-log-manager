/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.logmanager.model;

import lombok.Builder;
import lombok.Getter;
import lombok.Value;


@Builder
@Value
@Getter
public class LogFileInformation {
    private LogFile logFile;
    private long startPosition;
    private String fileHash;
}
