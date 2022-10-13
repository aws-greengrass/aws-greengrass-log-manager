/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.logmanager.model;

import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

@Builder
@Data
@Getter
@Setter
public class CloudWatchAttemptLogFileInformation {
    private long startPosition;
    private long bytesRead;
    private long lastModifiedTime;
    //TODO: this fileHash added here is only for passing the tests for the current small PR, it will be removed and
    // passed in another way, when the feature is fully enbaled.
    private String fileHash;
}
