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
    /**
     * The file's id in the listing it was read from.
     */
    private String fileId;
    /**
     * Offset where this stream's first entry from this file begins. -1 when unknown, read as startPosition.
     */
    @Builder.Default
    private long firstEntryStartPosition = -1;
}
