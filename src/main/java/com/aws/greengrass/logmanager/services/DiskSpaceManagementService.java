/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.logmanager.services;

import com.aws.greengrass.logmanager.model.LogFile;
import com.aws.greengrass.logmanager.model.LogFileGroup;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;


public class DiskSpaceManagementService {
    /**
     * Constructor.
     */
    public DiskSpaceManagementService() {}

    public void freeDiskSpace(LogFileGroup group, Instant lastUpdated) {
        if (group.hasExceededDiskUsage()) {
            return;
        }

        if (!group.getMaxBytes().isPresent()) {
            return;
        }

        long bytesDeleted = 0;
        long minimumBytesToBeDeleted = Math.max(group.totalSizeInBytes() - group.getMaxBytes().get(), 0);

        List<LogFile> deletableFiles = group.getLogFiles().stream()
                .filter(file ->  lastUpdated.isAfter(Instant.ofEpochMilli(file.lastModified())))
                .collect(Collectors.toList());

        for (LogFile logFile: deletableFiles) {
            if (bytesDeleted < minimumBytesToBeDeleted) break;
            group.remove(logFile);
        }
    }
}