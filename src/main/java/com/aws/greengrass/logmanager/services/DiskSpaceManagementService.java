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
     * Deleted the file to make sure the log group is below its configured
     * disk space usage limit.
     *
     * @param group - a Log File group
     * @param lastUpdated - the timestamp of the last processed file
     */
    public void freeDiskSpace(LogFileGroup group, Instant lastUpdated) {
        if (!group.hasExceededDiskUsage()) {
            return;
        }

        if (!group.getMaxBytes().isPresent()) {
            return;
        }

        long bytesDeleted = 0;
        long minimumBytesToBeDeleted = Math.max(group.totalSizeInBytes() - group.getMaxBytes().get(), 0);

        List<LogFile> deletableFiles = group.getLogFiles().stream()
                .filter(file ->  lastUpdated.isAfter(Instant.ofEpochMilli(file.lastModified())))
                .filter(file -> !group.isActiveFile(file))
                .collect(Collectors.toList());

        for (LogFile logFile: deletableFiles) {
            if (bytesDeleted >= minimumBytesToBeDeleted) {
                break;
            }
            long filesSize = logFile.length();

            if (group.remove(logFile)) {
                bytesDeleted += filesSize;
            }
        }
    }
}