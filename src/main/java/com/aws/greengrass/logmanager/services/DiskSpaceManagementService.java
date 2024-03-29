/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.logmanager.services;

import com.aws.greengrass.logmanager.model.LogFile;
import com.aws.greengrass.logmanager.model.LogFileGroup;

import java.util.ArrayList;
import java.util.List;


public class DiskSpaceManagementService {

    /**
     * Deleted the file to make sure the log group is below its configured
     * disk space usage limit.
     *
     * @param group - a Log File group
     */
    public List<String> freeDiskSpace(LogFileGroup group) {
        if (!group.hasExceededDiskUsage()) {
            return null;
        }

        if (!group.getMaxBytes().isPresent()) {
            return null;
        }

        long bytesDeleted = 0;
        long minimumBytesToBeDeleted = Math.max(group.totalSizeInBytes() - group.getMaxBytes().get(), 0);
        List<LogFile> deletableFiles = group.getProcessedLogFiles();
        List<String> deletedHashes = new ArrayList<>();

        for (LogFile logFile: deletableFiles) {
            if (bytesDeleted >= minimumBytesToBeDeleted) {
                break;
            }
            long filesSize = logFile.length();

            if (group.remove(logFile)) {
                deletedHashes.add(logFile.hashString());
                bytesDeleted += filesSize;
            }
        }

        return deletedHashes;
    }
}