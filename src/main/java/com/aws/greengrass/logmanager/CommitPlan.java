/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.logmanager;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.logmanager.model.CloudWatchAttempt;
import com.aws.greengrass.logmanager.model.CloudWatchAttemptLogFileInformation;
import com.aws.greengrass.logmanager.model.CloudWatchAttemptLogInformation;
import lombok.Value;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Decides which file positions an upload attempt commits. The attempt's read is one sequence of entries, files in
 * read order and entries in byte order. Everything before the first entry that went to a stream that did not upload
 * is committed. Nothing at or after it is.
 */
final class CommitPlan {
    private static final Logger logger = LogManager.getLogger(CommitPlan.class);

    private CommitPlan() {
    }

    /**
     * Position to save for one file.
     */
    @Value
    static class FileCommit {
        CloudWatchAttemptLogInformation stream;
        String fileId;
        long position;
        long lastModifiedTime;
    }

    /**
     * Plans the file positions to commit for an attempt the uploader finished.
     *
     * @param attempt the attempt, with the streams that uploaded in its uploaded set
     * @return one commit per file to update; files without a commit keep their saved position
     */
    static List<FileCommit> of(CloudWatchAttempt attempt) {
        List<String> readOrder = attempt.getFileIdsInReadOrder();
        FileCommit cut = null;
        int cutIndex = readOrder.size();
        for (Map.Entry<String, CloudWatchAttemptLogInformation> stream
                : attempt.getLogStreamsToLogEventsMap().entrySet()) {
            if (attempt.getLogStreamUploadedSet().contains(stream.getKey())) {
                continue;
            }
            for (Map.Entry<String, CloudWatchAttemptLogFileInformation> record
                    : stream.getValue().getAttemptLogFileInformationMap().entrySet()) {
                int index = readOrder.indexOf(record.getKey());
                if (index < 0) {
                    logger.atWarn().kv("logGroup", attempt.getLogGroupName()).kv("fileId", record.getKey())
                            .log("A failed log stream read a file missing from the read order. Committing nothing");
                    return Collections.emptyList();
                }
                long entryStart = firstEntryStart(record.getValue());
                if (cut == null || index < cutIndex || index == cutIndex && entryStart < cut.getPosition()) {
                    cutIndex = index;
                    cut = new FileCommit(stream.getValue(), record.getKey(), entryStart,
                            record.getValue().getLastModifiedTime());
                }
            }
        }
        Map<String, FileCommit> furthestPerFile = furthestPerFile(attempt.getLogStreamsToLogEventsMap().values());
        if (cut == null) {
            return new ArrayList<>(furthestPerFile.values());
        }

        List<FileCommit> commits = new ArrayList<>();
        for (String fileId : readOrder.subList(0, cutIndex)) {
            FileCommit commit = furthestPerFile.get(fileId);
            // Completing a file as new as the cut file would move the watermark past the cut file and skip it
            if (commit != null && commit.getLastModifiedTime() < cut.getLastModifiedTime()) {
                commits.add(commit);
            }
        }
        commits.add(cut);
        logger.atWarn().kv("logGroup", attempt.getLogGroupName()).kv("componentName", cut.getStream()
                .getComponentName()).kv("fileId", cut.getFileId()).kv("position", cut.getPosition())
                .log("Some log streams failed to upload. Committing up to the first entry of a failed stream");
        return commits;
    }

    private static Map<String, FileCommit> furthestPerFile(Collection<CloudWatchAttemptLogInformation> streams) {
        Map<String, FileCommit> furthest = new LinkedHashMap<>();
        streams.forEach(stream -> stream.getAttemptLogFileInformationMap().forEach((fileId, record) -> {
            FileCommit candidate = new FileCommit(stream, fileId, record.getStartPosition() + record.getBytesRead(),
                    record.getLastModifiedTime());
            furthest.merge(fileId, candidate, (current, next) -> next.getPosition() > current.getPosition()
                    ? next : current);
        }));
        return furthest;
    }

    private static long firstEntryStart(CloudWatchAttemptLogFileInformation record) {
        return record.getFirstEntryStartPosition() < 0 ? record.getStartPosition()
                : record.getFirstEntryStartPosition();
    }
}
