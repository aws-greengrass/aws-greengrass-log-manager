/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.logmanager.model;

import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;

@Builder
@Data
@Getter
@Setter
public class CloudWatchAttemptLogInformation {
    public static final Comparator<InputLogEvent> EVENT_COMPARATOR = Comparator.comparing(InputLogEvent::timestamp);
    @Builder.Default
    private Queue<InputLogEvent> logEvents = new PriorityQueue<>(EVENT_COMPARATOR);
    @Builder.Default
    private Map<String, CloudWatchAttemptLogFileInformation> attemptLogFileInformationMap = new HashMap<>();
    private String componentName;
    private LogFileGroup logFileGroup;

    /**
     * Get the log events in chronological order.
     *
     * @return sorted events
     */
    public List<InputLogEvent> getSortedLogEvents() {
        // Sort by timestamp because CloudWatch requires that the logs are in chronological order
        return new ArrayList<>(logEvents);
    }
}
