/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.logmanager.model;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

@Builder
@Data
@Getter
@Setter
public class CloudWatchAttemptLogInformation {
    public static final Comparator<InputLogEvent> EVENT_COMPARATOR = Comparator.comparing(InputLogEvent::timestamp);
    @Builder.Default
    @Setter(AccessLevel.NONE)
    private Queue<InputLogEvent> logEvents = new ArrayDeque<>();
    @Builder.Default
    @Setter(AccessLevel.NONE)
    private long earliestTimestamp = Long.MAX_VALUE;
    @Builder.Default
    private Map<String, CloudWatchAttemptLogFileInformation> attemptLogFileInformationMap = new HashMap<>();
    private String componentName;
    private LogFileGroup logFileGroup;

    /**
     * Add a log event and track the earliest timestamp.
     *
     * @param logEvent event to add
     */
    public void addLogEvent(InputLogEvent logEvent) {
        logEvents.add(logEvent);
        earliestTimestamp = Math.min(earliestTimestamp, logEvent.timestamp());
    }

    /**
     * Get a read-only view of the log events. Use {@link #addLogEvent} to add events.
     *
     * @return unmodifiable view of the events
     */
    public Collection<InputLogEvent> getLogEvents() {
        return Collections.unmodifiableCollection(logEvents);
    }

    /**
     * Get the log events in chronological order.
     *
     * @return sorted events
     */
    public List<InputLogEvent> getSortedLogEvents() {
        // CloudWatch requires chronological order. The sort is stable, so equal timestamps keep file order.
        ArrayList<InputLogEvent> l = new ArrayList<>(logEvents);
        l.sort(EVENT_COMPARATOR);
        return l;
    }
}
