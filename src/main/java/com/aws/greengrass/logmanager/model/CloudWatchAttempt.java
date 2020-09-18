/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.logmanager.model;

import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@NoArgsConstructor
@Getter
@Data
@Setter
public class CloudWatchAttempt {
    // TODO: Need to implement retry mechanism.
    protected static final int MAX_RETRIES = 5;
    //TODO: Check if we can consolidate logStreamUploadedMap here.
    private Map<String, CloudWatchAttemptLogInformation> logStreamsToLogEventsMap;
    private String logGroupName;

    /**
     * This will be used in the uploader to keep track of which log groups and log streams in an attempt have been
     * successfully uploaded to cloud.
     */
    private Set<String> logStreamUploadedSet = new HashSet<>();
}
