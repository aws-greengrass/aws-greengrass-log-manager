/*
 *  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *  SPDX-License-Identifier: Apache-2.0
 */

package com.aws.iot.evergreen.logmanager.model;

import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@NoArgsConstructor
@Getter
@Data
@Setter
public class CloudWatchAttempt {
    // TODO: Need to implement retry mechanism.
    protected static final int MAX_RETRIES = 5;
    private Map<String, Map<String, CloudWatchAttemptLogInformation>> logGroupsToLogStreamsMap;

    /**
     * This will be used in the uploader to determine whether it is time to stop retrying to upload this attempt.
     * TODO: Need to implement this.
     */
    private int retryCounts;
    /**
     * This will be used in the uploader to keep track of which log groups and log streams in an attempt have been
     * successfully uploaded to cloud.
     */
    private Map<String, List<String>> logStreamUploadedMap = new ConcurrentHashMap<>();
}
