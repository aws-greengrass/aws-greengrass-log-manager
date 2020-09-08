/*
 *  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *  SPDX-License-Identifier: Apache-2.0
 */

package com.aws.iot.evergreen.logmanager;

import com.aws.iot.evergreen.logmanager.model.CloudWatchAttempt;

import java.util.function.Consumer;

public class CloudWatchLogsUploader {
    /**
     * Create Log group and log stream if necessary.
     *
     * @param attempt {@link CloudWatchAttempt}
     */
    public boolean upload(CloudWatchAttempt attempt) {
        return true;
    }

    /**
     * Register a listener to get cloud watch attempt status.
     *
     * @param callback  The callback function to invoke.
     * @param name      The unique name for the service subscribing.
     */
    public void registerAttemptStatus(String name, Consumer<CloudWatchAttempt> callback) {
    }

    /**
     * Unregister a listener to get cloud watch attempt status.
     *
     * @param name      The unique name for the service subscribing.
     */
    public void unregisterAttemptStatus(String name) {
    }
}
