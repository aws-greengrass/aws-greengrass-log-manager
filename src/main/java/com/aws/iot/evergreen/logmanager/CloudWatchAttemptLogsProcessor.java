/*
 *  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *  SPDX-License-Identifier: Apache-2.0
 */

package com.aws.iot.evergreen.logmanager;

import com.aws.iot.evergreen.logmanager.model.CloudWatchAttempt;
import com.aws.iot.evergreen.logmanager.model.ComponentLogFileInformation;

public class CloudWatchAttemptLogsProcessor {
    /**
     * Gets logs from the component which processLogFiles need to be uploaded to CloudWatch.
     *
     * @param componentLogFileInformation log files information for a component to read logs from.
     * @return CloudWatch attempt containing information needed to upload logs from the component to the cloud.
     */
    public CloudWatchAttempt processLogFiles(ComponentLogFileInformation componentLogFileInformation) {
        //TODO: Implement this.
        return new CloudWatchAttempt();
    }
}
