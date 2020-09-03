/*
 *  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *  SPDX-License-Identifier: Apache-2.0
 */

package com.aws.iot.evergreen.logmanager;

import com.aws.iot.evergreen.logmanager.model.CloudWatchAttempt;
import com.aws.iot.evergreen.logmanager.model.ComponentLogFileInformation;

import java.util.Collection;

public class CloudWatchLogsMerger {
    /**
     * Does a k-way merge on all the files from all components.
     *
     * @param componentLogFileInformation log files information for each component to read logs from.
     * @return CloudWatch attempt containing information needed to upload logs from different component to the cloud.
     */
    public CloudWatchAttempt performKWayMerge(Collection<ComponentLogFileInformation> componentLogFileInformation) {
        //TODO: Implement this.
        return new CloudWatchAttempt();
    }
}
