/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.resources;

import com.aws.greengrass.testing.api.model.TestingModel;
import com.aws.greengrass.testing.resources.AWSResources;
import com.aws.greengrass.testing.resources.ResourceSpec;
import org.immutables.value.Value;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;

@TestingModel
@Value.Immutable
public interface CloudWatchLogStreamSpecModel extends ResourceSpec<CloudWatchLogsClient, CloudWatchLogStream> {
    String logGroupName();

    String logStreamName();

    @Override
    default CloudWatchLogStreamSpec create(CloudWatchLogsClient client, AWSResources resources) {
        // This method doesn't actually create a CloudWatch stream. It just registers the resource as
        // created do it can get deleted during teardown.
        CloudWatchLogStream stream = CloudWatchLogStream.builder()
                .groupName(logGroupName())
                .streamName(logStreamName())
                .build();

        return CloudWatchLogStreamSpec.builder()
                .from(this)
                .created(true)
                .resource(stream)
                .build();
    }
}
