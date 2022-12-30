/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.resources;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.testing.api.model.TestingModel;
import com.aws.greengrass.testing.resources.AWSResource;
import org.immutables.value.Value;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.DeleteLogStreamRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.ResourceNotFoundException;

@TestingModel
@Value.Immutable
public interface CloudWatchLogStreamModel extends AWSResource<CloudWatchLogsClient> {
    Logger logger = LogManager.getLogger(CloudWatchLogStream.class);

    String groupName();

    String streamName();



    @Override
    default void remove(CloudWatchLogsClient client) {
        DeleteLogStreamRequest request =
                DeleteLogStreamRequest.builder().logGroupName(groupName()).logStreamName(streamName()).build();

        try {
            client.deleteLogStream(request);
        } catch (ResourceNotFoundException notFound) {
            logger.atInfo().kv("streamName", streamName()).cause(notFound).log("Failed to delete stream");
        }
    }
}
