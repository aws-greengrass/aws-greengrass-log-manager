/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.resources;

import com.aws.greengrass.testing.resources.AWSResourceLifecycle;
import com.aws.greengrass.testing.resources.AbstractAWSResourceLifecycle;
import com.google.auto.service.AutoService;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogStreamsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogStreamsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.LogStream;

import java.util.List;
import javax.inject.Inject;

@AutoService(AWSResourceLifecycle.class)
public class CloudWatchLogsLifecycle extends AbstractAWSResourceLifecycle<CloudWatchLogsClient> {

    @Inject
    public CloudWatchLogsLifecycle(CloudWatchLogsClient client) {
        super(client, CloudWatchLogStreamSpec.class);
    }

    /**
     * Retrieves the streams for a given CloudWatch log group if there are any.
     * @param groupName   name of the CloudWatch group
     * @param logStreamNamePattern name of the log CloudWatch log stream
     */
    public List<LogStream> findStream(String groupName, String logStreamNamePattern) {
        DescribeLogStreamsRequest request = DescribeLogStreamsRequest.builder()
                .logGroupName(groupName)
                .logStreamNamePrefix(logStreamNamePattern)
                .descending(true)
                .build();

        DescribeLogStreamsResponse response = client.describeLogStreams(request);
        return response.logStreams();
    }
}
