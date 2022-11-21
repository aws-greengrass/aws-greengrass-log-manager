/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.aws.greengrass;

import com.aws.greengrass.testing.resources.AWSResourceLifecycle;
import com.aws.greengrass.testing.resources.AbstractAWSResourceLifecycle;
import com.google.auto.service.AutoService;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogGroupsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogGroupsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogStreamsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogStreamsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.LogGroup;
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
     * Retrieves the logGroups matching the prefix.
     * @param prefix log group prefix
     */
    public List<LogGroup> logGroupsByPrefix(String prefix) {
        DescribeLogGroupsRequest request = DescribeLogGroupsRequest.builder().logGroupNamePrefix(prefix).build();
        DescribeLogGroupsResponse response  = client.describeLogGroups(request);
        return response.logGroups();
    }
    /**
     * Retrieves the streams for a given CloudWatch log group if there are any.
     * @param groupName   name of the CloudWatch group
     */
    public List<LogStream> streamsByLogGroupName(String groupName) {
        DescribeLogStreamsRequest request = DescribeLogStreamsRequest.builder().logGroupName(groupName).build();
        DescribeLogStreamsResponse response = client.describeLogStreams(request);
        return response.logStreams();
    }
    public CloudWatchLogsClient getClient(){
        return client;
    }
}