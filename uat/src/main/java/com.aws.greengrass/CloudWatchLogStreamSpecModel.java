/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass;

import com.aws.greengrass.testing.api.model.TestingModel;
import com.aws.greengrass.testing.resources.AWSResources;
import com.aws.greengrass.testing.resources.ResourceSpec;
import org.immutables.value.Value;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import javax.annotation.Nullable;
@TestingModel
@Value.Immutable
public interface CloudWatchLogStreamSpecModel extends ResourceSpec<CloudWatchLogsClient, CloudWatchLogStream> {
    @Nullable
    @Override
    CloudWatchLogStream resource();
    String groupName();
    String streamName();
    @Override
    default CloudWatchLogStreamSpec create(CloudWatchLogsClient client, AWSResources resources) {
        return CloudWatchLogStreamSpec.builder().build();

    }
}