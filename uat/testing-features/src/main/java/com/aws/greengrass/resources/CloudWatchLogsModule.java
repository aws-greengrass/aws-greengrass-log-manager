/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.resources;

import com.aws.greengrass.testing.modules.AbstractAWSResourceModule;
import com.aws.greengrass.testing.modules.model.AWSResourcesContext;
import com.google.auto.service.AutoService;
import com.google.inject.Module;
import com.google.inject.Provides;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;

import javax.inject.Singleton;

@AutoService(Module.class)
public class CloudWatchLogsModule extends AbstractAWSResourceModule<CloudWatchLogsClient, CloudWatchLogsLifecycle> {

    @Provides
    @Singleton
    @Override
    protected CloudWatchLogsClient providesClient(
            AwsCredentialsProvider provider,
            AWSResourcesContext context,
            ApacheHttpClient.Builder httpClientBuilder) {
        return CloudWatchLogsClient.builder()
                .credentialsProvider(provider)
                .region(context.region())
                .httpClientBuilder(httpClientBuilder)
                .build();
    }
}
