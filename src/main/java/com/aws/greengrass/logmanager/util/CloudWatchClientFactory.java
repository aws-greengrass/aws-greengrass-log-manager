/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.logmanager.util;

import com.aws.greengrass.deployment.DeviceConfiguration;
import com.aws.greengrass.tes.LazyCredentialProvider;
import com.aws.greengrass.util.Coerce;
import com.aws.greengrass.util.ProxyUtils;
import lombok.Getter;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy;
import software.amazon.awssdk.core.retry.conditions.OrRetryCondition;
import software.amazon.awssdk.core.retry.conditions.RetryCondition;
import software.amazon.awssdk.core.retry.conditions.RetryOnExceptionsCondition;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.LimitExceededException;
import software.amazon.awssdk.services.iam.model.ServiceFailureException;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;
import javax.inject.Inject;

@Getter
public class CloudWatchClientFactory {
    private final Supplier<CloudWatchLogsClient> clientFactory;
    private final SdkClientWrapper<CloudWatchLogsClient> wrapper;
    private final Region region;
    private final LazyCredentialProvider credentialsProvider;
    //TODO: Handle fips
    //private static String CLOUD_WATCH_FIPS_HOST = "logs-fips.%s.amazonaws.com";
    private static final Set<Class<? extends Exception>> retryableCWLogsExceptions =
            new HashSet<>(Arrays.asList(LimitExceededException.class, ServiceFailureException.class));

    private static final RetryCondition retryCondition = OrRetryCondition
            .create(RetryCondition.defaultRetryCondition(),
                    RetryOnExceptionsCondition.create(retryableCWLogsExceptions));

    private static final RetryPolicy retryPolicy =
            RetryPolicy.builder().numRetries(5).backoffStrategy(BackoffStrategy.defaultStrategy())
                    .retryCondition(retryCondition).build();

    /**
     * Constructor.
     *
     * @param deviceConfiguration device configuration
     * @param credentialsProvider credential provider from TES
     */
    @Inject
    public CloudWatchClientFactory(DeviceConfiguration deviceConfiguration,
                                   LazyCredentialProvider credentialsProvider) {
        this.region = Region.of(Coerce.toString(deviceConfiguration.getAWSRegion()));
        this.credentialsProvider = credentialsProvider;
        this.clientFactory = this::createClient;
        this.wrapper = new SdkClientWrapper<>(clientFactory);
    }

    private CloudWatchLogsClient createClient() {
        return CloudWatchLogsClient.builder()
                .credentialsProvider(credentialsProvider)
                .httpClient(ProxyUtils.getSdkHttpClient())
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .retryPolicy(retryPolicy)
                        .build())
                .region(region)
                .build();
    }
}
