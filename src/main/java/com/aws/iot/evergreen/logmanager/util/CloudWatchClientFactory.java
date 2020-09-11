/* Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0 */

package com.aws.iot.evergreen.logmanager.util;

import com.aws.iot.evergreen.deployment.DeviceConfiguration;
import com.aws.iot.evergreen.tes.LazyCredentialProvider;
import com.aws.iot.evergreen.util.Coerce;
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
import javax.inject.Inject;

@Getter
public class CloudWatchClientFactory {
    private final CloudWatchLogsClient cloudWatchLogsClient;
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
        Region region = Region.of(Coerce.toString(deviceConfiguration.getAWSRegion()));

        this.cloudWatchLogsClient = CloudWatchLogsClient.builder().credentialsProvider(credentialsProvider)
                .overrideConfiguration(ClientOverrideConfiguration.builder().retryPolicy(retryPolicy).build())
                .region(region).build();
    }
}
