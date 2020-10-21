/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.logmanager.util;

import com.aws.greengrass.deployment.DeviceConfiguration;
import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import javax.inject.Inject;

@Getter
public class CloudWatchClientFactory {
    private static final Logger LOGGER = LogManager.getLogger(CloudWatchClientFactory.class);
    private CloudWatchLogsClient cloudWatchLogsClient;
    private static final String CW_LOGS_FIPS_ENDPOINT = "logs-fips.%s.amazonaws.com";
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
     * @param deviceConfiguration   device configuration
     * @param credentialsProvider   credential provider from TES
     * @param awsConfig             AWS Configuration.
     */
    @Inject
    public CloudWatchClientFactory(DeviceConfiguration deviceConfiguration,
                                   LazyCredentialProvider credentialsProvider,
                                   AWSConfig awsConfig) {
        Region region = Region.of(Coerce.toString(deviceConfiguration.getAWSRegion()));

        if (awsConfig.isFipsEnabled()) {
            try {
                this.cloudWatchLogsClient = CloudWatchLogsClient.builder().credentialsProvider(credentialsProvider)
                        .endpointOverride(new URI(String.format(CW_LOGS_FIPS_ENDPOINT, region)))
                        .overrideConfiguration(ClientOverrideConfiguration.builder().retryPolicy(retryPolicy).build())
                        .region(region).build();
                return;
            } catch (URISyntaxException e) {
                LOGGER.atError().log("Unable to use FIPS endpoint for CW logs", e);
            }
        }

        this.cloudWatchLogsClient = CloudWatchLogsClient.builder().credentialsProvider(credentialsProvider)
                .httpClient(ProxyUtils.getSdkHttpClient())
                .overrideConfiguration(ClientOverrideConfiguration.builder().retryPolicy(retryPolicy).build())
                .region(region).build();
    }
}
