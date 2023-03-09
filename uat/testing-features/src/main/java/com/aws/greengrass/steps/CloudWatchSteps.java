/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.steps;

import com.aws.greengrass.resources.CloudWatchLogStreamSpec;
import com.aws.greengrass.resources.CloudWatchLogsLifecycle;
import com.aws.greengrass.testing.features.WaitSteps;
import com.aws.greengrass.testing.model.TestContext;
import com.aws.greengrass.testing.modules.model.AWSResourcesContext;
import com.aws.greengrass.testing.resources.AWSResources;
import com.google.inject.Inject;
import io.cucumber.guice.ScenarioScoped;
import io.cucumber.java.en.Then;
import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.DeleteLogGroupRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.LogStream;
import software.amazon.awssdk.services.cloudwatchlogs.model.ResourceNotFoundException;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

@ScenarioScoped
public class CloudWatchSteps {
    private final CloudWatchLogsLifecycle logsLifecycle;
    private final TestContext testContext;
    private final WaitSteps waitSteps;
    private static final Logger LOGGER = LogManager.getLogger(CloudWatchSteps.class);
    private final AWSResources resources;
    private final AWSResourcesContext resourceContext;
    private final CloudWatchLogsClient cwClient;
    private final long VERIFICATION_RATE_MILLISECONDS = 5000L;

    Logger logger = LogManager.getLogger(CloudWatchSteps.class);

    @Inject
    @SuppressWarnings("MissingJavadocMethod")
    public CloudWatchSteps(
            CloudWatchLogsLifecycle logsLifecycle,
            TestContext testContext,
            AWSResourcesContext resourcesContext,
            AWSResources resources,
            WaitSteps waitSteps,
            CloudWatchLogsClient cwClient
    ) {
        this.logsLifecycle = logsLifecycle;
        this.testContext = testContext;
        this.resourceContext = resourcesContext;
        this.resources = resources;
        this.waitSteps = waitSteps;
        this.cwClient = cwClient;
    }

    @Then("I delete the log group of type {word} for component {word} if it exists")
    public void deleteLogGroup(String componentType, String componentName) {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy/MM/dd", Locale.ENGLISH);
        formatter.setTimeZone(TimeZone.getTimeZone("UTC")); // All dates are UTC, not local time

        String region = resourceContext.region().toString();
        String logGroupName = String.format("/aws/greengrass/%s/%s/%s", componentType, region, componentName);
        DeleteLogGroupRequest request = DeleteLogGroupRequest.builder().logGroupName(logGroupName).build();

        try {
            this.cwClient.deleteLogGroup(request);
        } catch (ResourceNotFoundException notFound) {
            logger.atDebug().kv("groupName", logGroupName).cause(notFound).log("Failed to delete group");
        }
    }

    /**
     * Verifies if a group with the name /aws/greengrass/[componentType]/[region]/[componentName] was created
     * in cloudwatch and additionally verifies if there is a stream named /[yyyy\/MM\/dd]/thing/[thingName] that
     * created within the group.
     *
     * @param componentType           The type of the component {GreengrassSystemComponent, UserComponent}
     * @param componentName           The name of your component e.g. ComponentA, aws.greengrass.LogManager
     * @param timeout                 Number of seconds to wait before timing out the operation
     * @throws InterruptedException   {@link InterruptedException}
     */

    @Then("I verify that it created a log group for component type {word} for component {word}, with streams within "
            + "{int} seconds in CloudWatch")
    public void verifyCloudWatchGroupWithStreams(String componentType, String componentName, int timeout) throws
            InterruptedException {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy/MM/dd", Locale.ENGLISH);
        formatter.setTimeZone(TimeZone.getTimeZone("UTC")); // All dates are UTC, not local time

        String thingName = testContext.coreThingName();
        String region = resourceContext.region().toString();

        String logGroupName = String.format("/aws/greengrass/%s/%s/%s", componentType, region, componentName);
        String logStreamNamePattern = String.format("/%s/thing/%s", formatter.format(new Date()), thingName);

        resources.create(CloudWatchLogStreamSpec
                .builder()
                .logGroupName(logGroupName)
                .logStreamName(logStreamNamePattern)
                .build());

        LOGGER.info("Verifying log group {} with stream {} was created", logGroupName, logStreamNamePattern);
        waitSteps.untilTrue(() -> doesStreamExistInGroup(logGroupName, logStreamNamePattern), timeout,
                TimeUnit.SECONDS);
    }

    private boolean doesStreamExistInGroup(String logGroupName, String streamName) {
        try {
            // The OTF watch steps check evey 100ms this to avoids hammering the api. Ideally OTF
            // can allow us to configure the check interval rate
            Thread.sleep(VERIFICATION_RATE_MILLISECONDS);
            List<LogStream> streams = logsLifecycle.findStream(logGroupName, streamName);
            boolean exists = streams.stream().anyMatch(stream -> stream.logStreamName().matches(streamName));

            if (exists) {
                LOGGER.info("Found logStream {} in group {}", streamName, logGroupName);
                return  true;
            }
        } catch (ResourceNotFoundException e) {
            LOGGER.info("Did not find logStream {} in group {}", streamName, logGroupName);
        } catch (InterruptedException e) {
            return false;
        }
        return false;
    }
}
