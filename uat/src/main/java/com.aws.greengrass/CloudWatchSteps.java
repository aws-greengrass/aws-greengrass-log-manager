/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass;

import com.aws.greengrass.testing.model.TestContext;
import com.aws.greengrass.testing.features.WaitSteps;
import com.aws.greengrass.testing.modules.model.AWSResourcesContext;
import com.aws.greengrass.testing.resources.cloudwatch.CloudWatchLogsLifecycle;
import com.google.inject.Inject;
import io.cucumber.guice.ScenarioScoped;
import io.cucumber.java.en.Then;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.services.cloudwatchlogs.model.LogGroup;
import software.amazon.awssdk.services.cloudwatchlogs.model.LogStream;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

@ScenarioScoped
public class CloudWatchSteps{
    private final CloudWatchLogsLifecycle logsLifecycle;
    private final TestContext testContext;
    private final AWSResourcesContext resourceContext;
    private final WaitSteps waitSteps;

    private static final Logger LOGGER = LogManager.getLogger(CloudWatchSteps.class);


    @Inject
    @SuppressWarnings("MissingJavadocMethod")
    public CloudWatchSteps(
            CloudWatchLogsLifecycle logsLifecycle,
            TestContext testContext,
            AWSResourcesContext resourcesContext,
            WaitSteps waitSteps
    ) {
        this.logsLifecycle = logsLifecycle;
        this.testContext = testContext;
        this.resourceContext = resourcesContext;
        this.waitSteps = waitSteps;
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

        LOGGER.info("Verifying log group {} with stream {} was created", logGroupName, logStreamNamePattern);

        int operationTimeout = timeout / 2;
        waitSteps.untilTrue(() -> doesLogGroupExist(logGroupName), operationTimeout, TimeUnit.SECONDS);
        waitSteps.untilTrue(() -> doesStreamExistInGroup(logGroupName, logStreamNamePattern), operationTimeout,
                TimeUnit.SECONDS);
    }

    private boolean doesLogGroupExist(String logGroupName) {
        List<LogGroup> groups = logsLifecycle.logGroupsByPrefix(logGroupName);
        boolean exists = groups.stream().anyMatch(group -> group.logGroupName().equals(logGroupName));

        if (exists) {
            LOGGER.info("Found logGroup {}", logGroupName);
        }

        return exists;
    }

    private boolean doesStreamExistInGroup(String logGroupName, String streamName) {
        List<LogStream> streams = logsLifecycle.streamsByLogGroupName(logGroupName);
        boolean exists = streams.stream().anyMatch(stream -> stream.logStreamName().matches(streamName));

        if (exists) {
            LOGGER.info("Found logStream {} in group {}", streamName, logGroupName);
        }

        return exists;
    }
}