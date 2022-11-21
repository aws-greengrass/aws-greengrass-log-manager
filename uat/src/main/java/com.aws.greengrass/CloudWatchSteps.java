/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass;

import com.aws.greengrass.testing.features.WaitSteps;
import com.aws.greengrass.testing.model.TestContext;
import com.aws.greengrass.testing.modules.model.AWSResourcesContext;
import com.aws.greengrass.testing.resources.AWSResources;
import com.google.inject.Inject;
import io.cucumber.guice.ScenarioScoped;
import io.cucumber.java.en.And;
import io.cucumber.java.en.Then;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.services.cloudwatchlogs.model.GetLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.LogGroup;
import software.amazon.awssdk.services.cloudwatchlogs.model.LogStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
@ScenarioScoped
public class CloudWatchSteps {
    private final CloudWatchLogsLifecycle logsLifecycle;
    private final TestContext testContext;
    private final FileSteps fileSteps;
    private final AWSResourcesContext resourceContext;
    private final AWSResources resources;
    private final WaitSteps waitSteps;
    private static final Logger LOGGER = LogManager.getLogger(CloudWatchSteps.class);
    @Inject
    @SuppressWarnings("MissingJavadocMethod")
    public CloudWatchSteps(
            CloudWatchLogsLifecycle logsLifecycle,
            FileSteps fileSteps,
            TestContext testContext,
            AWSResourcesContext resourcesContext,
            AWSResources resources,
            WaitSteps waitSteps
    ) {
        this.logsLifecycle = logsLifecycle;
        this.testContext = testContext;
        this.resourceContext = resourcesContext;
        this.waitSteps = waitSteps;
        this.resources = resources;
        this.fileSteps=fileSteps;
    }
    /**
     * Verifies if a group with the name /aws/greengrass/[componentType]/[region]/[componentName] was created
     * in cloudwatch and additionally verifies if there is a stream named /[yyyy\/MM\/dd]/thing/[thingName] that
     * created within the group.
     * @param componentType The type of the component {GreengrassSystemComponent, UserComponent}
     * @param componentName The name of your component e.g. ComponentA, aws.greengrass.LogManager
     * @param timeout       Number of seconds to wait before timing out the operation
     * @throws InterruptedException {@link InterruptedException}
     */
    @Then("I verify that it created a log group for component type {word} for component {word}, with streams within "
            + "{int} seconds in CloudWatch")
    public void verifyCloudWatchGroupWithStreams(String componentType, String componentName, int timeout) throws
            InterruptedException {
        LOGGER.info("Start verifyCloudWatchGroupWithStreams");
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
        LOGGER.info("End verifyCloudWatchGroupWithStreams");
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
    /**
     * Arranges some log files with content on the /logs folder for a component
     * to simulate a devices where logs have already bee written.
     * Storing the filedata in msgMap where {map key=filename,map value=random msg }
     * @throws IOException thrown when file fails to be written.
     */
    @And("I verify contains of the {int} file uploaded to Cloudwatch")
    public void verifyFileContains(int numFiles) {
        LOGGER.info("verifyFileContains Start");
        for (int i = 0; i < numFiles; i++) {
            String fileName = String.format("greengrass_%d.log", i);
            try {
                getFileAndReadDataFromCloudWatch(fileSteps.msgMap.get(fileName.toString()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
    /**
     * Gets the currently processing logstreams
     * to simulate a devices where logs have already bee written.
     * filedataBefore  where data is stored before processing
     * GetLogEventsRequest gets the log events with given Group name starting with initial LogstreamName
     * @throws IOException   thrown when file fails to be written.
     */
    private void  getFileAndReadDataFromCloudWatch(List<String> randomMessages)
            throws IOException {
        LOGGER.info("getFileAndReadDataFromCloudWatch Start");
        List<LogStream> streams = logsLifecycle.streamsByLogGroupName("/aws/greengrass/GreengrassSystemComponent/us-west-2/System");
        String filedataBefore = randomMessages.stream().collect(Collectors.joining("\n"));
        GetLogEventsRequest getLogEventsRequest =GetLogEventsRequest.builder()
                .logGroupName( "/aws/greengrass/GreengrassSystemComponent/us-west-2/System")
                .logStreamName(streams.get(0).logStreamName())
                .build();
        String filedata = "";
        int logLimit = logsLifecycle.getClient().getLogEvents(getLogEventsRequest).events().size();
        for (int c = logLimit-1; c >= logLimit -51; c--) {
            filedata=(logsLifecycle.getClient().getLogEvents(getLogEventsRequest).events().get(c).message());
            if(filedataBefore.trim().contains(filedata.trim()))
            {
                LOGGER.info("Inside getFileAndReadData Matched");
                break;
            }
        }
    }
}