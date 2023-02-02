/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass;

import com.aws.greengrass.resources.CloudWatchLogStreamSpec;
import com.aws.greengrass.resources.CloudWatchLogsLifecycle;
import com.aws.greengrass.testing.features.WaitSteps;
import com.aws.greengrass.testing.model.TestContext;
import com.aws.greengrass.testing.modules.model.AWSResourcesContext;
import com.aws.greengrass.testing.resources.AWSResources;
import com.google.inject.Inject;
import io.cucumber.guice.ScenarioScoped;
import io.cucumber.java.en.Then;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.services.cloudwatchlogs.model.FilteredLogEvent;
import software.amazon.awssdk.services.cloudwatchlogs.model.LogStream;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@ScenarioScoped
public class CloudWatchSteps {
    private final CloudWatchLogsLifecycle logsLifecycle;
    private final TestContext testContext;
    private final WaitSteps waitSteps;
    private static final Logger LOGGER = LogManager.getLogger(CloudWatchSteps.class);
    private final AWSResources resources;
    private final AWSResourcesContext resourceContext;

    @Inject
    @SuppressWarnings("MissingJavadocMethod")
    public CloudWatchSteps(
            CloudWatchLogsLifecycle logsLifecycle,
            TestContext testContext,
            AWSResourcesContext resourcesContext,
            AWSResources resources,
            WaitSteps waitSteps
    ) {
        this.logsLifecycle = logsLifecycle;
        this.testContext = testContext;
        this.resourceContext = resourcesContext;
        this.resources = resources;
        this.waitSteps = waitSteps;
    }

    /**
     * Verifies if a group with the name /aws/greengrass/[componentType]/[region]/[componentName] was created
     * in cloudwatch and additionally verifies if there is a stream named /[yyyy\/MM\/dd]/thing/[thingName] that
     * created within the group.
     *
     * @param componentType           The type of the component {GreengrassSystemComponent, UserComponent}
     * @param componentName           The name of your component e.g. ComponentA, aws.greengrass.LogManager
     * @param logEventsNumber         Number of log events uploaded to the logGroup and specified log stream pattern
     * @param timeout                 Number of seconds to wait before timing out the operation
     * @throws InterruptedException   {@link InterruptedException}
     */

    @Then("I verify that it created a log group for component type {word} for component {word}, and uploaded {int} "
            + "log events within "
            + "{int} seconds in CloudWatch")
    public void verifyCloudWatchGroupWithStreams(String componentType, String componentName, int
                                                 logEventsNumber, int timeout) throws InterruptedException,
            IOException {
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

        Set<String> messages = getLocalLogs("logGeneratorLogger\\w*.log");

        LOGGER.info("Getting log events from log group {} with stream pattern {}", logGroupName,
                logStreamNamePattern);

        waitSteps.untilTrue(() -> isLogEventsSufficient(logGroupName, logStreamNamePattern, logEventsNumber, messages),
                timeout,
                TimeUnit.SECONDS);
    }

    private boolean doesStreamExistInGroup(String logGroupName, String streamName) {
        List<LogStream> streams = logsLifecycle.findStream(logGroupName, streamName);
        boolean exists = streams.stream().anyMatch(stream -> stream.logStreamName().matches(streamName));

        if (exists) {
            LOGGER.info("Found logStream {} in group {}", streamName, logGroupName);
        }

        return exists;
    }

    private boolean isLogEventsSufficient(String logGroupName, String streamName, int targetNumber,
                                          Set<String> messages) {
        List<FilteredLogEvent> events = logsLifecycle.getAllLogEvents(logGroupName, streamName);
        LOGGER.info("events number: " + events.size() + " target number: {}", targetNumber);

        for (FilteredLogEvent event : events) {
            messages.add(event.message());
            LOGGER.info("add one message {}", event.message());
        }

        boolean amountMatched = events.size() == targetNumber;
        if (amountMatched) {
            LOGGER.info("events number: " + events.size());
        }

        return amountMatched;
    }

    private Set<String> getLocalLogs(String pattern) throws IOException {
        Path logsDirectory = testContext.installRoot().resolve("logs");
        File logFiles = logsDirectory.toFile();
        LOGGER.info("grabbing {} file from DUT: ", logFiles.listFiles().length);

        FilenameFilter filter = (d, s) -> {
            return s.matches(pattern);
        };

        File[] filteredFiles = logFiles.listFiles(filter);

        Set<String> localLogs = new HashSet<>();
        LOGGER.info("grabbing file {} matches pattern", filteredFiles.length);
        for (File logfile : filteredFiles) {
            LOGGER.info(logfile.getName());
            localLogs =
                    Files.lines(logfile.toPath()).collect(Collectors.toSet());

        }
        Iterator itr = localLogs.iterator();
        while (itr.hasNext()) {
            LOGGER.info(itr.next());
        }

        return localLogs;
    }
}
