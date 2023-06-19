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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.core.pagination.sync.SdkIterable;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.GetLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.LogStream;
import software.amazon.awssdk.services.cloudwatchlogs.model.OutputLogEvent;
import software.amazon.awssdk.services.cloudwatchlogs.model.ResourceNotFoundException;
import software.amazon.awssdk.services.cloudwatchlogs.model.ServiceUnavailableException;
import software.amazon.awssdk.services.cloudwatchlogs.paginators.GetLogEventsIterable;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertTrue;


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
    private List<OutputLogEvent> lastReceivedCloudWatchEvents;


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

    private String getLogStreamName() {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy/MM/dd", Locale.ENGLISH);
        formatter.setTimeZone(TimeZone.getTimeZone("UTC")); // All dates are UTC, not local time

        String thingName = testContext.coreThingName();
        return String.format("/%s/thing/%s", formatter.format(new Date()), thingName);
    }

    private String getLogGroupName(String componentType, String componentName) {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy/MM/dd", Locale.ENGLISH);
        formatter.setTimeZone(TimeZone.getTimeZone("UTC")); // All dates are UTC, not local time

        String region = resourceContext.region().toString();
        return String.format("/aws/greengrass/%s/%s/%s", componentType, region, componentName);
    }

    /**
     * Verifies if a group with the name /aws/greengrass/[componentType]/[region]/[componentName] was created
     * in cloudwatch and additionally verifies if there is a stream named /[yyyy\/MM\/dd]/thing/[thingName] that
     * created within the group.
     *
     * @param componentType The type of the component {GreengrassSystemComponent, UserComponent}
     * @param componentName The name of your component e.g. ComponentA, aws.greengrass.LogManager
     * @param timeout       Number of seconds to wait before timing out the operation
     * @throws InterruptedException {@link InterruptedException}
     */

    @Then("I verify that it created a log group of type {word} for component {word}, with streams within "
            + "{int} seconds in CloudWatch")
    public void verifyCloudWatchGroupWithStreams(String componentType, String componentName, int timeout) throws
            Exception {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy/MM/dd", Locale.ENGLISH);
        formatter.setTimeZone(TimeZone.getTimeZone("UTC")); // All dates are UTC, not local time

        String logGroupName = getLogGroupName(componentType, componentName);
        String logStreamName = getLogStreamName();

        resources.create(CloudWatchLogStreamSpec
                .builder()
                .logGroupName(logGroupName)
                .logStreamName(logStreamName)
                .build());

        LOGGER.info("Verifying log group {} with stream {} was created", logGroupName, logStreamName);
        boolean result = waitSteps.untilTrue(() -> doesStreamExistInGroup(logGroupName, logStreamName), timeout,
                TimeUnit.SECONDS);

        if (!result) {
            throw new Exception(String.format("Failed to find log stream %s within group %s",
                    logStreamName, logGroupName));
        }
    }


    @Then("I verify {int} logs for {word} of type {word} have been uploaded to Cloudwatch within {int} seconds")
    public void verifyLogs(int numberOfLogLines, String componentName, String componentType, int timeout) throws
            Exception {
        boolean result = waitSteps.untilTrue(() -> haveAllLogsBeenUploaded(numberOfLogLines, componentName,
                componentType), timeout, TimeUnit.SECONDS);

        if (!result) {
            // Print all the cloudwatch logs it fetched, so we can debug what failed to get uploaded.
            LOGGER.error("Failed to verify {} logs were uploaded to cloudwatch. Below are the logs in CW",
                    numberOfLogLines);
            this.lastReceivedCloudWatchEvents.forEach(e -> LOGGER.info(e.message()));

            throw new Exception(String.format("Failed to verify that %d logs were uploaded to CloudWatch",
                    numberOfLogLines));
        }
    }

    @Then("I verify that logs for {word} of type {word} uploaded with a rate greater than {double} MBps")
    public void verifyLogRate(String componentName, String componentType, double desiredMBps) throws
            Exception {
        // Logs written by the log generator append a sequence number per log line along with the component name
        GetLogEventsRequest request = GetLogEventsRequest.builder()
                .logGroupName(getLogGroupName(componentType, componentName))
                .logStreamName(getLogStreamName())
                .startFromHead(true)
                .endTime(Instant.now().toEpochMilli())
                .limit(10_000) // limit of 10000 logs
                .build();
        GetLogEventsIterable response = cwClient.getLogEventsPaginator(request);
        List<OutputLogEvent> events = response.events().stream().collect(Collectors.toCollection(ArrayList::new));
        events.sort(Comparator.comparingLong(OutputLogEvent::ingestionTime));

        OutputLogEvent first = events.get(0);
        OutputLogEvent last = events.get(events.size() - 1);

        long totalSizeBytes = events.stream().mapToLong(i -> i.message().getBytes(StandardCharsets.UTF_8).length).sum();
        long ingestionTimeSpanMs = last.ingestionTime() - first.ingestionTime();

        double bytesPerSecond = totalSizeBytes / (ingestionTimeSpanMs / 1000.0);

        double gotMBps = bytesPerSecond / (1024.0 * 1024.0);
        LOGGER.info("Found {} logs uploaded at an average rate of {} MBps", events.size(), gotMBps);
        assertTrue(gotMBps >= desiredMBps,
                String.format("Got %f MBps uploaded to CloudWatch, but wanted at least %f", gotMBps, desiredMBps));
    }


    private boolean haveAllLogsBeenUploaded(int numberOfLogLines, String componentName, String componentType) {
        // Logs written by the log generator append a sequence number per log line along with the component name
        GetLogEventsRequest request = GetLogEventsRequest.builder()
                .logGroupName(getLogGroupName(componentType, componentName))
                .logStreamName(getLogStreamName())
                .startFromHead(true)
                .endTime(Instant.now().toEpochMilli())
                .limit(10_000) // limit of 10000 logs
                .build();

        try {
            // The OTF watch steps check evey 100ms this to avoids hammering the api. Ideally OTF
            // can allow us to configure the check interval rate
            TimeUnit.SECONDS.sleep(5L);
            GetLogEventsIterable response = cwClient.getLogEventsPaginator(request);
            List<OutputLogEvent> events = response.events().stream().collect(Collectors.toCollection(ArrayList::new));

            if (events.size() != numberOfLogLines) {
                this.lastReceivedCloudWatchEvents = events;
                LOGGER.info("Found {} events, not the expected {}", events.size(), numberOfLogLines);
                return false;
            }

            events.sort(Comparator.comparingLong(OutputLogEvent::timestamp));
            return wereThereDuplicatesOrMisses(numberOfLogLines, componentName, events);
        } catch (ServiceUnavailableException | InterruptedException e) {
            return false;
        }
    }

    private boolean wereThereDuplicatesOrMisses(int expectedLogLines, String componentName,
                                                List<OutputLogEvent> events) {

        // Logs written by the log generator append a sequence number per log line along with the component name
        for (int i = 1; i <= expectedLogLines; i++) {
            String expected = String.format("(seq: %d)", i, componentName);
            OutputLogEvent event = events.get(i - 1);

            if (!event.message().contains(expected)) {
                LOGGER.error("Mismatch on uploaded logs. Expected log line {} to contain {}", event.message(),
                        expected);
                // Print all the cloudwatch logs it fetched, so we can debug what failed to get uploaded.
                events.forEach(e -> LOGGER.info(e.message()));
                // Exit the test process and log the logs. Sound the alarms we have likely found a bug
                System.exit(1);
                return false;
            }
        }

        return true;
    }

    private boolean doesStreamExistInGroup(String logGroupName, String streamName) {
        try {
            // The OTF watch steps check evey 100ms this to avoids hammering the api. Ideally OTF
            // can allow us to configure the check interval rate
            Thread.sleep(VERIFICATION_RATE_MILLISECONDS);
            SdkIterable<LogStream> streams = logsLifecycle.findStream(logGroupName, streamName);
            boolean exists = streams.stream().anyMatch(stream -> stream.logStreamName().matches(streamName));

            if (exists) {
                LOGGER.info("Found logStream {} in group {}", streamName, logGroupName);
                return true;
            }
        } catch (ResourceNotFoundException e) {
            LOGGER.info("Did not find logStream {} in group {}", streamName, logGroupName);
        } catch (InterruptedException e) {
            return false;
        }
        return false;
    }
}
