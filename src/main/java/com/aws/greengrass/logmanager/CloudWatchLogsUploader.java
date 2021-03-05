/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.logmanager;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.logmanager.model.CloudWatchAttempt;
import com.aws.greengrass.logmanager.util.CloudWatchClientFactory;
import lombok.Setter;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.CloudWatchLogsException;
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogGroupRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogStreamRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DataAlreadyAcceptedException;
import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;
import software.amazon.awssdk.services.cloudwatchlogs.model.InvalidSequenceTokenException;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.ResourceAlreadyExistsException;
import software.amazon.awssdk.services.cloudwatchlogs.model.ResourceNotFoundException;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import javax.inject.Inject;

public class CloudWatchLogsUploader {
    private final Logger logger = LogManager.getLogger(CloudWatchLogsUploader.class);
    private final Map<String, Consumer<CloudWatchAttempt>> listeners = new ConcurrentHashMap<>();
    @Setter
    private CloudWatchLogsClient cloudWatchLogsClient;
    private static final int MAX_RETRIES = 3;

    final Map<String, Map<String, String>> logGroupsToSequenceTokensMap = new ConcurrentHashMap<>();

    @Inject
    public CloudWatchLogsUploader(CloudWatchClientFactory cloudWatchClientFactory) {
        this.cloudWatchLogsClient = cloudWatchClientFactory.getCloudWatchLogsClient();
    }

    /**
     * Uploads the input log events for each stream within the CloudWatchAttempt. It will create the log group/stream
     * if necessary.
     * After successfully uploading a log stream to cloudwatch, it will add the necessary information so that the log
     * manager can persist that information.
     *
     * @param attempt   {@link CloudWatchAttempt}
     * @param tryCount  The upload try count.
     */
    public void upload(CloudWatchAttempt attempt, int tryCount) {
        try {
            attempt.getLogStreamsToLogEventsMap().forEach((streamName, attemptLogInformation) -> {
                boolean success = uploadLogs(attempt.getLogGroupName(), streamName,
                        attemptLogInformation.getLogEvents(), tryCount);
                if (success) {
                    attempt.getLogStreamUploadedSet().add(streamName);
                }
            });
        } catch (SdkException e) {
            logger.atError().cause(e).log("Unable to upload logs for log group {}", attempt.getLogGroupName());
        }
        listeners.values().forEach(consumer -> consumer.accept(attempt));
    }

    /**
     * Register a listener to get cloud watch attempt status.
     *
     * @param callback The callback function to invoke.
     * @param name     The unique name for the service subscribing.
     */
    public void registerAttemptStatus(String name, Consumer<CloudWatchAttempt> callback) {
        listeners.putIfAbsent(name, callback);
    }

    /**
     * Unregister a listener to get cloud watch attempt status.
     *
     * @param name The unique name for the service subscribing.
     */
    public void unregisterAttemptStatus(String name) {
        listeners.remove(name);
    }

    /**
     * Uploads logs to CloudWatch.
     *
     * @param logEvents     The log events to upload to CloudWatch.
     * @param logGroupName  The log group name to upload the logs to.
     * @param logStreamName The log steam name to upload the logs to.
     * @param tryCount      The upload try count.
     */
    private boolean uploadLogs(String logGroupName, String logStreamName, List<InputLogEvent> logEvents,
                               int tryCount) {
        if (tryCount > MAX_RETRIES) {
            logger.atWarn().log("Unable to upload {} logs to {}-{} as max retry ({}) times reached",
                    logEvents.size(), logGroupName, logStreamName, MAX_RETRIES);
            return false;
        }
        // If there are no logs available to upload, then return true so that we don't read those files again.
        // This can occur if the log files read have logs below the desired log level.
        // By returning true, we will ensure that we won't read those log files again.
        if (logEvents.isEmpty()) {
            return true;
        }
        logger.atTrace().log("Uploading {} logs to {}-{}", logEvents.size(), logGroupName, logStreamName);
        AtomicReference<String> sequenceToken = new AtomicReference<>();
        logGroupsToSequenceTokensMap.computeIfPresent(logGroupName, (groupName, streamToSequenceTokenMap) -> {
            streamToSequenceTokenMap.computeIfPresent(logStreamName, (streamName, savedSequenceToken) -> {
                sequenceToken.set(savedSequenceToken);
                return savedSequenceToken;
            });
            return streamToSequenceTokenMap;
        });
        PutLogEventsRequest request = PutLogEventsRequest.builder()
                .logEvents(logEvents)
                .logGroupName(logGroupName)
                .logStreamName(logStreamName)
                .sequenceToken(sequenceToken.get())
                .build();
        try {
            PutLogEventsResponse putLogEventsResponse = this.cloudWatchLogsClient.putLogEvents(request);
            addNextSequenceToken(logGroupName, logStreamName, putLogEventsResponse.nextSequenceToken());
            return true;
        } catch (InvalidSequenceTokenException e) {
            // Get correct token using describe
            logger.atError().cause(e)
                    .log("Invalid token while uploading logs to {}-{}. Getting the correct sequence token.",
                            logGroupName,
                    logStreamName);
            addNextSequenceToken(logGroupName, logStreamName, e.expectedSequenceToken());
            // TODO: better do the retry mechanism? Maybe need to have a scheduled task to handle this.
            return uploadLogs(logGroupName, logStreamName, logEvents, tryCount + 1);
        } catch (DataAlreadyAcceptedException e) {
            // Don't do anything since the data already exists.
            addNextSequenceToken(logGroupName, logStreamName, e.expectedSequenceToken());
            return true;
        } catch (ResourceNotFoundException e) {
            // Handle no log group/log stream
            logger.atInfo().log("Unable to find log group- {} or log stream - {}. Creating them now.",
                    logGroupName, logStreamName);
            createNewLogGroup(logGroupName);
            createNewLogSteam(logGroupName, logStreamName);
            return uploadLogs(logGroupName, logStreamName, logEvents, tryCount + 1);
        } catch (AwsServiceException e) {
            // Back off for some time and then retry
            logger.atError().cause(e).log("Unable to upload {} logs to {}-{}", logEvents.size(), logGroupName,
                    logStreamName);
        }
        return false;
    }

    /**
     * Creates the log group on CloudWatch.
     *
     * @param logGroupName  The log group name.
     */
    private void createNewLogGroup(String logGroupName) {
        logger.atDebug().log("Creating log group {}", logGroupName);
        CreateLogGroupRequest request = CreateLogGroupRequest.builder().logGroupName(logGroupName).build();
        try {
            this.cloudWatchLogsClient.createLogGroup(request);
        } catch (ResourceAlreadyExistsException e) {
            // Don't do anything if the resource already exists.
        } catch (CloudWatchLogsException e) {
            logger.atError().cause(e).log("Unable to create log group {}.", logGroupName);
            throw e;
        }
    }

    /**
     * Creates the log stream within the log group.
     *
     * @param logGroupName  The log group name.
     * @param logStreamName The log stream name.
     */
    private void createNewLogSteam(String logGroupName, String logStreamName) {
        logger.atDebug().log("Creating log stream {} for group {}", logStreamName, logGroupName);
        CreateLogStreamRequest request = CreateLogStreamRequest.builder()
                .logGroupName(logGroupName)
                .logStreamName(logStreamName)
                .build();
        try {
            this.cloudWatchLogsClient.createLogStream(request);
        } catch (ResourceAlreadyExistsException e) {
            // Don't do anything if the resource already exists.
        } catch (CloudWatchLogsException e) {
            logger.atError().cause(e).log("Unable to create log stream {} for group {}.", logStreamName, logGroupName);
            throw e;
        }
    }

    /**
     * Keeping this package-private for unit tests.
     *
     * @param logGroupName      The CloudWatch log group
     * @param logStreamName     The CloudWatch log stream within the log group
     * @param nextSequenceToken The next token to be associated to the PutEvents request for the log group and stream.
     */
    void addNextSequenceToken(String logGroupName, String logStreamName, String nextSequenceToken) {
        // TODO: clean up old streams/tokens. Maybe allow a max of 5 streams for each log group.
        logGroupsToSequenceTokensMap.computeIfAbsent(logGroupName, key -> new ConcurrentHashMap<>())
                .put(logStreamName, nextSequenceToken);
    }
}
