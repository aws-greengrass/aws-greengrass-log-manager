/*
 *  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *  SPDX-License-Identifier: Apache-2.0
 */

package com.aws.iot.evergreen.logmanager;

import com.aws.iot.evergreen.logging.api.Logger;
import com.aws.iot.evergreen.logging.impl.LogManager;
import com.aws.iot.evergreen.logmanager.model.CloudWatchAttempt;
import com.aws.iot.evergreen.logmanager.util.CloudWatchClientFactory;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.CloudWatchLogsException;
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogGroupRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogStreamRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DataAlreadyAcceptedException;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogStreamsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogStreamsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;
import software.amazon.awssdk.services.cloudwatchlogs.model.InvalidSequenceTokenException;
import software.amazon.awssdk.services.cloudwatchlogs.model.LimitExceededException;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.ResourceAlreadyExistsException;
import software.amazon.awssdk.services.cloudwatchlogs.model.ResourceNotFoundException;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import javax.inject.Inject;

public class CloudWatchLogsUploader {
    private final Logger logger = LogManager.getLogger(CloudWatchLogsUploader.class);
    private final Map<String, Consumer<CloudWatchAttempt>> listeners = new ConcurrentHashMap<>();
    // https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/cloudwatch_limits_cwl.html
    // TODO: Implement some back off.
    //private static final int MAX_TPS_SEC = 5;
    //private final List<CloudWatchAttempt> retryAttemptList = new ArrayList<>();
    private final CloudWatchLogsClient cloudWatchLogsClient;
    private static final int MAX_RETRIES = 5;

    final Map<String, Map<String, String>> logGroupsToSequenceTokensMap = new ConcurrentHashMap<>();

    @Inject
    public CloudWatchLogsUploader(CloudWatchClientFactory cloudWatchClientFactory) {
        this.cloudWatchLogsClient = cloudWatchClientFactory.getCloudWatchLogsClient();
    }

    /**
     * Create Log group and log stream if necessary.
     *
     * @param attempt {@link CloudWatchAttempt}
     */
    public boolean upload(CloudWatchAttempt attempt) {
        try {
            attempt.getLogStreamsToLogEventsMap().forEach((streamName, attemptLogInformation) -> {
                boolean success = uploadLogs(attempt.getLogGroupName(), streamName,
                        attemptLogInformation.getLogEvents(), 1);
                if (success) {
                    attempt.getLogStreamUploadedSet().add(streamName);
                }
            });
        } catch (LimitExceededException e) {
            attempt.setLastException(e);
        }
        listeners.values().forEach(consumer -> consumer.accept(attempt));
        return true;
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
    public void unRegisterAttemptStatus(String name) {
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
            logger.atDebug().log("Unable to upload {} logs to {}-{}", logEvents.size(), logGroupName,
                    logStreamName);
            return false;
        }
        logger.atDebug().log("Uploading {} logs to {}-{}", logEvents.size(), logGroupName, logStreamName);
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
            logger.atError().cause(e).log("Get correct token.");
            Optional<String> sequenceNumber = getSequenceToken(logGroupName, logStreamName);
            sequenceNumber.ifPresent(s -> addNextSequenceToken(logGroupName, logStreamName, s));
            // TODO: better do the retry mechanism? Maybe need to have a scheduled task to handle this.
            return uploadLogs(logGroupName, logStreamName, logEvents, tryCount + 1);
        } catch (DataAlreadyAcceptedException e) {
            // Don't do anything since the data already exists.
        } catch (ResourceNotFoundException e) {
            // Handle no log group/log stream
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

    private void createNewLogGroup(String logGroupName) {
        logger.atDebug().log("Creating log group {}", logGroupName);
        CreateLogGroupRequest request = CreateLogGroupRequest.builder().logGroupName(logGroupName).build();
        try {
            this.cloudWatchLogsClient.createLogGroup(request);
        } catch (ResourceAlreadyExistsException e) {
            // Don't do anything if the resource already exists.
        } catch (LimitExceededException e) {
            // Back off for some time before retrying.
            // TODO: implement backoff.
            logger.atError().cause(e).log("Unable to create log group {}. Retrying in some time.",
                    logGroupName);
            throw e;
        } catch (CloudWatchLogsException e) {
            logger.atError().cause(e).log("Unable to create log group {}.", logGroupName);
        }
    }

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
        } catch (LimitExceededException e) {
            // Back off for some time before retrying.
            logger.atError().cause(e)
                    .log("Unable to create log stream {} for group {}. Retrying in some time.",
                            logStreamName, logGroupName);
            throw e;
        } catch (CloudWatchLogsException e) {
            logger.atError().cause(e).log("Unable to create log stream {} for group {}.", logStreamName, logGroupName);
        }
    }

    private Optional<String> getSequenceToken(String logGroupName, String logStreamName) {
        logger.atDebug().log("Getting sequence number for stream {} in group {}", logStreamName, logGroupName);
        DescribeLogStreamsRequest request = DescribeLogStreamsRequest.builder()
                .logGroupName(logGroupName)
                .logStreamNamePrefix(logStreamName)
                .build();
        try {
            DescribeLogStreamsResponse response = this.cloudWatchLogsClient.describeLogStreams(request);
            return Optional.of(response.nextToken());
        } catch (ResourceNotFoundException e) {
            // Should never happen since we would make put request before this. If the log group/stream does not exist,
            // we would have gotten this exception that time.
        } catch (CloudWatchLogsException e) {
            logger.atError().cause(e).log("Unable to get next sequence number for stream {} in group {}.",
                    logStreamName, logGroupName);
        }
        return Optional.empty();
    }

    /**
     * Keeping this package-private for unit tests.
     *
     * @param logGroupName      The CloudWatch log group
     * @param logStreamName     The CloudWatch log stream within the log group
     * @param nextSequenceToken The next token to be associated to the PutEvents request for the log group and stream.
     */
    void addNextSequenceToken(String logGroupName, String logStreamName, String nextSequenceToken) {
        Map<String, String> logStreamToSequenceTokenMap =
                logGroupsToSequenceTokensMap.getOrDefault(logGroupName, new ConcurrentHashMap<>());
        logStreamToSequenceTokenMap.put(logStreamName, nextSequenceToken);
        // TODO: clean up old streams/tokens. Maybe allow a mex of 5 streams for each log group.
        logGroupsToSequenceTokensMap.put(logGroupName, logStreamToSequenceTokenMap);
    }
}
