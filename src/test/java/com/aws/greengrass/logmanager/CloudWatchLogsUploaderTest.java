/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.logmanager;

import com.aws.greengrass.logmanager.model.CloudWatchAttempt;
import com.aws.greengrass.logmanager.model.CloudWatchAttemptLogFileInformation;
import com.aws.greengrass.logmanager.model.CloudWatchAttemptLogInformation;
import com.aws.greengrass.logmanager.util.CloudWatchClientFactory;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.testcommons.testutilities.GGServiceTestUtil;
import org.hamcrest.collection.IsEmptyCollection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.RequestOverrideConfiguration;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogGroupRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogStreamRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DataAlreadyAcceptedException;
import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;
import software.amazon.awssdk.services.cloudwatchlogs.model.InvalidSequenceTokenException;
import software.amazon.awssdk.services.cloudwatchlogs.model.LimitExceededException;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.ResourceAlreadyExistsException;
import software.amazon.awssdk.services.cloudwatchlogs.model.ResourceNotFoundException;

import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.aws.greengrass.logmanager.model.CloudWatchAttemptLogInformation.EVENT_COMPARATOR;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, GGExtension.class})
public class CloudWatchLogsUploaderTest extends GGServiceTestUtil {
    @Mock
    private CloudWatchClientFactory mockCloudWatchClientFactory;
    @Mock
    private CloudWatchLogsClient mockCloudWatchLogsClient;
    @Captor
    private ArgumentCaptor<PutLogEventsRequest> putLogEventsRequestArgumentCaptor;

    private CloudWatchLogsUploader uploader;

    @BeforeEach
    public void setup() {
        when(mockCloudWatchClientFactory.getCloudWatchLogsClient()).thenReturn(mockCloudWatchLogsClient);
    }

    @Test
    public void GIVEN_mock_cloud_watch_attempt_WHEN_put_events_called_THEN_successfully_uploads_all_log_events(
            ExtensionContext context1) throws InterruptedException {
        ignoreExceptionOfType(context1, ResourceNotFoundException.class);
        String mockGroupName = "testGroup";
        String mockStreamNameForGroup = "testStream";
        String mockSequenceToken = UUID.randomUUID().toString();
        String mockNextSequenceToken = UUID.randomUUID().toString();
        CloudWatchAttempt attempt = new CloudWatchAttempt();
        Map<String, CloudWatchAttemptLogInformation> logSteamForGroup1Map = new ConcurrentHashMap<>();
        Queue<InputLogEvent> inputLogEventsForStream1OfGroup1 = new PriorityQueue<>(EVENT_COMPARATOR);
        inputLogEventsForStream1OfGroup1.add(InputLogEvent.builder()
                .timestamp(Instant.now().toEpochMilli() + 5_000) // Put this in the future to show that sorting on
                // timestamp works
                .message("test")
                .build());
        inputLogEventsForStream1OfGroup1.add(InputLogEvent.builder()
                .timestamp(Instant.now().toEpochMilli())
                .message("test2")
                .build());
        Map<String, CloudWatchAttemptLogFileInformation> attemptLogFileInformationMap = new HashMap<>();
        attemptLogFileInformationMap.put("test.log", CloudWatchAttemptLogFileInformation.builder()
                .startPosition(0)
                .bytesRead(100)
                .build());
        CloudWatchAttemptLogInformation logInfo =
                CloudWatchAttemptLogInformation.builder().logEvents(inputLogEventsForStream1OfGroup1)
                        .attemptLogFileInformationMap(attemptLogFileInformationMap).build();
        logSteamForGroup1Map.put(mockStreamNameForGroup, logInfo);

        // Verify that log events were sorted correctly
        assertThat(logInfo.getSortedLogEvents().get(0).timestamp(),
                is(lessThan(logInfo.getSortedLogEvents().get(1).timestamp())));

        attempt.setLogGroupName(mockGroupName);
        attempt.setLogStreamsToLogEventsMap(logSteamForGroup1Map);
        PutLogEventsResponse response = PutLogEventsResponse.builder().nextSequenceToken(mockNextSequenceToken).build();
        when(mockCloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class)))
                .thenThrow(ResourceNotFoundException.class)
                .thenReturn(response);

        uploader = new CloudWatchLogsUploader(mockCloudWatchClientFactory);
        uploader.addNextSequenceToken(mockGroupName, mockStreamNameForGroup, mockSequenceToken);
        CountDownLatch attemptFinishedLatch = new CountDownLatch(1);
        uploader.registerAttemptStatus(UUID.randomUUID().toString(), cloudWatchAttempt -> {
            assertTrue(cloudWatchAttempt.getLogStreamUploadedSet().contains("testStream"));
            attemptFinishedLatch.countDown();
        });

        uploader.upload(attempt, 1);

        assertTrue(attemptFinishedLatch.await(5, TimeUnit.SECONDS));
        verify(mockCloudWatchLogsClient, times(1)).createLogStream(any(CreateLogStreamRequest.class));
        verify(mockCloudWatchLogsClient, times(1)).createLogGroup(any(CreateLogGroupRequest.class));
        verify(mockCloudWatchLogsClient, times(2)).putLogEvents(putLogEventsRequestArgumentCaptor.capture());

        Set<String> messageTextToCheck = new HashSet<>();
        messageTextToCheck.add("test");
        messageTextToCheck.add("test2");

        List<PutLogEventsRequest> putLogEventsRequests = putLogEventsRequestArgumentCaptor.getAllValues();
        assertEquals(2, putLogEventsRequests.size());

        PutLogEventsRequest request = putLogEventsRequests.get(1);
        assertTrue(request.hasLogEvents());
        assertTrue(request.overrideConfiguration().isPresent());
        RequestOverrideConfiguration requestOverrideConfiguration = request.overrideConfiguration().get();
        List<String> amznLogFormatHeader = requestOverrideConfiguration.headers().get("x-amzn-logs-format");
        assertNotNull(amznLogFormatHeader);
        assertTrue(amznLogFormatHeader.contains("json/emf"));
        assertEquals(mockGroupName, request.logGroupName());
        assertEquals(mockStreamNameForGroup, request.logStreamName());
        assertEquals(mockSequenceToken, request.sequenceToken());
        assertEquals(2, request.logEvents().size());
        request.logEvents().forEach(inputLogEvent -> {
            assertNotNull(inputLogEvent.message());
            assertNotNull(inputLogEvent.timestamp());
            messageTextToCheck.remove(inputLogEvent.message());
        });

        assertEquals(0, messageTextToCheck.size());
        assertEquals(mockNextSequenceToken, uploader.logGroupsToSequenceTokensMap.get(mockGroupName).get(mockStreamNameForGroup));
    }

    @Test
    public void GIVEN_mock_cloud_watch_attempt_WHEN_put_events_throws_DataAlreadyAcceptedException_THEN_successfully_uploads_all_log_events(
            ExtensionContext context1) throws InterruptedException {
        ignoreExceptionOfType(context1, ResourceNotFoundException.class);
        String mockGroupName = "testGroup";
        String mockStreamNameForGroup = "testStream";
        String mockSequenceToken = UUID.randomUUID().toString();
        String mockNextSequenceToken = UUID.randomUUID().toString();
        CloudWatchAttempt attempt = new CloudWatchAttempt();
        Map<String, CloudWatchAttemptLogInformation> logSteamForGroup1Map = new ConcurrentHashMap<>();
        Queue<InputLogEvent> inputLogEventsForStream1OfGroup1 = new PriorityQueue<>(EVENT_COMPARATOR);
        inputLogEventsForStream1OfGroup1.add(InputLogEvent.builder()
                .timestamp(Instant.now().toEpochMilli())
                .message("test")
                .build());
        inputLogEventsForStream1OfGroup1.add(InputLogEvent.builder()
                .timestamp(Instant.now().toEpochMilli())
                .message("test2")
                .build());
        Map<String, CloudWatchAttemptLogFileInformation> attemptLogFileInformationMap = new HashMap<>();
        attemptLogFileInformationMap.put("test.log", CloudWatchAttemptLogFileInformation.builder()
                .startPosition(0)
                .bytesRead(100)
                .build());
        logSteamForGroup1Map.put(mockStreamNameForGroup,
                CloudWatchAttemptLogInformation.builder()
                        .logEvents(inputLogEventsForStream1OfGroup1)
                        .attemptLogFileInformationMap(attemptLogFileInformationMap)
                        .build());
        attempt.setLogGroupName(mockGroupName);
        attempt.setLogStreamsToLogEventsMap(logSteamForGroup1Map);
        DataAlreadyAcceptedException exception = DataAlreadyAcceptedException.builder()
                .expectedSequenceToken(mockNextSequenceToken).build();
        when(mockCloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class)))
                .thenThrow(exception);

        uploader = new CloudWatchLogsUploader(mockCloudWatchClientFactory);
        uploader.addNextSequenceToken(mockGroupName, mockStreamNameForGroup, mockSequenceToken);
        CountDownLatch attemptFinishedLatch = new CountDownLatch(1);
        uploader.registerAttemptStatus(UUID.randomUUID().toString(), cloudWatchAttempt -> {
            assertTrue(cloudWatchAttempt.getLogStreamUploadedSet().contains("testStream"));
            attemptFinishedLatch.countDown();
        });

        uploader.upload(attempt, 1);

        assertTrue(attemptFinishedLatch.await(5, TimeUnit.SECONDS));
        verify(mockCloudWatchLogsClient, times(1)).putLogEvents(putLogEventsRequestArgumentCaptor.capture());

        Set<String> messageTextToCheck = new HashSet<>();
        messageTextToCheck.add("test");
        messageTextToCheck.add("test2");

        List<PutLogEventsRequest> putLogEventsRequests = putLogEventsRequestArgumentCaptor.getAllValues();
        assertEquals(1, putLogEventsRequests.size());

        PutLogEventsRequest request = putLogEventsRequests.get(0);
        assertTrue(request.hasLogEvents());
        assertEquals(mockGroupName, request.logGroupName());
        assertEquals(mockStreamNameForGroup, request.logStreamName());
        assertEquals(mockSequenceToken, request.sequenceToken());
        assertEquals(2, request.logEvents().size());
        request.logEvents().forEach(inputLogEvent -> {
            assertNotNull(inputLogEvent.message());
            assertNotNull(inputLogEvent.timestamp());
            messageTextToCheck.remove(inputLogEvent.message());
        });

        assertEquals(0, messageTextToCheck.size());
        assertEquals(mockNextSequenceToken, uploader.logGroupsToSequenceTokensMap.get(mockGroupName).get(mockStreamNameForGroup));
    }

    @Test
    public void GIVEN_mock_cloud_watch_attempt_WHEN_create_stream_group_throw_ResourceAlreadyExistsException_THEN_successfully_uploads_all_log_events(
            ExtensionContext context1) throws InterruptedException {
        ignoreExceptionOfType(context1, ResourceNotFoundException.class);
        String mockGroupName = "testGroup";
        String mockStreamNameForGroup = "testStream";
        String mockSequenceToken = UUID.randomUUID().toString();
        String mockNextSequenceToken = UUID.randomUUID().toString();
        CloudWatchAttempt attempt = new CloudWatchAttempt();
        Map<String, CloudWatchAttemptLogInformation> logSteamForGroup1Map = new ConcurrentHashMap<>();
        Queue<InputLogEvent> inputLogEventsForStream1OfGroup1 = new PriorityQueue<>(EVENT_COMPARATOR);
        inputLogEventsForStream1OfGroup1.add(InputLogEvent.builder()
                .timestamp(Instant.now().toEpochMilli())
                .message("test")
                .build());
        inputLogEventsForStream1OfGroup1.add(InputLogEvent.builder()
                .timestamp(Instant.now().toEpochMilli())
                .message("test2")
                .build());
        Map<String, CloudWatchAttemptLogFileInformation> attemptLogFileInformationMap = new HashMap<>();
        attemptLogFileInformationMap.put("test.log", CloudWatchAttemptLogFileInformation.builder()
                .startPosition(0)
                .bytesRead(100)
                .build());
        logSteamForGroup1Map.put(mockStreamNameForGroup,
                CloudWatchAttemptLogInformation.builder()
                        .logEvents(inputLogEventsForStream1OfGroup1)
                        .attemptLogFileInformationMap(attemptLogFileInformationMap)
                        .build());
        attempt.setLogGroupName(mockGroupName);
        attempt.setLogStreamsToLogEventsMap(logSteamForGroup1Map);
        PutLogEventsResponse response = PutLogEventsResponse.builder().nextSequenceToken(mockNextSequenceToken).build();
        when(mockCloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class)))
                .thenThrow(ResourceNotFoundException.class)
                .thenReturn(response);
        when(mockCloudWatchLogsClient.createLogGroup(any(CreateLogGroupRequest.class))).thenThrow(ResourceAlreadyExistsException.class);
        when(mockCloudWatchLogsClient.createLogStream(any(CreateLogStreamRequest.class))).thenThrow(ResourceAlreadyExistsException.class);
        uploader = new CloudWatchLogsUploader(mockCloudWatchClientFactory);
        uploader.addNextSequenceToken(mockGroupName, mockStreamNameForGroup, mockSequenceToken);
        CountDownLatch attemptFinishedLatch = new CountDownLatch(1);
        uploader.registerAttemptStatus(UUID.randomUUID().toString(), cloudWatchAttempt -> {
            assertTrue(cloudWatchAttempt.getLogStreamUploadedSet().contains("testStream"));
            attemptFinishedLatch.countDown();
        });

        uploader.upload(attempt, 1);

        assertTrue(attemptFinishedLatch.await(5, TimeUnit.SECONDS));
        verify(mockCloudWatchLogsClient, times(1)).createLogStream(any(CreateLogStreamRequest.class));
        verify(mockCloudWatchLogsClient, times(1)).createLogGroup(any(CreateLogGroupRequest.class));
        verify(mockCloudWatchLogsClient, times(2)).putLogEvents(putLogEventsRequestArgumentCaptor.capture());

        Set<String> messageTextToCheck = new HashSet<>();
        messageTextToCheck.add("test");
        messageTextToCheck.add("test2");

        List<PutLogEventsRequest> putLogEventsRequests = putLogEventsRequestArgumentCaptor.getAllValues();
        assertEquals(2, putLogEventsRequests.size());

        PutLogEventsRequest request = putLogEventsRequests.get(1);
        assertTrue(request.hasLogEvents());
        assertEquals(mockGroupName, request.logGroupName());
        assertEquals(mockStreamNameForGroup, request.logStreamName());
        assertEquals(mockSequenceToken, request.sequenceToken());
        assertEquals(2, request.logEvents().size());
        request.logEvents().forEach(inputLogEvent -> {
            assertNotNull(inputLogEvent.message());
            assertNotNull(inputLogEvent.timestamp());
            messageTextToCheck.remove(inputLogEvent.message());
        });

        assertEquals(0, messageTextToCheck.size());
        assertEquals(mockNextSequenceToken, uploader.logGroupsToSequenceTokensMap.get(mockGroupName).get(mockStreamNameForGroup));
    }

    @Test
    public void GIVEN_mock_cloud_watch_attempt_WHEN_bad_sequence_token_THEN_successfully_gets_correct_token(
            ExtensionContext context1) {
        ignoreExceptionOfType(context1, InvalidSequenceTokenException.class);
        String mockGroupName = "testGroup";
        String mockStreamNameForGroup = "testStream";
        String mockSequenceToken = UUID.randomUUID().toString();
        String mockNextSequenceToken = UUID.randomUUID().toString();
        String mockNextSequenceToken2 = UUID.randomUUID().toString();
        CloudWatchAttempt attempt = new CloudWatchAttempt();
        Map<String, CloudWatchAttemptLogInformation> logSteamForGroup1Map = new ConcurrentHashMap<>();
        Queue<InputLogEvent> inputLogEventsForStream1OfGroup1 = new PriorityQueue<>(EVENT_COMPARATOR);
        inputLogEventsForStream1OfGroup1.add(InputLogEvent.builder()
                .timestamp(Instant.now().toEpochMilli())
                .message("test")
                .build());
        inputLogEventsForStream1OfGroup1.add(InputLogEvent.builder()
                .timestamp(Instant.now().toEpochMilli())
                .message("test2")
                .build());
        Map<String, CloudWatchAttemptLogFileInformation> attemptLogFileInformationMap = new HashMap<>();
        attemptLogFileInformationMap.put("test.log", CloudWatchAttemptLogFileInformation.builder()
                .startPosition(0)
                .bytesRead(100)
                .build());
        logSteamForGroup1Map.put(mockStreamNameForGroup,
                CloudWatchAttemptLogInformation.builder()
                        .logEvents(inputLogEventsForStream1OfGroup1)
                        .attemptLogFileInformationMap(attemptLogFileInformationMap)
                        .build());
        attempt.setLogGroupName(mockGroupName);
        attempt.setLogStreamsToLogEventsMap(logSteamForGroup1Map);
        PutLogEventsResponse putLogEventsResponse = PutLogEventsResponse.builder().nextSequenceToken(mockNextSequenceToken).build();
        when(mockCloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class)))
                .thenThrow(InvalidSequenceTokenException.builder().expectedSequenceToken(mockNextSequenceToken2).build())
                .thenReturn(putLogEventsResponse);

        uploader = new CloudWatchLogsUploader(mockCloudWatchClientFactory);
        uploader.addNextSequenceToken(mockGroupName, mockStreamNameForGroup, mockSequenceToken);
        uploader.upload(attempt, 1);

        verify(mockCloudWatchLogsClient, times(2)).putLogEvents(putLogEventsRequestArgumentCaptor.capture());

        Set<String> messageTextToCheck = new HashSet<>();
        messageTextToCheck.add("test");
        messageTextToCheck.add("test2");

        List<PutLogEventsRequest> putLogEventsRequests = putLogEventsRequestArgumentCaptor.getAllValues();
        assertEquals(2, putLogEventsRequests.size());
        PutLogEventsRequest firstRequest = putLogEventsRequests.get(0);
        assertTrue(firstRequest.hasLogEvents());
        assertEquals(mockGroupName, firstRequest.logGroupName());
        assertEquals(mockStreamNameForGroup, firstRequest.logStreamName());
        assertEquals(mockSequenceToken, firstRequest.sequenceToken());
        assertEquals(2, firstRequest.logEvents().size());

        PutLogEventsRequest request = putLogEventsRequests.get(1);
        assertTrue(request.hasLogEvents());
        assertEquals(mockGroupName, request.logGroupName());
        assertEquals(mockStreamNameForGroup, request.logStreamName());
        assertEquals(mockNextSequenceToken2, request.sequenceToken());
        assertEquals(2, request.logEvents().size());
        request.logEvents().forEach(inputLogEvent -> {
            assertNotNull(inputLogEvent.message());
            assertNotNull(inputLogEvent.timestamp());
            messageTextToCheck.remove(inputLogEvent.message());
        });

        assertEquals(0, messageTextToCheck.size());
        assertEquals(mockNextSequenceToken, uploader.logGroupsToSequenceTokensMap.get(mockGroupName).get(mockStreamNameForGroup));
    }

    @Test
    public void GIVEN_mock_cloud_watch_attempt_WHEN_create_group_throws_LimitExceededException_THEN_attempt_has_nothing_uploaded(
            ExtensionContext context1) throws InterruptedException {
        ignoreExceptionOfType(context1, LimitExceededException.class);
        ignoreExceptionOfType(context1, ResourceNotFoundException.class);
        String mockGroupName = "testGroup";
        String mockStreamNameForGroup = "testStream";
        String mockSequenceToken = UUID.randomUUID().toString();
        CloudWatchAttempt attempt = new CloudWatchAttempt();
        Map<String, CloudWatchAttemptLogInformation> logSteamForGroup1Map = new ConcurrentHashMap<>();
        Queue<InputLogEvent> inputLogEventsForStream1OfGroup1 = new PriorityQueue<>(EVENT_COMPARATOR);
        inputLogEventsForStream1OfGroup1.add(InputLogEvent.builder()
                .timestamp(Instant.now().toEpochMilli())
                .message("test")
                .build());
        inputLogEventsForStream1OfGroup1.add(InputLogEvent.builder()
                .timestamp(Instant.now().toEpochMilli())
                .message("test2")
                .build());
        Map<String, CloudWatchAttemptLogFileInformation> attemptLogFileInformationMap = new HashMap<>();
        attemptLogFileInformationMap.put("test.log", CloudWatchAttemptLogFileInformation.builder()
                .startPosition(0)
                .bytesRead(100)
                .build());
        logSteamForGroup1Map.put(mockStreamNameForGroup,
                CloudWatchAttemptLogInformation.builder()
                        .logEvents(inputLogEventsForStream1OfGroup1)
                        .attemptLogFileInformationMap(attemptLogFileInformationMap)
                        .build());
        attempt.setLogGroupName(mockGroupName);
        attempt.setLogStreamsToLogEventsMap(logSteamForGroup1Map);
        when(mockCloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class))).thenThrow(ResourceNotFoundException.class);
        when(mockCloudWatchLogsClient.createLogGroup(any(CreateLogGroupRequest.class))).thenThrow(LimitExceededException.class);

        CountDownLatch attemptFinishedLatch = new CountDownLatch(1);
        uploader = new CloudWatchLogsUploader(mockCloudWatchClientFactory);
        uploader.registerAttemptStatus(UUID.randomUUID().toString(), cloudWatchAttempt -> {
            assertThat(cloudWatchAttempt.getLogStreamUploadedSet(), IsEmptyCollection.empty());
            attemptFinishedLatch.countDown();
        });
        uploader.addNextSequenceToken(mockGroupName, mockStreamNameForGroup, mockSequenceToken);
        uploader.upload(attempt, 1);

        assertTrue(attemptFinishedLatch.await(5, TimeUnit.SECONDS));
    }

    @Test
    public void GIVEN_mock_cloud_watch_attempt_WHEN_create_stream_throws_LimitExceededException_THEN_attempt_has_nothing_uploaded(
            ExtensionContext context1) throws InterruptedException {
        ignoreExceptionOfType(context1, LimitExceededException.class);
        ignoreExceptionOfType(context1, ResourceNotFoundException.class);
        String mockGroupName = "testGroup";
        String mockStreamNameForGroup = "testStream";
        String mockSequenceToken = UUID.randomUUID().toString();
        CloudWatchAttempt attempt = new CloudWatchAttempt();
        Map<String, CloudWatchAttemptLogInformation> logSteamForGroup1Map = new ConcurrentHashMap<>();
        Queue<InputLogEvent> inputLogEventsForStream1OfGroup1 = new PriorityQueue<>(EVENT_COMPARATOR);
        inputLogEventsForStream1OfGroup1.add(InputLogEvent.builder()
                .timestamp(Instant.now().toEpochMilli())
                .message("test")
                .build());
        inputLogEventsForStream1OfGroup1.add(InputLogEvent.builder()
                .timestamp(Instant.now().toEpochMilli())
                .message("test2")
                .build());
        Map<String, CloudWatchAttemptLogFileInformation> attemptLogFileInformationMap = new HashMap<>();
        attemptLogFileInformationMap.put("test.log", CloudWatchAttemptLogFileInformation.builder()
                .startPosition(0)
                .bytesRead(100)
                .build());
        logSteamForGroup1Map.put(mockStreamNameForGroup,
                CloudWatchAttemptLogInformation.builder()
                        .logEvents(inputLogEventsForStream1OfGroup1)
                        .attemptLogFileInformationMap(attemptLogFileInformationMap)
                        .build());
        attempt.setLogGroupName(mockGroupName);
        attempt.setLogStreamsToLogEventsMap(logSteamForGroup1Map);
        when(mockCloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class))).thenThrow(ResourceNotFoundException.class);
        when(mockCloudWatchLogsClient.createLogStream(any(CreateLogStreamRequest.class))).thenThrow(LimitExceededException.class);

        CountDownLatch attemptFinishedLatch = new CountDownLatch(1);
        uploader = new CloudWatchLogsUploader(mockCloudWatchClientFactory);
        uploader.registerAttemptStatus(UUID.randomUUID().toString(), cloudWatchAttempt -> {
            assertThat(cloudWatchAttempt.getLogStreamUploadedSet(), IsEmptyCollection.empty());
            attemptFinishedLatch.countDown();
        });
        uploader.addNextSequenceToken(mockGroupName, mockStreamNameForGroup, mockSequenceToken);
        uploader.upload(attempt, 1);

        assertTrue(attemptFinishedLatch.await(5, TimeUnit.SECONDS));
    }

    @Test
    public void GIVEN_mock_cloud_watch_attempt_WHEN_put_throws_AwsServiceExceptionTHEN_attempt_has_nothing_uploaded(
            ExtensionContext context1) throws InterruptedException {
        ignoreExceptionOfType(context1, AwsServiceException.class);
        String mockGroupName = "testGroup";
        String mockStreamNameForGroup = "testStream";
        String mockSequenceToken = UUID.randomUUID().toString();
        CloudWatchAttempt attempt = new CloudWatchAttempt();
        Map<String, CloudWatchAttemptLogInformation> logSteamForGroup1Map = new ConcurrentHashMap<>();
        Queue<InputLogEvent> inputLogEventsForStream1OfGroup1 = new PriorityQueue<>(EVENT_COMPARATOR);
        inputLogEventsForStream1OfGroup1.add(InputLogEvent.builder()
                .timestamp(Instant.now().toEpochMilli())
                .message("test")
                .build());
        inputLogEventsForStream1OfGroup1.add(InputLogEvent.builder()
                .timestamp(Instant.now().toEpochMilli())
                .message("test2")
                .build());
        Map<String, CloudWatchAttemptLogFileInformation> attemptLogFileInformationMap = new HashMap<>();
        attemptLogFileInformationMap.put("test.log", CloudWatchAttemptLogFileInformation.builder()
                .startPosition(0)
                .bytesRead(100)
                .build());
        logSteamForGroup1Map.put(mockStreamNameForGroup,
                CloudWatchAttemptLogInformation.builder()
                        .logEvents(inputLogEventsForStream1OfGroup1)
                        .attemptLogFileInformationMap(attemptLogFileInformationMap)
                        .build());
        attempt.setLogGroupName(mockGroupName);
        attempt.setLogStreamsToLogEventsMap(logSteamForGroup1Map);
        when(mockCloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class))).thenThrow(AwsServiceException.class);

        CountDownLatch attemptFinishedLatch = new CountDownLatch(1);
        uploader = new CloudWatchLogsUploader(mockCloudWatchClientFactory);
        uploader.registerAttemptStatus(UUID.randomUUID().toString(), cloudWatchAttempt -> {
            assertThat(cloudWatchAttempt.getLogStreamUploadedSet(), IsEmptyCollection.empty());
            attemptFinishedLatch.countDown();
        });
        uploader.addNextSequenceToken(mockGroupName, mockStreamNameForGroup, mockSequenceToken);
        uploader.upload(attempt, 1);

        assertTrue(attemptFinishedLatch.await(5, TimeUnit.SECONDS));
    }

    @Test
    public void GIVEN_mock_cloud_watch_attempt_WHEN_put_throws_InvalidSequenceTokenException_THEN_attempt_has_nothing_uploaded(
            ExtensionContext context1) throws InterruptedException {
        ignoreExceptionOfType(context1, InvalidSequenceTokenException.class);
        String mockGroupName = "testGroup";
        String mockStreamNameForGroup = "testStream";
        String mockSequenceToken = UUID.randomUUID().toString();
        CloudWatchAttempt attempt = new CloudWatchAttempt();
        Map<String, CloudWatchAttemptLogInformation> logSteamForGroup1Map = new ConcurrentHashMap<>();
        Queue<InputLogEvent> inputLogEventsForStream1OfGroup1 = new PriorityQueue<>(EVENT_COMPARATOR);
        inputLogEventsForStream1OfGroup1.add(InputLogEvent.builder()
                .timestamp(Instant.now().toEpochMilli())
                .message("test")
                .build());
        inputLogEventsForStream1OfGroup1.add(InputLogEvent.builder()
                .timestamp(Instant.now().toEpochMilli())
                .message("test2")
                .build());
        Map<String, CloudWatchAttemptLogFileInformation> attemptLogFileInformationMap = new HashMap<>();
        attemptLogFileInformationMap.put("test.log", CloudWatchAttemptLogFileInformation.builder()
                .startPosition(0)
                .bytesRead(100)
                .build());
        logSteamForGroup1Map.put(mockStreamNameForGroup,
                CloudWatchAttemptLogInformation.builder()
                        .logEvents(inputLogEventsForStream1OfGroup1)
                        .attemptLogFileInformationMap(attemptLogFileInformationMap)
                        .build());
        attempt.setLogGroupName(mockGroupName);
        attempt.setLogStreamsToLogEventsMap(logSteamForGroup1Map);
        InvalidSequenceTokenException e = InvalidSequenceTokenException.builder().expectedSequenceToken("nextToken").build();
        when(mockCloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class))).thenThrow(e);

        CountDownLatch attemptFinishedLatch = new CountDownLatch(1);
        uploader = new CloudWatchLogsUploader(mockCloudWatchClientFactory);
        uploader.registerAttemptStatus(UUID.randomUUID().toString(), cloudWatchAttempt -> {
            assertThat(cloudWatchAttempt.getLogStreamUploadedSet(), IsEmptyCollection.empty());
            attemptFinishedLatch.countDown();
        });
        uploader.addNextSequenceToken(mockGroupName, mockStreamNameForGroup, mockSequenceToken);
        uploader.upload(attempt, 1);

        assertTrue(attemptFinishedLatch.await(5, TimeUnit.SECONDS));

        verify(mockCloudWatchLogsClient, times(3)).putLogEvents(putLogEventsRequestArgumentCaptor.capture());
    }

    @Test
    public void GIVEN_mock_cloud_watch_attempt_WHEN_unregister_listener_THEN_no_status_is_published(
            ExtensionContext context1) throws InterruptedException {
        ignoreExceptionOfType(context1, InvalidSequenceTokenException.class);
        String mockGroupName = "testGroup";
        String mockStreamNameForGroup = "testStream";
        String componentName = UUID.randomUUID().toString();
        String mockSequenceToken = UUID.randomUUID().toString();
        CloudWatchAttempt attempt = new CloudWatchAttempt();
        Map<String, CloudWatchAttemptLogInformation> logSteamForGroup1Map = new ConcurrentHashMap<>();
        Queue<InputLogEvent> inputLogEventsForStream1OfGroup1 = new PriorityQueue<>(EVENT_COMPARATOR);
        inputLogEventsForStream1OfGroup1.add(InputLogEvent.builder()
                .timestamp(Instant.now().toEpochMilli())
                .message("test")
                .build());
        inputLogEventsForStream1OfGroup1.add(InputLogEvent.builder()
                .timestamp(Instant.now().toEpochMilli())
                .message("test2")
                .build());
        Map<String, CloudWatchAttemptLogFileInformation> attemptLogFileInformationMap = new HashMap<>();
        attemptLogFileInformationMap.put("test.log", CloudWatchAttemptLogFileInformation.builder()
                .startPosition(0)
                .bytesRead(100)
                .build());
        logSteamForGroup1Map.put(mockStreamNameForGroup,
                CloudWatchAttemptLogInformation.builder()
                        .logEvents(inputLogEventsForStream1OfGroup1)
                        .attemptLogFileInformationMap(attemptLogFileInformationMap)
                        .build());
        attempt.setLogGroupName(mockGroupName);
        attempt.setLogStreamsToLogEventsMap(logSteamForGroup1Map);
        InvalidSequenceTokenException e = InvalidSequenceTokenException.builder().expectedSequenceToken("nextToken").build();
        when(mockCloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class))).thenThrow(e);

        CountDownLatch attemptFinishedLatch = new CountDownLatch(1);
        uploader = new CloudWatchLogsUploader(mockCloudWatchClientFactory);
        uploader.registerAttemptStatus(componentName, cloudWatchAttempt -> {
            assertThat(cloudWatchAttempt.getLogStreamUploadedSet(), IsEmptyCollection.empty());
            attemptFinishedLatch.countDown();
        });
        uploader.addNextSequenceToken(mockGroupName, mockStreamNameForGroup, mockSequenceToken);
        uploader.unregisterAttemptStatus(componentName);
        uploader.upload(attempt, 1);

        assertFalse(attemptFinishedLatch.await(5, TimeUnit.SECONDS));

        verify(mockCloudWatchLogsClient, times(3)).putLogEvents(putLogEventsRequestArgumentCaptor.capture());
    }

    @Test
    public void GIVEN_mock_cloud_watch_attempt_with_no_log_events_WHEN_put_events_called_THEN_does_not_upload_to_cloud(
            ExtensionContext context1) throws InterruptedException {
        ignoreExceptionOfType(context1, ResourceNotFoundException.class);
        String mockGroupName = "testGroup";
        String mockStreamNameForGroup = "testStream";
        String mockSequenceToken = UUID.randomUUID().toString();
        CloudWatchAttempt attempt = new CloudWatchAttempt();
        Map<String, CloudWatchAttemptLogInformation> logSteamForGroup1Map = new ConcurrentHashMap<>();
        Map<String, CloudWatchAttemptLogFileInformation> attemptLogFileInformationMap = new HashMap<>();
        attemptLogFileInformationMap.put("test.log", CloudWatchAttemptLogFileInformation.builder()
                .startPosition(0)
                .bytesRead(100)
                .build());
        logSteamForGroup1Map.put(mockStreamNameForGroup,
                CloudWatchAttemptLogInformation.builder()
                        .logEvents(new PriorityQueue<>(EVENT_COMPARATOR))
                        .attemptLogFileInformationMap(attemptLogFileInformationMap)
                        .build());
        attempt.setLogGroupName(mockGroupName);
        attempt.setLogStreamsToLogEventsMap(logSteamForGroup1Map);

        uploader = new CloudWatchLogsUploader(mockCloudWatchClientFactory);
        uploader.addNextSequenceToken(mockGroupName, mockStreamNameForGroup, mockSequenceToken);
        CountDownLatch attemptFinishedLatch = new CountDownLatch(1);
        uploader.registerAttemptStatus(UUID.randomUUID().toString(), cloudWatchAttempt -> {
            assertTrue(cloudWatchAttempt.getLogStreamUploadedSet().contains("testStream"));
            attemptFinishedLatch.countDown();
        });

        uploader.upload(attempt, 1);

        assertTrue(attemptFinishedLatch.await(5, TimeUnit.SECONDS));
        verify(mockCloudWatchLogsClient, times(0)).putLogEvents(any(PutLogEventsRequest.class));
    }
}
