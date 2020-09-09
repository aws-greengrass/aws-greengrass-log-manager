/*
 *  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *  SPDX-License-Identifier: Apache-2.0
 */

package com.aws.iot.evergreen.logmanager;

import com.aws.iot.evergreen.logmanager.model.CloudWatchAttempt;
import com.aws.iot.evergreen.logmanager.model.CloudWatchAttemptLogFileInformation;
import com.aws.iot.evergreen.logmanager.model.CloudWatchAttemptLogInformation;
import com.aws.iot.evergreen.logmanager.util.CloudWatchClientFactory;
import com.aws.iot.evergreen.testcommons.testutilities.EGExtension;
import com.aws.iot.evergreen.testcommons.testutilities.EGServiceTestUtil;
import org.hamcrest.collection.IsEmptyCollection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogGroupRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogStreamRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogStreamsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogStreamsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;
import software.amazon.awssdk.services.cloudwatchlogs.model.InvalidSequenceTokenException;
import software.amazon.awssdk.services.cloudwatchlogs.model.LimitExceededException;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.ResourceNotFoundException;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.aws.iot.evergreen.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, EGExtension.class})
public class CloudWatchLogsUploaderTest extends EGServiceTestUtil  {
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
        List<InputLogEvent> inputLogEventsForStream1OfGroup1 = new ArrayList<>();
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
            ExtensionContext context1) throws InterruptedException {
        ignoreExceptionOfType(context1, InvalidSequenceTokenException.class);
        String mockGroupName = "testGroup";
        String mockStreamNameForGroup = "testStream";
        String mockSequenceToken = UUID.randomUUID().toString();
        String mockNextSequenceToken = UUID.randomUUID().toString();
        String mockNextSequenceToken2 = UUID.randomUUID().toString();
        CloudWatchAttempt attempt = new CloudWatchAttempt();
        Map<String, CloudWatchAttemptLogInformation> logSteamForGroup1Map = new ConcurrentHashMap<>();
        List<InputLogEvent> inputLogEventsForStream1OfGroup1 = new ArrayList<>();
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
                .thenThrow(InvalidSequenceTokenException.class)
                .thenReturn(putLogEventsResponse);
        DescribeLogStreamsResponse response = DescribeLogStreamsResponse.builder().nextToken(mockNextSequenceToken2).build();
        when(mockCloudWatchLogsClient.describeLogStreams(any(DescribeLogStreamsRequest.class))).thenReturn(response);

        uploader = new CloudWatchLogsUploader(mockCloudWatchClientFactory);
        uploader.addNextSequenceToken(mockGroupName, mockStreamNameForGroup, mockSequenceToken);
        uploader.upload(attempt, 1);

        verify(mockCloudWatchLogsClient, times(1)).describeLogStreams(any(DescribeLogStreamsRequest.class));
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
        List<InputLogEvent> inputLogEventsForStream1OfGroup1 = new ArrayList<>();
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
        List<InputLogEvent> inputLogEventsForStream1OfGroup1 = new ArrayList<>();
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
}
