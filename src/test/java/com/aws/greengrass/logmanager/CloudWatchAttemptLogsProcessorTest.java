/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.logmanager;

import com.aws.greengrass.config.Topic;
import com.aws.greengrass.deployment.DeviceConfiguration;
import com.aws.greengrass.logmanager.model.CloudWatchAttempt;
import com.aws.greengrass.logmanager.model.CloudWatchAttemptLogInformation;
import com.aws.greengrass.logmanager.model.ComponentLogFileInformation;
import com.aws.greengrass.logmanager.model.ComponentType;
import com.aws.greengrass.logmanager.model.LogFileInformation;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.testcommons.testutilities.GGServiceTestUtil;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.hamcrest.collection.IsEmptyCollection;
import org.hamcrest.core.IsNot;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.event.Level;
import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

import static com.aws.greengrass.deployment.DeviceConfiguration.DEVICE_PARAM_AWS_REGION;
import static com.aws.greengrass.deployment.DeviceConfiguration.DEVICE_PARAM_THING_NAME;
import static com.aws.greengrass.logmanager.CloudWatchAttemptLogsProcessor.DEFAULT_LOG_GROUP_NAME;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;


@ExtendWith({MockitoExtension.class, GGExtension.class})
class CloudWatchAttemptLogsProcessorTest extends GGServiceTestUtil {
    private static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyyy/MM/dd", Locale.ENGLISH);
    @Mock
    private DeviceConfiguration mockDeviceConfiguration;
    @TempDir
    static Path directoryPath;

    private CloudWatchAttemptLogsProcessor logsProcessor;

    @BeforeEach
    public void startup() {
        Topic thingNameTopic = Topic.of(context, DEVICE_PARAM_THING_NAME, "testThing");
        Topic regionTopic = Topic.of(context, DEVICE_PARAM_AWS_REGION, "testRegion");
        lenient().when(mockDeviceConfiguration.getThingName()).thenReturn(thingNameTopic);
        lenient().when(mockDeviceConfiguration.getAWSRegion()).thenReturn(regionTopic);
    }

    @Test
    void GIVEN_one_system_component_one_file_less_than_max_WHEN_merge_THEN_reads_entire_file(ExtensionContext ec)
            throws URISyntaxException {
        ignoreExceptionOfType(ec, DateTimeParseException.class);
        File file1 = new File(getClass().getResource("testlogs2.log").toURI());
        List<LogFileInformation> logFileInformationSet = new ArrayList<>();
        logFileInformationSet.add(LogFileInformation.builder().startPosition(0).file(file1).build());
        ComponentLogFileInformation componentLogFileInformation = ComponentLogFileInformation.builder()
                .name("TestComponent")
                .multiLineStartPattern(Pattern.compile("^[^\\s]+(\\s+[^\\s]+)*$"))
                .desiredLogLevel(Level.INFO)
                .componentType(ComponentType.GreengrassSystemComponent)
                .logFileInformationList(logFileInformationSet)
                .build();
        logsProcessor = new CloudWatchAttemptLogsProcessor(mockDeviceConfiguration);
        CloudWatchAttempt attempt = logsProcessor.processLogFiles(componentLogFileInformation);
        assertNotNull(attempt);

        assertNotNull(attempt.getLogStreamsToLogEventsMap());
        assertThat(attempt.getLogStreamsToLogEventsMap().entrySet(), IsNot.not(IsEmptyCollection.empty()));
        String logGroup = calculateLogGroupName(ComponentType.GreengrassSystemComponent, "testRegion", "TestComponent");
        assertEquals(attempt.getLogGroupName(), logGroup);
        String logStream = calculateLogStreamName("testThing");
        assertTrue(attempt.getLogStreamsToLogEventsMap().containsKey(logStream));
        CloudWatchAttemptLogInformation logEventsForStream1 = attempt.getLogStreamsToLogEventsMap().get(logStream);
        assertNotNull(logEventsForStream1.getLogEvents());
        assertEquals(13, logEventsForStream1.getLogEvents().size());
        assertTrue(logEventsForStream1.getAttemptLogFileInformationMap().containsKey(file1.getAbsolutePath()));
        assertEquals(0, logEventsForStream1.getAttemptLogFileInformationMap().get(file1.getAbsolutePath()).getStartPosition());
        assertEquals(2943, logEventsForStream1.getAttemptLogFileInformationMap().get(file1.getAbsolutePath()).getBytesRead());
        assertEquals("TestComponent", logEventsForStream1.getComponentName());
        for (InputLogEvent logEvent: logEventsForStream1.getLogEvents()) {
            Instant logTimestamp = Instant.ofEpochMilli(logEvent.timestamp());
            assertTrue(logTimestamp.isBefore(Instant.now()));
            LocalDateTime localDate = LocalDateTime.ofInstant(logTimestamp, ZoneOffset.UTC);
            assertEquals(2020, localDate.getYear());
            assertEquals(12, localDate.getMonth().getValue());
            assertEquals(17, localDate.getDayOfMonth());
        }
    }

    @Test
    void GIVEN_one_user_component_one_file_less_than_max_WHEN_merge_THEN_reads_entire_file(ExtensionContext ec)
            throws URISyntaxException {
        ignoreExceptionOfType(ec, DateTimeParseException.class);

        File file1 = new File(getClass().getResource("testlogs2.log").toURI());
        List<LogFileInformation> logFileInformationSet = new ArrayList<>();
        logFileInformationSet.add(LogFileInformation.builder().startPosition(0).file(file1).build());
        ComponentLogFileInformation componentLogFileInformation = ComponentLogFileInformation.builder()
                .name("TestComponent")
                .multiLineStartPattern(Pattern.compile("^[^\\s]+(\\s+[^\\s]+)*$"))
                .desiredLogLevel(Level.INFO)
                .componentType(ComponentType.UserComponent)
                .logFileInformationList(logFileInformationSet)
                .build();
        logsProcessor = new CloudWatchAttemptLogsProcessor(mockDeviceConfiguration);
        CloudWatchAttempt attempt = logsProcessor.processLogFiles(componentLogFileInformation);
        assertNotNull(attempt);

        assertNotNull(attempt.getLogStreamsToLogEventsMap());
        assertThat(attempt.getLogStreamsToLogEventsMap().entrySet(), IsNot.not(IsEmptyCollection.empty()));
        String logGroup = calculateLogGroupName(ComponentType.UserComponent, "testRegion", "TestComponent");
        assertEquals(attempt.getLogGroupName(), logGroup);
        String logStream = calculateLogStreamName("testThing");
        assertTrue(attempt.getLogStreamsToLogEventsMap().containsKey(logStream));
        CloudWatchAttemptLogInformation logEventsForStream1 = attempt.getLogStreamsToLogEventsMap().get(logStream);
        assertNotNull(logEventsForStream1.getLogEvents());
        assertEquals(13, logEventsForStream1.getLogEvents().size());
        assertTrue(logEventsForStream1.getAttemptLogFileInformationMap().containsKey(file1.getAbsolutePath()));
        assertEquals(0, logEventsForStream1.getAttemptLogFileInformationMap().get(file1.getAbsolutePath()).getStartPosition());
        assertEquals(2943, logEventsForStream1.getAttemptLogFileInformationMap().get(file1.getAbsolutePath()).getBytesRead());
        assertEquals("TestComponent", logEventsForStream1.getComponentName());
        for (InputLogEvent logEvent: logEventsForStream1.getLogEvents()) {
            Instant logTimestamp = Instant.ofEpochMilli(logEvent.timestamp());
            assertTrue(logTimestamp.isBefore(Instant.now()));
            LocalDateTime localDate = LocalDateTime.ofInstant(logTimestamp, ZoneOffset.UTC);
            assertEquals(2020, localDate.getYear());
            assertEquals(12, localDate.getMonth().getValue());
            assertEquals(17, localDate.getDayOfMonth());
        }
    }

    @Test
    void GIVEN_one_component_WHEN_one_file_less_than_max_THEN_reads_entire_file(
            ExtensionContext ec) throws URISyntaxException {
        ignoreExceptionOfType(ec, DateTimeParseException.class);

        File file1 = new File(getClass().getResource("testlogs2.log").toURI());
        List<LogFileInformation> logFileInformationSet = new ArrayList<>();
        logFileInformationSet.add(LogFileInformation.builder().startPosition(0).file(file1).build());
        ComponentLogFileInformation componentLogFileInformation = ComponentLogFileInformation.builder()
                .name("TestComponent")
                .multiLineStartPattern(Pattern.compile("^[^\\s]+(\\s+[^\\s]+)*$"))
                .desiredLogLevel(Level.INFO)
                .componentType(ComponentType.GreengrassSystemComponent)
                .logFileInformationList(logFileInformationSet)
                .build();
        logsProcessor = new CloudWatchAttemptLogsProcessor(mockDeviceConfiguration);
        CloudWatchAttempt attempt = logsProcessor.processLogFiles(componentLogFileInformation);
        assertNotNull(attempt);

        assertNotNull(attempt.getLogStreamsToLogEventsMap());
        assertThat(attempt.getLogStreamsToLogEventsMap().entrySet(), IsNot.not(IsEmptyCollection.empty()));
        String logGroup = calculateLogGroupName(ComponentType.GreengrassSystemComponent, "testRegion", "TestComponent");
        assertEquals(attempt.getLogGroupName(), logGroup);
        String logStream = calculateLogStreamName("testThing");
        assertTrue(attempt.getLogStreamsToLogEventsMap().containsKey(logStream));
        CloudWatchAttemptLogInformation logEventsForStream1 = attempt.getLogStreamsToLogEventsMap().get(logStream);
        assertNotNull(logEventsForStream1.getLogEvents());
        assertEquals(13, logEventsForStream1.getLogEvents().size());
        assertTrue(logEventsForStream1.getAttemptLogFileInformationMap().containsKey(file1.getAbsolutePath()));
        assertEquals(0, logEventsForStream1.getAttemptLogFileInformationMap().get(file1.getAbsolutePath()).getStartPosition());
        assertEquals(2943, logEventsForStream1.getAttemptLogFileInformationMap().get(file1.getAbsolutePath()).getBytesRead());
        assertEquals("TestComponent", logEventsForStream1.getComponentName());
        for (InputLogEvent logEvent: logEventsForStream1.getLogEvents()) {
            Instant logTimestamp = Instant.ofEpochMilli(logEvent.timestamp());
            assertTrue(logTimestamp.isBefore(Instant.now()));
            LocalDateTime localDate = LocalDateTime.ofInstant(logTimestamp, ZoneOffset.UTC);
            assertEquals(2020, localDate.getYear());
            assertEquals(12, localDate.getMonth().getValue());
            assertEquals(17, localDate.getDayOfMonth());
        }
    }

    @Test
    void GIVEN_one_component_one_file_more_than_max_WHEN_merge_THEN_reads_partial_file(ExtensionContext context1)
            throws IOException {

        ignoreExceptionOfType(context1, DateTimeParseException.class);
        File file = new File(directoryPath.resolve("greengrass_test.log").toUri());
        assertTrue(file.createNewFile());
        assertTrue(file.setReadable(true));
        assertTrue(file.setWritable(true));
        try (OutputStream fileOutputStream = Files.newOutputStream(file.toPath())) {
            for (int i = 0; i < 1024; i++) {
                int length = 1024;
                boolean useLetters = true;
                boolean useNumbers = false;
                StringBuilder generatedString = new StringBuilder(RandomStringUtils.random(length, useLetters, useNumbers));
                generatedString.append("\r\n");
                fileOutputStream.write(generatedString.toString().getBytes(StandardCharsets.UTF_8));
            }
        }

        try {
            List<LogFileInformation> logFileInformationSet = new ArrayList<>();
            logFileInformationSet.add(LogFileInformation.builder().startPosition(0).file(file).build());
            ComponentLogFileInformation componentLogFileInformation = ComponentLogFileInformation.builder()
                    .name("TestComponent")
                    .multiLineStartPattern(Pattern.compile("^[^\\s]+(\\s+[^\\s]+)*$"))
                    .desiredLogLevel(Level.INFO)
                    .componentType(ComponentType.GreengrassSystemComponent)
                    .logFileInformationList(logFileInformationSet)
                    .build();

            logsProcessor = new CloudWatchAttemptLogsProcessor(mockDeviceConfiguration);
            CloudWatchAttempt attempt = logsProcessor.processLogFiles(componentLogFileInformation);
            assertNotNull(attempt);

            assertNotNull(attempt.getLogStreamsToLogEventsMap());
            assertThat(attempt.getLogStreamsToLogEventsMap().entrySet(), IsNot.not(IsEmptyCollection.empty()));
            String logGroup = calculateLogGroupName(ComponentType.GreengrassSystemComponent, "testRegion", "TestComponent");
            assertEquals(attempt.getLogGroupName(), logGroup);
            String logStream = calculateLogStreamName("testThing");
            assertTrue(attempt.getLogStreamsToLogEventsMap().containsKey(logStream));
            CloudWatchAttemptLogInformation logEventsForStream1 = attempt.getLogStreamsToLogEventsMap().get(logStream);
            assertNotNull(logEventsForStream1.getLogEvents());
            assertEquals(991, logEventsForStream1.getLogEvents().size());
            assertTrue(logEventsForStream1.getAttemptLogFileInformationMap().containsKey(file.getAbsolutePath()));
            assertEquals(0, logEventsForStream1.getAttemptLogFileInformationMap().get(file.getAbsolutePath()).getStartPosition());
            assertEquals(1016766, logEventsForStream1.getAttemptLogFileInformationMap().get(file.getAbsolutePath()).getBytesRead());
            assertEquals("TestComponent", logEventsForStream1.getComponentName());
            LocalDateTime localDateTimeNow = LocalDateTime.now(ZoneOffset.UTC);
            for (InputLogEvent logEvent: logEventsForStream1.getLogEvents()) {
                Instant logTimestamp = Instant.ofEpochMilli(logEvent.timestamp());
                assertTrue(logTimestamp.isBefore(Instant.now()));
                LocalDateTime localDate = LocalDateTime.ofInstant(logTimestamp, ZoneOffset.UTC);
                assertEquals(localDateTimeNow.getYear(), localDate.getYear());
                assertEquals(localDateTimeNow.getMonth().getValue(), localDate.getMonth().getValue());
                assertEquals(localDateTimeNow.getDayOfMonth(), localDate.getDayOfMonth());
            }
        } finally {
            assertTrue(file.delete());
        }
    }

    @Test
    void GIVEN_one_components_two_file_less_than_max_WHEN_merge_THEN_reads_and_merges_both_files(ExtensionContext ec)
            throws URISyntaxException {
        ignoreExceptionOfType(ec, DateTimeParseException.class);

        File file1 = new File(getClass().getResource("testlogs2.log").toURI());
        File file2 = new File(getClass().getResource("testlogs1.log").toURI());
        List<LogFileInformation> logFileInformationSet = new ArrayList<>();
        logFileInformationSet.add(LogFileInformation.builder().startPosition(0).file(file1).build());
        logFileInformationSet.add(LogFileInformation.builder().startPosition(0).file(file2).build());
        ComponentLogFileInformation componentLogFileInformation = ComponentLogFileInformation.builder()
                .name("TestComponent")
                .multiLineStartPattern(Pattern.compile("^[^\\s]+(\\s+[^\\s]+)*$"))
                .desiredLogLevel(Level.INFO)
                .componentType(ComponentType.GreengrassSystemComponent)
                .logFileInformationList(logFileInformationSet)
                .build();
        logsProcessor = new CloudWatchAttemptLogsProcessor(mockDeviceConfiguration);
        CloudWatchAttempt attempt = logsProcessor.processLogFiles(componentLogFileInformation);

        assertNotNull(attempt);

        assertNotNull(attempt.getLogStreamsToLogEventsMap());
        assertThat(attempt.getLogStreamsToLogEventsMap().entrySet(), IsNot.not(IsEmptyCollection.empty()));
        String logGroup = calculateLogGroupName(ComponentType.GreengrassSystemComponent, "testRegion", "TestComponent");
        assertEquals(attempt.getLogGroupName(), logGroup);
        String logStream = calculateLogStreamName("testThing");
        String logStream2 = "/2020/02/10/thing/testThing";
        assertTrue(attempt.getLogStreamsToLogEventsMap().containsKey(logStream));
        assertTrue(attempt.getLogStreamsToLogEventsMap().containsKey(logStream2));
        CloudWatchAttemptLogInformation logEventsForStream1 = attempt.getLogStreamsToLogEventsMap().get(logStream);
        CloudWatchAttemptLogInformation logEventsForStream2 = attempt.getLogStreamsToLogEventsMap().get(logStream2);
        assertNotNull(logEventsForStream1.getLogEvents());
        assertEquals(13, logEventsForStream1.getLogEvents().size());
        assertTrue(logEventsForStream1.getAttemptLogFileInformationMap().containsKey(file1.getAbsolutePath()));
        assertEquals(0, logEventsForStream1.getAttemptLogFileInformationMap().get(file1.getAbsolutePath()).getStartPosition());
        assertEquals(2943, logEventsForStream1.getAttemptLogFileInformationMap().get(file1.getAbsolutePath()).getBytesRead());
        assertEquals("TestComponent", logEventsForStream1.getComponentName());
        for (InputLogEvent logEvent: logEventsForStream1.getLogEvents()) {
            Instant logTimestamp = Instant.ofEpochMilli(logEvent.timestamp());
            assertTrue(logTimestamp.isBefore(Instant.now()));
            LocalDateTime localDate = LocalDateTime.ofInstant(logTimestamp, ZoneOffset.UTC);
            assertEquals(2020, localDate.getYear());
            assertEquals(12, localDate.getMonth().getValue());
            assertEquals(17, localDate.getDayOfMonth());
        }

        assertNotNull(logEventsForStream2.getLogEvents());
        assertEquals(4, logEventsForStream2.getLogEvents().size());
        assertTrue(logEventsForStream2.getAttemptLogFileInformationMap().containsKey(file2.getAbsolutePath()));
        assertEquals(0, logEventsForStream2.getAttemptLogFileInformationMap().get(file2.getAbsolutePath()).getStartPosition());
        assertEquals(1239, logEventsForStream2.getAttemptLogFileInformationMap().get(file2.getAbsolutePath()).getBytesRead());
        assertEquals("TestComponent", logEventsForStream2.getComponentName());
    }

    @Test
    void GIVEN_componentsToGroups_WHEN_getLogStreamName_THEN_return_valid_log_stream_name() {
        logsProcessor = new CloudWatchAttemptLogsProcessor(mockDeviceConfiguration);

        String thingName1 = StringUtils.repeat("1", 86);
        String logStream1 = logsProcessor.getLogStreamName(thingName1);
        assertEquals(String.format("/{date}/thing/%s", thingName1), logStream1);

        String thingName2 = StringUtils.repeat("2", 85);
        String logStream2 = logsProcessor.getLogStreamName(thingName2);
        assertEquals(String.format("/{date}/thing/%s", thingName2), logStream2);

        String thingName3 = "a:zA_Z:0-9";
        String logStream3 = logsProcessor.getLogStreamName(thingName3);
        assertEquals("/{date}/thing/a+zA_Z+0-9", logStream3);
    }

    private String calculateLogGroupName(ComponentType componentType, String awsRegion, String componentName) {
        return DEFAULT_LOG_GROUP_NAME
                .replace("{componentType}", componentType.toString())
                .replace("{region}", awsRegion)
                .replace("{componentName}", componentName);
    }

    private String calculateLogStreamName(String thingName) {
        synchronized (DATE_FORMATTER) {
            return CloudWatchAttemptLogsProcessor.DEFAULT_LOG_STREAM_NAME
                    .replace("{thingName}", thingName)
                    .replace("{date}", DATE_FORMATTER.format(new Date()));
        }
    }
}
