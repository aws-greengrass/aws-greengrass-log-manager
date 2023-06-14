/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.logmanager;

import com.aws.greengrass.config.Topic;
import com.aws.greengrass.deployment.DeviceConfiguration;
import com.aws.greengrass.logmanager.exceptions.InvalidLogGroupException;
import com.aws.greengrass.logmanager.model.CloudWatchAttempt;
import com.aws.greengrass.logmanager.model.CloudWatchAttemptLogInformation;
import com.aws.greengrass.logmanager.model.ComponentLogFileInformation;
import com.aws.greengrass.logmanager.model.ComponentType;
import com.aws.greengrass.logmanager.model.LogFile;
import com.aws.greengrass.logmanager.model.LogFileInformation;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.testcommons.testutilities.GGServiceTestUtil;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.hamcrest.collection.IsEmptyCollection;
import org.hamcrest.core.IsNot;
import org.hamcrest.core.StringContains;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.event.Level;
import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import java.util.regex.Pattern;

import static com.aws.greengrass.deployment.DeviceConfiguration.DEVICE_PARAM_AWS_REGION;
import static com.aws.greengrass.deployment.DeviceConfiguration.DEVICE_PARAM_THING_NAME;
import static com.aws.greengrass.logmanager.CloudWatchAttemptLogsProcessor.DEFAULT_LOG_GROUP_NAME;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;


@SuppressWarnings("PMD.UnsynchronizedStaticFormatter")
@ExtendWith({MockitoExtension.class, GGExtension.class})
class CloudWatchAttemptLogsProcessorTest extends GGServiceTestUtil {
    private static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyyy/MM/dd", Locale.ENGLISH);
    private static final int EVENT_STORAGE_OVERHEAD = 26;
    private static final int TIMESTAMP_BYTES = 8;
    private static final int MAX_EVENT_LENGTH = 1024 * 256 - TIMESTAMP_BYTES - EVENT_STORAGE_OVERHEAD;
    private static final int MAX_NUM_OF_LOG_EVENTS = 10000;
    static {
        DATE_FORMATTER.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    private final Pattern WHITESPACE = Pattern.compile("[^\\s]");

    @Mock
    private DeviceConfiguration mockDeviceConfiguration;
    private final Clock defaultClock = Clock.fixed(Instant.parse("2020-12-17T12:00:00.00Z"), ZoneId.systemDefault());
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
            throws URISyntaxException, IOException, InvalidLogGroupException {
        ignoreExceptionOfType(ec, DateTimeParseException.class);
        File file1 = new File(getClass().getResource("testlogs2.log").toURI());
        LogFile logFile1 = LogFile.of(file1);
        String fileHash = logFile1.hashString();
        List<LogFileInformation> logFileInformationSet = new ArrayList<>();
        logFileInformationSet.add(LogFileInformation.builder().startPosition(0).logFile(logFile1).fileHash(fileHash).build());
        ComponentLogFileInformation componentLogFileInformation = ComponentLogFileInformation.builder()
                .name("TestComponent")
                .desiredLogLevel(Level.INFO)
                .componentType(ComponentType.GreengrassSystemComponent)
                .logFileInformationList(logFileInformationSet)
                .build();
        logsProcessor = new CloudWatchAttemptLogsProcessor(mockDeviceConfiguration, defaultClock);
        CloudWatchAttempt attempt = logsProcessor.processLogFiles(componentLogFileInformation);
        assertNotNull(attempt);

        assertNotNull(attempt.getLogStreamsToLogEventsMap());
        assertThat(attempt.getLogStreamsToLogEventsMap().entrySet(), IsNot.not(IsEmptyCollection.empty()));
        String logGroup = calculateLogGroupName(ComponentType.GreengrassSystemComponent, "testRegion", "TestComponent");
        assertEquals(attempt.getLogGroupName(), logGroup);
        String logStream = "/2020/12/17/thing/testThing";
        assertTrue(attempt.getLogStreamsToLogEventsMap().containsKey(logStream));
        CloudWatchAttemptLogInformation logEventsForStream1 = attempt.getLogStreamsToLogEventsMap().get(logStream);
        assertNotNull(logEventsForStream1.getLogEvents());
        assertEquals(13, logEventsForStream1.getLogEvents().size());
        assertTrue(logEventsForStream1.getAttemptLogFileInformationMap().containsKey(fileHash));
        assertEquals(0, logEventsForStream1.getAttemptLogFileInformationMap().get(fileHash).getStartPosition());
        assertEquals(2943, logEventsForStream1.getAttemptLogFileInformationMap().get(fileHash).getBytesRead());
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
        LogFile logFile1 = LogFile.of(file1);
        String fileHash = logFile1.hashString();
        List<LogFileInformation> logFileInformationSet = new ArrayList<>();
        logFileInformationSet.add(LogFileInformation.builder().startPosition(0).logFile(logFile1).fileHash(fileHash).build());
        ComponentLogFileInformation componentLogFileInformation = ComponentLogFileInformation.builder()
                .name("TestComponent")
                .desiredLogLevel(Level.INFO)
                .componentType(ComponentType.UserComponent)
                .logFileInformationList(logFileInformationSet)
                .build();
        logsProcessor = new CloudWatchAttemptLogsProcessor(mockDeviceConfiguration, defaultClock);
        CloudWatchAttempt attempt = logsProcessor.processLogFiles(componentLogFileInformation);
        assertNotNull(attempt);

        assertNotNull(attempt.getLogStreamsToLogEventsMap());
        assertThat(attempt.getLogStreamsToLogEventsMap().entrySet(), IsNot.not(IsEmptyCollection.empty()));
        String logGroup = calculateLogGroupName(ComponentType.UserComponent, "testRegion", "TestComponent");
        assertEquals(attempt.getLogGroupName(), logGroup);
        String logStream = "/2020/12/17/thing/testThing";
        assertTrue(attempt.getLogStreamsToLogEventsMap().containsKey(logStream));
        CloudWatchAttemptLogInformation logEventsForStream1 = attempt.getLogStreamsToLogEventsMap().get(logStream);
        assertNotNull(logEventsForStream1.getLogEvents());
        assertEquals(13, logEventsForStream1.getLogEvents().size());
        assertTrue(logEventsForStream1.getAttemptLogFileInformationMap().containsKey(fileHash));
        assertEquals(0, logEventsForStream1.getAttemptLogFileInformationMap().get(fileHash).getStartPosition());
        assertEquals(2943, logEventsForStream1.getAttemptLogFileInformationMap().get(fileHash).getBytesRead());
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
        LogFile logFile1 = LogFile.of(file1);
        String fileHash = logFile1.hashString();
        List<LogFileInformation> logFileInformationSet = new ArrayList<>();
        logFileInformationSet.add(LogFileInformation.builder().startPosition(0).logFile(logFile1).fileHash(fileHash).build());
        ComponentLogFileInformation componentLogFileInformation = ComponentLogFileInformation.builder()
                .name("TestComponent")
                .desiredLogLevel(Level.INFO)
                .componentType(ComponentType.GreengrassSystemComponent)
                .logFileInformationList(logFileInformationSet)
                .build();
        logsProcessor = new CloudWatchAttemptLogsProcessor(mockDeviceConfiguration, defaultClock);
        CloudWatchAttempt attempt = logsProcessor.processLogFiles(componentLogFileInformation);
        assertNotNull(attempt);

        assertNotNull(attempt.getLogStreamsToLogEventsMap());
        assertThat(attempt.getLogStreamsToLogEventsMap().entrySet(), IsNot.not(IsEmptyCollection.empty()));
        String logGroup = calculateLogGroupName(ComponentType.GreengrassSystemComponent, "testRegion", "TestComponent");
        assertEquals(attempt.getLogGroupName(), logGroup);
        String logStream = "/2020/12/17/thing/testThing";
        assertTrue(attempt.getLogStreamsToLogEventsMap().containsKey(logStream));
        CloudWatchAttemptLogInformation logEventsForStream1 = attempt.getLogStreamsToLogEventsMap().get(logStream);
        assertNotNull(logEventsForStream1.getLogEvents());
        assertEquals(13, logEventsForStream1.getLogEvents().size());
        assertTrue(logEventsForStream1.getAttemptLogFileInformationMap().containsKey(fileHash));
        assertEquals(0, logEventsForStream1.getAttemptLogFileInformationMap().get(fileHash).getStartPosition());
        assertEquals(2943, logEventsForStream1.getAttemptLogFileInformationMap().get(fileHash).getBytesRead());
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
    void GIVEN_one_component_WHEN_log_events_more_than_max_THEN_read_only_max_num_of_log_events(ExtensionContext context1)
            throws IOException {
        ignoreExceptionOfType(context1, DateTimeParseException.class);
        File file = new File(directoryPath.resolve("greengrass_test.log").toUri());
        assertTrue(file.createNewFile());
        assertTrue(file.setReadable(true));
        assertTrue(file.setWritable(true));
        /* LOG_EVENT_LENGTH = MESSAGE_LENGTH + TIMESTAMP_BYTES + EVENT_STORAGE_OVERHEAD => MESSAGE_LENGTH + 34
        * LOG_EVENT_LENGTH * (MAX_NUM_OF_LOG_EVENTS) <= MAX_BATCH_SIZE => ((MESSAGE_LENGTH +34)*10000)<=1024*1024
        * MESSAGE_LENGTH <= ~72
         */
        int logEventMessageLength = 70; // "\r\n" takes 2 bytes
        try (OutputStream fileOutputStream = Files.newOutputStream(file.toPath())) {
            for (int i = 0; i <= 10000; i++) { // 10001 log events
                boolean useLetters = true;
                boolean useNumbers = false;
                StringBuilder generatedString = new StringBuilder(RandomStringUtils.random(logEventMessageLength, useLetters, useNumbers));
                generatedString.append("\r\n");
                fileOutputStream.write(generatedString.toString().getBytes(StandardCharsets.UTF_8));
            }
        }
        LogFile logFile = LogFile.of(file);
        String fileHash = logFile.hashString();
        try {
            List<LogFileInformation> logFileInformationSet = new ArrayList<>();
            logFileInformationSet.add(LogFileInformation.builder().startPosition(0).logFile(logFile).fileHash(fileHash).build());
            ComponentLogFileInformation componentLogFileInformation = ComponentLogFileInformation.builder()
                    .name("TestComponent")
                    .desiredLogLevel(Level.INFO)
                    .multiLineStartPattern(WHITESPACE)
                    .componentType(ComponentType.GreengrassSystemComponent)
                    .logFileInformationList(logFileInformationSet)
                    .build();

            logsProcessor = new CloudWatchAttemptLogsProcessor(mockDeviceConfiguration, defaultClock);
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
            assertEquals(MAX_NUM_OF_LOG_EVENTS, logEventsForStream1.getLogEvents().size());
            assertTrue(logEventsForStream1.getAttemptLogFileInformationMap().containsKey(fileHash));
            assertEquals(0, logEventsForStream1.getAttemptLogFileInformationMap().get(fileHash).getStartPosition());
            assertEquals((logEventMessageLength+2)*10000, logEventsForStream1.getAttemptLogFileInformationMap().get(fileHash).getBytesRead());
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
                StringBuilder generatedString =
                        new StringBuilder(RandomStringUtils.random(length, useLetters, useNumbers));
                generatedString.append("\r\n");
                fileOutputStream.write(generatedString.toString().getBytes(StandardCharsets.UTF_8));
            }
        }
        LogFile logFile = LogFile.of(file);
        String fileHash = logFile.hashString();
        try {
            List<LogFileInformation> logFileInformationSet = new ArrayList<>();
            logFileInformationSet.add(LogFileInformation.builder().startPosition(0).logFile(logFile).fileHash(fileHash).build());
            ComponentLogFileInformation componentLogFileInformation = ComponentLogFileInformation.builder()
                    .name("TestComponent")
                    .multiLineStartPattern(WHITESPACE)
                    .desiredLogLevel(Level.INFO)
                    .componentType(ComponentType.GreengrassSystemComponent)
                    .logFileInformationList(logFileInformationSet)
                    .build();

            logsProcessor = new CloudWatchAttemptLogsProcessor(mockDeviceConfiguration, defaultClock);
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
            assertTrue(logEventsForStream1.getAttemptLogFileInformationMap().containsKey(fileHash));
            assertEquals(0, logEventsForStream1.getAttemptLogFileInformationMap().get(fileHash).getStartPosition());
            assertEquals(1016766, logEventsForStream1.getAttemptLogFileInformationMap().get(fileHash).getBytesRead());
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
    void GIVEN_one_component_one_file_24h_gap_WHEN_merge_THEN_reads_partial_file()
            throws IOException {
        File file = new File(directoryPath.resolve("greengrass_test.log").toUri());
        assertTrue(file.createNewFile());
        assertTrue(file.setReadable(true));
        assertTrue(file.setWritable(true));
        Instant now = Instant.now();
        Instant then = now.minus(1, ChronoUnit.DAYS);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
        try (OutputStream fileOutputStream = Files.newOutputStream(file.toPath())) {
            fileOutputStream.write((sdf.format(new Date(then.toEpochMilli())) + "T01:00:00Z ABC1\n").getBytes(StandardCharsets.UTF_8));
            fileOutputStream.write((sdf.format(new Date(then.toEpochMilli())) + "T02:00:00Z ABC2\n").getBytes(StandardCharsets.UTF_8));
            fileOutputStream.write((sdf.format(new Date(now.toEpochMilli())) + "T01:00:00Z ABC3\n").getBytes(StandardCharsets.UTF_8));
            fileOutputStream.write((sdf.format(new Date(now.toEpochMilli())) + "T02:00:00Z ABC4\n").getBytes(StandardCharsets.UTF_8));
        }
        LogFile logFile = LogFile.of(file);
        String fileHash = logFile.hashString();
        try {
            List<LogFileInformation> logFileInformationSet = new ArrayList<>();
            logFileInformationSet.add(LogFileInformation.builder().startPosition(0).logFile(logFile).fileHash(fileHash).build());
            ComponentLogFileInformation componentLogFileInformation = ComponentLogFileInformation.builder()
                    .name("TestComponent")
                    .desiredLogLevel(Level.INFO)
                    .componentType(ComponentType.GreengrassSystemComponent)
                    .logFileInformationList(logFileInformationSet)
                    .build();

            logsProcessor = new CloudWatchAttemptLogsProcessor(mockDeviceConfiguration);
            CloudWatchAttempt attempt = logsProcessor.processLogFiles(componentLogFileInformation);
            assertNotNull(attempt);
            sdf = new SimpleDateFormat("/yyyy/MM/dd/'thing/testThing'", Locale.ENGLISH);
            String logStream1 = sdf.format(new Date(then.toEpochMilli()));
            String logStream2 = sdf.format(new Date(now.toEpochMilli()));
            assertThat(attempt.getLogStreamsToLogEventsMap().get(logStream1).getLogEvents(), hasSize(2));
            assertThat(attempt.getLogStreamsToLogEventsMap().get(logStream2).getLogEvents(), hasSize(2));
        } finally {
            assertTrue(file.delete());
        }
    }

    @Test
    void GIVEN_one_component_WHEN_file_older_than_14_days_THEN_skip_file()
            throws IOException {
        File file = new File(directoryPath.resolve("greengrass_test.log").toUri());
        assertTrue(file.createNewFile());
        assertTrue(file.setReadable(true));
        assertTrue(file.setWritable(true));
        Instant now = Instant.now();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
        try (OutputStream fileOutputStream = Files.newOutputStream(file.toPath())) {
            fileOutputStream.write("2021-06-08T01:00:00Z ABC1\n".getBytes(StandardCharsets.UTF_8));
            fileOutputStream.write("2021-06-08T02:00:00Z ABC2\n".getBytes(StandardCharsets.UTF_8));
            fileOutputStream.write("2021-06-09T01:00:00Z ABC3\n".getBytes(StandardCharsets.UTF_8));
            fileOutputStream.write("2021-06-09T02:00:00Z ABC4\n".getBytes(StandardCharsets.UTF_8));
            fileOutputStream.write((sdf.format(new Date(now.toEpochMilli())) + "T02:00:00Z ABC5\n").getBytes(StandardCharsets.UTF_8));
        }
        LogFile logFile = LogFile.of(file);
        String fileHash = logFile.hashString();
        try {
            List<LogFileInformation> logFileInformationSet = new ArrayList<>();
            logFileInformationSet.add(LogFileInformation.builder().startPosition(0).logFile(logFile).fileHash(fileHash).build());
            ComponentLogFileInformation componentLogFileInformation = ComponentLogFileInformation.builder()
                    .name("TestComponent")
                    .desiredLogLevel(Level.INFO)
                    .componentType(ComponentType.GreengrassSystemComponent)
                    .logFileInformationList(logFileInformationSet)
                    .build();

            logsProcessor = new CloudWatchAttemptLogsProcessor(mockDeviceConfiguration);
            CloudWatchAttempt attempt = logsProcessor.processLogFiles(componentLogFileInformation);
            assertNotNull(attempt);
            String logStream1 = "/2021/06/08/thing/testThing";
            String logStream2 = "/2021/06/09/thing/testThing";
            sdf = new SimpleDateFormat("/yyyy/MM/dd/'thing/testThing'", Locale.ENGLISH);
            String logStream3 = sdf.format(new Date(now.toEpochMilli()));
            assertThat(attempt.getLogStreamsToLogEventsMap().get(logStream1).getLogEvents(), hasSize(0));
            assertThat(attempt.getLogStreamsToLogEventsMap().get(logStream2).getLogEvents(), hasSize(0));
            assertThat(attempt.getLogStreamsToLogEventsMap().get(logStream3).getLogEvents(), hasSize(1));
        } finally {
            assertTrue(file.delete());
        }
    }

    @Test
    void GIVEN_one_components_two_file_less_than_max_WHEN_merge_THEN_reads_and_merges_both_files(ExtensionContext ec)
            throws URISyntaxException {
        ignoreExceptionOfType(ec, DateTimeParseException.class);

        LogFile logFile1 = new LogFile(getClass().getResource("testlogs2.log").toURI());
        String fileHash1 = logFile1.hashString();
        LogFile logFile2 = new LogFile(getClass().getResource("testlogs1.log").toURI());
        String fileHash2 = logFile2.hashString();
        List<LogFileInformation> logFileInformationSet = new ArrayList<>();
        logFileInformationSet.add(LogFileInformation.builder().startPosition(0).logFile(logFile1).fileHash(fileHash1).build());
        logFileInformationSet.add(LogFileInformation.builder().startPosition(0).logFile(logFile2).fileHash(fileHash2).build());
        ComponentLogFileInformation componentLogFileInformation = ComponentLogFileInformation.builder()
                .name("TestComponent")
                .desiredLogLevel(Level.INFO)
                .multiLineStartPattern(WHITESPACE)
                .componentType(ComponentType.GreengrassSystemComponent)
                .logFileInformationList(logFileInformationSet)
                .build();
        logsProcessor = new CloudWatchAttemptLogsProcessor(mockDeviceConfiguration, defaultClock);
        CloudWatchAttempt attempt = logsProcessor.processLogFiles(componentLogFileInformation);

        assertNotNull(attempt);

        assertNotNull(attempt.getLogStreamsToLogEventsMap());
        assertThat(attempt.getLogStreamsToLogEventsMap().entrySet(), IsNot.not(IsEmptyCollection.empty()));
        String logGroup = calculateLogGroupName(ComponentType.GreengrassSystemComponent, "testRegion", "TestComponent");
        assertEquals(attempt.getLogGroupName(), logGroup);
        String logStream = "/2020/12/17/thing/testThing";
        String logStream2 = "/2020/12/18/thing/testThing";
        assertTrue(attempt.getLogStreamsToLogEventsMap().containsKey(logStream));
        assertTrue(attempt.getLogStreamsToLogEventsMap().containsKey(logStream2));
        CloudWatchAttemptLogInformation logEventsForStream1 = attempt.getLogStreamsToLogEventsMap().get(logStream);
        CloudWatchAttemptLogInformation logEventsForStream2 = attempt.getLogStreamsToLogEventsMap().get(logStream2);
        assertNotNull(logEventsForStream1.getLogEvents());
        assertEquals(13, logEventsForStream1.getLogEvents().size());
        assertTrue(logEventsForStream1.getAttemptLogFileInformationMap().containsKey(fileHash1));
        assertEquals(0, logEventsForStream1.getAttemptLogFileInformationMap().get(fileHash1).getStartPosition());
        assertEquals(2943, logEventsForStream1.getAttemptLogFileInformationMap().get(fileHash1).getBytesRead());
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
        assertTrue(logEventsForStream2.getAttemptLogFileInformationMap().containsKey(fileHash2));
        assertEquals(0, logEventsForStream2.getAttemptLogFileInformationMap().get(fileHash2).getStartPosition());
        assertEquals(1239, logEventsForStream2.getAttemptLogFileInformationMap().get(fileHash2).getBytesRead());
        assertEquals("TestComponent", logEventsForStream2.getComponentName());
    }

    @Test
    void GIVEN_unstructured_log_WHEN_breaches_event_size_limit_THEN_split_line(ExtensionContext ec) throws IOException {

        ignoreExceptionOfType(ec, DateTimeParseException.class);
        File file = new File(directoryPath.resolve("greengrass_test.log").toUri());
        assertTrue(file.createNewFile());
        assertTrue(file.setReadable(true));
        assertTrue(file.setWritable(true));
        try (OutputStream fileOutputStream = Files.newOutputStream(file.toPath())) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
            sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
            String timestampPrefix = sdf.format(new Date(Instant.now().toEpochMilli())) + "T02:00:00Z [INFO] ";
            StringBuilder fileContent = new StringBuilder();
            // 1 log line of size ~1KB i.e. within event size limit of 256KB, should amount to 1 log event
            fileContent = fileContent.append(timestampPrefix).append(RandomStringUtils.random(1024*1, true, true))
                    .append("end\n");
            // 1 log line of size ~256KB i.e. larger than event size limit of 256KB (after additional bytes for
            // timestamp and overhead), should amount to 2 log events.
            String overSizeLogLine = timestampPrefix + RandomStringUtils.random(1024*256, true, true) + "end";
            fileContent = fileContent.append(overSizeLogLine).append("\n");
            // 1 more log line of size ~1KB i.e. within event size limit of 256KB, should amount to 1 log event
            fileContent = fileContent.append(timestampPrefix)
                    .append(StringUtils.repeat(RandomStringUtils.random(1, true, true), 1024 * 1)).append("end\n");
            byte[] fileContentBytes = fileContent.toString().getBytes(StandardCharsets.UTF_8);
            fileOutputStream.write(fileContentBytes);

            LogFile logFile = LogFile.of(file);
            String fileHash = logFile.hashString();
            List<LogFileInformation> logFileInformationSet = new ArrayList<>();
            logFileInformationSet.add(LogFileInformation.builder().startPosition(0).logFile(logFile).fileHash(fileHash).build());
            ComponentLogFileInformation componentLogFileInformation =
                    ComponentLogFileInformation.builder().name("TestComponent")
                            .desiredLogLevel(Level.INFO).componentType(ComponentType.GreengrassSystemComponent)
                            .logFileInformationList(logFileInformationSet).build();

            logsProcessor = new CloudWatchAttemptLogsProcessor(mockDeviceConfiguration, defaultClock);
            CloudWatchAttempt attempt = logsProcessor.processLogFiles(componentLogFileInformation);
            assertNotNull(attempt);

            assertNotNull(attempt.getLogStreamsToLogEventsMap());
            assertThat(attempt.getLogStreamsToLogEventsMap().entrySet(), IsNot.not(IsEmptyCollection.empty()));
            String logGroup =
                    calculateLogGroupName(ComponentType.GreengrassSystemComponent, "testRegion", "TestComponent");
            assertEquals(attempt.getLogGroupName(), logGroup);
            String logStream = calculateLogStreamName("testThing");
            assertTrue(attempt.getLogStreamsToLogEventsMap().containsKey(logStream));
            CloudWatchAttemptLogInformation logEventsForStream1 = attempt.getLogStreamsToLogEventsMap().get(logStream);
            assertNotNull(logEventsForStream1.getLogEvents());

            List<InputLogEvent> logEvents = logEventsForStream1.getLogEvents();
            assertEquals(4, logEvents.size());
            // Over size log line got divided and successfully added as log events
            assertEquals(overSizeLogLine.substring(0, MAX_EVENT_LENGTH), logEvents.get(1).message());
            assertEquals(overSizeLogLine.substring(MAX_EVENT_LENGTH), logEvents.get(2).message());

            assertTrue(logEventsForStream1.getAttemptLogFileInformationMap().containsKey(fileHash));
            assertEquals(0, logEventsForStream1.getAttemptLogFileInformationMap().get(fileHash)
                    .getStartPosition());
            assertEquals(fileContentBytes.length,
                    logEventsForStream1.getAttemptLogFileInformationMap().get(fileHash).getBytesRead());
            assertEquals("TestComponent", logEventsForStream1.getComponentName());

        } finally {
            assertTrue(file.delete());
        }
    }

    @Test
    void GIVEN_unstructured_log_WHEN_breaches_event_size_and_batch_size_limits_THEN_split_line_and_skip_extra(
            ExtensionContext ec) throws IOException {

        ignoreExceptionOfType(ec, DateTimeParseException.class);
        File file = new File(directoryPath.resolve("greengrass_test.log").toUri());
        assertTrue(file.createNewFile());
        assertTrue(file.setReadable(true));
        assertTrue(file.setWritable(true));
        try (OutputStream fileOutputStream = Files.newOutputStream(file.toPath())) {
            StringBuilder fileContent = new StringBuilder();
            // 1 log line of size ~1KB i.e. within event size limit of 256KB, should amount to 1 log event
            fileContent = fileContent.append(RandomStringUtils.random(1024*1, true, true)).append("end\n");
            // 1 log line of size 2 * MAX_EVENT_LENGTH KB, should amount to 2 log events
            fileContent = fileContent.append(RandomStringUtils.random(MAX_EVENT_LENGTH * 2 - 3, true, true)).append(
                    "end\n");
            // 2 log lines of size ~256KB i.e. larger than event size limit of 256KB (after additional bytes for
            // timestamp and overhead), should amount to 2 log events each.
            // This should max out batch size limit in the 2nd line of these 2 and it will not get processed
            for (int i = 0; i < 2; i++) {
                fileContent = fileContent.append(RandomStringUtils.random(1024*256, true, true)).append("end\n");
            }
            // 3 additional byte in each line for 'end\n', 1 additional for '\n'
            int expectedBytesRead = 2*4 + 1024*1 + MAX_EVENT_LENGTH*2 + 1 + 1*1024*256;
            fileOutputStream.write(fileContent.toString().getBytes(StandardCharsets.UTF_8));

            LogFile logFile = LogFile.of(file);
            String fileHash = logFile.hashString();
            List<LogFileInformation> logFileInformationSet = new ArrayList<>();
            logFileInformationSet.add(LogFileInformation.builder().startPosition(0).logFile(logFile).fileHash(fileHash).build());
            ComponentLogFileInformation componentLogFileInformation =
                    ComponentLogFileInformation.builder().name("TestComponent")
                            .desiredLogLevel(Level.INFO)
                            .multiLineStartPattern(WHITESPACE)
                            .componentType(ComponentType.GreengrassSystemComponent)
                            .logFileInformationList(logFileInformationSet).build();

            logsProcessor = new CloudWatchAttemptLogsProcessor(mockDeviceConfiguration, defaultClock);
            CloudWatchAttempt attempt = logsProcessor.processLogFiles(componentLogFileInformation);
            assertNotNull(attempt);

            assertNotNull(attempt.getLogStreamsToLogEventsMap());
            assertThat(attempt.getLogStreamsToLogEventsMap().entrySet(), IsNot.not(IsEmptyCollection.empty()));
            String logGroup =
                    calculateLogGroupName(ComponentType.GreengrassSystemComponent, "testRegion", "TestComponent");
            assertEquals(attempt.getLogGroupName(), logGroup);
            String logStream = calculateLogStreamName("testThing");
            assertTrue(attempt.getLogStreamsToLogEventsMap().containsKey(logStream));
            CloudWatchAttemptLogInformation logEventsForStream1 = attempt.getLogStreamsToLogEventsMap().get(logStream);
            assertNotNull(logEventsForStream1.getLogEvents());
            // Total log events: 1 (first line) + 2 (second line) + 2 (third line) = 5
            assertEquals(5, logEventsForStream1.getLogEvents().size());
            assertTrue(logEventsForStream1.getAttemptLogFileInformationMap().containsKey(fileHash));
            assertEquals(0, logEventsForStream1.getAttemptLogFileInformationMap().get(fileHash)
                    .getStartPosition());
            assertEquals(expectedBytesRead,
                    logEventsForStream1.getAttemptLogFileInformationMap().get(fileHash).getBytesRead());
            assertEquals("TestComponent", logEventsForStream1.getComponentName());

        } finally {
            assertTrue(file.delete());
        }
    }
    @Test
    void GIVEN_null_log_message_WHEN_upload_attempted_THEN_null_message_considered_as_unstructured_log(ExtensionContext ec) throws IOException {

        ignoreExceptionOfType(ec, DateTimeParseException.class);
        File file = new File(directoryPath.resolve("greengrass_test.log").toUri());
        assertTrue(file.createNewFile());
        assertTrue(file.setReadable(true));
        assertTrue(file.setWritable(true));
        try (OutputStream fileOutputStream = Files.newOutputStream(file.toPath())) {
            fileOutputStream.write("null\n".getBytes(StandardCharsets.UTF_8));
        }

        LogFile logFile = LogFile.of(file);
        String fileHash = logFile.hashString();

        try {
            List<LogFileInformation> logFileInformationSet = new ArrayList<>();
            logFileInformationSet.add(LogFileInformation.builder().startPosition(0).logFile(logFile).fileHash(fileHash).build());
            ComponentLogFileInformation componentLogFileInformation = ComponentLogFileInformation.builder()
                    .name("TestComponent")
                    .desiredLogLevel(Level.INFO)
                    .componentType(ComponentType.GreengrassSystemComponent)
                    .logFileInformationList(logFileInformationSet)
                    .build();

            logsProcessor = new CloudWatchAttemptLogsProcessor(mockDeviceConfiguration, defaultClock);
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
            assertEquals(1, logEventsForStream1.getLogEvents().size());
            assertTrue(logEventsForStream1.getAttemptLogFileInformationMap().containsKey(fileHash));
            assertEquals(0, logEventsForStream1.getAttemptLogFileInformationMap().get(fileHash).getStartPosition());
            assertEquals("TestComponent", logEventsForStream1.getComponentName());
            LocalDateTime localDateTimeNow = LocalDateTime.now(ZoneOffset.UTC);
            for (InputLogEvent logEvent: logEventsForStream1.getLogEvents()) {
                Instant logTimestamp = Instant.ofEpochMilli(logEvent.timestamp());
                assertFalse(logTimestamp.isAfter(Instant.now()));
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
    void GIVEN_empty_log_message_WHEN_upload_attempted_THEN_empty_message_skipped(ExtensionContext ec) throws IOException {

        ignoreExceptionOfType(ec, DateTimeParseException.class);
        File file = new File(directoryPath.resolve("greengrass_test.log").toUri());
        assertTrue(file.createNewFile());
        assertTrue(file.setReadable(true));
        assertTrue(file.setWritable(true));
        try (OutputStream fileOutputStream = Files.newOutputStream(file.toPath())) {
            // Large line with an empty chunk in the middle, empty chunk should be filtered out and only two log
            // events should be uploaded
            StringBuilder largeLineWithEmptyChunk =
                    new StringBuilder(RandomStringUtils.random(MAX_EVENT_LENGTH, true, true))
                            .append(StringUtils.repeat(" ", MAX_EVENT_LENGTH))
                            .append(RandomStringUtils.random(MAX_EVENT_LENGTH, true, true))
                            .append("\r\n");
            fileOutputStream.write(largeLineWithEmptyChunk.toString().getBytes(StandardCharsets.UTF_8));
        }

        LogFile logFile = LogFile.of(file);
        String fileHash = logFile.hashString();

        try {
            List<LogFileInformation> logFileInformationSet = new ArrayList<>();
            logFileInformationSet.add(LogFileInformation.builder().startPosition(0).logFile(logFile).fileHash(fileHash).build());
            ComponentLogFileInformation componentLogFileInformation = ComponentLogFileInformation.builder()
                    .name("TestComponent")
                    .desiredLogLevel(Level.INFO)
                    .componentType(ComponentType.GreengrassSystemComponent)
                    .logFileInformationList(logFileInformationSet)
                    .build();

            logsProcessor = new CloudWatchAttemptLogsProcessor(mockDeviceConfiguration, defaultClock);
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
            assertEquals(2, logEventsForStream1.getLogEvents().size());
            assertTrue(logEventsForStream1.getAttemptLogFileInformationMap().containsKey(fileHash));
            assertEquals(0, logEventsForStream1.getAttemptLogFileInformationMap().get(fileHash).getStartPosition());
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
    void GIVEN_component_multiline_pattern_default_WHEN_logs_start_with_whitespace_THEN_append_to_previous_log() throws IOException{
        File file = new File(directoryPath.resolve("greengrass_test.log").toUri());
        assertTrue(file.createNewFile());
        assertTrue(file.setReadable(true));
        assertTrue(file.setWritable(true));
        try (OutputStream fileOutputStream = Files.newOutputStream(file.toPath())) {
            fileOutputStream.write("[2022-01-10] [INFO] (pool-2-thread-5) ComponentManager:\n".getBytes(StandardCharsets.UTF_8));
            fileOutputStream.write("[2022-01-10] [ERROR] (pool-2-thread-5) DeploymentService:\n".getBytes(StandardCharsets.UTF_8));
            fileOutputStream.write("java.util.concurrent.ExecutionException: NoAvailableComponentVersion\n".getBytes(StandardCharsets.UTF_8));
            fileOutputStream.write("    at java.util.concurrent\n".getBytes(StandardCharsets.UTF_8));
            fileOutputStream.write("    at java.util.concurrent\n".getBytes(StandardCharsets.UTF_8));
            fileOutputStream.write("Caused by: NoAvailableComponentVersionException\n".getBytes(StandardCharsets.UTF_8));
        }
        LogFile logFile = LogFile.of(file);
        String fileHash = logFile.hashString();
        try {
            List<LogFileInformation> logFileInformationSet = new ArrayList<>();
            logFileInformationSet.add(LogFileInformation.builder().startPosition(0).logFile(logFile).fileHash(fileHash).build());
            ComponentLogFileInformation componentLogFileInformation = ComponentLogFileInformation.builder()
                    .name("TestComponent")
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
            assertEquals(1, attempt.getLogStreamsToLogEventsMap().keySet().size());
            CloudWatchAttemptLogInformation logEventsForStream1 = attempt.getLogStreamsToLogEventsMap().values().iterator()
                    .next();
            assertNotNull(logEventsForStream1.getLogEvents());
            // We expect only 2 logs. 1 is the info log, 1 is the error log including stacktrace in the same log event
            assertEquals(2, logEventsForStream1.getLogEvents().size());
        } finally {
            assertTrue(file.delete());
        }
    }

    @Test
    void GIVEN_component_multiline_pattern_set_WHEN_log_lines_do_not_match_pattern_THEN_append_to_previous_log() throws IOException{
        File file = new File(directoryPath.resolve("greengrass_test.log").toUri());
        assertTrue(file.createNewFile());
        assertTrue(file.setReadable(true));
        assertTrue(file.setWritable(true));
        try (OutputStream fileOutputStream = Files.newOutputStream(file.toPath())) {
            fileOutputStream.write("1[INFO] (pool-2-thread-5) ComponentManager:\n".getBytes(StandardCharsets.UTF_8));
            fileOutputStream.write("2[ERROR] (pool-2-thread-5) DeploymentService:\n".getBytes(StandardCharsets.UTF_8));
            fileOutputStream.write("3java.util.concurrent.ExecutionException: NoAvailableComponentVersion\n".getBytes(
                    StandardCharsets.UTF_8));
            fileOutputStream.write("   at java.util.concurrent\n".getBytes(StandardCharsets.UTF_8));
            fileOutputStream.write("   at java.util.concurrent\n".getBytes(StandardCharsets.UTF_8));
            fileOutputStream.write("4Caused by: NoAvailableComponentVersionException\n".getBytes(StandardCharsets.UTF_8));
        }
        LogFile logFile = LogFile.of(file);
        String fileHash = logFile.hashString();
        try {
            List<LogFileInformation> logFileInformationSet = new ArrayList<>();
            logFileInformationSet.add(LogFileInformation.builder().startPosition(0).logFile(logFile).fileHash(fileHash).build());
            ComponentLogFileInformation componentLogFileInformation = ComponentLogFileInformation.builder()
                    .name("TestComponent")
                    .desiredLogLevel(Level.INFO)
                    .componentType(ComponentType.GreengrassSystemComponent)
                    //it's newline if line starts with number
                    .multiLineStartPattern(Pattern.compile("^\\d.*$"))
                    .logFileInformationList(logFileInformationSet)
                    .build();

            logsProcessor = new CloudWatchAttemptLogsProcessor(mockDeviceConfiguration);
            CloudWatchAttempt attempt = logsProcessor.processLogFiles(componentLogFileInformation);
            assertNotNull(attempt);
            assertNotNull(attempt.getLogStreamsToLogEventsMap());
            assertThat(attempt.getLogStreamsToLogEventsMap().entrySet(), IsNot.not(IsEmptyCollection.empty()));
            String logGroup = calculateLogGroupName(ComponentType.GreengrassSystemComponent, "testRegion", "TestComponent");
            assertEquals(attempt.getLogGroupName(), logGroup);
            assertEquals(1, attempt.getLogStreamsToLogEventsMap().keySet().size());
            CloudWatchAttemptLogInformation logEventsForStream1 = attempt.getLogStreamsToLogEventsMap().values().iterator()
                    .next();
            assertNotNull(logEventsForStream1.getLogEvents());
            assertEquals(4, logEventsForStream1.getLogEvents().size());
        } finally {
            assertTrue(file.delete());
        }
    }

    @SuppressWarnings("PMD.CloseResource")
    @Test
    void GIVEN_component_multiline_pattern_set_WHEN_log_lines_trigger_stack_overflow_THEN_use_default_pattern_and_log_warning()
            throws URISyntaxException {
        ByteArrayOutputStream outputCaptor = new ByteArrayOutputStream();
        PrintStream old = System.out;
        System.setOut(new PrintStream(outputCaptor));

        File file = new File(getClass().getResource("stackoverflow.log").toURI());
        LogFile logFile = LogFile.of(file);
        String fileHash = logFile.hashString();
        List<LogFileInformation> logFileInformationSet = new ArrayList<>();
        logFileInformationSet.add(LogFileInformation.builder().startPosition(0).logFile(logFile).fileHash(fileHash).build());
        ComponentLogFileInformation componentLogFileInformation = ComponentLogFileInformation.builder()
                .name("TestComponent")
                .desiredLogLevel(Level.INFO)
                .multiLineStartPattern(Pattern.compile("^[^\\s]+(\\s+[^\\s]+)*$"))
                .componentType(ComponentType.GreengrassSystemComponent)
                .logFileInformationList(logFileInformationSet)
                .build();
        logsProcessor = new CloudWatchAttemptLogsProcessor(mockDeviceConfiguration, defaultClock);
        CloudWatchAttempt attempt = logsProcessor.processLogFiles(componentLogFileInformation);

        assertNotNull(attempt);
        assertNotNull(attempt.getLogStreamsToLogEventsMap());
        assertThat(attempt.getLogStreamsToLogEventsMap().entrySet(), IsNot.not(IsEmptyCollection.empty()));
        String logGroup = calculateLogGroupName(ComponentType.GreengrassSystemComponent, "testRegion", "TestComponent");
        assertEquals(attempt.getLogGroupName(), logGroup);
        String logStream = "/2020/12/18/thing/testThing";
        assertTrue(attempt.getLogStreamsToLogEventsMap().containsKey(logStream));
        CloudWatchAttemptLogInformation logEventsForStream = attempt.getLogStreamsToLogEventsMap().get(logStream);

        assertNotNull(logEventsForStream.getLogEvents());
        // 6 expected due to logs splitted after meeting size limit
        assertEquals(6, logEventsForStream.getLogEvents().size());

        // verify that StackOverflowError is triggered and a WARN log printed
        System.out.flush();
        System.setOut(old);
        String output = outputCaptor.toString();
        assertThat(output, StringContains.containsString("WARN"));
        assertThat(output, StringContains.containsString("StackOverflowError thrown when matching log against pattern"));
        assertThat(output, StringContains.containsString("TestComponent"));
    }

    @Test
    void GIVEN_componentsToGroups_WHEN_getLogStreamName_THEN_return_valid_log_stream_name() {
        logsProcessor = new CloudWatchAttemptLogsProcessor(mockDeviceConfiguration, defaultClock);

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
