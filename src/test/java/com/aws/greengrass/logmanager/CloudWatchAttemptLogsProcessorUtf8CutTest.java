/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.logmanager;

import com.aws.greengrass.config.Topic;
import com.aws.greengrass.deployment.DeviceConfiguration;
import com.aws.greengrass.logmanager.model.CloudWatchAttempt;
import com.aws.greengrass.logmanager.model.ComponentLogFileInformation;
import com.aws.greengrass.logmanager.model.ComponentType;
import com.aws.greengrass.logmanager.model.LogFile;
import com.aws.greengrass.logmanager.model.LogFileInformation;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.testcommons.testutilities.GGServiceTestUtil;
import org.apache.commons.lang3.StringUtils;
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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

import static com.aws.greengrass.deployment.DeviceConfiguration.DEVICE_PARAM_AWS_REGION;
import static com.aws.greengrass.deployment.DeviceConfiguration.DEVICE_PARAM_THING_NAME;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;

@ExtendWith({MockitoExtension.class, GGExtension.class})
class CloudWatchAttemptLogsProcessorUtf8CutTest extends GGServiceTestUtil {
    private static final int EVENT_STORAGE_OVERHEAD = 26;
    private static final int TIMESTAMP_BYTES = 8;
    private static final int MAX_EVENT_LENGTH = 1024 * 256 - TIMESTAMP_BYTES - EVENT_STORAGE_OVERHEAD;

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
    void GIVEN_oversize_log_line_with_ascii_on_event_boundary_WHEN_split_THEN_text_intact(ExtensionContext ec)
            throws IOException {
        assertOversizeLineSplitKeepsText(ec, "a", 1);
    }

    @Test
    void GIVEN_oversize_log_line_with_two_byte_char_on_event_boundary_WHEN_split_THEN_text_intact(
            ExtensionContext ec) throws IOException {
        assertOversizeLineSplitKeepsText(ec, "\u00e9", 1);
    }

    @Test
    void GIVEN_oversize_log_line_with_three_byte_char_on_event_boundary_WHEN_split_THEN_text_intact(
            ExtensionContext ec) throws IOException {
        assertOversizeLineSplitKeepsText(ec, "\u20ac", 1);
    }

    @Test
    void GIVEN_oversize_log_line_with_four_byte_char_on_event_boundary_WHEN_split_THEN_text_intact(
            ExtensionContext ec) throws IOException {
        assertOversizeLineSplitKeepsText(ec, "\uD83D\uDE00", 1);
    }

    @Test
    void GIVEN_oversize_log_line_with_three_byte_char_cut_two_bytes_in_WHEN_split_THEN_text_intact(
            ExtensionContext ec) throws IOException {
        assertOversizeLineSplitKeepsText(ec, "\u20ac", 2);
    }

    @Test
    void GIVEN_oversize_log_line_with_four_byte_char_cut_two_bytes_in_WHEN_split_THEN_text_intact(
            ExtensionContext ec) throws IOException {
        assertOversizeLineSplitKeepsText(ec, "\uD83D\uDE00", 2);
    }

    @Test
    void GIVEN_oversize_log_line_with_four_byte_char_cut_three_bytes_in_WHEN_split_THEN_text_intact(
            ExtensionContext ec) throws IOException {
        assertOversizeLineSplitKeepsText(ec, "\uD83D\uDE00", 3);
    }

    private void assertOversizeLineSplitKeepsText(ExtensionContext ec, String character, int bytesBeforeCut)
            throws IOException {
        ignoreExceptionOfType(ec, DateTimeParseException.class);
        File file = new File(directoryPath.resolve("greengrass_test.log").toUri());
        assertTrue(file.createNewFile());
        try {
            String timestampPrefix = dateFormat("yyyy-MM-dd") + "T02:00:00Z [INFO] ";
            // The character starts bytesBeforeCut bytes before the first event boundary, so the cut falls that many
            // bytes into it
            String logLine = timestampPrefix
                    + StringUtils.repeat("a", MAX_EVENT_LENGTH - bytesBeforeCut - timestampPrefix.length())
                    + StringUtils.repeat(character, 100) + "end";
            Files.write(file.toPath(), (logLine + "\n").getBytes(StandardCharsets.UTF_8));

            LogFile logFile = LogFile.of(file);
            List<LogFileInformation> logFileInformationSet = new ArrayList<>();
            logFileInformationSet.add(LogFileInformation.builder().startPosition(0).logFile(logFile)
                    .fileHash(logFile.hashString()).build());
            ComponentLogFileInformation componentLogFileInformation =
                    ComponentLogFileInformation.builder().name("TestComponent")
                            .desiredLogLevel(Level.INFO).componentType(ComponentType.GreengrassSystemComponent)
                            .logFileInformationList(logFileInformationSet).build();

            logsProcessor = new CloudWatchAttemptLogsProcessor(mockDeviceConfiguration, defaultClock);
            CloudWatchAttempt attempt = logsProcessor.processLogFiles(componentLogFileInformation);

            List<InputLogEvent> logEvents = attempt.getLogStreamsToLogEventsMap()
                    .get("/" + dateFormat("yyyy/MM/dd") + "/thing/testThing").getSortedLogEvents();
            StringBuilder delivered = new StringBuilder();
            logEvents.forEach(logEvent -> delivered.append(logEvent.message()));
            int at = StringUtils.indexOfDifference(logLine, delivered);
            assertEquals(-1, at, "first difference at char " + at + ": expected "
                    + StringUtils.substring(logLine, at - 4, at + 4) + " but was "
                    + StringUtils.substring(delivered.toString(), at - 4, at + 4));
            for (InputLogEvent logEvent : logEvents) {
                assertThat(logEvent.message().getBytes(StandardCharsets.UTF_8).length,
                        lessThanOrEqualTo(MAX_EVENT_LENGTH));
            }
        } finally {
            assertTrue(file.delete());
        }
    }

    private static String dateFormat(String pattern) {
        SimpleDateFormat sdf = new SimpleDateFormat(pattern, Locale.ENGLISH);
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        return sdf.format(new Date());
    }
}
