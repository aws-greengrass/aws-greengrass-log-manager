/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.logmanager;

import com.aws.greengrass.config.Topic;
import com.aws.greengrass.deployment.DeviceConfiguration;
import com.aws.greengrass.logmanager.model.CloudWatchAttempt;
import com.aws.greengrass.logmanager.model.CloudWatchAttemptLogFileInformation;
import com.aws.greengrass.logmanager.model.CloudWatchAttemptLogInformation;
import com.aws.greengrass.logmanager.model.ComponentLogFileInformation;
import com.aws.greengrass.logmanager.model.ComponentType;
import com.aws.greengrass.logmanager.model.LogFile;
import com.aws.greengrass.logmanager.model.LogFileInformation;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.testcommons.testutilities.GGServiceTestUtil;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.event.Level;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.aws.greengrass.deployment.DeviceConfiguration.DEVICE_PARAM_AWS_REGION;
import static com.aws.greengrass.deployment.DeviceConfiguration.DEVICE_PARAM_THING_NAME;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;

@ExtendWith({MockitoExtension.class, GGExtension.class})
class CloudWatchAttemptLogsProcessorPartialEntryTest extends GGServiceTestUtil {
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
    void GIVEN_single_line_entry_over_batch_size_WHEN_uploaded_in_cycles_THEN_no_bytes_lost(ExtensionContext ec)
            throws IOException {
        assertUploadedInCyclesWithoutLoss(ec, entry("\n", 1_200_000));
    }

    @Test
    void GIVEN_single_line_crlf_entry_over_batch_size_WHEN_uploaded_in_cycles_THEN_no_bytes_lost(
            ExtensionContext ec) throws IOException {
        assertUploadedInCyclesWithoutLoss(ec, entry("\r\n", 1_200_000));
    }

    @Test
    void GIVEN_multi_line_entry_over_batch_size_WHEN_uploaded_in_cycles_THEN_no_bytes_lost(ExtensionContext ec)
            throws IOException {
        int[] lineLengths = new int[120];
        Arrays.fill(lineLengths, 10_000);
        assertUploadedInCyclesWithoutLoss(ec, entry("\n", lineLengths));
    }

    @Test
    void GIVEN_multi_line_crlf_entry_over_batch_size_WHEN_uploaded_in_cycles_THEN_no_bytes_lost(
            ExtensionContext ec) throws IOException {
        int[] lineLengths = new int[120];
        Arrays.fill(lineLengths, 10_000);
        assertUploadedInCyclesWithoutLoss(ec, entry("\r\n", lineLengths));
    }

    @Test
    void GIVEN_entry_cut_exactly_at_line_end_WHEN_uploaded_in_cycles_THEN_no_bytes_lost(ExtensionContext ec)
            throws IOException {
        // Four full events fill one batch, so the cut falls right after the first line
        assertUploadedInCyclesWithoutLoss(ec, entry("\n", 4 * MAX_EVENT_LENGTH, 100_000));
    }

    @Test
    void GIVEN_entry_with_empty_lines_WHEN_uploaded_in_cycles_THEN_no_bytes_lost(ExtensionContext ec)
            throws IOException {
        assertUploadedInCyclesWithoutLoss(ec, entry("\n", 500_000, 0, 600_000, 0, 0, 1000));
    }

    @Test
    void GIVEN_entry_with_lone_cr_line_endings_WHEN_uploaded_in_cycles_THEN_no_bytes_lost(ExtensionContext ec)
            throws IOException {
        assertUploadedInCyclesWithoutLoss(ec, entry("\r", 600_000, 600_000));
    }

    private static String entry(String lineTerminator, int... lineLengths) {
        StringBuilder entry = new StringBuilder();
        for (int lineLength : lineLengths) {
            entry.append(RandomStringUtils.random(lineLength, true, true)).append(lineTerminator);
        }
        return entry.toString();
    }

    private void assertUploadedInCyclesWithoutLoss(ExtensionContext ec, String fileContent) throws IOException {
        ignoreExceptionOfType(ec, DateTimeParseException.class);
        File file = new File(directoryPath.resolve("partial_entry_test.log").toUri());
        try {
            Files.write(file.toPath(), fileContent.getBytes(StandardCharsets.UTF_8));
            LogFile logFile = LogFile.of(file);
            String fileHash = logFile.hashString();
            logsProcessor = new CloudWatchAttemptLogsProcessor(mockDeviceConfiguration, defaultClock);
            StringBuilder uploaded = new StringBuilder();
            long position = 0;
            while (position < file.length()) {
                List<LogFileInformation> logFileInformationList = new ArrayList<>();
                logFileInformationList.add(LogFileInformation.builder().startPosition(position).logFile(logFile)
                        .fileHash(fileHash).build());
                CloudWatchAttempt attempt = logsProcessor.processLogFiles(ComponentLogFileInformation.builder()
                        .name("TestComponent").desiredLogLevel(Level.INFO)
                        .componentType(ComponentType.GreengrassSystemComponent)
                        .logFileInformationList(logFileInformationList).build());
                long nextPosition = position;
                for (CloudWatchAttemptLogInformation logInformation :
                        attempt.getLogStreamsToLogEventsMap().values()) {
                    logInformation.getSortedLogEvents().forEach(event -> uploaded.append(event.message()));
                    CloudWatchAttemptLogFileInformation fileInformation =
                            logInformation.getAttemptLogFileInformationMap().get(fileHash);
                    nextPosition = fileInformation.getStartPosition() + fileInformation.getBytesRead();
                }
                assertTrue(nextPosition > position, "each cycle advances the saved position");
                position = nextPosition;
            }
            String expected = fileContent.replaceAll("\\r\\n|\\r|\\n", "");
            assertEquals(expected.length(), uploaded.length(), "uploaded length");
            assertEquals(expected, uploaded.toString());
        } finally {
            assertTrue(file.delete());
        }
    }
}
