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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.event.Level;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.aws.greengrass.deployment.DeviceConfiguration.DEVICE_PARAM_AWS_REGION;
import static com.aws.greengrass.deployment.DeviceConfiguration.DEVICE_PARAM_THING_NAME;
import static com.aws.greengrass.logmanager.util.TestUtils.jsonLogLine;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, GGExtension.class})
class CloudWatchAttemptLogsProcessorFilteredEntryTest extends GGServiceTestUtil {
    private final Clock clock = Clock.fixed(Instant.parse("2020-12-17T12:00:00.00Z"), ZoneId.systemDefault());
    @Mock
    private DeviceConfiguration mockDeviceConfiguration;
    @TempDir
    Path directoryPath;

    @BeforeEach
    void startup() {
        Topic thingName = Topic.of(context, DEVICE_PARAM_THING_NAME, "thing");
        Topic region = Topic.of(context, DEVICE_PARAM_AWS_REGION, "region");
        when(mockDeviceConfiguration.getThingName()).thenReturn(thingName);
        when(mockDeviceConfiguration.getAWSRegion()).thenReturn(region);
    }

    @Test
    void GIVEN_json_file_ending_below_minimum_level_WHEN_processed_THEN_whole_file_is_consumed() throws IOException {
        assertWholeFileConsumedAndOnlyInfoSent("INFO", "DEBUG", "INFO", "DEBUG", "TRACE");
    }

    @Test
    void GIVEN_json_file_entirely_below_minimum_level_WHEN_processed_THEN_whole_file_is_consumed()
            throws IOException {
        assertWholeFileConsumedAndOnlyInfoSent("DEBUG", "TRACE");
    }

    @Test
    void GIVEN_json_file_ending_in_filtered_entry_of_a_later_date_WHEN_processed_THEN_whole_file_is_consumed()
            throws IOException {
        Instant now = Instant.now(clock);
        assertWholeFileConsumedAndOnlyInfoSent(new Instant[]{now.minus(2, ChronoUnit.DAYS),
                now.minus(1, ChronoUnit.DAYS)}, "INFO", "DEBUG");
    }

    private void assertWholeFileConsumedAndOnlyInfoSent(String... levels) throws IOException {
        Instant[] timestamps = new Instant[levels.length];
        Arrays.fill(timestamps, Instant.now(clock).minus(1, ChronoUnit.HOURS));
        assertWholeFileConsumedAndOnlyInfoSent(timestamps, levels);
    }

    /**
     * The saved position is the furthest any stream's record reaches, as that is where the next read starts.
     */
    private void assertWholeFileConsumedAndOnlyInfoSent(Instant[] timestamps, String... levels) throws IOException {
        List<String> lines = new ArrayList<>();
        List<String> expectedSent = new ArrayList<>();
        for (int i = 0; i < levels.length; i++) {
            String line = jsonLogLine(levels[i], timestamps[i], "line-" + i);
            lines.add(line);
            if ("INFO".equals(levels[i])) {
                expectedSent.add(line);
            }
        }
        Path path = Files.write(directoryPath.resolve("test.log.1"), lines, StandardCharsets.UTF_8);
        LogFile logFile = LogFile.of(path.toFile());
        String fileHash = logFile.hashString();
        List<LogFileInformation> logFileInformationList = new ArrayList<>(Collections.singletonList(
                LogFileInformation.builder().startPosition(0).logFile(logFile).fileHash(fileHash).build()));
        ComponentLogFileInformation componentLogFileInformation = ComponentLogFileInformation.builder()
                .name("TestComponent")
                .desiredLogLevel(Level.INFO)
                .componentType(ComponentType.UserComponent)
                .logFileInformationList(logFileInformationList)
                .build();

        CloudWatchAttempt attempt = new CloudWatchAttemptLogsProcessor(mockDeviceConfiguration, clock)
                .processLogFiles(componentLogFileInformation);

        long savedPosition = 0;
        List<String> sent = new ArrayList<>();
        for (CloudWatchAttemptLogInformation stream : attempt.getLogStreamsToLogEventsMap().values()) {
            CloudWatchAttemptLogFileInformation record = stream.getAttemptLogFileInformationMap().get(fileHash);
            if (record != null) {
                savedPosition = Math.max(savedPosition, record.getStartPosition() + record.getBytesRead());
            }
            stream.getLogEvents().forEach(event -> sent.add(event.message()));
        }
        assertEquals(Files.size(path), savedPosition, "saved position");
        assertEquals(expectedSent, sent);
    }
}
