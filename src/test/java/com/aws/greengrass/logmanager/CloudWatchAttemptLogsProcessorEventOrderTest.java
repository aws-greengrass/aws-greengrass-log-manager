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
import com.aws.greengrass.logmanager.model.LogFile;
import com.aws.greengrass.logmanager.model.LogFileInformation;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.testcommons.testutilities.GGServiceTestUtil;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
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
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.aws.greengrass.deployment.DeviceConfiguration.DEVICE_PARAM_AWS_REGION;
import static com.aws.greengrass.deployment.DeviceConfiguration.DEVICE_PARAM_THING_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;

@ExtendWith({MockitoExtension.class, GGExtension.class})
class CloudWatchAttemptLogsProcessorEventOrderTest extends GGServiceTestUtil {
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
    void GIVEN_entry_split_into_events_WHEN_earlier_timestamp_follows_THEN_events_sorted_with_ties_in_file_order()
            throws IOException {
        String first = "2020-12-16T10:00:01Z [INFO] a";
        String big = "2020-12-16T10:00:05Z [INFO] big " + StringUtils.repeat('y', 300 * 1024);
        String later = "2020-12-16T10:00:02Z [INFO] d";

        List<InputLogEvent> events = processSingleFile(first + "\n" + big + "\n" + later + "\n")
                .getLogStreamsToLogEventsMap().get("/2020/12/16/thing/testThing").getSortedLogEvents();

        Map<String, String> labels = new HashMap<>();
        labels.put(first, "a");
        labels.put(later, "d");
        labels.put(big.substring(0, MAX_EVENT_LENGTH), "big part 1");
        labels.put(big.substring(MAX_EVENT_LENGTH), "big part 2");
        List<String> order = new ArrayList<>();
        events.forEach(event -> order.add(labels.getOrDefault(event.message(), "unexpected event")));
        assertEquals(Arrays.asList("a", "d", "big part 1", "big part 2"), order);
    }

    @Test
    void GIVEN_entry_over_23_hours_after_earliest_WHEN_merge_THEN_batch_stops_before_it() throws IOException {
        String first = "2020-12-16T10:00:00Z [INFO] first\n";
        String earliest = "2020-12-16T00:30:00Z [INFO] earliest\n";
        String tooLate = "2020-12-16T23:45:00Z [INFO] too late\n";

        CloudWatchAttemptLogInformation logInformation = processSingleFile(first + earliest + tooLate)
                .getLogStreamsToLogEventsMap().get("/2020/12/16/thing/testThing");

        assertThat(logInformation.getLogEvents(), hasSize(2));
        assertEquals((first + earliest).getBytes(StandardCharsets.UTF_8).length,
                logInformation.getAttemptLogFileInformationMap().values().iterator().next().getBytesRead());
    }

    private CloudWatchAttempt processSingleFile(String content) throws IOException {
        File file = directoryPath.resolve("greengrass_test.log").toFile();
        Files.write(file.toPath(), content.getBytes(StandardCharsets.UTF_8));
        try {
            LogFile logFile = LogFile.of(file);
            List<LogFileInformation> logFileInformationSet = new ArrayList<>();
            logFileInformationSet.add(LogFileInformation.builder().startPosition(0).logFile(logFile)
                    .fileHash(logFile.hashString()).build());
            ComponentLogFileInformation componentLogFileInformation = ComponentLogFileInformation.builder()
                    .name("TestComponent")
                    .desiredLogLevel(Level.INFO)
                    .componentType(ComponentType.GreengrassSystemComponent)
                    .logFileInformationList(logFileInformationSet)
                    .build();
            logsProcessor = new CloudWatchAttemptLogsProcessor(mockDeviceConfiguration, defaultClock);
            return logsProcessor.processLogFiles(componentLogFileInformation);
        } finally {
            assertTrue(file.delete());
        }
    }
}
