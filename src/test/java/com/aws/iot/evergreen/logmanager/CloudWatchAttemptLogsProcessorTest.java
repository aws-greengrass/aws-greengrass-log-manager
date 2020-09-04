/*
 *  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *  SPDX-License-Identifier: Apache-2.0
 */

package com.aws.iot.evergreen.logmanager;

import com.aws.iot.evergreen.config.Topic;
import com.aws.iot.evergreen.deployment.DeploymentService;
import com.aws.iot.evergreen.deployment.DeviceConfiguration;
import com.aws.iot.evergreen.kernel.Kernel;
import com.aws.iot.evergreen.kernel.exceptions.ServiceLoadException;
import com.aws.iot.evergreen.logmanager.model.CloudWatchAttempt;
import com.aws.iot.evergreen.logmanager.model.CloudWatchAttemptLogInformation;
import com.aws.iot.evergreen.logmanager.model.ComponentLogFileInformation;
import com.aws.iot.evergreen.logmanager.model.ComponentType;
import com.aws.iot.evergreen.logmanager.model.LogFileInformation;
import com.aws.iot.evergreen.testcommons.testutilities.EGExtension;
import com.aws.iot.evergreen.testcommons.testutilities.EGServiceTestUtil;
import org.apache.commons.lang3.RandomStringUtils;
import org.hamcrest.collection.IsEmptyCollection;
import org.hamcrest.core.IsNot;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.event.Level;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

import static com.aws.iot.evergreen.deployment.DeviceConfiguration.DEVICE_PARAM_AWS_REGION;
import static com.aws.iot.evergreen.deployment.DeviceConfiguration.DEVICE_PARAM_THING_NAME;
import static com.aws.iot.evergreen.logmanager.CloudWatchAttemptLogsProcessor.DEFAULT_LOG_GROUP_NAME;
import static com.aws.iot.evergreen.logmanager.CloudWatchAttemptLogsProcessor.DEFAULT_LOG_STREAM_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, EGExtension.class})
public class CloudWatchAttemptLogsProcessorTest extends EGServiceTestUtil {
    private static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyyy/MM/dd", Locale.ENGLISH);
    @Mock
    private DeviceConfiguration mockDeviceConfiguration;
    @Mock
    private DeploymentService mockDeploymentService;
    @Mock
    private Kernel mockKernel;
    @TempDir
    static Path directoryPath;

    private CloudWatchAttemptLogsProcessor logsProcessor;

    @BeforeEach
    public void startup() throws ServiceLoadException {
        Topic thingNameTopic = Topic.of(context, DEVICE_PARAM_THING_NAME, "testThing");
        Topic regionTopic = Topic.of(context, DEVICE_PARAM_AWS_REGION, "testRegion");
        when(mockDeviceConfiguration.getThingName()).thenReturn(thingNameTopic);
        when(mockDeviceConfiguration.getAWSRegion()).thenReturn(regionTopic);

        lenient().when(mockKernel.locate(DeploymentService.DEPLOYMENT_SERVICE_TOPICS)).thenReturn(mockDeploymentService);
        lenient().when(mockDeploymentService.getGroupConfigsForUserComponent(anyString()))
                .thenReturn(new HashSet<>(Collections.singletonList("testGroup2")));
        lenient().when(mockDeploymentService.getAllGroupConfigs())
                .thenReturn(new HashSet<>(Collections.singletonList("testGroup1")));
    }

    @Test
    public void GIVEN_one_component_one_file_less_than_max_WHEN_merge_THEN_reads_entire_file()
            throws URISyntaxException {
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
        logsProcessor = new CloudWatchAttemptLogsProcessor(mockDeviceConfiguration, mockKernel);
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
        assertEquals(7, logEventsForStream1.getLogEvents().size());
        assertTrue(logEventsForStream1.getAttemptLogFileInformationMap().containsKey(file1.getAbsolutePath()));
        assertEquals(0, logEventsForStream1.getAttemptLogFileInformationMap().get(file1.getAbsolutePath()).getStartPosition());
        assertEquals(13, logEventsForStream1.getAttemptLogFileInformationMap().get(file1.getAbsolutePath()).getBytesRead());
        assertEquals("TestComponent", logEventsForStream1.getComponentName());
    }

    @Test
    public void GIVEN_one_component_one_file_more_than_max_WHEN_merge_THEN_reads_partial_file() throws IOException {
        File file = new File(directoryPath.resolve("evergreen_test.log").toUri());
        file.createNewFile();
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

            logsProcessor = new CloudWatchAttemptLogsProcessor(mockDeviceConfiguration, mockKernel);
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
        } finally {
            assertTrue(file.delete());
        }
    }

    @Test
    public void GIVEN_one_components_two_file_less_than_max_WHEN_merge_THEN_reads_and_merges_both_files()
            throws URISyntaxException {
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
        logsProcessor = new CloudWatchAttemptLogsProcessor(mockDeviceConfiguration, mockKernel);
        CloudWatchAttempt attempt = logsProcessor.processLogFiles(componentLogFileInformation);

        assertNotNull(attempt);

        assertNotNull(attempt.getLogStreamsToLogEventsMap());
        assertThat(attempt.getLogStreamsToLogEventsMap().entrySet(), IsNot.not(IsEmptyCollection.empty()));
        String logGroup = calculateLogGroupName(ComponentType.GreengrassSystemComponent, "testRegion", "TestComponent");
        assertEquals(attempt.getLogGroupName(), logGroup);
        String logStream = calculateLogStreamName("testThing");
        String logStream2 = "/2020/02/10/testGroup1/testThing";
        assertTrue(attempt.getLogStreamsToLogEventsMap().containsKey(logStream));
        assertTrue(attempt.getLogStreamsToLogEventsMap().containsKey(logStream2));
        CloudWatchAttemptLogInformation logEventsForStream1 = attempt.getLogStreamsToLogEventsMap().get(logStream);
        CloudWatchAttemptLogInformation logEventsForStream2 = attempt.getLogStreamsToLogEventsMap().get(logStream2);
        assertNotNull(logEventsForStream1.getLogEvents());
        assertEquals(7, logEventsForStream1.getLogEvents().size());
        assertTrue(logEventsForStream1.getAttemptLogFileInformationMap().containsKey(file1.getAbsolutePath()));
        assertEquals(0, logEventsForStream1.getAttemptLogFileInformationMap().get(file1.getAbsolutePath()).getStartPosition());
        assertEquals(13, logEventsForStream1.getAttemptLogFileInformationMap().get(file1.getAbsolutePath()).getBytesRead());
        assertEquals("TestComponent", logEventsForStream1.getComponentName());

        assertNotNull(logEventsForStream2.getLogEvents());
        assertEquals(4, logEventsForStream2.getLogEvents().size());
        assertTrue(logEventsForStream2.getAttemptLogFileInformationMap().containsKey(file2.getAbsolutePath()));
        assertEquals(0, logEventsForStream2.getAttemptLogFileInformationMap().get(file2.getAbsolutePath()).getStartPosition());
        assertEquals(1256, logEventsForStream2.getAttemptLogFileInformationMap().get(file2.getAbsolutePath()).getBytesRead());
        assertEquals("TestComponent", logEventsForStream2.getComponentName());
    }

    private String calculateLogGroupName(ComponentType componentType, String awsRegion, String componentName) {
        return DEFAULT_LOG_GROUP_NAME
                .replace("{componentType}", componentType.toString())
                .replace("{region}", awsRegion)
                .replace("{componentName}", componentName);
    }

    private String calculateLogStreamName(String thingName) {
        synchronized (DATE_FORMATTER) {
            return DEFAULT_LOG_STREAM_NAME
                    .replace("{thingName}", thingName)
                    .replace("{ggFleetId}", "testGroup1")
                    .replace("{date}", DATE_FORMATTER.format(new Date()));
        }
    }
}
