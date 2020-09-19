/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.logmanager;

import com.aws.greengrass.config.Topic;
import com.aws.greengrass.config.Topics;
import com.aws.greengrass.config.UpdateBehaviorTree;
import com.aws.greengrass.logging.impl.config.LogConfig;
import com.aws.greengrass.logging.impl.config.LogStore;
import com.aws.greengrass.logmanager.model.CloudWatchAttempt;
import com.aws.greengrass.logmanager.model.CloudWatchAttemptLogFileInformation;
import com.aws.greengrass.logmanager.model.CloudWatchAttemptLogInformation;
import com.aws.greengrass.logmanager.model.ComponentLogFileInformation;
import com.aws.greengrass.logmanager.model.ComponentType;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.testcommons.testutilities.GGServiceTestUtil;
import com.aws.greengrass.util.Coerce;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import org.apache.commons.lang3.RandomStringUtils;
import org.hamcrest.collection.IsEmptyCollection;
import org.hamcrest.core.IsNot;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
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
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static com.aws.greengrass.componentmanager.KernelConfigResolver.PARAMETERS_CONFIG_KEY;
import static com.aws.greengrass.lifecyclemanager.GreengrassService.RUNTIME_STORE_NAMESPACE_TOPIC;
import static com.aws.greengrass.logmanager.LogManagerService.LOGS_UPLOADER_CONFIGURATION_TOPIC;
import static com.aws.greengrass.logmanager.LogManagerService.LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC;
import static com.aws.greengrass.logmanager.LogManagerService.PERSISTED_COMPONENT_CURRENT_PROCESSING_FILE_INFORMATION;
import static com.aws.greengrass.logmanager.LogManagerService.PERSISTED_COMPONENT_LAST_FILE_PROCESSED_TIMESTAMP;
import static com.aws.greengrass.logmanager.LogManagerService.PERSISTED_LAST_FILE_PROCESSED_TIMESTAMP;
import static com.aws.greengrass.logmanager.LogManagerService.SYSTEM_LOGS_COMPONENT_NAME;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, GGExtension.class})
public class LogManagerServiceTest extends GGServiceTestUtil {
    @Mock
    private CloudWatchLogsUploader mockUploader;
    @Mock
    private CloudWatchAttemptLogsProcessor mockMerger;
    @Captor
    private ArgumentCaptor<ComponentLogFileInformation> componentLogsInformationCaptor;
    @Captor
    private ArgumentCaptor<Consumer<CloudWatchAttempt>> callbackCaptor;
    @Captor
    private ArgumentCaptor<Map<Object, Object>> replaceAndWaitCaptor;
    @Captor
    private ArgumentCaptor<Object> objectCaptor;

    @TempDir
    static Path directoryPath;
    private LogManagerService logsUploaderService;
    private final ExecutorService executor = Executors.newCachedThreadPool();

    @BeforeAll
    static void setupBefore() throws IOException, InterruptedException {
        LogConfig.getInstance().setLevel(Level.TRACE);
        LogConfig.getInstance().setStoreType(LogStore.FILE);
        LogConfig.getInstance().setStorePath(directoryPath.resolve("evergreen.log"));
        for (int i = 0; i < 5; i++) {
            File file = new File(directoryPath.resolve("evergreen_test_" + i + ".log").toUri());
            assertTrue(file.createNewFile());
            assertTrue(file.setReadable(true));
            assertTrue(file.setWritable(true));

            try (OutputStream fileOutputStream = Files.newOutputStream(file.toPath())) {
                fileOutputStream.write("TEST".getBytes(StandardCharsets.UTF_8));
            }
            TimeUnit.SECONDS.sleep(1);
        }
        File currentFile = new File(directoryPath.resolve("evergreen.log").toUri());
        try (OutputStream currentFileOutputStream = Files.newOutputStream(currentFile.toPath())) {
            currentFileOutputStream.write("TEST".getBytes(StandardCharsets.UTF_8));
        }
    }

    @AfterAll
    static void cleanUpAfter() {
        LogConfig.getInstance().setLevel(Level.INFO);
        LogConfig.getInstance().setStoreType(LogStore.CONSOLE);
        final File folder = new File(directoryPath.toUri());
        final File[] files = folder.listFiles();
        if (files != null) {
            for (final File file : files) {
                if (file.getName().startsWith("evergreen") && !file.delete()) {
                    System.err.println("Can't remove " + file.getAbsolutePath());
                }
            }
        }
    }

    @BeforeEach
    public void setup() {
        serviceFullName = "aws.greengrass.LogManager";
        initializeMockedConfig();
    }

    private void mockDefaultPersistedState() {
        Topics allCurrentProcessingComponentTopics1 =
                Topics.of(context, PERSISTED_COMPONENT_CURRENT_PROCESSING_FILE_INFORMATION, null);
        Topics currentProcessingComponentTopics1 =
                Topics.of(context, SYSTEM_LOGS_COMPONENT_NAME, allCurrentProcessingComponentTopics1);
        Topics currentProcessingComponentTopics2 =
                Topics.of(context, "UserComponentA", allCurrentProcessingComponentTopics1);

        lenient().when(config.lookupTopics(RUNTIME_STORE_NAMESPACE_TOPIC)
                .lookupTopics(PERSISTED_COMPONENT_CURRENT_PROCESSING_FILE_INFORMATION, SYSTEM_LOGS_COMPONENT_NAME))
                .thenReturn(currentProcessingComponentTopics1);
        lenient().when(config.lookupTopics(RUNTIME_STORE_NAMESPACE_TOPIC)
                .lookupTopics(PERSISTED_COMPONENT_CURRENT_PROCESSING_FILE_INFORMATION, "UserComponentA"))
                .thenReturn(currentProcessingComponentTopics2);

        Topics allLastFileProcessedComponentTopics =
                Topics.of(context, PERSISTED_COMPONENT_LAST_FILE_PROCESSED_TIMESTAMP, null);
        Topics lastFileProcessedComponentTopics1 =
                Topics.of(context, SYSTEM_LOGS_COMPONENT_NAME, allLastFileProcessedComponentTopics);
        Topics lastFileProcessedComponentTopics2 =
                Topics.of(context, "UserComponentA", allLastFileProcessedComponentTopics);

        lenient().when(config.lookupTopics(RUNTIME_STORE_NAMESPACE_TOPIC)
                .lookupTopics(PERSISTED_COMPONENT_LAST_FILE_PROCESSED_TIMESTAMP, SYSTEM_LOGS_COMPONENT_NAME))
                .thenReturn(lastFileProcessedComponentTopics1);
        lenient().when(config.lookupTopics(RUNTIME_STORE_NAMESPACE_TOPIC)
                .lookupTopics(PERSISTED_COMPONENT_LAST_FILE_PROCESSED_TIMESTAMP, "UserComponentA"))
                .thenReturn(lastFileProcessedComponentTopics2);
    }

    @AfterEach
    public void cleanup() throws InterruptedException {
        logsUploaderService.componentCurrentProcessingLogFile.clear();
        logsUploaderService.lastComponentUploadedLogFileInstantMap.clear();
        logsUploaderService.shutdown();
    }

    @Test
    public void GIVEN_system_log_files_to_be_uploaded_WHEN_merger_merges_THEN_we_get_all_log_files()
            throws InterruptedException {
        mockDefaultPersistedState();
        Topic periodicUpdateIntervalMsTopic = Topic.of(context, LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC, "1");
        when(config.lookup(PARAMETERS_CONFIG_KEY, LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC))
                .thenReturn(periodicUpdateIntervalMsTopic);
        when(mockMerger.processLogFiles(componentLogsInformationCaptor.capture())).thenReturn(new CloudWatchAttempt());

        String configuration =
                "{\"ComponentLogInformation\": " +
                        "[{\"LogFileRegex\": \"^log.txt\\\\w*\",\"LogFileDirectoryPath\": \"" +
                        directoryPath.toAbsolutePath().toString() + "\", " +
                        "\"MultiLineStartPattern\": \"\\\\{'timestamp\",\"MinimumLogLevel\": \"DEBUG\"," +
                        "\"DiskSpaceLimit\": \"10\",\"ComponentName\": \"UserComponentA\"," +
                        "\"DiskSpaceLimitUnit\": \"GB\",\"DeleteLogFileAfterCloudUpload\": \"false\"}]," +
                        "\"SystemLogsConfiguration\":{\"UploadToCloudWatch\": true,\"MinimumLogLevel\": \"INFO\"," +
                        "\"DiskSpaceLimit\": \"25\"," +
                        "\"DiskSpaceLimitUnit\": \"MB\"}}";
        Topic configTopic = Topic.of(context, LOGS_UPLOADER_CONFIGURATION_TOPIC, configuration);
        when(config.lookup(PARAMETERS_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC))
                .thenReturn(configTopic);
        doNothing().when(mockUploader).registerAttemptStatus(anyString(), callbackCaptor.capture());

        logsUploaderService = new LogManagerService(config, mockUploader, mockMerger, executor);
        startServiceOnAnotherThread();

        TimeUnit.SECONDS.sleep(5);

        assertNotNull(componentLogsInformationCaptor.getValue());
        ComponentLogFileInformation componentLogFileInformation = componentLogsInformationCaptor.getValue();
        assertNotNull(componentLogFileInformation);
        assertEquals("System", componentLogFileInformation.getName());
        assertEquals(ComponentType.GreengrassSystemComponent, componentLogFileInformation.getComponentType());
        assertEquals(Level.INFO, componentLogFileInformation.getDesiredLogLevel());
        assertNotNull(componentLogFileInformation.getLogFileInformationList());
        assertThat(componentLogFileInformation.getLogFileInformationList(), IsNot.not(IsEmptyCollection.empty()));
        assertTrue(componentLogFileInformation.getLogFileInformationList().size() >= 5);
        verify(mockUploader, times(1)).upload(any(CloudWatchAttempt.class), anyInt());
    }

    private void startServiceOnAnotherThread() {
        executor.submit(() -> {
            try {
                logsUploaderService.startup();
            } catch (InterruptedException ignored) {
            }
        });
    }

    @Test
    public void GIVEN_invalid_config_WHEN_config_is_processed_THEN_no_component_config_is_added(
            ExtensionContext context1) {
        mockDefaultPersistedState();
        ignoreExceptionOfType(context1, MismatchedInputException.class);
        Topic periodicUpdateIntervalMsTopic = Topic.of(context, LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC, "3");
        when(config.lookup(PARAMETERS_CONFIG_KEY, LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC))
                .thenReturn(periodicUpdateIntervalMsTopic);
        String configuration =
                "{\"ComponentLogInformation\": {}" +
                        "\"SystemLogsConfiguration\":{\"UploadToCloudWatch\": true,\"MinimumLogLevel\": \"INFO\"," +
                        "\"DiskSpaceLimit\": \"25\"," +
                        "\"DiskSpaceLimitUnit\": \"MB\"}}";
        Topic configTopic = Topic.of(context, LOGS_UPLOADER_CONFIGURATION_TOPIC, configuration);
        when(config.lookup(PARAMETERS_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC))
                .thenReturn(configTopic);

        logsUploaderService = new LogManagerService(config, mockUploader, mockMerger, executor);
        startServiceOnAnotherThread();
        assertThat(logsUploaderService.componentCurrentProcessingLogFile.values(), IsEmptyCollection.empty());
    }

    @Test
    public void GIVEN_null_config_WHEN_config_is_processed_THEN_no_component_config_is_added(
            ExtensionContext context1) {
        ignoreExceptionOfType(context1, MismatchedInputException.class);
        Topic periodicUpdateIntervalMsTopic = Topic.of(context, LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC, "3");
        when(config.lookup(PARAMETERS_CONFIG_KEY, LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC))
                .thenReturn(periodicUpdateIntervalMsTopic);
        Topic configTopic = Topic.of(context, LOGS_UPLOADER_CONFIGURATION_TOPIC, null);
        when(config.lookup(PARAMETERS_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC))
                .thenReturn(configTopic);

        logsUploaderService = new LogManagerService(config, mockUploader, mockMerger, executor);
        startServiceOnAnotherThread();
        assertThat(logsUploaderService.componentCurrentProcessingLogFile.values(), IsEmptyCollection.empty());
    }

    @Test
    public void GIVEN_cloud_watch_attempt_handler_WHEN_attempt_completes_THEN_successfully_updates_states_for_each_component()
            throws URISyntaxException {
        mockDefaultPersistedState();
        Topic periodicUpdateIntervalMsTopic = Topic.of(context, LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC, "1000");
        when(config.lookup(PARAMETERS_CONFIG_KEY, LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC))
                .thenReturn(periodicUpdateIntervalMsTopic);

        String configuration =
                "{\"ComponentLogInformation\": " +
                        "[],\"SystemLogsConfiguration\":{\"UploadToCloudWatch\": true,\"MinimumLogLevel\": \"INFO\"," +
                        "\"DiskSpaceLimit\": \"25\"," +
                        "\"DiskSpaceLimitUnit\": \"MB\"}}";
        Topic configTopic = Topic.of(context, LOGS_UPLOADER_CONFIGURATION_TOPIC, configuration);
        when(config.lookup(PARAMETERS_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC))
                .thenReturn(configTopic);

        Topics componentTopics2 = mock(Topics.class);
        Topic lastFileProcessedTimeStampTopics = mock(Topic.class);
        when(componentTopics2.createLeafChild(any())).thenReturn(lastFileProcessedTimeStampTopics);
        when(lastFileProcessedTimeStampTopics.withValue(objectCaptor.capture()))
                .thenReturn(lastFileProcessedTimeStampTopics);

        Topics componentTopics3 = mock(Topics.class);
        doNothing().when(componentTopics3).replaceAndWait(replaceAndWaitCaptor.capture());

        CloudWatchAttempt attempt = new CloudWatchAttempt();
        Map<String, CloudWatchAttemptLogInformation> logStreamsToLogInformationMap = new HashMap<>();
        File file1 = new File(getClass().getResource("testlogs2.log").toURI());
        File file2 = new File(getClass().getResource("testlogs1.log").toURI());
        Map<String, CloudWatchAttemptLogFileInformation> attemptLogFileInformationMap1 = new HashMap<>();
        attemptLogFileInformationMap1.put(file1.getAbsolutePath(), CloudWatchAttemptLogFileInformation.builder()
                .startPosition(0)
                .bytesRead(13)
                .lastModifiedTime(file1.lastModified())
                .build());
        Map<String, CloudWatchAttemptLogFileInformation> attemptLogFileInformationMap2 = new HashMap<>();
        attemptLogFileInformationMap2.put(file2.getAbsolutePath(), CloudWatchAttemptLogFileInformation.builder()
                .startPosition(0)
                .bytesRead(1061)
                .lastModifiedTime(file2.lastModified())
                .build());

        CloudWatchAttemptLogInformation attemptLogInformation1 = CloudWatchAttemptLogInformation.builder()
                .componentName("TestComponent")
                .attemptLogFileInformationMap(attemptLogFileInformationMap1)
                .build();
        CloudWatchAttemptLogInformation attemptLogInformation2 = CloudWatchAttemptLogInformation.builder()
                .componentName("TestComponent2")
                .attemptLogFileInformationMap(attemptLogFileInformationMap2)
                .build();
        logStreamsToLogInformationMap.put("testStream", attemptLogInformation1);
        logStreamsToLogInformationMap.put("testStream2", attemptLogInformation2);
        attempt.setLogStreamsToLogEventsMap(logStreamsToLogInformationMap);
        attempt.setLogStreamUploadedSet(new HashSet<>(Arrays.asList("testStream", "testStream2")));
        doNothing().when(mockUploader).registerAttemptStatus(anyString(), callbackCaptor.capture());
        when(config.lookupTopics(RUNTIME_STORE_NAMESPACE_TOPIC)
                .lookupTopics(PERSISTED_COMPONENT_CURRENT_PROCESSING_FILE_INFORMATION, "TestComponent2"))
                .thenReturn(componentTopics3);
        when(config.lookupTopics(RUNTIME_STORE_NAMESPACE_TOPIC)
                .lookupTopics(PERSISTED_COMPONENT_LAST_FILE_PROCESSED_TIMESTAMP, "TestComponent"))
                .thenReturn(componentTopics2);

        logsUploaderService = new LogManagerService(config, mockUploader, mockMerger, executor);
        startServiceOnAnotherThread();

        callbackCaptor.getValue().accept(attempt);

        assertThat(replaceAndWaitCaptor.getAllValues(), IsNot.not(IsEmptyCollection.empty()));
        assertThat(objectCaptor.getAllValues(), IsNot.not(IsEmptyCollection.empty()));
        List<Object> completedComponentLastProcessedFileInformation = objectCaptor.getAllValues();
        List<Map<Object, Object>> partiallyReadComponentLogFileInformation = replaceAndWaitCaptor.getAllValues();
        assertEquals(1, completedComponentLastProcessedFileInformation.size());
        assertEquals(1, partiallyReadComponentLogFileInformation.size());
        assertEquals(file1.lastModified(), Coerce.toLong(completedComponentLastProcessedFileInformation.get(0)));
        LogManagerService.CurrentProcessingFileInformation currentProcessingFileInformation =
                LogManagerService.CurrentProcessingFileInformation
                        .convertFromMapOfObjects(partiallyReadComponentLogFileInformation.get(0));
        assertEquals(file2.getAbsolutePath(), currentProcessingFileInformation.getFileName());
        assertEquals(1061, currentProcessingFileInformation.getStartPosition());
        assertEquals(file2.lastModified(), currentProcessingFileInformation.getLastModifiedTime());


        assertNotNull(logsUploaderService.lastComponentUploadedLogFileInstantMap);
        assertThat(logsUploaderService.lastComponentUploadedLogFileInstantMap.entrySet(), IsNot.not(IsEmptyCollection.empty()));
        assertTrue(logsUploaderService.lastComponentUploadedLogFileInstantMap.containsKey("TestComponent"));
        assertEquals(Instant.ofEpochMilli(file1.lastModified()), logsUploaderService.lastComponentUploadedLogFileInstantMap.get("TestComponent"));
        assertNotNull(logsUploaderService.componentCurrentProcessingLogFile);
        assertThat(logsUploaderService.componentCurrentProcessingLogFile.entrySet(), IsNot.not(IsEmptyCollection.empty()));
        assertTrue(logsUploaderService.componentCurrentProcessingLogFile.containsKey("TestComponent2"));
        assertEquals(file2.getAbsolutePath(), logsUploaderService.componentCurrentProcessingLogFile.get("TestComponent2").getFileName());
        assertEquals(1061, logsUploaderService.componentCurrentProcessingLogFile.get("TestComponent2").getStartPosition());
    }

    @Test
    public void GIVEN_some_system_files_uploaded_and_another_partially_uploaded_WHEN_merger_merges_THEN_sets_the_start_position_correctly()
            throws InterruptedException {
        mockDefaultPersistedState();
        Topic periodicUpdateIntervalMsTopic = Topic.of(context, LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC, "3");
        when(config.lookup(PARAMETERS_CONFIG_KEY, LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC))
                .thenReturn(periodicUpdateIntervalMsTopic);
        when(mockMerger.processLogFiles(componentLogsInformationCaptor.capture())).thenReturn(new CloudWatchAttempt());

        String configuration =
                "{\"ComponentLogInformation\": " +
                        "[],\"SystemLogsConfiguration\":{\"UploadToCloudWatch\": true,\"MinimumLogLevel\": \"INFO\"," +
                        "\"DiskSpaceLimit\": \"25\"," +
                        "\"DiskSpaceLimitUnit\": \"MB\"}}";
        Topic configTopic = Topic.of(context, LOGS_UPLOADER_CONFIGURATION_TOPIC, configuration);
        when(config.lookup(PARAMETERS_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC))
                .thenReturn(configTopic);

        logsUploaderService = new LogManagerService(config, mockUploader, mockMerger, executor);
        File file = new File(directoryPath.resolve("evergreen_test_2.log").toUri());
        File currentProcessingFile = new File(directoryPath.resolve("evergreen_test_3.log").toUri());
        logsUploaderService.lastComponentUploadedLogFileInstantMap.put(SYSTEM_LOGS_COMPONENT_NAME,
                Instant.ofEpochMilli(file.lastModified()));
        logsUploaderService.componentCurrentProcessingLogFile.put(SYSTEM_LOGS_COMPONENT_NAME,
                LogManagerService.CurrentProcessingFileInformation.builder()
                        .fileName(currentProcessingFile.getAbsolutePath())
                        .lastModifiedTime(currentProcessingFile.lastModified())
                        .startPosition(2)
                        .build());

        startServiceOnAnotherThread();
        TimeUnit.SECONDS.sleep(5);

        assertNotNull(componentLogsInformationCaptor.getValue());
        ComponentLogFileInformation componentLogFileInformation = componentLogsInformationCaptor.getValue();
        assertNotNull(componentLogFileInformation);
        assertEquals("System", componentLogFileInformation.getName());
        assertEquals(ComponentType.GreengrassSystemComponent, componentLogFileInformation.getComponentType());
        assertEquals(Level.INFO, componentLogFileInformation.getDesiredLogLevel());
        assertNotNull(componentLogFileInformation.getLogFileInformationList());
        assertThat(componentLogFileInformation.getLogFileInformationList(), IsNot.not(IsEmptyCollection.empty()));
        assertTrue(componentLogFileInformation.getLogFileInformationList().size() >= 2);
        componentLogFileInformation.getLogFileInformationList().forEach(logFileInformation -> {
            if (logFileInformation.getFile().getAbsolutePath().equals(currentProcessingFile.getAbsolutePath())) {
                assertEquals(2, logFileInformation.getStartPosition());
            } else {
                assertEquals(0, logFileInformation.getStartPosition());
            }
        });
        verify(mockUploader, times(1)).upload(any(CloudWatchAttempt.class), anyInt());
    }

    @Test
    public void GIVEN_user_component_with_space_management_WHEN_log_file_size_exceeds_limit_THEN_deletes_excess_log_files()
            throws InterruptedException, IOException {
        mockDefaultPersistedState();
        Topic periodicUpdateIntervalMsTopic = Topic.of(context, LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC, "3");
        when(config.lookup(PARAMETERS_CONFIG_KEY, LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC))
                .thenReturn(periodicUpdateIntervalMsTopic);

        String configuration =
                "{\"ComponentLogInformation\": " +
                        "[{\"LogFileRegex\": \"^log.txt\\\\w*\",\"LogFileDirectoryPath\": \"" +
                        directoryPath.toAbsolutePath().toString() + "\", " +
                        "\"MultiLineStartPattern\": \"\\\\{'timestamp\",\"MinimumLogLevel\": \"DEBUG\"," +
                        "\"DiskSpaceLimit\": \"2\",\"ComponentName\": \"UserComponentA\"," +
                        "\"DiskSpaceLimitUnit\": \"KB\",\"DeleteLogFileAfterCloudUpload\": \"true\"}]," +
                        "\"SystemLogsConfiguration\":{\"UploadToCloudWatch\": true,\"MinimumLogLevel\": \"INFO\"," +
                        "\"DiskSpaceLimit\": \"25\"," +
                        "\"DiskSpaceLimitUnit\": \"KB\"}}";
        Topic configTopic = Topic.of(context, LOGS_UPLOADER_CONFIGURATION_TOPIC, configuration);
        when(config.lookup(PARAMETERS_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC))
                .thenReturn(configTopic);

        logsUploaderService = new LogManagerService(config, mockUploader, mockMerger, executor);
        startServiceOnAnotherThread();
        TimeUnit.SECONDS.sleep(5);
        List<String> fileNames = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            Path fileNamePath = directoryPath.resolve("log.txt_" + UUID.randomUUID().toString());
            fileNames.add(fileNamePath.toAbsolutePath().toString());
            File file1 = new File(fileNamePath.toUri());
            assertTrue(file1.createNewFile());
            assertTrue(file1.setReadable(true));
            assertTrue(file1.setWritable(true));

            try (OutputStream fileOutputStream = Files.newOutputStream(file1.toPath())) {
                String generatedString = RandomStringUtils.randomAlphabetic(1024);
                fileOutputStream.write(generatedString.getBytes(StandardCharsets.UTF_8));
            }
            TimeUnit.SECONDS.sleep(1);
        }
        TimeUnit.SECONDS.sleep(5);

        for (int i = 0; i < 3; i++) {
            assertTrue(Files.notExists(Paths.get(fileNames.get(i))));
        }

        for (int i = 3; i < 5; i++) {
            assertTrue(Files.exists(Paths.get(fileNames.get(i))));
            assertEquals(1024 ,new File(Paths.get(fileNames.get(i)).toUri()).length());
        }
    }

    @Test
    public void GIVEN_user_component_logs_delete_file_after_upload_set_WHEN_upload_logs_THEN_deletes_uploaded_log_files()
            throws InterruptedException, IOException {
        mockDefaultPersistedState();
        Topic periodicUpdateIntervalMsTopic = Topic.of(context, LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC, "3");
        when(config.lookup(PARAMETERS_CONFIG_KEY, LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC))
                .thenReturn(periodicUpdateIntervalMsTopic);

        String configuration =
                "{\"ComponentLogInformation\": " +
                        "[{\"LogFileRegex\": \"^log2.txt\\\\w*\",\"LogFileDirectoryPath\": \"" +
                        directoryPath.toAbsolutePath().toString() + "\", " +
                        "\"MultiLineStartPattern\": \"\\\\{'timestamp\",\"MinimumLogLevel\": \"DEBUG\"," +
                        "\"DiskSpaceLimit\": \"25\",\"ComponentName\": \"UserComponentA\"," +
                        "\"DiskSpaceLimitUnit\": \"KB\",\"DeleteLogFileAfterCloudUpload\": \"true\"}]," +
                        "\"SystemLogsConfiguration\":{\"UploadToCloudWatch\": true,\"MinimumLogLevel\": \"INFO\"," +
                        "\"DiskSpaceLimit\": \"25\"," +
                        "\"DiskSpaceLimitUnit\": \"KB\"}}";
        Topic configTopic = Topic.of(context, LOGS_UPLOADER_CONFIGURATION_TOPIC, configuration);
        when(config.lookup(PARAMETERS_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC))
                .thenReturn(configTopic);

        List<String> fileNames = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            Path fileNamePath = directoryPath.resolve("log2.txt_" + UUID.randomUUID().toString());
            fileNames.add(fileNamePath.toAbsolutePath().toString());
            File file1 = new File(fileNamePath.toUri());
            if (Files.notExists(file1.toPath())) {
                assertTrue(file1.createNewFile());
            }
            assertTrue(file1.setReadable(true));
            assertTrue(file1.setWritable(true));

            try (OutputStream fileOutputStream = Files.newOutputStream(file1.toPath())) {
                String generatedString = RandomStringUtils.randomAlphabetic(1024);
                fileOutputStream.write(generatedString.getBytes(StandardCharsets.UTF_8));
            }
            TimeUnit.SECONDS.sleep(1);
        }
        CloudWatchAttempt attempt = new CloudWatchAttempt();
        Map<String, CloudWatchAttemptLogInformation> logStreamsToLogInformationMap = new HashMap<>();
        File file1 = new File(directoryPath.resolve(fileNames.get(0)).toUri());
        File file2 = new File(directoryPath.resolve(fileNames.get(1)).toUri());
        Map<String, CloudWatchAttemptLogFileInformation> attemptLogFileInformationMap1 = new HashMap<>();
        attemptLogFileInformationMap1.put(file1.getAbsolutePath(), CloudWatchAttemptLogFileInformation.builder()
                .startPosition(0)
                .bytesRead(file1.length())
                .lastModifiedTime(file1.lastModified())
                .build());
        attemptLogFileInformationMap1.put(file2.getAbsolutePath(), CloudWatchAttemptLogFileInformation.builder()
                .startPosition(0)
                .bytesRead(file2.length())
                .lastModifiedTime(file2.lastModified())
                .build());

        CloudWatchAttemptLogInformation attemptLogInformation1 = CloudWatchAttemptLogInformation.builder()
                .componentName("UserComponentA")
                .attemptLogFileInformationMap(attemptLogFileInformationMap1)
                .build();
        logStreamsToLogInformationMap.put("testStream", attemptLogInformation1);
        attempt.setLogStreamsToLogEventsMap(logStreamsToLogInformationMap);
        attempt.setLogStreamUploadedSet(new HashSet<>(Collections.singletonList("testStream")));
        doNothing().when(mockUploader).registerAttemptStatus(anyString(), callbackCaptor.capture());
        Topics componentTopics1 = mock(Topics.class);
        Topic lastFileProcessedTimeStampTopics = mock(Topic.class);
        when(componentTopics1.createLeafChild(any())).thenReturn(lastFileProcessedTimeStampTopics);
        when(lastFileProcessedTimeStampTopics.withValue(objectCaptor.capture()))
                .thenReturn(lastFileProcessedTimeStampTopics);
        when(config.lookupTopics(RUNTIME_STORE_NAMESPACE_TOPIC)
                .lookupTopics(PERSISTED_COMPONENT_LAST_FILE_PROCESSED_TIMESTAMP, "UserComponentA"))
                .thenReturn(componentTopics1);

        logsUploaderService = new LogManagerService(config, mockUploader, mockMerger, executor);
        startServiceOnAnotherThread();

        callbackCaptor.getValue().accept(attempt);

        TimeUnit.SECONDS.sleep(5);

        for (int i = 0; i < 2; i++) {
            assertTrue(Files.notExists(Paths.get(fileNames.get(i))));
        }
        for (int i = 2; i < 5; i++) {
            assertTrue(Files.exists(Paths.get(fileNames.get(i))));
        }
    }

    @Test
    public void GIVEN_a_partially_uploaded_file_but_rotated_WHEN_merger_merges_THEN_sets_the_start_position_correctly()
            throws InterruptedException {
        mockDefaultPersistedState();
        Topic periodicUpdateIntervalMsTopic = Topic.of(context, LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC, "3");
        when(config.lookup(PARAMETERS_CONFIG_KEY, LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC))
                .thenReturn(periodicUpdateIntervalMsTopic);
        when(mockMerger.processLogFiles(componentLogsInformationCaptor.capture())).thenReturn(new CloudWatchAttempt());

        String configuration =
                "{\"ComponentLogInformation\": " +
                        "[],\"SystemLogsConfiguration\":{\"UploadToCloudWatch\": true,\"MinimumLogLevel\": \"INFO\"," +
                        "\"DiskSpaceLimit\": \"25\"," +
                        "\"DiskSpaceLimitUnit\": \"MB\"}}";
        Topic configTopic = Topic.of(context, LOGS_UPLOADER_CONFIGURATION_TOPIC, configuration);
        when(config.lookup(PARAMETERS_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC))
                .thenReturn(configTopic);

        logsUploaderService = new LogManagerService(config, mockUploader, mockMerger, executor);
        startServiceOnAnotherThread();

        File file = new File(directoryPath.resolve("evergreen.log_test_2").toUri());
        File currentProcessingFile = new File(directoryPath.resolve("evergreen.log_test-3").toUri());
        logsUploaderService.lastComponentUploadedLogFileInstantMap.put(SYSTEM_LOGS_COMPONENT_NAME,
                Instant.ofEpochMilli(file.lastModified()));
        logsUploaderService.componentCurrentProcessingLogFile.put(SYSTEM_LOGS_COMPONENT_NAME,
                LogManagerService.CurrentProcessingFileInformation.builder()
                        .fileName(currentProcessingFile.getAbsolutePath())
                        .lastModifiedTime(currentProcessingFile.lastModified() - 1000)
                        .startPosition(2)
                        .build());

        TimeUnit.SECONDS.sleep(5);

        assertNotNull(componentLogsInformationCaptor.getValue());
        ComponentLogFileInformation componentLogFileInformation = componentLogsInformationCaptor.getValue();
        assertNotNull(componentLogFileInformation);
        assertEquals("System", componentLogFileInformation.getName());
        assertEquals(ComponentType.GreengrassSystemComponent, componentLogFileInformation.getComponentType());
        assertEquals(Level.INFO, componentLogFileInformation.getDesiredLogLevel());
        assertNotNull(componentLogFileInformation.getLogFileInformationList());
        assertThat(componentLogFileInformation.getLogFileInformationList(), IsNot.not(IsEmptyCollection.empty()));
        assertTrue(componentLogFileInformation.getLogFileInformationList().size() >= 2);
        componentLogFileInformation.getLogFileInformationList().forEach(logFileInformation ->
                assertEquals(0, logFileInformation.getStartPosition()));
        verify(mockUploader, times(1)).upload(any(CloudWatchAttempt.class), anyInt());
    }

    @Test
    public void GIVEN_persisted_data_WHEN_log_uploader_initialises_THEN_correctly_sets_the_persisted_data() {
        Topic periodicUpdateIntervalMsTopic = Topic.of(context, LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC, "3");
        when(config.lookup(PARAMETERS_CONFIG_KEY, LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC))
                .thenReturn(periodicUpdateIntervalMsTopic);
        String configuration =
                "{\"ComponentLogInformation\": " +
                        "[{\"LogFileRegex\": \"^log.txt\\\\w*\",\"LogFileDirectoryPath\": \"" +
                        directoryPath.toAbsolutePath().toString() + "\", " +
                        "\"MultiLineStartPattern\": \"\\\\{'timestamp\",\"MinimumLogLevel\": \"DEBUG\"," +
                        "\"DiskSpaceLimit\": \"10\",\"ComponentName\": \"UserComponentA\"," +
                        "\"DiskSpaceLimitUnit\": \"GB\",\"DeleteLogFileAfterCloudUpload\": \"false\"}]," +
                        "\"SystemLogsConfiguration\":{\"UploadToCloudWatch\": true,\"MinimumLogLevel\": \"INFO\"," +
                        "\"DiskSpaceLimit\": \"25\"," +
                        "\"DiskSpaceLimitUnit\": \"MB\"}}";
        Topic configTopic = Topic.of(context, LOGS_UPLOADER_CONFIGURATION_TOPIC, configuration);
        when(config.lookup(PARAMETERS_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC))
                .thenReturn(configTopic);

        Instant now = Instant.now();
        Instant tenSecondsAgo = Instant.now().minusSeconds(10);
        Topics allCurrentProcessingComponentTopics1 =
                Topics.of(context, PERSISTED_COMPONENT_CURRENT_PROCESSING_FILE_INFORMATION, null);
        Topics currentProcessingComponentTopics1 =
                Topics.of(context, SYSTEM_LOGS_COMPONENT_NAME, allCurrentProcessingComponentTopics1);
        Topics currentProcessingComponentTopics2 =
                Topics.of(context, "UserComponentA", allCurrentProcessingComponentTopics1);
        LogManagerService.CurrentProcessingFileInformation currentProcessingFileInformation1 =
                LogManagerService.CurrentProcessingFileInformation.builder()
                        .fileName("TestFile")
                        .lastModifiedTime(Instant.EPOCH.toEpochMilli())
                        .startPosition(200)
                        .build();
        LogManagerService.CurrentProcessingFileInformation currentProcessingFileInformation2 =
                LogManagerService.CurrentProcessingFileInformation.builder()
                        .fileName("TestFile2")
                        .lastModifiedTime(now.toEpochMilli())
                        .startPosition(10000)
                        .build();
        currentProcessingComponentTopics1.updateFromMap(now.toEpochMilli(),
                currentProcessingFileInformation1.convertToMapOfObjects(),
                new UpdateBehaviorTree(UpdateBehaviorTree.UpdateBehavior.REPLACE));
        currentProcessingComponentTopics2.updateFromMap(now.toEpochMilli(),
                currentProcessingFileInformation2.convertToMapOfObjects(),
                new UpdateBehaviorTree(UpdateBehaviorTree.UpdateBehavior.REPLACE));

        Topics allLastFileProcessedComponentTopics =
                Topics.of(context, PERSISTED_COMPONENT_LAST_FILE_PROCESSED_TIMESTAMP, null);
        Topics lastFileProcessedComponentTopics1 =
                Topics.of(context, SYSTEM_LOGS_COMPONENT_NAME, allLastFileProcessedComponentTopics);
        Topics lastFileProcessedComponentTopics2 =
                Topics.of(context, "UserComponentA", allLastFileProcessedComponentTopics);
        Topic leafChild1 = lastFileProcessedComponentTopics1.createLeafChild(PERSISTED_LAST_FILE_PROCESSED_TIMESTAMP);
        leafChild1.withValue(tenSecondsAgo.toEpochMilli());

        when(config.lookupTopics(RUNTIME_STORE_NAMESPACE_TOPIC)
                .lookupTopics(PERSISTED_COMPONENT_LAST_FILE_PROCESSED_TIMESTAMP, SYSTEM_LOGS_COMPONENT_NAME))
                .thenReturn(lastFileProcessedComponentTopics1);
        when(config.lookupTopics(RUNTIME_STORE_NAMESPACE_TOPIC)
                .lookupTopics(PERSISTED_COMPONENT_LAST_FILE_PROCESSED_TIMESTAMP, "UserComponentA"))
                .thenReturn(lastFileProcessedComponentTopics2);
        when(config.lookupTopics(RUNTIME_STORE_NAMESPACE_TOPIC)
                .lookupTopics(PERSISTED_COMPONENT_CURRENT_PROCESSING_FILE_INFORMATION, SYSTEM_LOGS_COMPONENT_NAME))
                .thenReturn(currentProcessingComponentTopics1);
        when(config.lookupTopics(RUNTIME_STORE_NAMESPACE_TOPIC)
                .lookupTopics(PERSISTED_COMPONENT_CURRENT_PROCESSING_FILE_INFORMATION, "UserComponentA"))
                .thenReturn(currentProcessingComponentTopics2);

        logsUploaderService = new LogManagerService(config, mockUploader, mockMerger, executor);

        assertNotNull(logsUploaderService.componentCurrentProcessingLogFile);
        assertNotNull(logsUploaderService.lastComponentUploadedLogFileInstantMap);
        assertTrue(logsUploaderService.lastComponentUploadedLogFileInstantMap.containsKey(SYSTEM_LOGS_COMPONENT_NAME));
        assertFalse(logsUploaderService.lastComponentUploadedLogFileInstantMap.containsKey("UserComponentA"));
        assertEquals(tenSecondsAgo,
                logsUploaderService.lastComponentUploadedLogFileInstantMap.get(SYSTEM_LOGS_COMPONENT_NAME));

        assertTrue(logsUploaderService.componentCurrentProcessingLogFile.containsKey(SYSTEM_LOGS_COMPONENT_NAME));
        assertTrue(logsUploaderService.componentCurrentProcessingLogFile.containsKey("UserComponentA"));
        LogManagerService.CurrentProcessingFileInformation systemInfo =
                logsUploaderService.componentCurrentProcessingLogFile.get(SYSTEM_LOGS_COMPONENT_NAME);
        assertEquals("TestFile", systemInfo.getFileName());
        assertEquals(200, systemInfo.getStartPosition());
        assertEquals(Instant.EPOCH.toEpochMilli(), systemInfo.getLastModifiedTime());
        LogManagerService.CurrentProcessingFileInformation userComponentInfo =
                logsUploaderService.componentCurrentProcessingLogFile.get("UserComponentA");
        assertEquals("TestFile2", userComponentInfo.getFileName());
        assertEquals(10000, userComponentInfo.getStartPosition());
        assertEquals(now.toEpochMilli(), userComponentInfo.getLastModifiedTime());
    }
}
