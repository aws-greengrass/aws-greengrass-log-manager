/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.logmanager;

import com.aws.greengrass.config.Topic;
import com.aws.greengrass.config.Topics;
import com.aws.greengrass.config.UnsupportedInputTypeException;
import com.aws.greengrass.config.UpdateBehaviorTree;
import com.aws.greengrass.dependency.Crashable;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.logging.impl.config.LogConfig;
import com.aws.greengrass.logging.impl.config.LogStore;
import com.aws.greengrass.logging.impl.config.model.LogConfigUpdate;
import com.aws.greengrass.logmanager.exceptions.InvalidLogGroupException;
import com.aws.greengrass.logmanager.model.CloudWatchAttempt;
import com.aws.greengrass.logmanager.model.CloudWatchAttemptLogFileInformation;
import com.aws.greengrass.logmanager.model.CloudWatchAttemptLogInformation;
import com.aws.greengrass.logmanager.model.ComponentLogFileInformation;
import com.aws.greengrass.logmanager.model.ComponentType;
import com.aws.greengrass.logmanager.model.LogFile;
import com.aws.greengrass.logmanager.model.LogFileGroup;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.testcommons.testutilities.GGServiceTestUtil;
import com.aws.greengrass.util.Coerce;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import lombok.extern.java.Log;
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
import java.nio.file.NoSuchFileException;
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
import java.util.regex.Pattern;

import static com.aws.greengrass.componentmanager.KernelConfigResolver.CONFIGURATION_CONFIG_KEY;
import static com.aws.greengrass.lifecyclemanager.GreengrassService.RUNTIME_STORE_NAMESPACE_TOPIC;
import static com.aws.greengrass.logging.impl.config.LogConfig.newLogConfigFromRootConfig;
import static com.aws.greengrass.logmanager.LogManagerService.COMPONENT_LOGS_CONFIG_MAP_TOPIC_NAME;
import static com.aws.greengrass.logmanager.LogManagerService.COMPONENT_LOGS_CONFIG_TOPIC_NAME;
import static com.aws.greengrass.logmanager.LogManagerService.COMPONENT_NAME_CONFIG_TOPIC_NAME;
import static com.aws.greengrass.logmanager.LogManagerService.DELETE_LOG_FILES_AFTER_UPLOAD_CONFIG_TOPIC_NAME;
import static com.aws.greengrass.logmanager.LogManagerService.DISK_SPACE_LIMIT_CONFIG_TOPIC_NAME;
import static com.aws.greengrass.logmanager.LogManagerService.DISK_SPACE_LIMIT_UNIT_CONFIG_TOPIC_NAME;
import static com.aws.greengrass.logmanager.LogManagerService.FILE_DIRECTORY_PATH_CONFIG_TOPIC_NAME;
import static com.aws.greengrass.logmanager.LogManagerService.FILE_REGEX_CONFIG_TOPIC_NAME;
import static com.aws.greengrass.logmanager.LogManagerService.LOGS_UPLOADER_CONFIGURATION_TOPIC;
import static com.aws.greengrass.logmanager.LogManagerService.LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC;
import static com.aws.greengrass.logmanager.LogManagerService.MIN_LOG_LEVEL_CONFIG_TOPIC_NAME;
import static com.aws.greengrass.logmanager.LogManagerService.PERSISTED_COMPONENT_CURRENT_PROCESSING_FILE_INFORMATION;
import static com.aws.greengrass.logmanager.LogManagerService.PERSISTED_COMPONENT_LAST_FILE_PROCESSED_TIMESTAMP;
import static com.aws.greengrass.logmanager.LogManagerService.PERSISTED_LAST_FILE_PROCESSED_TIMESTAMP;
import static com.aws.greengrass.logmanager.LogManagerService.SYSTEM_LOGS_COMPONENT_NAME;
import static com.aws.greengrass.logmanager.LogManagerService.SYSTEM_LOGS_CONFIG_TOPIC_NAME;
import static com.aws.greengrass.logmanager.LogManagerService.UPLOAD_TO_CW_CONFIG_TOPIC_NAME;
import static com.aws.greengrass.logmanager.model.LogFile.bytesNeeded;
import static com.aws.greengrass.logmanager.util.TestUtils.createLogFileWithSize;
import static com.aws.greengrass.logmanager.util.TestUtils.givenAStringOfSize;
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
class LogManagerServiceTest extends GGServiceTestUtil {
    @Mock
    private CloudWatchLogsUploader mockUploader;
    @Mock
    private CloudWatchAttemptLogsProcessor mockMerger;
    @Captor
    private ArgumentCaptor<ComponentLogFileInformation> componentLogsInformationCaptor;
    @Captor
    private ArgumentCaptor<Consumer<CloudWatchAttempt>> callbackCaptor;
    @Captor
    private ArgumentCaptor<Map<String, Object>> updateFromMapCaptor;
    @Captor
    private ArgumentCaptor<Number> numberObjectCaptor;

    @TempDir
    static Path directoryPath;
    private LogManagerService logsUploaderService;
    private final ExecutorService executor = Executors.newCachedThreadPool();
    private static final Instant mockInstant = Instant.EPOCH;

    @BeforeAll
    static void setupBefore() throws IOException, InterruptedException {
        LogConfig.getRootLogConfig().setLevel(Level.TRACE);
        LogConfig.getRootLogConfig().setStore(LogStore.FILE);
        LogConfig.getRootLogConfig().setStoreDirectory(directoryPath);
        LogManager.getLogConfigurations().putIfAbsent("UserComponentA",
                newLogConfigFromRootConfig(LogConfigUpdate.builder().fileName("UserComponentA.log").build()));


        for (int i = 0; i < 5; i++) {
            File file = new File(directoryPath.resolve("UserComponentA_" + i + ".log").toUri());
            assertTrue(file.createNewFile());
            assertTrue(file.setReadable(true));
            assertTrue(file.setWritable(true));

            try (OutputStream fileOutputStream = Files.newOutputStream(file.toPath())) {
                fileOutputStream.write(givenAStringOfSize(bytesNeeded).getBytes(StandardCharsets.UTF_8));
            }
            TimeUnit.SECONDS.sleep(1);
        }
        File currentFile = new File(directoryPath.resolve("UserComponentA.log").toUri());
        try (OutputStream currentFileOutputStream = Files.newOutputStream(currentFile.toPath())) {
            currentFileOutputStream.write(givenAStringOfSize(bytesNeeded).getBytes(StandardCharsets.UTF_8));
        }

        LogManager.getLogConfigurations().putIfAbsent("UserComponentB",
                newLogConfigFromRootConfig(LogConfigUpdate.builder().fileName("UserComponentB.log").build()));
        for (int i = 0; i < 5; i++) {
            File file = new File(directoryPath.resolve("UserComponentB_" + i + ".log").toUri());
            assertTrue(file.createNewFile());
            assertTrue(file.setReadable(true));
            assertTrue(file.setWritable(true));

            try (OutputStream fileOutputStream = Files.newOutputStream(file.toPath())) {
                fileOutputStream.write(givenAStringOfSize(bytesNeeded).getBytes(StandardCharsets.UTF_8));
            }
            TimeUnit.SECONDS.sleep(1);
        }
        currentFile = new File(directoryPath.resolve("UserComponentB.log").toUri());
        try (OutputStream currentFileOutputStream = Files.newOutputStream(currentFile.toPath())) {
            currentFileOutputStream.write(givenAStringOfSize(bytesNeeded).getBytes(StandardCharsets.UTF_8));
        }
        for (int i = 0; i < 5; i++) {
            File file = new File(directoryPath.resolve("greengrass_test_" + i + ".log").toUri());
            assertTrue(file.createNewFile());
            assertTrue(file.setReadable(true));
            assertTrue(file.setWritable(true));

            try (OutputStream fileOutputStream = Files.newOutputStream(file.toPath())) {
                fileOutputStream.write(givenAStringOfSize(bytesNeeded).getBytes(StandardCharsets.UTF_8));
            }
            TimeUnit.SECONDS.sleep(1);
        }
        currentFile = new File(directoryPath.resolve("greengrass.log").toUri());
        try (OutputStream currentFileOutputStream = Files.newOutputStream(currentFile.toPath())) {
            currentFileOutputStream.write(givenAStringOfSize(bytesNeeded).getBytes(StandardCharsets.UTF_8));
        }
    }

    @AfterAll
    static void cleanUpAfter() {
        LogConfig.getRootLogConfig().setLevel(Level.INFO);
        LogConfig.getRootLogConfig().setStore(LogStore.CONSOLE);
        final File folder = new File(directoryPath.toUri());
        final File[] files = folder.listFiles();
        if (files != null) {
            for (final File file : files) {
                if (file.getName().startsWith("greengrass") && !file.delete()) {
                    System.err.println("Can't remove " + file.getAbsolutePath());
                }
            }
        }
    }

    @BeforeEach
    public void setup() {
        serviceFullName = "aws.greengrass.LogManager";
        initializeMockedConfig();
        lenient().when(context.runOnPublishQueueAndWait(any())).thenAnswer((s) -> {
            ((Crashable)s.getArgument(0)).run();
            return null;
        });
    }

    private void mockDefaultPersistedState() {
        Topics allCurrentProcessingComponentTopics1 =
                Topics.of(context, PERSISTED_COMPONENT_CURRENT_PROCESSING_FILE_INFORMATION, null);
        Topics currentProcessingComponentTopics1 =
                Topics.of(context, SYSTEM_LOGS_COMPONENT_NAME, allCurrentProcessingComponentTopics1);
        Topics currentProcessingComponentTopics2 =
                Topics.of(context, "UserComponentA", allCurrentProcessingComponentTopics1);
        Topics currentProcessingComponentTopics3 =
                Topics.of(context, "UserComponentB", allCurrentProcessingComponentTopics1);

        lenient().when(config.lookupTopics(RUNTIME_STORE_NAMESPACE_TOPIC)
                .lookupTopics(PERSISTED_COMPONENT_CURRENT_PROCESSING_FILE_INFORMATION, SYSTEM_LOGS_COMPONENT_NAME))
                .thenReturn(currentProcessingComponentTopics1);
        lenient().when(config.lookupTopics(RUNTIME_STORE_NAMESPACE_TOPIC)
                .lookupTopics(PERSISTED_COMPONENT_CURRENT_PROCESSING_FILE_INFORMATION, "UserComponentA"))
                .thenReturn(currentProcessingComponentTopics2);
        lenient().when(config.lookupTopics(RUNTIME_STORE_NAMESPACE_TOPIC)
                .lookupTopics(PERSISTED_COMPONENT_CURRENT_PROCESSING_FILE_INFORMATION, "UserComponentB"))
                .thenReturn(currentProcessingComponentTopics3);

        Topics allLastFileProcessedComponentTopics =
                Topics.of(context, PERSISTED_COMPONENT_LAST_FILE_PROCESSED_TIMESTAMP, null);
        Topics lastFileProcessedComponentTopics1 =
                Topics.of(context, SYSTEM_LOGS_COMPONENT_NAME, allLastFileProcessedComponentTopics);
        Topics lastFileProcessedComponentTopics2 =
                Topics.of(context, "UserComponentA", allLastFileProcessedComponentTopics);
        Topics lastFileProcessedComponentTopics3 =
                Topics.of(context, "UserComponentB", allLastFileProcessedComponentTopics);

        lenient().when(config.lookupTopics(RUNTIME_STORE_NAMESPACE_TOPIC)
                .lookupTopics(PERSISTED_COMPONENT_LAST_FILE_PROCESSED_TIMESTAMP, SYSTEM_LOGS_COMPONENT_NAME))
                .thenReturn(lastFileProcessedComponentTopics1);
        lenient().when(config.lookupTopics(RUNTIME_STORE_NAMESPACE_TOPIC)
                .lookupTopics(PERSISTED_COMPONENT_LAST_FILE_PROCESSED_TIMESTAMP, "UserComponentA"))
                .thenReturn(lastFileProcessedComponentTopics2);
        lenient().when(config.lookupTopics(RUNTIME_STORE_NAMESPACE_TOPIC)
                .lookupTopics(PERSISTED_COMPONENT_LAST_FILE_PROCESSED_TIMESTAMP, "UserComponentB"))
                .thenReturn(lastFileProcessedComponentTopics3);
    }

    @AfterEach
    public void cleanup() throws InterruptedException {
        logsUploaderService.componentCurrentProcessingLogFile.clear();
        logsUploaderService.lastComponentUploadedLogFileInstantMap.clear();
        logsUploaderService.shutdown();
        executor.shutdownNow();
    }

    @Test
    void GIVEN_system_log_files_to_be_uploaded_WHEN_merger_merges_THEN_we_get_all_log_files()
            throws InterruptedException, UnsupportedInputTypeException {
        mockDefaultPersistedState();
        logsUploaderService.ACTIVE_LOG_FILE_FEATURE_ENABLED_FLAG.set(true);
        Topic periodicUpdateIntervalMsTopic = Topic.of(context, LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC, "1");
        when(config.lookup(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC))
                .thenReturn(periodicUpdateIntervalMsTopic);
        when(mockMerger.processLogFiles(componentLogsInformationCaptor.capture())).thenReturn(new CloudWatchAttempt());

        Topics configTopics = Topics.of(context, CONFIGURATION_CONFIG_KEY, null);
        when(config.lookupTopics(CONFIGURATION_CONFIG_KEY)).thenReturn(configTopics);
        Topics logsUploaderConfigTopics = Topics.of(context, LOGS_UPLOADER_CONFIGURATION_TOPIC, null);

        Topics componentConfigTopics = logsUploaderConfigTopics.createInteriorChild(COMPONENT_LOGS_CONFIG_MAP_TOPIC_NAME);
        Topics componentATopic = componentConfigTopics.createInteriorChild("UserComponentA");
        componentATopic.createLeafChild(FILE_REGEX_CONFIG_TOPIC_NAME).withValue("^log.txt\\\\w*");
        componentATopic.createLeafChild(FILE_DIRECTORY_PATH_CONFIG_TOPIC_NAME).withValue(directoryPath.toAbsolutePath().toString());
        componentATopic.createLeafChild(MIN_LOG_LEVEL_CONFIG_TOPIC_NAME).withValue("DEBUG");
        componentATopic.createLeafChild(DISK_SPACE_LIMIT_CONFIG_TOPIC_NAME).withValue("10");
        componentATopic.createLeafChild(DISK_SPACE_LIMIT_UNIT_CONFIG_TOPIC_NAME).withValue("GB");
        componentATopic.createLeafChild(DELETE_LOG_FILES_AFTER_UPLOAD_CONFIG_TOPIC_NAME).withValue("false");

        Topics systemConfigTopics = logsUploaderConfigTopics.createInteriorChild(SYSTEM_LOGS_CONFIG_TOPIC_NAME);
        systemConfigTopics.createLeafChild(UPLOAD_TO_CW_CONFIG_TOPIC_NAME).withValue("true");
        systemConfigTopics.createLeafChild(MIN_LOG_LEVEL_CONFIG_TOPIC_NAME).withValue("INFO");
        systemConfigTopics.createLeafChild(DISK_SPACE_LIMIT_CONFIG_TOPIC_NAME).withValue("25");
        systemConfigTopics.createLeafChild(DISK_SPACE_LIMIT_UNIT_CONFIG_TOPIC_NAME).withValue("MB");
        when(config.lookupTopics(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC))
                .thenReturn(logsUploaderConfigTopics);
        doNothing().when(mockUploader).registerAttemptStatus(anyString(), callbackCaptor.capture());

        logsUploaderService = new LogManagerService(config, mockUploader, mockMerger, executor);
        startServiceOnAnotherThread();

        TimeUnit.SECONDS.sleep(5);

        assertThat(logsUploaderService.componentLogConfigurations.entrySet(), IsNot.not(IsEmptyCollection.empty()));
        assertEquals(2, logsUploaderService.componentLogConfigurations.size());
        assertTrue(logsUploaderService.componentLogConfigurations.containsKey("UserComponentA"));
        assertTrue(logsUploaderService.componentLogConfigurations.containsKey("System"));

        assertNotNull(componentLogsInformationCaptor.getValue());
        ComponentLogFileInformation componentLogFileInformation = componentLogsInformationCaptor.getValue();
        assertNotNull(componentLogFileInformation);
        assertEquals("System", componentLogFileInformation.getName());
        assertEquals(ComponentType.GreengrassSystemComponent, componentLogFileInformation.getComponentType());
        assertEquals(Level.INFO, componentLogFileInformation.getDesiredLogLevel());
        assertNotNull(componentLogFileInformation.getLogFileInformationList());
        assertThat(componentLogFileInformation.getLogFileInformationList(), IsNot.not(IsEmptyCollection.empty()));
        assertTrue(componentLogFileInformation.getLogFileInformationList().size() >= 6);
        verify(mockUploader, times(1)).upload(any(CloudWatchAttempt.class), anyInt());
        logsUploaderService.ACTIVE_LOG_FILE_FEATURE_ENABLED_FLAG.set(false);
    }

    @Test
    void GIVEN_user_component_log_files_to_be_uploaded_with_required_config_as_array_WHEN_merger_merges_THEN_we_get_all_log_files()
            throws InterruptedException, UnsupportedInputTypeException {
        mockDefaultPersistedState();
        logsUploaderService.ACTIVE_LOG_FILE_FEATURE_ENABLED_FLAG.set(true);
        Topic periodicUpdateIntervalMsTopic = Topic.of(context, LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC, "1");
        when(config.lookup(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC))
                .thenReturn(periodicUpdateIntervalMsTopic);
        when(mockMerger.processLogFiles(componentLogsInformationCaptor.capture())).thenReturn(new CloudWatchAttempt());

        Topics configTopics = Topics.of(context, CONFIGURATION_CONFIG_KEY, null);
        when(config.lookupTopics(CONFIGURATION_CONFIG_KEY)).thenReturn(configTopics);
        Topics logsUploaderConfigTopics = Topics.of(context, LOGS_UPLOADER_CONFIGURATION_TOPIC, null);

        List<Map<String, Object>> componentLogsInformationList = new ArrayList<>();
        Map<String, Object> componentAConfig = new HashMap<>();
        componentAConfig.put(COMPONENT_NAME_CONFIG_TOPIC_NAME, "UserComponentA");
        componentAConfig.put(MIN_LOG_LEVEL_CONFIG_TOPIC_NAME, "DEBUG");
        componentAConfig.put(DISK_SPACE_LIMIT_CONFIG_TOPIC_NAME, "10");
        componentAConfig.put(DISK_SPACE_LIMIT_UNIT_CONFIG_TOPIC_NAME, "GB");
        componentAConfig.put(DELETE_LOG_FILES_AFTER_UPLOAD_CONFIG_TOPIC_NAME, "false");
        componentLogsInformationList.add(componentAConfig);
        logsUploaderConfigTopics.createLeafChild(COMPONENT_LOGS_CONFIG_TOPIC_NAME).withValueChecked(componentLogsInformationList);

        Topics systemConfigTopics = logsUploaderConfigTopics.createInteriorChild(SYSTEM_LOGS_CONFIG_TOPIC_NAME);
        systemConfigTopics.createLeafChild(UPLOAD_TO_CW_CONFIG_TOPIC_NAME).withValue("false");
        systemConfigTopics.createLeafChild(MIN_LOG_LEVEL_CONFIG_TOPIC_NAME).withValue("INFO");
        systemConfigTopics.createLeafChild(DISK_SPACE_LIMIT_CONFIG_TOPIC_NAME).withValue("25");
        systemConfigTopics.createLeafChild(DISK_SPACE_LIMIT_UNIT_CONFIG_TOPIC_NAME).withValue("MB");
        when(config.lookupTopics(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC))
                .thenReturn(logsUploaderConfigTopics);

        doNothing().when(mockUploader).registerAttemptStatus(anyString(), callbackCaptor.capture());

        logsUploaderService = new LogManagerService(config, mockUploader, mockMerger, executor);
        startServiceOnAnotherThread();

        TimeUnit.SECONDS.sleep(5);

        assertThat(logsUploaderService.componentLogConfigurations.entrySet(), IsNot.not(IsEmptyCollection.empty()));
        assertEquals(1, logsUploaderService.componentLogConfigurations.size());
        assertTrue(logsUploaderService.componentLogConfigurations.containsKey("UserComponentA"));

        assertNotNull(componentLogsInformationCaptor.getValue());
        ComponentLogFileInformation componentLogFileInformation = componentLogsInformationCaptor.getValue();
        assertNotNull(componentLogFileInformation);
        assertEquals(ComponentType.UserComponent, componentLogFileInformation.getComponentType());
        assertEquals(Level.DEBUG, componentLogFileInformation.getDesiredLogLevel());
        assertNotNull(componentLogFileInformation.getLogFileInformationList());
        assertThat(componentLogFileInformation.getLogFileInformationList(), IsNot.not(IsEmptyCollection.empty()));
        assertTrue(componentLogFileInformation.getLogFileInformationList().size() >= 6);
        verify(mockUploader, times(1)).upload(any(CloudWatchAttempt.class), anyInt());
        logsUploaderService.ACTIVE_LOG_FILE_FEATURE_ENABLED_FLAG.set(false);
    }

    @Test
    void GIVEN_user_component_log_files_to_be_uploaded_with_required_config_WHEN_merger_merges_THEN_we_get_all_log_files()
            throws InterruptedException, UnsupportedInputTypeException {
        mockDefaultPersistedState();
        logsUploaderService.ACTIVE_LOG_FILE_FEATURE_ENABLED_FLAG.set(true);
        Topic periodicUpdateIntervalMsTopic = Topic.of(context, LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC, "1");
        when(config.lookup(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC))
                .thenReturn(periodicUpdateIntervalMsTopic);
        when(mockMerger.processLogFiles(componentLogsInformationCaptor.capture())).thenReturn(new CloudWatchAttempt());

        Topics configTopics = Topics.of(context, CONFIGURATION_CONFIG_KEY, null);
        when(config.lookupTopics(CONFIGURATION_CONFIG_KEY)).thenReturn(configTopics);
        Topics logsUploaderConfigTopics = Topics.of(context, LOGS_UPLOADER_CONFIGURATION_TOPIC, null);

        Topics componentConfigTopics = logsUploaderConfigTopics.createInteriorChild(COMPONENT_LOGS_CONFIG_MAP_TOPIC_NAME);
        Topics componentATopic = componentConfigTopics.createInteriorChild("UserComponentA");
        componentATopic.createLeafChild(FILE_DIRECTORY_PATH_CONFIG_TOPIC_NAME).withValue(directoryPath.toAbsolutePath().toString());
        componentATopic.createLeafChild(MIN_LOG_LEVEL_CONFIG_TOPIC_NAME).withValue("DEBUG");
        componentATopic.createLeafChild(DISK_SPACE_LIMIT_CONFIG_TOPIC_NAME).withValue("10");
        componentATopic.createLeafChild(DISK_SPACE_LIMIT_UNIT_CONFIG_TOPIC_NAME).withValue("GB");
        componentATopic.createLeafChild(DELETE_LOG_FILES_AFTER_UPLOAD_CONFIG_TOPIC_NAME).withValue("false");

        Topics systemConfigTopics = logsUploaderConfigTopics.createInteriorChild(SYSTEM_LOGS_CONFIG_TOPIC_NAME);
        systemConfigTopics.createLeafChild(UPLOAD_TO_CW_CONFIG_TOPIC_NAME).withValue("false");
        systemConfigTopics.createLeafChild(MIN_LOG_LEVEL_CONFIG_TOPIC_NAME).withValue("INFO");
        systemConfigTopics.createLeafChild(DISK_SPACE_LIMIT_CONFIG_TOPIC_NAME).withValue("25");
        systemConfigTopics.createLeafChild(DISK_SPACE_LIMIT_UNIT_CONFIG_TOPIC_NAME).withValue("MB");
        when(config.lookupTopics(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC))
                .thenReturn(logsUploaderConfigTopics);

        doNothing().when(mockUploader).registerAttemptStatus(anyString(), callbackCaptor.capture());

        logsUploaderService = new LogManagerService(config, mockUploader, mockMerger, executor);
        startServiceOnAnotherThread();

        TimeUnit.SECONDS.sleep(5);

        assertThat(logsUploaderService.componentLogConfigurations.entrySet(), IsNot.not(IsEmptyCollection.empty()));
        assertEquals(1, logsUploaderService.componentLogConfigurations.size());
        assertTrue(logsUploaderService.componentLogConfigurations.containsKey("UserComponentA"));

        assertNotNull(componentLogsInformationCaptor.getValue());
        ComponentLogFileInformation componentLogFileInformation = componentLogsInformationCaptor.getValue();
        assertNotNull(componentLogFileInformation);
        assertEquals(ComponentType.UserComponent, componentLogFileInformation.getComponentType());
        assertEquals(Level.DEBUG, componentLogFileInformation.getDesiredLogLevel());
        assertNotNull(componentLogFileInformation.getLogFileInformationList());
        assertThat(componentLogFileInformation.getLogFileInformationList(), IsNot.not(IsEmptyCollection.empty()));
        assertTrue(componentLogFileInformation.getLogFileInformationList().size() >= 6);
        verify(mockUploader, times(1)).upload(any(CloudWatchAttempt.class), anyInt());
        logsUploaderService.ACTIVE_LOG_FILE_FEATURE_ENABLED_FLAG.set(false);
    }

    @Test
    void GIVEN_user_component_log_files_to_be_uploaded_with_all_config_WHEN_merger_merges_THEN_we_get_all_log_files()
            throws InterruptedException, UnsupportedInputTypeException {
        mockDefaultPersistedState();
        logsUploaderService.ACTIVE_LOG_FILE_FEATURE_ENABLED_FLAG.set(true);
        Topic periodicUpdateIntervalMsTopic = Topic.of(context, LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC, "1");
        when(config.lookup(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC))
                .thenReturn(periodicUpdateIntervalMsTopic);
        when(mockMerger.processLogFiles(componentLogsInformationCaptor.capture())).thenReturn(new CloudWatchAttempt());

        Topics configTopics = Topics.of(context, CONFIGURATION_CONFIG_KEY, null);
        when(config.lookupTopics(CONFIGURATION_CONFIG_KEY)).thenReturn(configTopics);
        Topics logsUploaderConfigTopics = Topics.of(context, LOGS_UPLOADER_CONFIGURATION_TOPIC, null);

        Topics componentConfigTopics = logsUploaderConfigTopics.createInteriorChild(COMPONENT_LOGS_CONFIG_MAP_TOPIC_NAME);
        Topics componentATopic = componentConfigTopics.createInteriorChild("UserComponentA");
        componentATopic.createLeafChild(FILE_REGEX_CONFIG_TOPIC_NAME).withValue("^UserComponentA\\w*\\.log");
        componentATopic.createLeafChild(FILE_DIRECTORY_PATH_CONFIG_TOPIC_NAME).withValue(directoryPath.toAbsolutePath().toString());
        componentATopic.createLeafChild(MIN_LOG_LEVEL_CONFIG_TOPIC_NAME).withValue("DEBUG");
        componentATopic.createLeafChild(DISK_SPACE_LIMIT_CONFIG_TOPIC_NAME).withValue("10");
        componentATopic.createLeafChild(DISK_SPACE_LIMIT_UNIT_CONFIG_TOPIC_NAME).withValue("GB");
        componentATopic.createLeafChild(DELETE_LOG_FILES_AFTER_UPLOAD_CONFIG_TOPIC_NAME).withValue("false");

        Topics systemConfigTopics = logsUploaderConfigTopics.createInteriorChild(SYSTEM_LOGS_CONFIG_TOPIC_NAME);
        systemConfigTopics.createLeafChild(UPLOAD_TO_CW_CONFIG_TOPIC_NAME).withValue("false");
        systemConfigTopics.createLeafChild(MIN_LOG_LEVEL_CONFIG_TOPIC_NAME).withValue("INFO");
        systemConfigTopics.createLeafChild(DISK_SPACE_LIMIT_CONFIG_TOPIC_NAME).withValue("25");
        systemConfigTopics.createLeafChild(DISK_SPACE_LIMIT_UNIT_CONFIG_TOPIC_NAME).withValue("MB");
        when(config.lookupTopics(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC))
                .thenReturn(logsUploaderConfigTopics);

        doNothing().when(mockUploader).registerAttemptStatus(anyString(), callbackCaptor.capture());

        logsUploaderService = new LogManagerService(config, mockUploader, mockMerger, executor);
        startServiceOnAnotherThread();

        TimeUnit.SECONDS.sleep(5);

        assertThat(logsUploaderService.componentLogConfigurations.entrySet(), IsNot.not(IsEmptyCollection.empty()));
        assertEquals(1, logsUploaderService.componentLogConfigurations.size());
        assertTrue(logsUploaderService.componentLogConfigurations.containsKey("UserComponentA"));

        assertNotNull(componentLogsInformationCaptor.getValue());
        ComponentLogFileInformation componentLogFileInformation = componentLogsInformationCaptor.getValue();
        assertNotNull(componentLogFileInformation);
        assertEquals(ComponentType.UserComponent, componentLogFileInformation.getComponentType());
        assertEquals(Level.DEBUG, componentLogFileInformation.getDesiredLogLevel());
        assertNotNull(componentLogFileInformation.getLogFileInformationList());
        assertThat(componentLogFileInformation.getLogFileInformationList(), IsNot.not(IsEmptyCollection.empty()));
        assertTrue(componentLogFileInformation.getLogFileInformationList().size() >= 6);
        verify(mockUploader, times(1)).upload(any(CloudWatchAttempt.class), anyInt());
        logsUploaderService.ACTIVE_LOG_FILE_FEATURE_ENABLED_FLAG.set(false);
    }

    @Test
    void GIVEN_multiple_user_components_log_files_to_be_uploaded_with_all_config_WHEN_merger_merges_THEN_we_get_all_log_files()
            throws InterruptedException, UnsupportedInputTypeException {
        mockDefaultPersistedState();
        logsUploaderService.ACTIVE_LOG_FILE_FEATURE_ENABLED_FLAG.set(true);
        Topic periodicUpdateIntervalMsTopic = Topic.of(context, LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC, "1");
        when(config.lookup(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC))
                .thenReturn(periodicUpdateIntervalMsTopic);
        when(mockMerger.processLogFiles(componentLogsInformationCaptor.capture())).thenReturn(new CloudWatchAttempt());

        Topics configTopics = Topics.of(context, CONFIGURATION_CONFIG_KEY, null);
        when(config.lookupTopics(CONFIGURATION_CONFIG_KEY)).thenReturn(configTopics);
        Topics logsUploaderConfigTopics = Topics.of(context, LOGS_UPLOADER_CONFIGURATION_TOPIC, null);

        Topics componentConfigTopics = logsUploaderConfigTopics.createInteriorChild(COMPONENT_LOGS_CONFIG_MAP_TOPIC_NAME);
        Topics componentATopic = componentConfigTopics.createInteriorChild("UserComponentA");
        componentATopic.createLeafChild(FILE_REGEX_CONFIG_TOPIC_NAME).withValue("^log.txt\\\\w*");
        componentATopic.createLeafChild(FILE_DIRECTORY_PATH_CONFIG_TOPIC_NAME).withValue(directoryPath.toAbsolutePath().toString());
        componentATopic.createLeafChild(MIN_LOG_LEVEL_CONFIG_TOPIC_NAME).withValue("DEBUG");
        componentATopic.createLeafChild(DISK_SPACE_LIMIT_CONFIG_TOPIC_NAME).withValue("10");
        componentATopic.createLeafChild(DISK_SPACE_LIMIT_UNIT_CONFIG_TOPIC_NAME).withValue("GB");
        componentATopic.createLeafChild(DELETE_LOG_FILES_AFTER_UPLOAD_CONFIG_TOPIC_NAME).withValue("false");

        Topics componentBTopic = componentConfigTopics.createInteriorChild("UserComponentB");
        componentBTopic.createLeafChild(FILE_REGEX_CONFIG_TOPIC_NAME).withValue("^UserComponentB\\w*\\.log");
        componentBTopic.createLeafChild(FILE_DIRECTORY_PATH_CONFIG_TOPIC_NAME).withValue(directoryPath.toAbsolutePath().toString());
        componentBTopic.createLeafChild(MIN_LOG_LEVEL_CONFIG_TOPIC_NAME).withValue("DEBUG");
        componentBTopic.createLeafChild(DISK_SPACE_LIMIT_CONFIG_TOPIC_NAME).withValue("10");
        componentBTopic.createLeafChild(DISK_SPACE_LIMIT_UNIT_CONFIG_TOPIC_NAME).withValue("GB");
        componentBTopic.createLeafChild(DELETE_LOG_FILES_AFTER_UPLOAD_CONFIG_TOPIC_NAME).withValue("false");

        Topics systemConfigTopics = logsUploaderConfigTopics.createInteriorChild(SYSTEM_LOGS_CONFIG_TOPIC_NAME);
        systemConfigTopics.createLeafChild(UPLOAD_TO_CW_CONFIG_TOPIC_NAME).withValue("false");
        systemConfigTopics.createLeafChild(MIN_LOG_LEVEL_CONFIG_TOPIC_NAME).withValue("INFO");
        systemConfigTopics.createLeafChild(DISK_SPACE_LIMIT_CONFIG_TOPIC_NAME).withValue("25");
        systemConfigTopics.createLeafChild(DISK_SPACE_LIMIT_UNIT_CONFIG_TOPIC_NAME).withValue("MB");
        when(config.lookupTopics(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC))
                .thenReturn(logsUploaderConfigTopics);

        doNothing().when(mockUploader).registerAttemptStatus(anyString(), callbackCaptor.capture());

        logsUploaderService = new LogManagerService(config, mockUploader, mockMerger, executor);
        startServiceOnAnotherThread();

        TimeUnit.SECONDS.sleep(5);

        assertThat(logsUploaderService.componentLogConfigurations.entrySet(), IsNot.not(IsEmptyCollection.empty()));
        assertEquals(2, logsUploaderService.componentLogConfigurations.size());
        assertTrue(logsUploaderService.componentLogConfigurations.containsKey("UserComponentA"));
        assertTrue(logsUploaderService.componentLogConfigurations.containsKey("UserComponentB"));
        assertNotNull(componentLogsInformationCaptor.getValue());
        ComponentLogFileInformation componentLogFileInformation = componentLogsInformationCaptor.getValue();
        assertNotNull(componentLogFileInformation);
        assertEquals(ComponentType.UserComponent, componentLogFileInformation.getComponentType());
        assertEquals(Level.DEBUG, componentLogFileInformation.getDesiredLogLevel());
        assertNotNull(componentLogFileInformation.getLogFileInformationList());
        assertThat(componentLogFileInformation.getLogFileInformationList(), IsNot.not(IsEmptyCollection.empty()));
        assertTrue(componentLogFileInformation.getLogFileInformationList().size() >= 6);
        verify(mockUploader, times(1)).upload(any(CloudWatchAttempt.class), anyInt());
        logsUploaderService.ACTIVE_LOG_FILE_FEATURE_ENABLED_FLAG.set(false);
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
    void GIVEN_null_config_WHEN_config_is_processed_THEN_no_component_config_is_added(
            ExtensionContext context1) {
        ignoreExceptionOfType(context1, MismatchedInputException.class);
        logsUploaderService.ACTIVE_LOG_FILE_FEATURE_ENABLED_FLAG.set(true);
        Topics configTopics = Topics.of(context, CONFIGURATION_CONFIG_KEY, null);
        when(config.lookupTopics(CONFIGURATION_CONFIG_KEY)).thenReturn(configTopics);
        Topic periodicUpdateIntervalMsTopic = Topic.of(context, LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC, "3");
        when(config.lookup(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC))
                .thenReturn(periodicUpdateIntervalMsTopic);
        Topics logsUploaderConfigTopic = Topics.of(context, LOGS_UPLOADER_CONFIGURATION_TOPIC, null);
        when(config.lookupTopics(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC))
                .thenReturn(logsUploaderConfigTopic);


        logsUploaderService = new LogManagerService(config, mockUploader, mockMerger, executor);
        startServiceOnAnotherThread();
        assertThat(logsUploaderService.componentCurrentProcessingLogFile.values(), IsEmptyCollection.empty());
        logsUploaderService.ACTIVE_LOG_FILE_FEATURE_ENABLED_FLAG.set(false);
    }

    @Test
    void GIVEN_cloud_watch_attempt_handler_WHEN_attempt_completes_THEN_successfully_updates_states_for_each_component()
            throws URISyntaxException, IOException, InvalidLogGroupException {
        mockDefaultPersistedState();
        logsUploaderService.ACTIVE_LOG_FILE_FEATURE_ENABLED_FLAG.set(true);
        Topic periodicUpdateIntervalMsTopic = Topic.of(context, LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC, "1000");
        when(config.lookup(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC))
                .thenReturn(periodicUpdateIntervalMsTopic);
        Topics configTopics = Topics.of(context, CONFIGURATION_CONFIG_KEY, null);
        when(config.lookupTopics(CONFIGURATION_CONFIG_KEY)).thenReturn(configTopics);
        Topics logsUploaderConfigTopics = Topics.of(context, LOGS_UPLOADER_CONFIGURATION_TOPIC, null);
        Topics systemConfigTopics = logsUploaderConfigTopics.createInteriorChild(SYSTEM_LOGS_CONFIG_TOPIC_NAME);
        systemConfigTopics.createLeafChild(UPLOAD_TO_CW_CONFIG_TOPIC_NAME).withValue("false");
        systemConfigTopics.createLeafChild(MIN_LOG_LEVEL_CONFIG_TOPIC_NAME).withValue("INFO");
        systemConfigTopics.createLeafChild(DISK_SPACE_LIMIT_CONFIG_TOPIC_NAME).withValue("25");
        systemConfigTopics.createLeafChild(DISK_SPACE_LIMIT_UNIT_CONFIG_TOPIC_NAME).withValue("MB");
        when(config.lookupTopics(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC))
                .thenReturn(logsUploaderConfigTopics);

        Topics componentTopics2 = mock(Topics.class);
        Topic lastFileProcessedTimeStampTopics = mock(Topic.class);
        when(componentTopics2.createLeafChild(any())).thenReturn(lastFileProcessedTimeStampTopics);
        when(lastFileProcessedTimeStampTopics.withValue(numberObjectCaptor.capture()))
                .thenReturn(lastFileProcessedTimeStampTopics);

        Topics componentTopics3 = mock(Topics.class);
        doNothing().when(componentTopics3).updateFromMap(updateFromMapCaptor.capture(), any());
        Topics runtimeConfig = mock(Topics.class);
        when(config.lookupTopics(RUNTIME_STORE_NAMESPACE_TOPIC))
                .thenReturn(runtimeConfig);
        when(runtimeConfig.lookupTopics(PERSISTED_COMPONENT_CURRENT_PROCESSING_FILE_INFORMATION, "TestComponent2"))
                .thenReturn(componentTopics3);
        when(runtimeConfig.lookupTopics(PERSISTED_COMPONENT_LAST_FILE_PROCESSED_TIMESTAMP, "TestComponent"))
                .thenReturn(componentTopics2);

        CloudWatchAttempt attempt = new CloudWatchAttempt();
        Map<String, CloudWatchAttemptLogInformation> logStreamsToLogInformationMap = new HashMap<>();

        LogFile lastProcessedFile = createLogFileWithSize(directoryPath.resolve("testlogs2.log").toUri(), 2943);
        Pattern pattern1 = Pattern.compile("^testlogs2.log\\w*$");
        // Create another file intentionally, so that the lastProcessedFile will be processed.
        createLogFileWithSize(directoryPath.resolve("testlogs2.log_active").toUri(), 2943);
        LogFileGroup LastProcessedLogFileGroup = LogFileGroup.create(pattern1, lastProcessedFile.getParentFile().toURI(), mockInstant);

        LogFile processingFile = createLogFileWithSize(directoryPath.resolve("testlogs1.log").toUri(), 1061);
        Pattern pattern2 = Pattern.compile("^testlogs1.log$");
        LogFileGroup processingLogFileGroup = LogFileGroup.create(pattern2, processingFile.getParentFile().toURI(), mockInstant);

        Map<String, CloudWatchAttemptLogFileInformation> LastProcessedAttemptLogFileInformationMap = new HashMap<>();
        LastProcessedAttemptLogFileInformationMap.put(lastProcessedFile.hashString(),
                CloudWatchAttemptLogFileInformation.builder()
                .startPosition(0)
                .bytesRead(2943)
                .lastModifiedTime(lastProcessedFile.lastModified())
                .fileHash(lastProcessedFile.hashString())
                .build());
        Map<String, CloudWatchAttemptLogFileInformation> processingAttemptLogFileInformationMap2 = new HashMap<>();
        processingAttemptLogFileInformationMap2.put(processingFile.hashString(), CloudWatchAttemptLogFileInformation.builder()
                .startPosition(0)
                .bytesRead(1061)
                .lastModifiedTime(processingFile.lastModified())
                .fileHash(processingFile.hashString())
                .build());

        CloudWatchAttemptLogInformation attemptLogInformation1 = CloudWatchAttemptLogInformation.builder()
                .componentName("TestComponent")
                .attemptLogFileInformationMap(LastProcessedAttemptLogFileInformationMap)
                .logFileGroup(LastProcessedLogFileGroup)
                .build();
        CloudWatchAttemptLogInformation attemptLogInformation2 = CloudWatchAttemptLogInformation.builder()
                .componentName("TestComponent2")
                .attemptLogFileInformationMap(processingAttemptLogFileInformationMap2)
                .logFileGroup(processingLogFileGroup)
                .build();
        logStreamsToLogInformationMap.put("testStream", attemptLogInformation1);
        logStreamsToLogInformationMap.put("testStream2", attemptLogInformation2);
        attempt.setLogStreamsToLogEventsMap(logStreamsToLogInformationMap);
        attempt.setLogStreamUploadedSet(new HashSet<>(Arrays.asList("testStream", "testStream2")));
        doNothing().when(mockUploader).registerAttemptStatus(anyString(), callbackCaptor.capture());

        logsUploaderService = new LogManagerService(config, mockUploader, mockMerger, executor);
        startServiceOnAnotherThread();

        callbackCaptor.getValue().accept(attempt);

        assertThat(updateFromMapCaptor.getAllValues(), IsNot.not(IsEmptyCollection.empty()));
        assertThat(numberObjectCaptor.getAllValues(), IsNot.not(IsEmptyCollection.empty()));
        List<Number> completedComponentLastProcessedFileInformation = numberObjectCaptor.getAllValues();
        List<Map<String, Object>> partiallyReadComponentLogFileInformation = updateFromMapCaptor.getAllValues();
        assertEquals(1, completedComponentLastProcessedFileInformation.size());
        assertEquals(1, partiallyReadComponentLogFileInformation.size());
        assertEquals(lastProcessedFile.lastModified(), Coerce.toLong(completedComponentLastProcessedFileInformation.get(0)));
        LogManagerService.CurrentProcessingFileInformation currentProcessingFileInformation =
                LogManagerService.CurrentProcessingFileInformation
                        .convertFromMapOfObjects(partiallyReadComponentLogFileInformation.get(0));
        assertEquals(processingFile.hashString(), currentProcessingFileInformation.getFileHash());
        assertEquals(1061, currentProcessingFileInformation.getStartPosition());
        assertEquals(processingFile.lastModified(), currentProcessingFileInformation.getLastModifiedTime());

        assertNotNull(logsUploaderService.lastComponentUploadedLogFileInstantMap);
        assertThat(logsUploaderService.lastComponentUploadedLogFileInstantMap.entrySet(), IsNot.not(IsEmptyCollection.empty()));
        assertTrue(logsUploaderService.lastComponentUploadedLogFileInstantMap.containsKey("TestComponent"));
        assertEquals(Instant.ofEpochMilli(lastProcessedFile.lastModified()), logsUploaderService.lastComponentUploadedLogFileInstantMap.get("TestComponent"));
        assertNotNull(logsUploaderService.componentCurrentProcessingLogFile);
        assertThat(logsUploaderService.componentCurrentProcessingLogFile.entrySet(), IsNot.not(IsEmptyCollection.empty()));
        assertTrue(logsUploaderService.componentCurrentProcessingLogFile.containsKey("TestComponent2"));
        assertEquals(processingFile.hashString(), logsUploaderService.componentCurrentProcessingLogFile.get(
                "TestComponent2").getFileHash());
        assertEquals(1061, logsUploaderService.componentCurrentProcessingLogFile.get("TestComponent2").getStartPosition());
        logsUploaderService.ACTIVE_LOG_FILE_FEATURE_ENABLED_FLAG.set(false);
    }

    @Test
    void GIVEN_some_system_files_uploaded_and_another_partially_uploaded_WHEN_merger_merges_THEN_sets_the_start_position_correctly()
            throws InterruptedException {
        mockDefaultPersistedState();
        logsUploaderService.ACTIVE_LOG_FILE_FEATURE_ENABLED_FLAG.set(true);
        Topic periodicUpdateIntervalMsTopic = Topic.of(context, LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC, "3");
        when(config.lookup(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC))
                .thenReturn(periodicUpdateIntervalMsTopic);
        when(mockMerger.processLogFiles(componentLogsInformationCaptor.capture())).thenReturn(new CloudWatchAttempt());

        Topics configTopics = Topics.of(context, CONFIGURATION_CONFIG_KEY, null);
        when(config.lookupTopics(CONFIGURATION_CONFIG_KEY)).thenReturn(configTopics);
        Topics logsUploaderConfigTopics = Topics.of(context, LOGS_UPLOADER_CONFIGURATION_TOPIC, null);

        Topics systemConfigTopics = logsUploaderConfigTopics.createInteriorChild(SYSTEM_LOGS_CONFIG_TOPIC_NAME);
        systemConfigTopics.createLeafChild(UPLOAD_TO_CW_CONFIG_TOPIC_NAME).withValue("true");
        systemConfigTopics.createLeafChild(MIN_LOG_LEVEL_CONFIG_TOPIC_NAME).withValue("INFO");
        systemConfigTopics.createLeafChild(DISK_SPACE_LIMIT_CONFIG_TOPIC_NAME).withValue("25");
        systemConfigTopics.createLeafChild(DISK_SPACE_LIMIT_UNIT_CONFIG_TOPIC_NAME).withValue("MB");
        when(config.lookupTopics(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC))
                .thenReturn(logsUploaderConfigTopics);

        logsUploaderService = new LogManagerService(config, mockUploader, mockMerger, executor);
        // These two files are existed, and the active file is greengrass.log
        LogFile file = new LogFile(directoryPath.resolve("greengrass_test_2.log").toUri());
        LogFile currentProcessingFile = new LogFile(directoryPath.resolve("greengrass_test_3.log").toUri());
        logsUploaderService.lastComponentUploadedLogFileInstantMap.put(SYSTEM_LOGS_COMPONENT_NAME,
                Instant.ofEpochMilli(file.lastModified()));
        logsUploaderService.componentCurrentProcessingLogFile.put(SYSTEM_LOGS_COMPONENT_NAME,
                LogManagerService.CurrentProcessingFileInformation.builder()
                        .fileHash(currentProcessingFile.hashString())
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
            if (logFileInformation.getLogFile().getAbsolutePath().equals(currentProcessingFile.getAbsolutePath())) {
                assertEquals(2, logFileInformation.getStartPosition());
            } else {
                assertEquals(0, logFileInformation.getStartPosition());
            }
        });
        verify(mockUploader, times(1)).upload(any(CloudWatchAttempt.class), anyInt());
        logsUploaderService.ACTIVE_LOG_FILE_FEATURE_ENABLED_FLAG.set(false);
    }

    @Test
    void GIVEN_user_component_with_space_management_WHEN_log_file_size_exceeds_limit_THEN_deletes_excess_log_files()
            throws InterruptedException, IOException, UnsupportedInputTypeException {
        mockDefaultPersistedState();
        logsUploaderService.ACTIVE_LOG_FILE_FEATURE_ENABLED_FLAG.set(true);
        Topic periodicUpdateIntervalMsTopic = Topic.of(context, LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC, "3");
        when(config.lookup(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC))
                .thenReturn(periodicUpdateIntervalMsTopic);

        Topics configTopics = Topics.of(context, CONFIGURATION_CONFIG_KEY, null);
        when(config.lookupTopics(CONFIGURATION_CONFIG_KEY)).thenReturn(configTopics);
        Topics logsUploaderConfigTopics = Topics.of(context, LOGS_UPLOADER_CONFIGURATION_TOPIC, null);

        Topics componentConfigTopics = logsUploaderConfigTopics.createInteriorChild(COMPONENT_LOGS_CONFIG_MAP_TOPIC_NAME);
        Topics componentATopic = componentConfigTopics.createInteriorChild("UserComponentA");
        componentATopic.createLeafChild(FILE_REGEX_CONFIG_TOPIC_NAME).withValue("^log.txt\\w*");
        componentATopic.createLeafChild(FILE_DIRECTORY_PATH_CONFIG_TOPIC_NAME).withValue(directoryPath.toAbsolutePath().toString());
        componentATopic.createLeafChild(MIN_LOG_LEVEL_CONFIG_TOPIC_NAME).withValue("DEBUG");
        componentATopic.createLeafChild(DISK_SPACE_LIMIT_CONFIG_TOPIC_NAME).withValue("2");
        componentATopic.createLeafChild(DISK_SPACE_LIMIT_UNIT_CONFIG_TOPIC_NAME).withValue("KB");
        componentATopic.createLeafChild(DELETE_LOG_FILES_AFTER_UPLOAD_CONFIG_TOPIC_NAME).withValue("false");

        Topics systemConfigTopics = logsUploaderConfigTopics.createInteriorChild(SYSTEM_LOGS_CONFIG_TOPIC_NAME);
        systemConfigTopics.createLeafChild(UPLOAD_TO_CW_CONFIG_TOPIC_NAME).withValue("true");
        systemConfigTopics.createLeafChild(MIN_LOG_LEVEL_CONFIG_TOPIC_NAME).withValue("INFO");
        systemConfigTopics.createLeafChild(DISK_SPACE_LIMIT_CONFIG_TOPIC_NAME).withValue("25");
        systemConfigTopics.createLeafChild(DISK_SPACE_LIMIT_UNIT_CONFIG_TOPIC_NAME).withValue("KB");
        when(config.lookupTopics(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC))
                .thenReturn(logsUploaderConfigTopics);

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
        logsUploaderService.ACTIVE_LOG_FILE_FEATURE_ENABLED_FLAG.set(false);
    }

    @Test
    void GIVEN_user_component_logs_delete_file_after_upload_set_WHEN_upload_logs_THEN_deletes_uploaded_log_files(
            ExtensionContext ec)
            throws InterruptedException, IOException, UnsupportedInputTypeException, InvalidLogGroupException {
        ignoreExceptionOfType(ec, NoSuchFileException.class);
        mockDefaultPersistedState();
        logsUploaderService.ACTIVE_LOG_FILE_FEATURE_ENABLED_FLAG.set(true);
        Topic periodicUpdateIntervalMsTopic = Topic.of(context, LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC, "3");
        when(config.lookup(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC))
                .thenReturn(periodicUpdateIntervalMsTopic);

        Topics configTopics = Topics.of(context, CONFIGURATION_CONFIG_KEY, null);
        when(config.lookupTopics(CONFIGURATION_CONFIG_KEY)).thenReturn(configTopics);
        Topics logsUploaderConfigTopics = Topics.of(context, LOGS_UPLOADER_CONFIGURATION_TOPIC, null);

        Topics componentConfigTopics = logsUploaderConfigTopics.createInteriorChild(COMPONENT_LOGS_CONFIG_MAP_TOPIC_NAME);
        Topics componentATopic = componentConfigTopics.createInteriorChild("UserComponentA");
        componentATopic.createLeafChild(FILE_REGEX_CONFIG_TOPIC_NAME).withValue("^log2.txt\\w*");
        componentATopic.createLeafChild(FILE_DIRECTORY_PATH_CONFIG_TOPIC_NAME).withValue(directoryPath.toAbsolutePath().toString());
        componentATopic.createLeafChild(MIN_LOG_LEVEL_CONFIG_TOPIC_NAME).withValue("DEBUG");
        componentATopic.createLeafChild(DISK_SPACE_LIMIT_CONFIG_TOPIC_NAME).withValue("25");
        componentATopic.createLeafChild(DISK_SPACE_LIMIT_UNIT_CONFIG_TOPIC_NAME).withValue("KB");
        componentATopic.createLeafChild(DELETE_LOG_FILES_AFTER_UPLOAD_CONFIG_TOPIC_NAME).withValue("true");

        Topics systemConfigTopics = logsUploaderConfigTopics.createInteriorChild(SYSTEM_LOGS_CONFIG_TOPIC_NAME);
        systemConfigTopics.createLeafChild(UPLOAD_TO_CW_CONFIG_TOPIC_NAME).withValue("true");
        systemConfigTopics.createLeafChild(MIN_LOG_LEVEL_CONFIG_TOPIC_NAME).withValue("INFO");
        systemConfigTopics.createLeafChild(DISK_SPACE_LIMIT_CONFIG_TOPIC_NAME).withValue("25");
        systemConfigTopics.createLeafChild(DISK_SPACE_LIMIT_UNIT_CONFIG_TOPIC_NAME).withValue("KB");
        when(config.lookupTopics(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC))
                .thenReturn(logsUploaderConfigTopics);

        List<String> fileNames = new ArrayList<>();
        Instant instant = Instant.EPOCH;
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
        LogFile file1 = new LogFile(directoryPath.resolve(fileNames.get(0)).toUri());
        LogFile file2 = new LogFile(directoryPath.resolve(fileNames.get(1)).toUri());
        //TODO
        //System.out.println(file1.getName());
        //System.out.println(file2.getName());
        //System.out.println("test create logFileGroup");
        Pattern pattern = Pattern.compile("^log2.txt\\w*");
        LogFileGroup logFileGroup = LogFileGroup.create(pattern, file1.getParentFile().toURI(), instant);
        Map<String, CloudWatchAttemptLogFileInformation> attemptLogFileInformationMap1 = new HashMap<>();
        attemptLogFileInformationMap1.put(file1.hashString(), CloudWatchAttemptLogFileInformation.builder()
                .startPosition(0)
                .bytesRead(file1.length())
                .lastModifiedTime(file1.lastModified())
                .fileHash(file1.hashString())
                .build());
        attemptLogFileInformationMap1.put(file2.hashString(), CloudWatchAttemptLogFileInformation.builder()
                .startPosition(0)
                .bytesRead(file2.length())
                .lastModifiedTime(file2.lastModified())
                .fileHash(file2.hashString())
                .build());

        CloudWatchAttemptLogInformation attemptLogInformation1 = CloudWatchAttemptLogInformation.builder()
                .componentName("UserComponentA")
                .attemptLogFileInformationMap(attemptLogFileInformationMap1)
                .logFileGroup(logFileGroup)
                .build();
        logStreamsToLogInformationMap.put("testStream", attemptLogInformation1);
        attempt.setLogStreamsToLogEventsMap(logStreamsToLogInformationMap);
        attempt.setLogStreamUploadedSet(new HashSet<>(Collections.singletonList("testStream")));
        doNothing().when(mockUploader).registerAttemptStatus(anyString(), callbackCaptor.capture());
        Topics componentTopics1 = mock(Topics.class);
        Topic lastFileProcessedTimeStampTopics = mock(Topic.class);
        when(componentTopics1.createLeafChild(any())).thenReturn(lastFileProcessedTimeStampTopics);
        when(lastFileProcessedTimeStampTopics.withValue(any(Number.class)))
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
        logsUploaderService.ACTIVE_LOG_FILE_FEATURE_ENABLED_FLAG.set(false);
    }

    @Test
    void GIVEN_a_partially_uploaded_file_but_rotated_WHEN_merger_merges_THEN_sets_the_start_position_correctly()
            throws InterruptedException {
        mockDefaultPersistedState();
        logsUploaderService.ACTIVE_LOG_FILE_FEATURE_ENABLED_FLAG.set(true);
        Topic periodicUpdateIntervalMsTopic = Topic.of(context, LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC, "3");
        when(config.lookup(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC))
                .thenReturn(periodicUpdateIntervalMsTopic);
        when(mockMerger.processLogFiles(componentLogsInformationCaptor.capture())).thenReturn(new CloudWatchAttempt());

        Topics configTopics = Topics.of(context, CONFIGURATION_CONFIG_KEY, null);
        when(config.lookupTopics(CONFIGURATION_CONFIG_KEY)).thenReturn(configTopics);
        Topics logsUploaderConfigTopics = Topics.of(context, LOGS_UPLOADER_CONFIGURATION_TOPIC, null);

        Topics systemConfigTopics = logsUploaderConfigTopics.createInteriorChild(SYSTEM_LOGS_CONFIG_TOPIC_NAME);
        systemConfigTopics.createLeafChild(UPLOAD_TO_CW_CONFIG_TOPIC_NAME).withValue("true");
        systemConfigTopics.createLeafChild(MIN_LOG_LEVEL_CONFIG_TOPIC_NAME).withValue("INFO");
        systemConfigTopics.createLeafChild(DISK_SPACE_LIMIT_CONFIG_TOPIC_NAME).withValue("25");
        systemConfigTopics.createLeafChild(DISK_SPACE_LIMIT_UNIT_CONFIG_TOPIC_NAME).withValue("MB");
        when(config.lookupTopics(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC))
                .thenReturn(logsUploaderConfigTopics);

        logsUploaderService = new LogManagerService(config, mockUploader, mockMerger, executor);
        startServiceOnAnotherThread();

        LogFile file = new LogFile(directoryPath.resolve("greengrass.log_test_2").toUri());
        LogFile currentProcessingFile = new LogFile(directoryPath.resolve("greengrass.log_test-3").toUri());
        logsUploaderService.lastComponentUploadedLogFileInstantMap.put(SYSTEM_LOGS_COMPONENT_NAME,
                Instant.ofEpochMilli(file.lastModified()));
        logsUploaderService.componentCurrentProcessingLogFile.put(SYSTEM_LOGS_COMPONENT_NAME,
                LogManagerService.CurrentProcessingFileInformation.builder()
                        .fileHash(currentProcessingFile.hashString())
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
        logsUploaderService.ACTIVE_LOG_FILE_FEATURE_ENABLED_FLAG.set(false);
    }

    @Test
    void GIVEN_persisted_data_WHEN_log_uploader_initialises_THEN_correctly_sets_the_persisted_data() throws UnsupportedInputTypeException {
        logsUploaderService.ACTIVE_LOG_FILE_FEATURE_ENABLED_FLAG.set(true);
        Topic periodicUpdateIntervalMsTopic = Topic.of(context, LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC, "3");
        when(config.lookup(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC))
                .thenReturn(periodicUpdateIntervalMsTopic);
        Topics configTopics = Topics.of(context, CONFIGURATION_CONFIG_KEY, null);
        when(config.lookupTopics(CONFIGURATION_CONFIG_KEY)).thenReturn(configTopics);
        Topics logsUploaderConfigTopics = Topics.of(context, LOGS_UPLOADER_CONFIGURATION_TOPIC, null);

        Topics componentConfigTopics = logsUploaderConfigTopics.createInteriorChild(COMPONENT_LOGS_CONFIG_MAP_TOPIC_NAME);
        Topics componentATopic = componentConfigTopics.createInteriorChild("UserComponentA");
        componentATopic.createLeafChild(FILE_REGEX_CONFIG_TOPIC_NAME).withValue("^log.txt\\w*");
        componentATopic.createLeafChild(FILE_DIRECTORY_PATH_CONFIG_TOPIC_NAME).withValue(directoryPath.toAbsolutePath().toString());
        componentATopic.createLeafChild(MIN_LOG_LEVEL_CONFIG_TOPIC_NAME).withValue("DEBUG");
        componentATopic.createLeafChild(DISK_SPACE_LIMIT_CONFIG_TOPIC_NAME).withValue("10");
        componentATopic.createLeafChild(DISK_SPACE_LIMIT_UNIT_CONFIG_TOPIC_NAME).withValue("GB");
        componentATopic.createLeafChild(DELETE_LOG_FILES_AFTER_UPLOAD_CONFIG_TOPIC_NAME).withValue("false");

        Topics systemConfigTopics = logsUploaderConfigTopics.createInteriorChild(SYSTEM_LOGS_CONFIG_TOPIC_NAME);
        systemConfigTopics.createLeafChild(UPLOAD_TO_CW_CONFIG_TOPIC_NAME).withValue("true");
        systemConfigTopics.createLeafChild(MIN_LOG_LEVEL_CONFIG_TOPIC_NAME).withValue("INFO");
        systemConfigTopics.createLeafChild(DISK_SPACE_LIMIT_CONFIG_TOPIC_NAME).withValue("25");
        systemConfigTopics.createLeafChild(DISK_SPACE_LIMIT_UNIT_CONFIG_TOPIC_NAME).withValue("MB");
        when(config.lookupTopics(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC))
                .thenReturn(logsUploaderConfigTopics);

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
                        .fileHash("TestFileHash")
                        .lastModifiedTime(Instant.EPOCH.toEpochMilli())
                        .startPosition(200)
                        .build();
        LogManagerService.CurrentProcessingFileInformation currentProcessingFileInformation2 =
                LogManagerService.CurrentProcessingFileInformation.builder()
                        .fileHash("TestFileHash2")
                        .lastModifiedTime(now.toEpochMilli())
                        .startPosition(10000)
                        .build();
        currentProcessingComponentTopics1.updateFromMap(currentProcessingFileInformation1.convertToMapOfObjects(),
                new UpdateBehaviorTree(UpdateBehaviorTree.UpdateBehavior.REPLACE, now.toEpochMilli()));
        currentProcessingComponentTopics2.updateFromMap(currentProcessingFileInformation2.convertToMapOfObjects(),
                new UpdateBehaviorTree(UpdateBehaviorTree.UpdateBehavior.REPLACE, now.toEpochMilli()));

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
        assertEquals(tenSecondsAgo.toEpochMilli(),
                logsUploaderService.lastComponentUploadedLogFileInstantMap.get(SYSTEM_LOGS_COMPONENT_NAME).toEpochMilli());

        assertTrue(logsUploaderService.componentCurrentProcessingLogFile.containsKey(SYSTEM_LOGS_COMPONENT_NAME));
        assertTrue(logsUploaderService.componentCurrentProcessingLogFile.containsKey("UserComponentA"));
        LogManagerService.CurrentProcessingFileInformation systemInfo =
                logsUploaderService.componentCurrentProcessingLogFile.get(SYSTEM_LOGS_COMPONENT_NAME);
        assertEquals("TestFileHash", systemInfo.getFileHash());
        assertEquals(200, systemInfo.getStartPosition());
        assertEquals(Instant.EPOCH.toEpochMilli(), systemInfo.getLastModifiedTime());
        LogManagerService.CurrentProcessingFileInformation userComponentInfo =
                logsUploaderService.componentCurrentProcessingLogFile.get("UserComponentA");
        assertEquals("TestFileHash2", userComponentInfo.getFileHash());
        assertEquals(10000, userComponentInfo.getStartPosition());
        assertEquals(now.toEpochMilli(), userComponentInfo.getLastModifiedTime());
        logsUploaderService.ACTIVE_LOG_FILE_FEATURE_ENABLED_FLAG.set(false);
    }
}
