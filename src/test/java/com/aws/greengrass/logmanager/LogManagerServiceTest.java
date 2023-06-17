/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.logmanager;

import com.aws.greengrass.config.Configuration;
import com.aws.greengrass.config.Topic;
import com.aws.greengrass.config.Topics;
import com.aws.greengrass.config.UnsupportedInputTypeException;
import com.aws.greengrass.config.UpdateBehaviorTree;
import com.aws.greengrass.dependency.Context;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.logging.impl.config.LogConfig;
import com.aws.greengrass.logging.impl.config.LogStore;
import com.aws.greengrass.logging.impl.config.model.LogConfigUpdate;
import com.aws.greengrass.logmanager.exceptions.InvalidLogGroupException;
import com.aws.greengrass.logmanager.model.CloudWatchAttempt;
import com.aws.greengrass.logmanager.model.CloudWatchAttemptLogFileInformation;
import com.aws.greengrass.logmanager.model.CloudWatchAttemptLogInformation;
import com.aws.greengrass.logmanager.model.ComponentLogConfiguration;
import com.aws.greengrass.logmanager.model.ComponentLogFileInformation;
import com.aws.greengrass.logmanager.model.ComponentType;
import com.aws.greengrass.logmanager.model.LogFile;
import com.aws.greengrass.logmanager.model.LogFileGroup;
import com.aws.greengrass.logmanager.model.ProcessingFiles;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.util.Coerce;
import com.aws.greengrass.util.NucleusPaths;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import org.hamcrest.collection.IsEmptyCollection;
import org.hamcrest.core.IsNot;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
import static com.aws.greengrass.logmanager.LogManagerService.PERSISTED_COMPONENT_CURRENT_PROCESSING_FILE_INFORMATION_V2;
import static com.aws.greengrass.logmanager.LogManagerService.PERSISTED_COMPONENT_LAST_FILE_PROCESSED_TIMESTAMP;
import static com.aws.greengrass.logmanager.LogManagerService.PERSISTED_CURRENT_PROCESSING_FILE_NAME;
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
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, GGExtension.class})
@SuppressWarnings("PMD.ExcessiveClassLength")
class LogManagerServiceTest {
    @Mock
    private CloudWatchLogsUploader mockUploader;
    @Mock
    private CloudWatchAttemptLogsProcessor mockMerger;
    @Captor
    private ArgumentCaptor<ComponentLogFileInformation> componentLogsInformationCaptor;
    @Captor
    private ArgumentCaptor<Consumer<CloudWatchAttempt>> callbackCaptor;
    private static NucleusPaths nucleusPaths;

    @TempDir
    static Path directoryPath;
    @TempDir
    static Path workdirectory;
    private LogManagerService logsUploaderService;
    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final Instant mockInstant = Instant.EPOCH;
    private final Context context = new Context();
    private final Configuration configuration = new Configuration(context);
    private final Topics config = configuration.lookupTopics("a");

    @BeforeAll
    static void setupBefore() throws IOException, InterruptedException {
        nucleusPaths = mock(NucleusPaths.class);
        lenient().when(nucleusPaths.workPath("aws.greengrass.LogManager")).thenReturn(workdirectory);
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

    private void mockDefaultPersistedState() {
        // 2.3.0 and prior
        config.lookupTopics(RUNTIME_STORE_NAMESPACE_TOPIC, PERSISTED_COMPONENT_CURRENT_PROCESSING_FILE_INFORMATION,
                SYSTEM_LOGS_COMPONENT_NAME);
        // 2.3.1 and after
        config.lookupTopics(RUNTIME_STORE_NAMESPACE_TOPIC, PERSISTED_COMPONENT_CURRENT_PROCESSING_FILE_INFORMATION_V2,
                SYSTEM_LOGS_COMPONENT_NAME);

        // 2.3.0 and prior
        config.lookupTopics(RUNTIME_STORE_NAMESPACE_TOPIC,PERSISTED_COMPONENT_CURRENT_PROCESSING_FILE_INFORMATION,
                "UserComponentA");
        // 2.3.1 and after
        config.lookupTopics(RUNTIME_STORE_NAMESPACE_TOPIC,
                        PERSISTED_COMPONENT_CURRENT_PROCESSING_FILE_INFORMATION_V2, "UserComponentA");
        // 2.3.0 and prior
        config.lookupTopics(RUNTIME_STORE_NAMESPACE_TOPIC,
                PERSISTED_COMPONENT_CURRENT_PROCESSING_FILE_INFORMATION, "UserComponentB");
        config.lookupTopics(RUNTIME_STORE_NAMESPACE_TOPIC, PERSISTED_COMPONENT_CURRENT_PROCESSING_FILE_INFORMATION_V2,
                "UserComponentB");

        config.lookupTopics(RUNTIME_STORE_NAMESPACE_TOPIC,
                PERSISTED_COMPONENT_LAST_FILE_PROCESSED_TIMESTAMP, SYSTEM_LOGS_COMPONENT_NAME);
        config.lookupTopics(RUNTIME_STORE_NAMESPACE_TOPIC,
                PERSISTED_COMPONENT_LAST_FILE_PROCESSED_TIMESTAMP, "UserComponentA");
        config.lookupTopics(RUNTIME_STORE_NAMESPACE_TOPIC,
                PERSISTED_COMPONENT_LAST_FILE_PROCESSED_TIMESTAMP, "UserComponentB");
    }

    @AfterEach
    public void cleanup() throws InterruptedException {
        if (logsUploaderService != null) {
            logsUploaderService.processingFilesInformation.clear();
            logsUploaderService.lastComponentUploadedLogFileInstantMap.clear();
            logsUploaderService.shutdown();
        }

        executor.shutdownNow();
        context.shutdown();
    }

    @Test
    void GIVEN_system_log_files_to_be_uploaded_WHEN_merger_merges_THEN_we_get_all_log_files()
            throws InterruptedException, IOException {
        mockDefaultPersistedState();
        config.lookup(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC)
                .withValue("1");
        when(mockMerger.processLogFiles(componentLogsInformationCaptor.capture())).thenReturn(new CloudWatchAttempt());

        Topics componentConfigTopics = config.lookupTopics(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC,
                COMPONENT_LOGS_CONFIG_MAP_TOPIC_NAME);
        Topics componentATopic = componentConfigTopics.createInteriorChild("UserComponentA");
        componentATopic.createLeafChild(FILE_REGEX_CONFIG_TOPIC_NAME).withValue("^log.txt\\\\w*");
        componentATopic.createLeafChild(FILE_DIRECTORY_PATH_CONFIG_TOPIC_NAME).withValue(directoryPath.toAbsolutePath().toString());
        componentATopic.createLeafChild(MIN_LOG_LEVEL_CONFIG_TOPIC_NAME).withValue("DEBUG");
        componentATopic.createLeafChild(DISK_SPACE_LIMIT_CONFIG_TOPIC_NAME).withValue("10");
        componentATopic.createLeafChild(DISK_SPACE_LIMIT_UNIT_CONFIG_TOPIC_NAME).withValue("GB");
        componentATopic.createLeafChild(DELETE_LOG_FILES_AFTER_UPLOAD_CONFIG_TOPIC_NAME).withValue("false");

        Topics systemConfigTopics = config.lookupTopics(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC,
                SYSTEM_LOGS_CONFIG_TOPIC_NAME);
        systemConfigTopics.createLeafChild(UPLOAD_TO_CW_CONFIG_TOPIC_NAME).withValue("true");
        systemConfigTopics.createLeafChild(MIN_LOG_LEVEL_CONFIG_TOPIC_NAME).withValue("INFO");
        systemConfigTopics.createLeafChild(DISK_SPACE_LIMIT_CONFIG_TOPIC_NAME).withValue("25");
        systemConfigTopics.createLeafChild(DISK_SPACE_LIMIT_UNIT_CONFIG_TOPIC_NAME).withValue("MB");

        doNothing().when(mockUploader).registerAttemptStatus(anyString(), callbackCaptor.capture());

        logsUploaderService = new LogManagerService(config, mockUploader, mockMerger, nucleusPaths);
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
        verify(mockUploader, atLeastOnce()).upload(any(CloudWatchAttempt.class), anyInt());
    }

    @Test
    void GIVEN_user_component_log_files_to_be_uploaded_with_required_config_as_array_WHEN_merger_merges_THEN_we_get_all_log_files()
            throws InterruptedException, UnsupportedInputTypeException, IOException {
        mockDefaultPersistedState();
        config.lookup(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC)
                .withValue("1");
        when(mockMerger.processLogFiles(componentLogsInformationCaptor.capture())).thenReturn(new CloudWatchAttempt());

        List<Map<String, Object>> componentLogsInformationList = new ArrayList<>();
        Map<String, Object> componentAConfig = new HashMap<>();
        componentAConfig.put(COMPONENT_NAME_CONFIG_TOPIC_NAME, "UserComponentA");
        componentAConfig.put(MIN_LOG_LEVEL_CONFIG_TOPIC_NAME, "DEBUG");
        componentAConfig.put(DISK_SPACE_LIMIT_CONFIG_TOPIC_NAME, "10");
        componentAConfig.put(DISK_SPACE_LIMIT_UNIT_CONFIG_TOPIC_NAME, "GB");
        componentAConfig.put(DELETE_LOG_FILES_AFTER_UPLOAD_CONFIG_TOPIC_NAME, "false");
        componentLogsInformationList.add(componentAConfig);
        config.lookup(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC,
                COMPONENT_LOGS_CONFIG_TOPIC_NAME).withValueChecked(componentLogsInformationList);

        Topics systemConfigTopics = config.lookupTopics(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC,
                SYSTEM_LOGS_CONFIG_TOPIC_NAME);
        systemConfigTopics.createLeafChild(UPLOAD_TO_CW_CONFIG_TOPIC_NAME).withValue("false");
        systemConfigTopics.createLeafChild(MIN_LOG_LEVEL_CONFIG_TOPIC_NAME).withValue("INFO");
        systemConfigTopics.createLeafChild(DISK_SPACE_LIMIT_CONFIG_TOPIC_NAME).withValue("25");
        systemConfigTopics.createLeafChild(DISK_SPACE_LIMIT_UNIT_CONFIG_TOPIC_NAME).withValue("MB");

        doNothing().when(mockUploader).registerAttemptStatus(anyString(), callbackCaptor.capture());

        logsUploaderService = new LogManagerService(config, mockUploader, mockMerger, nucleusPaths);
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
        verify(mockUploader, atLeastOnce()).upload(any(CloudWatchAttempt.class), anyInt());
    }

    @Test
    void GIVEN_user_component_log_files_to_be_uploaded_with_required_config_WHEN_merger_merges_THEN_we_get_all_log_files()
            throws InterruptedException, IOException {
        mockDefaultPersistedState();
        config.lookup(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC)
                .withValue("1");
        when(mockMerger.processLogFiles(componentLogsInformationCaptor.capture())).thenReturn(new CloudWatchAttempt());

        Topics componentATopic = config.lookupTopics(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC,
                COMPONENT_LOGS_CONFIG_MAP_TOPIC_NAME, "UserComponentA");
        componentATopic.createLeafChild(FILE_DIRECTORY_PATH_CONFIG_TOPIC_NAME).withValue(directoryPath.toAbsolutePath().toString());
        componentATopic.createLeafChild(MIN_LOG_LEVEL_CONFIG_TOPIC_NAME).withValue("DEBUG");
        componentATopic.createLeafChild(DISK_SPACE_LIMIT_CONFIG_TOPIC_NAME).withValue("10");
        componentATopic.createLeafChild(DISK_SPACE_LIMIT_UNIT_CONFIG_TOPIC_NAME).withValue("GB");
        componentATopic.createLeafChild(DELETE_LOG_FILES_AFTER_UPLOAD_CONFIG_TOPIC_NAME).withValue("false");

        Topics systemConfigTopics = config.lookupTopics(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC,
                SYSTEM_LOGS_CONFIG_TOPIC_NAME);
        systemConfigTopics.createLeafChild(UPLOAD_TO_CW_CONFIG_TOPIC_NAME).withValue("false");
        systemConfigTopics.createLeafChild(MIN_LOG_LEVEL_CONFIG_TOPIC_NAME).withValue("INFO");
        systemConfigTopics.createLeafChild(DISK_SPACE_LIMIT_CONFIG_TOPIC_NAME).withValue("25");
        systemConfigTopics.createLeafChild(DISK_SPACE_LIMIT_UNIT_CONFIG_TOPIC_NAME).withValue("MB");

        doNothing().when(mockUploader).registerAttemptStatus(anyString(), callbackCaptor.capture());

        logsUploaderService = new LogManagerService(config, mockUploader, mockMerger, nucleusPaths);
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
        verify(mockUploader, atLeastOnce()).upload(any(CloudWatchAttempt.class), anyInt());
    }

    @Test
    void GIVEN_user_component_log_files_to_be_uploaded_with_all_config_WHEN_merger_merges_THEN_we_get_all_log_files()
            throws InterruptedException, IOException {
        mockDefaultPersistedState();
        config.lookup(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC)
                .withValue("1");
        when(mockMerger.processLogFiles(componentLogsInformationCaptor.capture())).thenReturn(new CloudWatchAttempt());

        Topics componentATopic = config.lookupTopics(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC,
                COMPONENT_LOGS_CONFIG_MAP_TOPIC_NAME, "UserComponentA");
        componentATopic.createLeafChild(FILE_REGEX_CONFIG_TOPIC_NAME).withValue("^UserComponentA\\w*\\.log");
        componentATopic.createLeafChild(FILE_DIRECTORY_PATH_CONFIG_TOPIC_NAME).withValue(directoryPath.toAbsolutePath().toString());
        componentATopic.createLeafChild(MIN_LOG_LEVEL_CONFIG_TOPIC_NAME).withValue("DEBUG");
        componentATopic.createLeafChild(DISK_SPACE_LIMIT_CONFIG_TOPIC_NAME).withValue("10");
        componentATopic.createLeafChild(DISK_SPACE_LIMIT_UNIT_CONFIG_TOPIC_NAME).withValue("GB");
        componentATopic.createLeafChild(DELETE_LOG_FILES_AFTER_UPLOAD_CONFIG_TOPIC_NAME).withValue("false");

        Topics systemConfigTopics = config.lookupTopics(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC,
                SYSTEM_LOGS_CONFIG_TOPIC_NAME);
        systemConfigTopics.createLeafChild(UPLOAD_TO_CW_CONFIG_TOPIC_NAME).withValue("false");
        systemConfigTopics.createLeafChild(MIN_LOG_LEVEL_CONFIG_TOPIC_NAME).withValue("INFO");
        systemConfigTopics.createLeafChild(DISK_SPACE_LIMIT_CONFIG_TOPIC_NAME).withValue("25");
        systemConfigTopics.createLeafChild(DISK_SPACE_LIMIT_UNIT_CONFIG_TOPIC_NAME).withValue("MB");

        doNothing().when(mockUploader).registerAttemptStatus(anyString(), callbackCaptor.capture());

        logsUploaderService = new LogManagerService(config, mockUploader, mockMerger, nucleusPaths);
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
        verify(mockUploader, atLeastOnce()).upload(any(CloudWatchAttempt.class), anyInt());
    }

    @Test
    void GIVEN_multiple_user_components_log_files_to_be_uploaded_with_all_config_WHEN_merger_merges_THEN_we_get_all_log_files()
            throws InterruptedException, IOException {
        mockDefaultPersistedState();
        config.lookup(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC)
                .withValue("1");
        when(mockMerger.processLogFiles(componentLogsInformationCaptor.capture())).thenReturn(new CloudWatchAttempt());

        Topics componentATopic = config.lookupTopics(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC,
                COMPONENT_LOGS_CONFIG_MAP_TOPIC_NAME, "UserComponentA");
        componentATopic.createLeafChild(FILE_REGEX_CONFIG_TOPIC_NAME).withValue("^log.txt\\\\w*");
        componentATopic.createLeafChild(FILE_DIRECTORY_PATH_CONFIG_TOPIC_NAME).withValue(directoryPath.toAbsolutePath().toString());
        componentATopic.createLeafChild(MIN_LOG_LEVEL_CONFIG_TOPIC_NAME).withValue("DEBUG");
        componentATopic.createLeafChild(DISK_SPACE_LIMIT_CONFIG_TOPIC_NAME).withValue("10");
        componentATopic.createLeafChild(DISK_SPACE_LIMIT_UNIT_CONFIG_TOPIC_NAME).withValue("GB");
        componentATopic.createLeafChild(DELETE_LOG_FILES_AFTER_UPLOAD_CONFIG_TOPIC_NAME).withValue("false");

        Topics componentBTopic = config.lookupTopics(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC,
                COMPONENT_LOGS_CONFIG_MAP_TOPIC_NAME, "UserComponentB");
        componentBTopic.createLeafChild(FILE_REGEX_CONFIG_TOPIC_NAME).withValue("^UserComponentB\\w*\\.log");
        componentBTopic.createLeafChild(FILE_DIRECTORY_PATH_CONFIG_TOPIC_NAME).withValue(directoryPath.toAbsolutePath().toString());
        componentBTopic.createLeafChild(MIN_LOG_LEVEL_CONFIG_TOPIC_NAME).withValue("DEBUG");
        componentBTopic.createLeafChild(DISK_SPACE_LIMIT_CONFIG_TOPIC_NAME).withValue("10");
        componentBTopic.createLeafChild(DISK_SPACE_LIMIT_UNIT_CONFIG_TOPIC_NAME).withValue("GB");
        componentBTopic.createLeafChild(DELETE_LOG_FILES_AFTER_UPLOAD_CONFIG_TOPIC_NAME).withValue("false");

        Topics systemConfigTopics = config.lookupTopics(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC,
                SYSTEM_LOGS_CONFIG_TOPIC_NAME);
        systemConfigTopics.createLeafChild(UPLOAD_TO_CW_CONFIG_TOPIC_NAME).withValue("false");
        systemConfigTopics.createLeafChild(MIN_LOG_LEVEL_CONFIG_TOPIC_NAME).withValue("INFO");
        systemConfigTopics.createLeafChild(DISK_SPACE_LIMIT_CONFIG_TOPIC_NAME).withValue("25");
        systemConfigTopics.createLeafChild(DISK_SPACE_LIMIT_UNIT_CONFIG_TOPIC_NAME).withValue("MB");

        doNothing().when(mockUploader).registerAttemptStatus(anyString(), callbackCaptor.capture());

        logsUploaderService = new LogManagerService(config, mockUploader, mockMerger, nucleusPaths);
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
        verify(mockUploader, atLeastOnce()).upload(any(CloudWatchAttempt.class), anyInt());
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
            ExtensionContext context1) throws IOException {
        ignoreExceptionOfType(context1, MismatchedInputException.class);
        config.lookup(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC)
                .withValue("3");
        config.lookupTopics(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC);

        logsUploaderService = new LogManagerService(config, mockUploader, mockMerger, nucleusPaths);
        startServiceOnAnotherThread();
        assertThat(logsUploaderService.processingFilesInformation.values(), IsEmptyCollection.empty());
    }

    @Test
    void GIVEN_cloud_watch_attempt_handler_WHEN_attempt_completes_THEN_successfully_updates_states_for_each_component()
            throws IOException, InvalidLogGroupException, InterruptedException {
        mockDefaultPersistedState();
        LogFile processingFile = createLogFileWithSize(directoryPath.resolve("testlogs1.log").toUri(), 1061);
        LogFile lastProcessedFile = createLogFileWithSize(directoryPath.resolve("testlogs2.log").toUri(), 2943);
        config.lookup(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC)
                .withValue("1000");

        Topics systemConfigTopics = config.lookupTopics(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC,
                SYSTEM_LOGS_CONFIG_TOPIC_NAME);
        systemConfigTopics.createLeafChild(UPLOAD_TO_CW_CONFIG_TOPIC_NAME).withValue("false");
        systemConfigTopics.createLeafChild(MIN_LOG_LEVEL_CONFIG_TOPIC_NAME).withValue("INFO");
        systemConfigTopics.createLeafChild(DISK_SPACE_LIMIT_CONFIG_TOPIC_NAME).withValue("25");
        systemConfigTopics.createLeafChild(DISK_SPACE_LIMIT_UNIT_CONFIG_TOPIC_NAME).withValue("MB");

        CloudWatchAttempt attempt = new CloudWatchAttempt();
        Map<String, CloudWatchAttemptLogInformation> logStreamsToLogInformationMap = new HashMap<>();

        Pattern pattern1 = Pattern.compile("^testlogs2.log\\w*$");
        // Create another file intentionally, so that the lastProcessedFile will be processed.
        TimeUnit.SECONDS.sleep(5);
        createLogFileWithSize(directoryPath.resolve("testlogs2.log_active").toUri(), 2943);
        ComponentLogConfiguration compLogInfo = ComponentLogConfiguration.builder()
                .directoryPath(directoryPath)
                .fileNameRegex(pattern1).name("TestComponent2").build();
        LogFileGroup lastProcessedLogFileGroup =
                LogFileGroup.create(compLogInfo, mockInstant, workdirectory);
        assertEquals(2, lastProcessedLogFileGroup.getLogFiles().size());
        assertFalse(lastProcessedLogFileGroup.isActiveFile(lastProcessedFile));

        createLogFileWithSize(directoryPath.resolve("testlogs1.log_processed").toUri(), 1061);
        // Intentionally sleep to differ the creation time of two files
        TimeUnit.SECONDS.sleep(5);
        Pattern pattern2 = Pattern.compile("^testlogs1.log$");
        ComponentLogConfiguration compLogInfo2 = ComponentLogConfiguration.builder()
                .directoryPath(directoryPath)
                .fileNameRegex(pattern2).name("TestComponent1").build();
        LogFileGroup processingLogFileGroup = LogFileGroup.create(compLogInfo2,
                Instant.ofEpochMilli(processingFile.lastModified() - 1), workdirectory);
        assertEquals(1, processingLogFileGroup.getLogFiles().size());
        assertTrue(processingLogFileGroup.isActiveFile(processingFile));

        Map<String, CloudWatchAttemptLogFileInformation> LastProcessedAttemptLogFileInformationMap = new HashMap<>();
        LastProcessedAttemptLogFileInformationMap.put(lastProcessedFile.hashString(),
                CloudWatchAttemptLogFileInformation.builder()
                        .startPosition(0)
                        .bytesRead(2943)
                        .lastModifiedTime(lastProcessedFile.lastModified())
                        .fileHash(lastProcessedFile.hashString())
                        .build());

        Map<String, CloudWatchAttemptLogFileInformation> processingAttemptLogFileInformationMap2 = new HashMap<>();
        processingAttemptLogFileInformationMap2.put(processingFile.hashString(),
                CloudWatchAttemptLogFileInformation.builder()
                .startPosition(0)
                .bytesRead(1061)
                .lastModifiedTime(processingFile.lastModified())
                .fileHash(processingFile.hashString())
                .build());

        CloudWatchAttemptLogInformation attemptLogInformation1 = CloudWatchAttemptLogInformation.builder()
                .componentName("TestComponent")
                .attemptLogFileInformationMap(LastProcessedAttemptLogFileInformationMap)
                .logFileGroup(lastProcessedLogFileGroup)
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
        CloudWatchAttempt attempt1 = new CloudWatchAttempt();
        ComponentLogFileInformation info1 = ComponentLogFileInformation.builder().build();
        lenient().doReturn(attempt1).when(mockMerger).processLogFiles(info1);
        lenient().doNothing().when(mockUploader).upload(attempt1, 1);
        logsUploaderService = new LogManagerService(config, mockUploader, mockMerger, nucleusPaths);
        startServiceOnAnotherThread();
        callbackCaptor.getValue().accept(attempt);

        assertEquals(lastProcessedFile.lastModified(), Coerce.toLong(config.find(RUNTIME_STORE_NAMESPACE_TOPIC,
                PERSISTED_COMPONENT_LAST_FILE_PROCESSED_TIMESTAMP, "TestComponent", PERSISTED_LAST_FILE_PROCESSED_TIMESTAMP)));

        LogManagerService.CurrentProcessingFileInformation currentProcessingFileInformation =
                LogManagerService.CurrentProcessingFileInformation.convertFromMapOfObjects(
                        (Map<String, Object>) config.lookupTopics(RUNTIME_STORE_NAMESPACE_TOPIC,
                                        PERSISTED_COMPONENT_CURRENT_PROCESSING_FILE_INFORMATION_V2, "TestComponent2")
                                .toPOJO()
                                .get(processingFile.hashString()));
        assertEquals(processingFile.hashString(), currentProcessingFileInformation.getFileHash());
        assertEquals(1061, currentProcessingFileInformation.getStartPosition());
        assertEquals(processingFile.lastModified(), currentProcessingFileInformation.getLastModifiedTime());

        assertNotNull(logsUploaderService.lastComponentUploadedLogFileInstantMap);
        assertThat(logsUploaderService.lastComponentUploadedLogFileInstantMap.entrySet(),
                IsNot.not(IsEmptyCollection.empty()));
        assertTrue(logsUploaderService.lastComponentUploadedLogFileInstantMap.containsKey("TestComponent"));
        assertEquals(Instant.ofEpochMilli(lastProcessedFile.lastModified()),
                logsUploaderService.lastComponentUploadedLogFileInstantMap.get("TestComponent"));
        assertNotNull(logsUploaderService.processingFilesInformation);
        assertThat(logsUploaderService.processingFilesInformation.entrySet(), IsNot.not(IsEmptyCollection.empty()));
        assertTrue(logsUploaderService.processingFilesInformation.containsKey("TestComponent2"));
        assertEquals(processingFile.hashString(), logsUploaderService.processingFilesInformation.get(
                "TestComponent2").get(processingFile.hashString()).getFileHash());
        assertEquals(1061, logsUploaderService.processingFilesInformation.get("TestComponent2")
                .get(processingFile.hashString()).getStartPosition());
    }

    @Test
    void GIVEN_some_system_files_uploaded_and_another_partially_uploaded_WHEN_merger_merges_THEN_sets_the_start_position_correctly()
            throws InterruptedException, IOException {
        mockDefaultPersistedState();
        config.lookup(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC)
                .withValue("3");
        when(mockMerger.processLogFiles(componentLogsInformationCaptor.capture())).thenReturn(new CloudWatchAttempt());

        Topics systemConfigTopics = config.lookupTopics(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC,
                SYSTEM_LOGS_CONFIG_TOPIC_NAME);
        systemConfigTopics.createLeafChild(UPLOAD_TO_CW_CONFIG_TOPIC_NAME).withValue("true");
        systemConfigTopics.createLeafChild(MIN_LOG_LEVEL_CONFIG_TOPIC_NAME).withValue("INFO");
        systemConfigTopics.createLeafChild(DISK_SPACE_LIMIT_CONFIG_TOPIC_NAME).withValue("25");
        systemConfigTopics.createLeafChild(DISK_SPACE_LIMIT_UNIT_CONFIG_TOPIC_NAME).withValue("MB");

        logsUploaderService = new LogManagerService(config, mockUploader, mockMerger, nucleusPaths);
        // These two files are existed, and the active file is greengrass.log
        LogFile file = new LogFile(directoryPath.resolve("greengrass_test_2.log").toUri());
        LogFile currentProcessingFile = new LogFile(directoryPath.resolve("greengrass_test_3.log").toUri());
        logsUploaderService.lastComponentUploadedLogFileInstantMap.put(SYSTEM_LOGS_COMPONENT_NAME,
                Instant.ofEpochMilli(file.lastModified()));
        ProcessingFiles processingFiles = new ProcessingFiles(5);
        processingFiles.put(LogManagerService.CurrentProcessingFileInformation.builder()
                .fileHash(currentProcessingFile.hashString())
                .lastModifiedTime(currentProcessingFile.lastModified())
                .startPosition(2)
                .build());
        logsUploaderService.processingFilesInformation.put(SYSTEM_LOGS_COMPONENT_NAME, processingFiles);

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
            if (logFileInformation.getLogFile().getSourcePath().equals(currentProcessingFile.getSourcePath())) {
                assertEquals(2, logFileInformation.getStartPosition());
            } else {
                assertEquals(0, logFileInformation.getStartPosition());
            }
        });
        verify(mockUploader, atLeastOnce()).upload(any(CloudWatchAttempt.class), anyInt());
    }

    @Test
    void GIVEN_a_partially_uploaded_file_but_rotated_WHEN_merger_merges_THEN_sets_the_start_position_correctly()
            throws InterruptedException, IOException {
        mockDefaultPersistedState();
        config.lookup(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC)
                .withValue("3");
        when(mockMerger.processLogFiles(componentLogsInformationCaptor.capture())).thenReturn(new CloudWatchAttempt());

        Topics systemConfigTopics = config.lookupTopics(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC,
                SYSTEM_LOGS_CONFIG_TOPIC_NAME);
        systemConfigTopics.createLeafChild(UPLOAD_TO_CW_CONFIG_TOPIC_NAME).withValue("true");
        systemConfigTopics.createLeafChild(MIN_LOG_LEVEL_CONFIG_TOPIC_NAME).withValue("INFO");
        systemConfigTopics.createLeafChild(DISK_SPACE_LIMIT_CONFIG_TOPIC_NAME).withValue("25");
        systemConfigTopics.createLeafChild(DISK_SPACE_LIMIT_UNIT_CONFIG_TOPIC_NAME).withValue("MB");

        logsUploaderService = new LogManagerService(config, mockUploader, mockMerger, nucleusPaths);
        startServiceOnAnotherThread();

        LogFile file = new LogFile(directoryPath.resolve("greengrass.log_test_2").toUri());
        LogFile currentProcessingFile = new LogFile(directoryPath.resolve("greengrass.log_test-3").toUri());
        logsUploaderService.lastComponentUploadedLogFileInstantMap.put(SYSTEM_LOGS_COMPONENT_NAME,
                Instant.ofEpochMilli(file.lastModified()));
        ProcessingFiles processingFiles = new ProcessingFiles(5);
        processingFiles.put(LogManagerService.CurrentProcessingFileInformation.builder()
                .fileHash(currentProcessingFile.hashString())
                .lastModifiedTime(currentProcessingFile.lastModified() - 1000)
                .startPosition(2)
                .build());
        logsUploaderService.processingFilesInformation.put(SYSTEM_LOGS_COMPONENT_NAME, processingFiles);

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
        verify(mockUploader, atLeastOnce()).upload(any(CloudWatchAttempt.class), anyInt());
    }

    @Test
    void GIVEN_persisted_data_WHEN_log_uploader_initialises_THEN_correctly_sets_the_persisted_data() throws
            IOException {
        config.lookup(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC)
                .withValue("1");

        Topics componentConfigTopics =
                config.lookupTopics(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC,
                        COMPONENT_LOGS_CONFIG_MAP_TOPIC_NAME);
        Topics componentATopic = componentConfigTopics.createInteriorChild("UserComponentA");
        componentATopic.createLeafChild(FILE_REGEX_CONFIG_TOPIC_NAME).withValue("^log.txt\\w*");
        componentATopic.createLeafChild(FILE_DIRECTORY_PATH_CONFIG_TOPIC_NAME).withValue(directoryPath.toAbsolutePath().toString());
        componentATopic.createLeafChild(MIN_LOG_LEVEL_CONFIG_TOPIC_NAME).withValue("DEBUG");
        componentATopic.createLeafChild(DISK_SPACE_LIMIT_CONFIG_TOPIC_NAME).withValue("10");
        componentATopic.createLeafChild(DISK_SPACE_LIMIT_UNIT_CONFIG_TOPIC_NAME).withValue("GB");
        componentATopic.createLeafChild(DELETE_LOG_FILES_AFTER_UPLOAD_CONFIG_TOPIC_NAME).withValue("false");

        Topics systemConfigTopics = config.lookupTopics(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC,
                SYSTEM_LOGS_CONFIG_TOPIC_NAME);
        systemConfigTopics.createLeafChild(UPLOAD_TO_CW_CONFIG_TOPIC_NAME).withValue("true");
        systemConfigTopics.createLeafChild(MIN_LOG_LEVEL_CONFIG_TOPIC_NAME).withValue("INFO");
        systemConfigTopics.createLeafChild(DISK_SPACE_LIMIT_CONFIG_TOPIC_NAME).withValue("25");
        systemConfigTopics.createLeafChild(DISK_SPACE_LIMIT_UNIT_CONFIG_TOPIC_NAME).withValue("MB");

        Instant now = Instant.now();
        Instant tenSecondsAgo = Instant.now().minusSeconds(10);
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

        config.lookupTopics(RUNTIME_STORE_NAMESPACE_TOPIC)
                .lookupTopics(PERSISTED_COMPONENT_CURRENT_PROCESSING_FILE_INFORMATION, SYSTEM_LOGS_COMPONENT_NAME)
                .updateFromMap(currentProcessingFileInformation1.convertToMapOfObjects(),
                        new UpdateBehaviorTree(UpdateBehaviorTree.UpdateBehavior.REPLACE, now.toEpochMilli()));
        config.lookupTopics(RUNTIME_STORE_NAMESPACE_TOPIC)
                .lookupTopics(PERSISTED_COMPONENT_CURRENT_PROCESSING_FILE_INFORMATION, "UserComponentA")
                .updateFromMap(currentProcessingFileInformation2.convertToMapOfObjects(),
                        new UpdateBehaviorTree(UpdateBehaviorTree.UpdateBehavior.REPLACE, now.toEpochMilli()));

        config.lookupTopics(RUNTIME_STORE_NAMESPACE_TOPIC)
                .lookup(PERSISTED_COMPONENT_LAST_FILE_PROCESSED_TIMESTAMP, SYSTEM_LOGS_COMPONENT_NAME,
                        PERSISTED_LAST_FILE_PROCESSED_TIMESTAMP).withValue(tenSecondsAgo.toEpochMilli());

        logsUploaderService = new LogManagerService(config, mockUploader, mockMerger, nucleusPaths);

        assertNotNull(logsUploaderService.processingFilesInformation);
        assertNotNull(logsUploaderService.lastComponentUploadedLogFileInstantMap);
        assertTrue(logsUploaderService.lastComponentUploadedLogFileInstantMap.containsKey(SYSTEM_LOGS_COMPONENT_NAME));
        assertFalse(logsUploaderService.lastComponentUploadedLogFileInstantMap.containsKey("UserComponentA"));
        assertEquals(tenSecondsAgo.toEpochMilli(),
                logsUploaderService.lastComponentUploadedLogFileInstantMap.get(SYSTEM_LOGS_COMPONENT_NAME).toEpochMilli());

        assertTrue(logsUploaderService.processingFilesInformation.containsKey(SYSTEM_LOGS_COMPONENT_NAME));
        assertTrue(logsUploaderService.processingFilesInformation.containsKey("UserComponentA"));
        LogManagerService.CurrentProcessingFileInformation systemInfo =
                logsUploaderService.processingFilesInformation
                        .get(SYSTEM_LOGS_COMPONENT_NAME)
                        .get(currentProcessingFileInformation1.getFileHash());
        assertEquals("TestFileHash", systemInfo.getFileHash());
        assertEquals(200, systemInfo.getStartPosition());
        assertEquals(Instant.EPOCH.toEpochMilli(), systemInfo.getLastModifiedTime());
        LogManagerService.CurrentProcessingFileInformation userComponentInfo =
                logsUploaderService.processingFilesInformation
                        .get("UserComponentA")
                        .get(currentProcessingFileInformation2.getFileHash());
        assertEquals("TestFileHash2", userComponentInfo.getFileHash());
        assertEquals(10000, userComponentInfo.getStartPosition());
        assertEquals(now.toEpochMilli(), userComponentInfo.getLastModifiedTime());
    }

    @Test
    void GIVEN_config_without_hash_but_name_WHEN_config_is_processed_THEN_processingFile_info_is_loaded()
            throws Exception {
        // Given

        // A rotated log file
        Path rotatedLogFilePath = directoryPath.resolve("testlogs1.log");
        LogFile rotatedLogFile = new LogFile(rotatedLogFilePath.toUri());
        createLogFileWithSize(rotatedLogFilePath.toUri(), 1061);

        // Configuration of file being currently processed with only the file name
        String componentName = "testlogs\\w*.log";
        Topics runtimeConfig = Topics.of(new Context(), RUNTIME_STORE_NAMESPACE_TOPIC, null);
        Topics currentlyProcessing = runtimeConfig
                .lookupTopics(PERSISTED_COMPONENT_CURRENT_PROCESSING_FILE_INFORMATION, componentName);
        currentlyProcessing.lookup(PERSISTED_CURRENT_PROCESSING_FILE_NAME)
                .withValue(rotatedLogFilePath.toAbsolutePath().toString());

        // When
        LogManagerService.CurrentProcessingFileInformation processingInfo =
                LogManagerService.CurrentProcessingFileInformation.builder().build();
        Topic nameTopic = currentlyProcessing.find(PERSISTED_CURRENT_PROCESSING_FILE_NAME);
        processingInfo.updateFromTopic(nameTopic);


        // Then
        assertEquals(processingInfo.getFileName(), rotatedLogFilePath.toAbsolutePath().toString());
        assertEquals(processingInfo.getFileHash(), rotatedLogFile.hashString());
    }
}
