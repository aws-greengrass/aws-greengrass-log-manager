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
import org.junit.jupiter.api.Disabled;
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
        
        // Add TestComponent1 and TestComponent2 persisted state
        config.lookupTopics(RUNTIME_STORE_NAMESPACE_TOPIC, PERSISTED_COMPONENT_CURRENT_PROCESSING_FILE_INFORMATION,
                "TestComponent1");
        config.lookupTopics(RUNTIME_STORE_NAMESPACE_TOPIC, PERSISTED_COMPONENT_CURRENT_PROCESSING_FILE_INFORMATION_V2,
                "TestComponent1");
        config.lookupTopics(RUNTIME_STORE_NAMESPACE_TOPIC, PERSISTED_COMPONENT_CURRENT_PROCESSING_FILE_INFORMATION,
                "TestComponent2");
        config.lookupTopics(RUNTIME_STORE_NAMESPACE_TOPIC, PERSISTED_COMPONENT_CURRENT_PROCESSING_FILE_INFORMATION_V2,
                "TestComponent2");
        config.lookupTopics(RUNTIME_STORE_NAMESPACE_TOPIC,
                PERSISTED_COMPONENT_LAST_FILE_PROCESSED_TIMESTAMP, "TestComponent1");
        config.lookupTopics(RUNTIME_STORE_NAMESPACE_TOPIC,
                PERSISTED_COMPONENT_LAST_FILE_PROCESSED_TIMESTAMP, "TestComponent2");
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

    @Test @Disabled
    void GIVEN_cloud_watch_attempt_handler_WHEN_attempt_completes_THEN_successfully_updates_states_for_each_component()
            throws IOException, InvalidLogGroupException, InterruptedException {
        mockDefaultPersistedState();
        LogFile component1TestLog = createLogFileWithSize(directoryPath.resolve("testlogs1.log").toUri(), 1061);
        LogFile component2TestLog = createLogFileWithSize(directoryPath.resolve("testlogs2.log").toUri(), 2943);
        // Create another file intentionally, so that the lastProcessedFile will be processed.
        TimeUnit.SECONDS.sleep(5);
        LogFile component1ActiveFile = createLogFileWithSize(directoryPath.resolve("testlogs1.log_active").toUri(),
                1061);
        LogFile component2ActiveFile = createLogFileWithSize(directoryPath.resolve("testlogs2.log_active").toUri(),
                2943);
        String pattern1 = "^testlogs1.log\\w*$";
        String pattern2 = "^testlogs2.log\\w*$";
        config.lookup(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC)
                .withValue("50000");

        Topics systemConfigTopics = config.lookupTopics(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC,
                SYSTEM_LOGS_CONFIG_TOPIC_NAME);
        systemConfigTopics.createLeafChild(UPLOAD_TO_CW_CONFIG_TOPIC_NAME).withValue("false");
        systemConfigTopics.createLeafChild(MIN_LOG_LEVEL_CONFIG_TOPIC_NAME).withValue("INFO");
        Topics testComponent1ConfigTopics = config.lookupTopics(CONFIGURATION_CONFIG_KEY,
                LOGS_UPLOADER_CONFIGURATION_TOPIC, COMPONENT_LOGS_CONFIG_MAP_TOPIC_NAME, "TestComponent1");
        testComponent1ConfigTopics.createLeafChild(UPLOAD_TO_CW_CONFIG_TOPIC_NAME).withValue("true");
        testComponent1ConfigTopics.createLeafChild(FILE_DIRECTORY_PATH_CONFIG_TOPIC_NAME).withValue(directoryPath.toString());
        testComponent1ConfigTopics.createLeafChild(FILE_REGEX_CONFIG_TOPIC_NAME).withValue(pattern1);

        Topics testComponent2ConfigTopics = config.lookupTopics(CONFIGURATION_CONFIG_KEY,
                LOGS_UPLOADER_CONFIGURATION_TOPIC,
                COMPONENT_LOGS_CONFIG_MAP_TOPIC_NAME, "TestComponent2");
        testComponent2ConfigTopics.createLeafChild(UPLOAD_TO_CW_CONFIG_TOPIC_NAME).withValue("true");
        testComponent2ConfigTopics.createLeafChild(FILE_REGEX_CONFIG_TOPIC_NAME).withValue(pattern2);
        testComponent2ConfigTopics.createLeafChild(FILE_DIRECTORY_PATH_CONFIG_TOPIC_NAME).withValue(directoryPath.toString());

        Map<String, CloudWatchAttemptLogInformation> logStreamsToLogInformationMap = new HashMap<>();

        ComponentLogConfiguration comp1LogInfo = ComponentLogConfiguration.builder()
                .directoryPath(directoryPath)
                .fileNameRegex(Pattern.compile(pattern1)).name("TestComponent1").build();
        LogFileGroup component1LogFileGroup = LogFileGroup.create(comp1LogInfo,
                Instant.ofEpochMilli(component1TestLog.lastModified() - 1), workdirectory);
        assertEquals(2, component1LogFileGroup.getLogFiles().size());
        assertFalse(component1LogFileGroup.isActiveFile(component1TestLog));
        assertTrue(component1LogFileGroup.isActiveFile(component1ActiveFile));


        ComponentLogConfiguration comp2LogInfo = ComponentLogConfiguration.builder()
                .directoryPath(directoryPath)
                .fileNameRegex(Pattern.compile(pattern2)).name("TestComponent2").build();
        LogFileGroup component2LogFileGroup = LogFileGroup.create(comp2LogInfo, mockInstant, workdirectory);
        assertEquals(2, component2LogFileGroup.getLogFiles().size());
        assertFalse(component2LogFileGroup.isActiveFile(component2TestLog));
        assertTrue(component2LogFileGroup.isActiveFile(component2ActiveFile));

        Map<String, CloudWatchAttemptLogFileInformation> component2AttemptLogFileInfo = new HashMap<>();
        component2AttemptLogFileInfo.put(component2TestLog.hashString(),
                CloudWatchAttemptLogFileInformation.builder()
                        .startPosition(0)
                        .bytesRead(2943)
                        .lastModifiedTime(component2TestLog.lastModified())
                        .fileHash(component2TestLog.hashString())
                        .build());
        component2AttemptLogFileInfo.put(component2ActiveFile.hashString(),
                CloudWatchAttemptLogFileInformation.builder()
                        .startPosition(0)
                        .bytesRead(0)
                        .lastModifiedTime(component2ActiveFile.lastModified())
                        .fileHash(component2ActiveFile.hashString())
                        .build());

        Map<String, CloudWatchAttemptLogFileInformation> component1AttemptLogFileInfo = new HashMap<>();
        component1AttemptLogFileInfo.put(component1TestLog.hashString(),
                CloudWatchAttemptLogFileInformation.builder()
                .startPosition(0)
                .bytesRead(1060)
                .lastModifiedTime(component1TestLog.lastModified())
                .fileHash(component1TestLog.hashString())
                .build());
        component1AttemptLogFileInfo.put(component1ActiveFile.hashString(),
                CloudWatchAttemptLogFileInformation.builder()
                        .startPosition(0)
                        .bytesRead(0)
                        .lastModifiedTime(component1ActiveFile.lastModified())
                        .fileHash(component1ActiveFile.hashString())
                        .build());

        CloudWatchAttemptLogInformation attemptLogInformation1 = CloudWatchAttemptLogInformation.builder()
                .componentName("TestComponent1")
                .attemptLogFileInformationMap(component1AttemptLogFileInfo)
                .logFileGroup(component1LogFileGroup)
                .build();
        CloudWatchAttemptLogInformation attemptLogInformation2 = CloudWatchAttemptLogInformation.builder()
                .componentName("TestComponent2")
                .attemptLogFileInformationMap(component2AttemptLogFileInfo)
                .logFileGroup(component2LogFileGroup)
                .build();
        logStreamsToLogInformationMap.put("testStream", attemptLogInformation1);
        logStreamsToLogInformationMap.put("testStream2", attemptLogInformation2);
        CloudWatchAttempt attempt = new CloudWatchAttempt();
        attempt.setLogStreamsToLogEventsMap(logStreamsToLogInformationMap);
        attempt.setLogStreamUploadedSet(new HashSet<>(Arrays.asList("testStream", "testStream2")));
        doNothing().when(mockUploader).registerAttemptStatus(anyString(), callbackCaptor.capture());
        lenient().doNothing().when(mockUploader).upload(attempt, 1);
        logsUploaderService = new LogManagerService(config, mockUploader, mockMerger, nucleusPaths);
        startServiceOnAnotherThread();
        Thread.sleep(5000);
        callbackCaptor.getValue().accept(attempt);
        assertEquals(component2TestLog.lastModified(), Coerce.toLong(config.find(RUNTIME_STORE_NAMESPACE_TOPIC,
                PERSISTED_COMPONENT_LAST_FILE_PROCESSED_TIMESTAMP, "TestComponent2",
                PERSISTED_LAST_FILE_PROCESSED_TIMESTAMP)));

       Map<String, Object> componentRuntimeConfig1 = config.lookupTopics(RUNTIME_STORE_NAMESPACE_TOPIC,
                                        PERSISTED_COMPONENT_CURRENT_PROCESSING_FILE_INFORMATION_V2, "TestComponent1").toPOJO();

       assertTrue(componentRuntimeConfig1.containsKey(component1TestLog.hashString()));
       assertTrue(componentRuntimeConfig1.containsKey(component1ActiveFile.hashString()));

        LogManagerService.CurrentProcessingFileInformation currentProcessingFileInformation1 =
                LogManagerService.CurrentProcessingFileInformation.convertFromMapOfObjects(
                        (Map<String, Object>) componentRuntimeConfig1.get(component1TestLog.hashString()));
        assertEquals(component1TestLog.hashString(), currentProcessingFileInformation1.getFileHash());
        assertEquals(1060, currentProcessingFileInformation1.getStartPosition());
        assertEquals(component1TestLog.lastModified(), currentProcessingFileInformation1.getLastModifiedTime());

        LogManagerService.CurrentProcessingFileInformation currentProcessingFileInformationActive1 =
                LogManagerService.CurrentProcessingFileInformation.convertFromMapOfObjects(
                        (Map<String, Object>) componentRuntimeConfig1.get(component1ActiveFile.hashString()));
        assertEquals(component1ActiveFile.hashString(), currentProcessingFileInformationActive1.getFileHash());
        assertEquals(0, currentProcessingFileInformationActive1.getStartPosition());
        assertEquals(component1ActiveFile.lastModified(), currentProcessingFileInformationActive1.getLastModifiedTime());


        Map<String, Object> componentRuntimeConfig2 = config.lookupTopics(RUNTIME_STORE_NAMESPACE_TOPIC,
                PERSISTED_COMPONENT_CURRENT_PROCESSING_FILE_INFORMATION_V2, "TestComponent2").toPOJO();

        assertFalse(componentRuntimeConfig2.containsKey(component2TestLog.hashString()));
        assertTrue(componentRuntimeConfig2.containsKey(component2ActiveFile.hashString()));

        LogManagerService.CurrentProcessingFileInformation currentProcessingFileInformationActive2 =
                LogManagerService.CurrentProcessingFileInformation.convertFromMapOfObjects(
                        (Map<String, Object>) componentRuntimeConfig2.get(component2ActiveFile.hashString()));

        assertEquals(component2ActiveFile.hashString(), currentProcessingFileInformationActive2.getFileHash());
        assertEquals(0, currentProcessingFileInformationActive2.getStartPosition());
        assertEquals(component2ActiveFile.lastModified(), currentProcessingFileInformationActive2.getLastModifiedTime());
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
                        .lastModifiedTime(now.toEpochMilli())
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
        assertEquals(now.toEpochMilli(), systemInfo.getLastModifiedTime());
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
