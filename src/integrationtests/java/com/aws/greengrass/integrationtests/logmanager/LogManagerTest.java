/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.integrationtests.logmanager;

import com.aws.greengrass.config.Topics;
import com.aws.greengrass.dependency.State;
import com.aws.greengrass.deployment.DeviceConfiguration;
import com.aws.greengrass.deployment.exceptions.DeviceConfigurationException;
import com.aws.greengrass.integrationtests.BaseITCase;
import com.aws.greengrass.integrationtests.util.ConfigPlatformResolver;
import com.aws.greengrass.lifecyclemanager.Kernel;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.logging.impl.config.LogConfig;
import com.aws.greengrass.logging.impl.config.LogStore;
import com.aws.greengrass.logging.impl.config.model.LogConfigUpdate;
import com.aws.greengrass.logmanager.LogManagerService;
import com.aws.greengrass.logmanager.model.ComponentLogConfiguration;
import com.aws.greengrass.logmanager.model.EventType;
import com.aws.greengrass.logmanager.model.LogFileGroup;
import com.aws.greengrass.logmanager.model.ProcessingFiles;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.util.exceptions.TLSAuthException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.event.Level;
import software.amazon.awssdk.crt.CrtRuntimeException;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsResponse;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static com.aws.greengrass.componentmanager.KernelConfigResolver.CONFIGURATION_CONFIG_KEY;
import static com.aws.greengrass.deployment.converter.DeploymentDocumentConverter.LOCAL_DEPLOYMENT_GROUP_NAME;
import static com.aws.greengrass.integrationtests.logmanager.util.LogFileHelper.DEFAULT_FILE_SIZE;
import static com.aws.greengrass.integrationtests.logmanager.util.LogFileHelper.DEFAULT_LOG_LINE_IN_FILE;
import static com.aws.greengrass.integrationtests.logmanager.util.LogFileHelper.createFileAndWriteData;
import static com.aws.greengrass.integrationtests.logmanager.util.LogFileHelper.createTempFileAndWriteData;
import static com.aws.greengrass.logging.impl.config.LogConfig.newLogConfigFromRootConfig;
import static com.aws.greengrass.logmanager.CloudWatchAttemptLogsProcessor.DEFAULT_LOG_STREAM_NAME;
import static com.aws.greengrass.logmanager.LogManagerService.COMPONENT_LOGS_CONFIG_MAP_TOPIC_NAME;
import static com.aws.greengrass.logmanager.LogManagerService.DEFAULT_FILE_REGEX;
import static com.aws.greengrass.logmanager.LogManagerService.LOGS_UPLOADER_CONFIGURATION_TOPIC;
import static com.aws.greengrass.logmanager.LogManagerService.MIN_LOG_LEVEL_CONFIG_TOPIC_NAME;
import static com.aws.greengrass.logmanager.LogManagerService.PERSISTED_COMPONENT_CURRENT_PROCESSING_FILE_INFORMATION_V2;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionWithMessage;
import static com.github.grantwest.eventually.EventuallyLambdaMatcher.eventuallyEval;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("PMD.UnsynchronizedStaticFormatter")
@ExtendWith({GGExtension.class, MockitoExtension.class})
class LogManagerTest extends BaseITCase {
    @TempDir
    private Path workDir;
    private static final String THING_NAME = "ThingName";
    private static final String AWS_REGION = "us-east-1";
    private static Kernel kernel;
    private static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyyy/MM/dd", Locale.ENGLISH);
    private static DeviceConfiguration deviceConfiguration;
    private LogManagerService logManagerService;
    private Path tempDirectoryPath;
    private final static ObjectMapper YAML_OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());
    private static final int testFileNumber = 4;
    private final Instant mockInstant = Instant.EPOCH;


    static {
        DATE_FORMATTER.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    @Mock
    private CloudWatchLogsClient cloudWatchLogsClient;
    @Captor
    private ArgumentCaptor<PutLogEventsRequest> captor;

    private String calculateLogStreamName(String thingName, String group) {
        synchronized (DATE_FORMATTER) {
            return DEFAULT_LOG_STREAM_NAME
                    .replace("{thingName}", thingName)
                    .replace("{ggFleetId}", group)
                    .replace("{date}", DATE_FORMATTER.format(new Date()));
        }
    }

    void setupKernel(Path storeDirectory, String configFileName) throws InterruptedException,
            URISyntaxException, IOException, DeviceConfigurationException {

        System.setProperty("root", tempRootDir.toAbsolutePath().toString());
        CountDownLatch logManagerRunning = new CountDownLatch(1);

        CompletableFuture<Void> cf = new CompletableFuture<>();
        cf.complete(null);

        Path testRecipePath = Paths.get(LogManagerTest.class.getResource(configFileName).toURI());
        String content = new String(Files.readAllBytes(testRecipePath), StandardCharsets.UTF_8);
        content = content.replaceAll("\\{\\{logFileDirectoryPath}}",
                storeDirectory.toAbsolutePath().toString());

        Map<String, Object> objectMap = YAML_OBJECT_MAPPER.readValue(content, Map.class);
        kernel.parseArgs();
        kernel.getConfig().mergeMap(System.currentTimeMillis(), ConfigPlatformResolver.resolvePlatformMap(objectMap));

        kernel.getContext().addGlobalStateChangeListener((service, oldState, newState) -> {
            if (service.getName().equals(LogManagerService.LOGS_UPLOADER_SERVICE_TOPICS)
                    && newState.equals(State.RUNNING)) {
                logManagerRunning.countDown();
                logManagerService = (LogManagerService) service;
                logManagerService.getUploader().setCloudWatchLogsClient(cloudWatchLogsClient);
            }
        });
        deviceConfiguration = new DeviceConfiguration(kernel, "ThingName", "xxxxxx-ats.iot.us-east-1.amazonaws.com",
                "xxxxxx.credentials.iot.us-east-1.amazonaws.com", "privKeyFilePath",
                "certFilePath", "caFilePath", "us-east-1", "roleAliasName");

        kernel.getContext().put(DeviceConfiguration.class, deviceConfiguration);
        // set required instances from context
        kernel.launch();
        assertTrue(logManagerRunning.await(30, TimeUnit.SECONDS));

    }

    @BeforeEach
    void beforeEach(ExtensionContext context) {
        kernel = new Kernel();
        ignoreExceptionOfType(context, TLSAuthException.class);
        ignoreExceptionOfType(context, InterruptedException.class);
        ignoreExceptionOfType(context, DateTimeParseException.class);
        ignoreExceptionOfType(context, CrtRuntimeException.class);
    }

    @AfterEach
    void afterEach() {
        kernel.shutdown();
    }

    @Test
    void GIVEN_user_component_config_with_small_periodic_interval_WHEN_interval_elapses_THEN_logs_are_uploaded_to_cloud()
            throws Exception {
        when(cloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class)))
                .thenReturn(PutLogEventsResponse.builder().nextSequenceToken("nextToken").build());

        tempDirectoryPath = Files.createTempDirectory(tempRootDir, "IntegrationTestsTemporaryLogFiles");

        for (int i = 0; i < testFileNumber - 1; i++) {
            createTempFileAndWriteData(tempDirectoryPath, "integTestRandomLogFiles.log_", "");
        }
        createTempFileAndWriteData(tempDirectoryPath, "integTestRandomLogFiles.log", "");

        setupKernel(tempDirectoryPath, "smallPeriodicIntervalUserComponentConfig.yaml");

        Runnable waitForActiveFileToBeProcessed = subscribeToActiveFileProcessed(logManagerService, 30);
        waitForActiveFileToBeProcessed.run();
        verify(cloudWatchLogsClient, atLeastOnce()).putLogEvents(captor.capture());

        List<PutLogEventsRequest> putLogEventsRequests = captor.getAllValues();
        assertEquals(1, putLogEventsRequests.size());
        PutLogEventsRequest request = putLogEventsRequests.get(0);
        assertEquals(calculateLogStreamName(THING_NAME, LOCAL_DEPLOYMENT_GROUP_NAME), request.logStreamName());
        assertEquals("/aws/greengrass/UserComponent/" + AWS_REGION + "/UserComponentA", request.logGroupName());
        assertNotNull(request.logEvents());
        assertEquals(DEFAULT_LOG_LINE_IN_FILE * testFileNumber, request.logEvents().size());
        assertEquals(DEFAULT_FILE_SIZE * testFileNumber,
                request.logEvents().stream().mapToLong(value -> value.message().length()).sum());

        Pattern logFileNamePattern = Pattern.compile("^integTestRandomLogFiles.log\\w*");
        ComponentLogConfiguration compLogInfo = ComponentLogConfiguration.builder()
                .directoryPath(tempDirectoryPath)
                .fileNameRegex(logFileNamePattern).name("UserComponentA").build();
        LogFileGroup logFileGroup =
                LogFileGroup.create(compLogInfo, mockInstant, workDir);
        assertEquals(1, logFileGroup.getLogFiles().size());
    }

    @Test
    void GIVEN_user_component_config_with_small_periodic_interval_and_only_required_config_WHEN_interval_elapses_THEN_logs_are_uploaded_to_cloud()
            throws Exception {
        when(cloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class)))
                .thenReturn(PutLogEventsResponse.builder().nextSequenceToken("nextToken").build());

        tempDirectoryPath = Files.createDirectory(tempRootDir.resolve("logs"));
        LogConfig.getRootLogConfig().setLevel(Level.TRACE);
        LogConfig.getRootLogConfig().setStore(LogStore.FILE);
        LogManager.getLogConfigurations().putIfAbsent("UserComponentB",
                newLogConfigFromRootConfig(LogConfigUpdate.builder().fileName("UserComponentB.log").build()));

        for (int i = 0; i < testFileNumber - 1; i++) {
            createFileAndWriteData(tempDirectoryPath, "UserComponentB_" + i);
        }
        createFileAndWriteData(tempDirectoryPath, "UserComponentB");

        setupKernel(tempDirectoryPath, "smallPeriodicIntervalOnlyReqUserComponentConfig.yaml");

        Runnable waitForActiveFileToBeProcessed = subscribeToActiveFileProcessed(logManagerService, 30);
        waitForActiveFileToBeProcessed.run();
        verify(cloudWatchLogsClient, atLeastOnce()).putLogEvents(captor.capture());

        List<PutLogEventsRequest> putLogEventsRequests = captor.getAllValues();
        assertEquals(1, putLogEventsRequests.size());
        PutLogEventsRequest request = putLogEventsRequests.get(0);
        assertEquals(calculateLogStreamName(THING_NAME, LOCAL_DEPLOYMENT_GROUP_NAME), request.logStreamName());
        assertEquals("/aws/greengrass/UserComponent/" + AWS_REGION + "/UserComponentB", request.logGroupName());
        assertNotNull(request.logEvents());
        assertEquals(DEFAULT_LOG_LINE_IN_FILE * testFileNumber, request.logEvents().size());
        assertEquals(DEFAULT_FILE_SIZE * testFileNumber,
                request.logEvents().stream().mapToLong(value -> value.message().length()).sum());
        Pattern logFileNamePattern = Pattern.compile("^UserComponentB\\w*.log");
        ComponentLogConfiguration compLogInfo = ComponentLogConfiguration.builder()
                .directoryPath(tempDirectoryPath)
                .fileNameRegex(logFileNamePattern).name("UserComponentB").build();
        LogFileGroup logFileGroup =
                LogFileGroup.create(compLogInfo, mockInstant, workDir);
        assertEquals(1, logFileGroup.getLogFiles().size());
    }

    @Test
    void GIVEN_system_config_with_small_periodic_interval_WHEN_interval_elapses_THEN_logs_are_uploaded_to_cloud(
            ExtensionContext ec) throws Exception {
        when(cloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class)))
                .thenReturn(PutLogEventsResponse.builder().nextSequenceToken("nextToken").build());

        ignoreExceptionOfType(ec, NoSuchFileException.class);
        LogManager.getRootLogConfiguration().setStoreDirectory(tempRootDir);
        tempDirectoryPath = LogManager.getRootLogConfiguration().getStoreDirectory().resolve("logs");
        String fileName = LogManager.getRootLogConfiguration().getFileName();
        Files.createDirectory(tempDirectoryPath);
        for (int i = 0; i < testFileNumber - 1; i++) {
            createTempFileAndWriteData(tempDirectoryPath, fileName, ".log");
        }
        createTempFileAndWriteData(tempDirectoryPath, fileName + ".log", "");

        setupKernel(tempRootDir, "smallPeriodicIntervalSystemComponentConfig.yaml");
        Runnable waitForActiveFileToBeProcessed = subscribeToActiveFileProcessed(logManagerService, 30);
        waitForActiveFileToBeProcessed.run();

        verify(cloudWatchLogsClient, atLeastOnce()).putLogEvents(captor.capture());

        List<PutLogEventsRequest> putLogEventsRequests = captor.getAllValues();
        assertEquals(1, putLogEventsRequests.size());
        PutLogEventsRequest request = putLogEventsRequests.get(0);
        assertEquals(calculateLogStreamName(THING_NAME, LOCAL_DEPLOYMENT_GROUP_NAME), request.logStreamName());
        assertEquals("/aws/greengrass/GreengrassSystemComponent/" + AWS_REGION + "/System", request.logGroupName());
        assertNotNull(request.logEvents());
        assertTrue(request.logEvents().size() >= DEFAULT_LOG_LINE_IN_FILE * testFileNumber);
        assertTrue(request.logEvents().stream().mapToLong(value -> value.message().length()).sum()
                >= DEFAULT_FILE_SIZE * testFileNumber);

        Pattern logFileNamePattern = Pattern.compile(String.format(DEFAULT_FILE_REGEX, fileName));
        ComponentLogConfiguration compLogInfo = ComponentLogConfiguration.builder()
                .directoryPath(tempDirectoryPath)
                .fileNameRegex(logFileNamePattern).name("System").build();
        LogFileGroup logFileGroup =
                LogFileGroup.create(compLogInfo, mockInstant, workDir);
        assertEquals(1, logFileGroup.getLogFiles().size());
    }

    @Test
    void GIVEN_log_manager_in_errored_state_WHEN_restarted_THEN_logs_upload_is_reattempted(ExtensionContext context)
            throws Exception {
        ignoreExceptionWithMessage(context, "Forcing error to trigger restart");
        CountDownLatch logManagerErrored = new CountDownLatch(1);
        CountDownLatch logManagerRunning = new CountDownLatch(1);
        when(cloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class)))
                .thenAnswer((x)->{
                    logManagerErrored.countDown();
                    throw new RuntimeException("Forcing error to trigger restart");
                })
                .thenAnswer((x)->{
                    logManagerRunning.countDown();
                    return PutLogEventsResponse.builder().nextSequenceToken("nextToken").build();
                });

        tempDirectoryPath = Files.createTempDirectory(tempRootDir, "IntegrationTestsTemporaryLogFiles");


        for (int i = 0; i < testFileNumber - 1; i++) {
            createTempFileAndWriteData(tempDirectoryPath, "integTestRandomLogFiles.log_", "");
        }
        createTempFileAndWriteData(tempDirectoryPath, "integTestRandomLogFiles.log", "");

        setupKernel(tempDirectoryPath, "smallPeriodicIntervalUserComponentConfig.yaml");
        TimeUnit.SECONDS.sleep(30);
        assertTrue(logManagerErrored.await(60, TimeUnit.SECONDS));
        assertTrue(logManagerRunning.await(60, TimeUnit.SECONDS));
        verify(cloudWatchLogsClient, times(2)).putLogEvents(captor.capture());

        List<PutLogEventsRequest> putLogEventsRequests = captor.getAllValues();
        assertEquals(2, putLogEventsRequests.size());
        for (PutLogEventsRequest request : putLogEventsRequests) {
            assertEquals(calculateLogStreamName(THING_NAME, LOCAL_DEPLOYMENT_GROUP_NAME), request.logStreamName());
            assertEquals("/aws/greengrass/UserComponent/" + AWS_REGION + "/UserComponentA", request.logGroupName());
            assertNotNull(request.logEvents());
            assertEquals(DEFAULT_LOG_LINE_IN_FILE * testFileNumber, request.logEvents().size());
            assertEquals(DEFAULT_FILE_SIZE * testFileNumber,
                    request.logEvents().stream().mapToLong(value -> value.message().length()).sum());
        }
        Pattern logFileNamePattern = Pattern.compile("^integTestRandomLogFiles.log\\w*");
        ComponentLogConfiguration compLogInfo = ComponentLogConfiguration.builder()
                .directoryPath(tempDirectoryPath)
                .fileNameRegex(logFileNamePattern).name("UserComponentA").build();
        LogFileGroup logFileGroup =
                LogFileGroup.create(compLogInfo, mockInstant, workDir);
        assertEquals(1, logFileGroup.getLogFiles().size());
    }

    @Test
    @Tag("processingFilesInformation")
    void GIVEN_filesDeletedAfterUpload_THEN_deletedFilesRemovedFromCache() throws Exception {
        // Given

        when(cloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class)))
                .thenReturn(PutLogEventsResponse.builder().nextSequenceToken("nextToken").build());

        int numberOfFiles = 100;
        tempDirectoryPath = Files.createTempDirectory(tempRootDir, "IntegrationTestsTemporaryLogFiles");
        for (int i = 0; i < numberOfFiles; i++) {
            createTempFileAndWriteData(tempDirectoryPath, "integTestRandomLogFiles.log_", "");
        }

        // When

        String componentName = "UserComponentA";
        // This configuration deletes files after upload
        setupKernel(tempDirectoryPath, "smallPeriodicIntervalUserComponentConfig.yaml");

        Runnable waitForActiveFileToBeProcessed = subscribeToActiveFileProcessed(logManagerService, 30);
        waitForActiveFileToBeProcessed.run();
        verify(cloudWatchLogsClient, atLeastOnce()).putLogEvents(captor.capture());

        // Then

        ProcessingFiles processingFiles = logManagerService.processingFilesInformation.get(componentName);
        assertNotNull(processingFiles);
        assertEquals(1, processingFiles.size()); // Active file not deleted

        // Check runtime config gets cleared once the files have deleted
        Topics componentTopics =
                logManagerService.getRuntimeConfig()
                        .lookupTopics(PERSISTED_COMPONENT_CURRENT_PROCESSING_FILE_INFORMATION_V2,
                                componentName);
        assertEquals(1, componentTopics.size());
    }

    @Test
    @Tag("processingFilesInformation")
    void GIVEN_filesNOTDeletedAfterUpload_THEN_filesGetCached() throws Exception {
        // Given

        when(cloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class)))
                .thenReturn(PutLogEventsResponse.builder().nextSequenceToken("nextToken").build());

        int numberOfFiles = 100;
        tempDirectoryPath = Files.createTempDirectory(tempRootDir, "IntegrationTestsTemporaryLogFiles");
        for (int i = 0; i < numberOfFiles; i++) {
            createTempFileAndWriteData(tempDirectoryPath, "integTestRandomLogFiles.log_", "");
        }

        // When
        String componentName = "UserComponentA";
        setupKernel(tempDirectoryPath, "doNotDeleteFilesAfterUpload.yaml");

        Runnable waitForActiveFileToBeProcessed = subscribeToActiveFileProcessed(logManagerService, 30);
        waitForActiveFileToBeProcessed.run();
        verify(cloudWatchLogsClient, atLeastOnce()).putLogEvents(captor.capture());

        // Then

        // Note we shouldn't be accessing methods like this. Refactor this tests later
        ProcessingFiles processingFiles = logManagerService.processingFilesInformation.get(componentName);
        assertNotNull(processingFiles);
        assertEquals(numberOfFiles, processingFiles.size());

        // Check runtime config gets cleared once the files have deleted
        Topics componentTopics =
                logManagerService.getRuntimeConfig()
                        .lookupTopics(PERSISTED_COMPONENT_CURRENT_PROCESSING_FILE_INFORMATION_V2,
                                componentName);
        assertEquals(numberOfFiles, componentTopics.size());
    }

    @Test
    @Tag("processingFilesInformation")
    void GIVEN_filesNOTDeletedAfterUpload_WHEN_removingComponentConfigurationNob_THEN_filesRemovedFromCache() throws
            Exception {
        // Given
        when(cloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class)))
                .thenReturn(PutLogEventsResponse.builder().nextSequenceToken("nextToken").build());

        int numberOfFiles = 100;
        tempDirectoryPath = Files.createTempDirectory(tempRootDir, "IntegrationTestsTemporaryLogFiles");
        for (int i = 0; i < numberOfFiles; i++) {
            createTempFileAndWriteData(tempDirectoryPath, "integTestRandomLogFiles.log_", "");
        }

        // When

        String componentName = "UserComponentA";
        setupKernel(tempDirectoryPath, "doNotDeleteFilesAfterUpload.yaml");

        Runnable waitForActiveFileToBeProcessed = subscribeToActiveFileProcessed(logManagerService, 30);
        waitForActiveFileToBeProcessed.run();
        verify(cloudWatchLogsClient, atLeastOnce()).putLogEvents(captor.capture());

        // Component configuration is removed

        logManagerService.getConfig().lookupTopics(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC,
                COMPONENT_LOGS_CONFIG_MAP_TOPIC_NAME, componentName).remove();

        assertThat(() -> logManagerService.getComponentLogConfigurations().get(componentName) == null,
                eventuallyEval(equalTo(true), Duration.ofSeconds(30)));

        // Then

        ProcessingFiles processingFiles = logManagerService.processingFilesInformation.get(componentName);
        assertNull(processingFiles);

        // Check runtime config gets cleared once the files have deleted
        Topics componentTopics =
                logManagerService.getRuntimeConfig()
                        .lookupTopics(PERSISTED_COMPONENT_CURRENT_PROCESSING_FILE_INFORMATION_V2,
                                componentName);
        assertEquals(0, componentTopics.size());
    }

    @Test
    @Tag("processingFilesInformation")
    void GIVEN_processingFileCached_WHEN_ConfigurationChanges_THEN_existingCachedFileInformationLastAccessedIsNotChanged() throws
            Exception {
        // Given
        when(cloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class)))
                .thenReturn(PutLogEventsResponse.builder().nextSequenceToken("nextToken").build());

        int numberOfFiles = 10;
        tempDirectoryPath = Files.createTempDirectory(tempRootDir, "IntegrationTestsTemporaryLogFiles");
        for (int i = 0; i < numberOfFiles; i++) {
            createTempFileAndWriteData(tempDirectoryPath, "integTestRandomLogFiles.log_", "");
        }

        // When

        String componentName = "UserComponentA";
        setupKernel(tempDirectoryPath, "doNotDeleteFilesAfterUpload.yaml");

        Runnable waitForActiveFileToBeProcessed = subscribeToActiveFileProcessed(logManagerService, 30);
        waitForActiveFileToBeProcessed.run();
        verify(cloudWatchLogsClient, atLeastOnce()).putLogEvents(captor.capture());

        ProcessingFiles processingFilesBefore = logManagerService.processingFilesInformation.get(componentName);
        Map<String, Object> beforeConfigurationUpdate = processingFilesBefore.toMap();

        logManagerService.getConfig().lookup(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC,
                COMPONENT_LOGS_CONFIG_MAP_TOPIC_NAME, componentName,  MIN_LOG_LEVEL_CONFIG_TOPIC_NAME)
                .withValue("WARN");

        assertThat(()-> logManagerService.getComponentLogConfigurations().get(componentName).getMinimumLogLevel(),
                eventuallyEval(is(Level.WARN), Duration.ofSeconds(30)));

        // Then

        ProcessingFiles processingFilesAfter = logManagerService.processingFilesInformation.get(componentName);
        Map<String, Object> afterConfigurationUpdate = processingFilesAfter.toMap();
        assertEquals(beforeConfigurationUpdate, afterConfigurationUpdate);
    }

    private Runnable subscribeToActiveFileProcessed(LogManagerService service, int waitTime) throws
            InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        service.registerEventStatusListener((EventType event) -> {
            if (event == EventType.ALL_COMPONENTS_PROCESSED) {
                latch.countDown();
            }
        });

        return () -> {
            try {
                latch.await(waitTime, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                //do nothing
            }
        };
    }
}
