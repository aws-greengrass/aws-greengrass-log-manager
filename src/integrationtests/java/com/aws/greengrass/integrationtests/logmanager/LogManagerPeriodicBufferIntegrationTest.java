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
import com.aws.greengrass.logmanager.LogManagerService;
import com.aws.greengrass.logmanager.model.ProcessingFiles;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.util.Coerce;
import com.aws.greengrass.util.exceptions.TLSAuthException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.time.format.DateTimeParseException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.crt.CrtRuntimeException;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsResponse;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.aws.greengrass.integrationtests.logmanager.util.LogFileHelper.createTempFileAndWriteData;
import static com.aws.greengrass.logmanager.LogManagerService.BUFFER_INTERVAL_SEC;
import static com.aws.greengrass.logmanager.LogManagerService.LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC;
import static com.aws.greengrass.logmanager.LogManagerService.LOGS_UPLOADER_SERVICE_TOPICS;
import static com.aws.greengrass.logmanager.LogManagerService.PERSISTED_COMPONENT_CURRENT_PROCESSING_FILE_INFORMATION_V2;
import static com.aws.greengrass.logmanager.LogManagerService.PERSISTED_COMPONENT_LAST_FILE_PROCESSED_TIMESTAMP;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith({GGExtension.class, MockitoExtension.class})
class LogManagerPeriodicBufferIntegrationTest extends BaseITCase {
    private static Kernel kernel;
    private static DeviceConfiguration deviceConfiguration;
    private LogManagerService logManagerService;
    private Path tempDirectoryPath;
    private final static ObjectMapper YAML_OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());

    @Mock
    private CloudWatchLogsClient cloudWatchLogsClient;
    @Captor
    private ArgumentCaptor<PutLogEventsRequest> captor;

    void setupKernel(Path storeDirectory, String configFileName)
            throws InterruptedException, URISyntaxException, IOException, DeviceConfigurationException {

        System.setProperty("root", tempRootDir.toAbsolutePath().toString());
        CountDownLatch logManagerRunning = new CountDownLatch(1);

        Path testRecipePath = Paths.get(LogManagerTest.class.getResource(configFileName).toURI());
        String content = new String(Files.readAllBytes(testRecipePath), StandardCharsets.UTF_8);
        content = content.replaceAll("\\{\\{logFileDirectoryPath}}",
                storeDirectory.toAbsolutePath().toString());

        Map<String, Object> objectMap = YAML_OBJECT_MAPPER.readValue(content, Map.class);

        kernel.parseArgs();
        kernel.getConfig().mergeMap(System.currentTimeMillis(), ConfigPlatformResolver.resolvePlatformMap(objectMap));

        kernel.getContext().addGlobalStateChangeListener((service, oldState, newState) -> {
            if (service.getName().equals(LOGS_UPLOADER_SERVICE_TOPICS)
                    && newState.equals(State.RUNNING)) {
                logManagerService = (LogManagerService) service;
                logManagerService.getUploader().getCloudWatchWrapper().setClient(cloudWatchLogsClient);
                logManagerRunning.countDown();
            }
        });
        deviceConfiguration = new DeviceConfiguration(kernel, "ThingName", "xxxxxx-ats.iot.us-east-1.amazonaws.com",
                "xxxxxx.credentials.iot.us-east-1.amazonaws.com", "privKeyFilePath",
                "certFilePath", "caFilePath", "us-east-1", "roleAliasName");

        kernel.getContext().put(DeviceConfiguration.class, deviceConfiguration);
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
    void GIVEN_user_component_config_with_periodic_interval_and_buffer_inverval_time_THEN_config_update_by_buffer() throws Exception {
        // Given randomly generated logs
        tempDirectoryPath = Files.createTempDirectory(tempRootDir, "IntegrationTestsTemporaryLogFiles");
        for (int i = 0; i < 5; i++) {
            createTempFileAndWriteData(tempDirectoryPath, "integTestRandomLogFiles.log_", Coerce.toString(i));
        }
        setupKernel(tempDirectoryPath, "smallPeriodicIntervalUserComponentConfig.yaml");

        // When
        String componentName = "UserComponentA";
        
        // Track how many times CloudWatch upload is called
        AtomicInteger uploadCount = new AtomicInteger(0);
        CountDownLatch uploadLatch = new CountDownLatch(2);
        CountDownLatch firstUploadLatch = new CountDownLatch(1);
        
        // Buffer flush countdown mechanism for each component map
        Map<String, AtomicInteger> bufferFlushCountdown = new HashMap<>();
        bufferFlushCountdown.put(componentName, new AtomicInteger(2)); // Expect 2 buffer flushes
        CountDownLatch bufferFlushLatch = new CountDownLatch(2);


        when(cloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class))).thenAnswer(invocation -> {
            uploadCount.incrementAndGet();
            firstUploadLatch.countDown();
            uploadLatch.countDown();
            return PutLogEventsResponse.builder().nextSequenceToken("nextToken").build();
        });

        // Subscribe to buffer flush events for countdown tracking and by this time, buffer shouldn't flush
        logManagerService.getRuntimeConfig()
                .lookupTopics(PERSISTED_COMPONENT_LAST_FILE_PROCESSED_TIMESTAMP, componentName)
                .subscribe((why, node) -> {
                    if (node != null && bufferFlushCountdown.containsKey(componentName)) {
                        int remaining = bufferFlushCountdown.get(componentName).decrementAndGet();
                        bufferFlushLatch.countDown();
                        System.out.println("Buffer flush countdown for " + componentName + ": " + remaining + " remaining");
                    }
                });

        // Check initial state of runtime config. should be 0 since no flush has been performed by buffer
        Topics initialComponentTopics = logManagerService.getRuntimeConfig()
                .lookupTopics(PERSISTED_COMPONENT_LAST_FILE_PROCESSED_TIMESTAMP, componentName);
        assertEquals(0, initialComponentTopics.size());

        // Wait for first upload to ensure log manager is processing
        assertTrue(firstUploadLatch.await(15, TimeUnit.SECONDS),
                "Expected first CloudWatch upload within 15 seconds");

        // Then write more logs and wait for the log manager to upload log
        for (int i = 5; i < 10; i++) {
            createTempFileAndWriteData(tempDirectoryPath, "integTestRandomLogFiles.log_", Coerce.toString(i));
        }
        assertTrue(uploadLatch.await(15, TimeUnit.SECONDS),
                "Expected 2 CloudWatch uploads within 15 seconds");

        // Wait for buffer flushes
        assertTrue(bufferFlushLatch.await(10, TimeUnit.SECONDS),
                "Expected buffer flush countdown to complete within 10 seconds");

        // Verify countdown reached zero, this includes the flush action for both maps
        assertEquals(0, bufferFlushCountdown.get(componentName).get(),
                "Buffer flush countdown should reach zero");

        // Verify processing files information is correctly persisted
        ProcessingFiles processingFiles = logManagerService.processingFilesInformation.get(componentName);
        assertNotNull(processingFiles);
        assertEquals(processingFiles.size(), 1);

        // Verify buffer has flushed exactly once by checking runtime config
        Topics afterFlushComponentTopics = logManagerService.getRuntimeConfig()
                .lookupTopics(PERSISTED_COMPONENT_LAST_FILE_PROCESSED_TIMESTAMP, componentName);
        assertEquals(afterFlushComponentTopics.size(), 1);

        // Verify last uploaded timestamp is also persisted
        Topics lastFileProcessedTopics = logManagerService.getRuntimeConfig()
                .lookupTopics(PERSISTED_COMPONENT_LAST_FILE_PROCESSED_TIMESTAMP, componentName);
        assertEquals(lastFileProcessedTopics.size(), 1);
    }

    @Test
    void testLogManagerShutdownFlushesBuffer() throws Exception {
        tempDirectoryPath = Files.createTempDirectory(tempRootDir, "IntegrationTestsTemporaryLogFiles");
        
        // Create initial test files
        for (int i = 0; i < 5; i++) {
            createTempFileAndWriteData(tempDirectoryPath, "integTestRandomLogFiles.log_", Coerce.toString(i));
        }
        
        setupKernel(tempDirectoryPath, "smallPeriodicIntervalUserComponentConfig.yaml");

        String componentName = "UserComponentA";
        
        // STEP 1: Track buffer flushes - we will verify a scheduled flush happens first
        AtomicInteger bufferFlushCount = new AtomicInteger(0);
        AtomicReference<Long> lastScheduledFlushTime = new AtomicReference<>(0L);
        CountDownLatch scheduledFlushLatch = new CountDownLatch(1);
        
        // Track CloudWatch uploads
        AtomicInteger uploadCount = new AtomicInteger(0);
        CountDownLatch firstUploadLatch = new CountDownLatch(1);
        CountDownLatch secondUploadLatch = new CountDownLatch(2);
        
        when(cloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class))).thenAnswer(invocation -> {
            uploadCount.incrementAndGet();
            firstUploadLatch.countDown();
            secondUploadLatch.countDown();
            return PutLogEventsResponse.builder().nextSequenceToken("nextToken").build();
        });
        
        // Subscribe to runtime config changes to detect buffer flushes
        logManagerService.getRuntimeConfig()
                .lookupTopics(PERSISTED_COMPONENT_LAST_FILE_PROCESSED_TIMESTAMP, componentName)
                .subscribe((why, node) -> {
                    if (node != null && node.childOf(PERSISTED_COMPONENT_LAST_FILE_PROCESSED_TIMESTAMP)) {
                        int flushNum = bufferFlushCount.incrementAndGet();
                        long flushTime = System.currentTimeMillis();
                        
                        // Record the timestamp of the first flush (scheduled flush)
                        if (flushNum == 1) {
                            lastScheduledFlushTime.set(flushTime);
                            scheduledFlushLatch.countDown();
                            System.out.println("First scheduled buffer flush detected at: " + flushTime);
                        } else {
                            System.out.println("Additional buffer flush detected at: " + flushTime);
                        }
                    }
                });
        
        // Wait for initial data to be processed and first scheduled flush to happen
        assertTrue(firstUploadLatch.await(10, TimeUnit.SECONDS),
                "Expected first CloudWatch upload within 10 seconds");
        assertTrue(scheduledFlushLatch.await(20, TimeUnit.SECONDS),
                "Expected the first scheduled buffer flush within 20 seconds");
        
        // STEP 2: Verify the first scheduled flush happened
        assertEquals(1, bufferFlushCount.get(), 
                "Should have exactly one buffer flush at this point (the scheduled flush)");

        // Now add more files to ensure buffer has new data that hasn't been flushed yet
        for (int i = 5; i < 10; i++) {
            createTempFileAndWriteData(tempDirectoryPath, "integTestRandomLogFiles.log_", Coerce.toString(i));
        }
        
        // Wait briefly for log manager to process the new files
        // but not long enough for another scheduled flush to happen
        assertTrue(secondUploadLatch.await(10, TimeUnit.SECONDS),
                "Expected second CloudWatch upload within 10 seconds");
        // Verify we have pending data in buffer
        ProcessingFiles processingFiles = logManagerService.processingFilesInformation.get(componentName);
        assertNotNull(processingFiles, "Processing files should exist for the component");
        // Buffer should contain data waiting to be flushed
        assertThat(processingFiles.size(), greaterThan(0));
        
        // Record the number of flushes before shutdown
        int flushesBeforeShutdown = bufferFlushCount.get();
        
        // STEP 3: Shutdown LogManagerService before next scheduled flush
        // This should trigger a manual flush during shutdown
        kernel.shutdown();
        
        // Wait briefly for shutdown to complete
        Thread.sleep(2000);
        
        // STEP 4: Verify that an additional flush happened during shutdown
        // An additional flush should have occurred during shutdown
        assertThat(bufferFlushCount.get(), greaterThan(flushesBeforeShutdown));
    }

    @Test
    void testBufferAccumulatesUpdatesCorrectly() throws Exception {
        // Given
        tempDirectoryPath = Files.createTempDirectory(tempRootDir, "IntegrationTestsTemporaryLogFiles");

        // Create initial test files
        for (int i = 0; i < 3; i++) {
            createTempFileAndWriteData(tempDirectoryPath, "integTestRandomLogFiles.log_", "");
        }

        // Configure with short intervals for faster testing
        Map<String, Object> additionalConfig = new HashMap<>();
        additionalConfig.put(LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC, 1);
        additionalConfig.put(BUFFER_INTERVAL_SEC, 3);

        setupKernel(tempDirectoryPath, "smallPeriodicIntervalUserComponentConfig.yaml");

        // When
        String componentName = "UserComponentA";
        
        // Track CloudWatch uploads and buffer flushes
        AtomicInteger uploadCount = new AtomicInteger(0);
        AtomicInteger bufferFlushCount = new AtomicInteger(0);
        CountDownLatch firstBufferFlush = new CountDownLatch(1);
        
        when(cloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class))).thenAnswer(invocation -> {
            uploadCount.incrementAndGet();
            return PutLogEventsResponse.builder().nextSequenceToken("nextToken").build();
        });

        // Subscribe to detect buffer flushes
        logManagerService.getRuntimeConfig()
                .lookupTopics(PERSISTED_COMPONENT_CURRENT_PROCESSING_FILE_INFORMATION_V2, componentName)
                .subscribe((why, node) -> {
                    if (node != null && node.childOf(PERSISTED_COMPONENT_CURRENT_PROCESSING_FILE_INFORMATION_V2)) {
                        Topics currentTopics = logManagerService.getRuntimeConfig()
                                .lookupTopics(PERSISTED_COMPONENT_CURRENT_PROCESSING_FILE_INFORMATION_V2, componentName);
                        if (currentTopics.size() > 0) {
                            bufferFlushCount.incrementAndGet();
                            firstBufferFlush.countDown();
                        }
                    }
                });

        // Wait for first buffer flush
        assertTrue(firstBufferFlush.await(10, TimeUnit.SECONDS), 
                "Expected first buffer flush within 10 seconds");

        // Then
        // Verify that multiple uploads happened before buffer flush
        assertThat(uploadCount.get(), greaterThanOrEqualTo(2));
        
        // Verify buffer flush happened
        assertThat(bufferFlushCount.get(), greaterThanOrEqualTo(1));
        
        // Verify CloudWatch uploads happened more frequently than buffer flushes
        assertTrue(uploadCount.get() > bufferFlushCount.get(), 
                "Upload count should be greater than buffer flush count");
    }
}
