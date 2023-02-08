/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.integrationtests.logmanager;

import ch.qos.logback.core.util.FileSize;
import com.aws.greengrass.config.Topics;
import com.aws.greengrass.config.UpdateBehaviorTree;
import com.aws.greengrass.dependency.State;
import com.aws.greengrass.deployment.DeviceConfiguration;
import com.aws.greengrass.deployment.exceptions.DeviceConfigurationException;
import com.aws.greengrass.integrationtests.BaseITCase;
import com.aws.greengrass.lifecyclemanager.Kernel;
import com.aws.greengrass.logmanager.LogManagerService;
import com.aws.greengrass.logmanager.exceptions.InvalidLogGroupException;
import com.aws.greengrass.logmanager.model.ComponentLogConfiguration;
import com.aws.greengrass.logmanager.model.LogFileGroup;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.util.exceptions.TLSAuthException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.crt.CrtRuntimeException;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsResponse;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static com.aws.greengrass.componentmanager.KernelConfigResolver.CONFIGURATION_CONFIG_KEY;
import static com.aws.greengrass.integrationtests.logmanager.util.LogFileHelper.createTempFileAndWriteData;
import static com.aws.greengrass.logmanager.LogManagerService.COMPONENT_LOGS_CONFIG_MAP_TOPIC_NAME;
import static com.aws.greengrass.logmanager.LogManagerService.DELETE_LOG_FILES_AFTER_UPLOAD_CONFIG_TOPIC_NAME;
import static com.aws.greengrass.logmanager.LogManagerService.LOGS_UPLOADER_CONFIGURATION_TOPIC;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static com.github.grantwest.eventually.EventuallyLambdaMatcher.eventuallyEval;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;

@ExtendWith({GGExtension.class, MockitoExtension.class})
class SpaceManagementTest extends BaseITCase {
    private static Kernel kernel;
    private static DeviceConfiguration deviceConfiguration;
    private LogManagerService logManagerService;
    private Path tempDirectoryPath;
    private static final Instant mockInstant = Instant.EPOCH;
    @TempDir
    private Path workDir;
    private static final String componentName = "UserComponentA";

    @Mock
    private CloudWatchLogsClient cloudWatchLogsClient;

    @BeforeEach
    void beforeEach(ExtensionContext context) {
        ignoreExceptionOfType(context, InterruptedException.class);
        ignoreExceptionOfType(context, TLSAuthException.class);
        ignoreExceptionOfType(context, NoSuchFileException.class);
        ignoreExceptionOfType(context, DateTimeParseException.class);
        ignoreExceptionOfType(context, CrtRuntimeException.class);
    }

    @AfterEach
    void afterEach() {
        kernel.shutdown();
    }

    private long writeNLogFiles(Path path, int numberOfFiles, String pattern) throws IOException {
        long totalLength = 0;

        for (int i = 0; i < numberOfFiles; i++) {
            File file = createTempFileAndWriteData(path, pattern, "");
            totalLength+= file.length();
        }

        return totalLength;
    }

    private long getTotalLogFilesBytesFor(ComponentLogConfiguration logConfiguration) throws InvalidLogGroupException {
        LogFileGroup logFileGroup = LogFileGroup.create(logConfiguration, mockInstant, workDir);
        return logFileGroup.totalSizeInBytes();
    }

    private void assertLogFileSizeEventuallyBelowBytes(ComponentLogConfiguration logConfiguration, long bytes) {
        assertThat(String.format("log group size should eventually be less than %s bytes", bytes),() -> {
            try {
                long logFilesSize = getTotalLogFilesBytesFor(logConfiguration);
                return logFilesSize <=  bytes;
            } catch (InvalidLogGroupException e) {
                return false;
            }
        }, eventuallyEval(is(true), Duration.ofSeconds(60)));
    }

    private void assertLogFilesAreNotDeleted(ComponentLogConfiguration logConfiguration) throws
            InvalidLogGroupException, InterruptedException {
        long originalBytes = getTotalLogFilesBytesFor(logConfiguration);
        Instant deadline = Instant.now().plusSeconds(20);

        while (Instant.now().isBefore(deadline)) {
            long currentBytes = getTotalLogFilesBytesFor(logConfiguration);
            assertEquals(originalBytes, currentBytes);
            Thread.sleep(500);
        }
    }


    void setupKernel(Path storeDirectory)
            throws InterruptedException, URISyntaxException, IOException, DeviceConfigurationException {
        lenient().when(cloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class)))
                .thenReturn(PutLogEventsResponse.builder().nextSequenceToken("nextToken").build());
        System.setProperty("root", tempRootDir.toAbsolutePath().toString());
        CountDownLatch logManagerRunning = new CountDownLatch(1);

        kernel = new Kernel();

        Path testRecipePath = Paths.get(LogManagerTest.class
                .getResource("smallSpaceManagementPeriodicIntervalConfig.yaml").toURI());
        String content = new String(Files.readAllBytes(testRecipePath), StandardCharsets.UTF_8);
        content = content.replaceAll("\\{\\{logFileDirectoryPath}}", storeDirectory.toAbsolutePath().toString());

        Path tempConfigPath = Files.createTempDirectory(tempRootDir, "config").resolve("smallSpaceManagementPeriodicIntervalConfig.yaml");
        Files.write(tempConfigPath, content.getBytes(StandardCharsets.UTF_8));

        kernel.parseArgs("-i", tempConfigPath.toAbsolutePath().toString());
        kernel.getContext().addGlobalStateChangeListener((service, oldState, newState) -> {
            if (service.getName().equals(LogManagerService.LOGS_UPLOADER_SERVICE_TOPICS)
                    && newState.equals(State.RUNNING)) {
                logManagerRunning.countDown();
                logManagerService = (LogManagerService) service;
            }
        });
        deviceConfiguration = new DeviceConfiguration(kernel, "ThingName", "xxxxxx-ats.iot.us-east-1.amazonaws.com",
                "xxxxxx.credentials.iot.us-east-1.amazonaws.com", "privKeyFilePath", "certFilePath", "caFilePath",
                "us-east-1", "roleAliasName");
        kernel.getContext().put(DeviceConfiguration.class, deviceConfiguration);
        // set required instances from context
        kernel.launch();
        assertTrue(logManagerRunning.await(10, TimeUnit.SECONDS));
        logManagerService.getUploader().setCloudWatchLogsClient(cloudWatchLogsClient);
        logManagerService.lastComponentUploadedLogFileInstantMap.put(componentName,
                Instant.now().plus(10, ChronoUnit.MINUTES));
    }

    @Test
    void GIVEN_user_component_config_with_space_management_WHEN_space_exceeds_THEN_excess_log_files_are_deleted()
            throws Exception {
        // Given

        tempDirectoryPath = Files.createDirectory(tempRootDir.resolve("IntegrationTestsTemporaryLogFiles"));
        // This method configures the LogManager to get logs with the pattern ^integTestRandomLogFiles.log\w* inside
        // then tempDirectoryPath with a diskSpaceLimit of 105kb
        setupKernel(tempDirectoryPath);

        // When

        // The total size will be 150kb (each log file written is 10kb * 15) given the max space is 105 kb, then we
        // expect it to delete 5 files for the total log size to be under 105kb
        writeNLogFiles(tempDirectoryPath,15, "integTestRandomLogFiles.log_");


        // Then

        ComponentLogConfiguration compLogInfo = ComponentLogConfiguration.builder()
                .directoryPath(tempDirectoryPath)
                .fileNameRegex(Pattern.compile("^integTestRandomLogFiles.log\\w*"))
                .name("IntegrationTestsTemporaryLogFiles")
                .build();
        assertLogFileSizeEventuallyBelowBytes(compLogInfo, 105 * FileSize.KB_COEFFICIENT);
    }

    @Test
    void GIVEN_diskSpaceManagementConfigured_WHEN_configurationRemoved_THEN_logFilesStopBeingDeleted() throws Exception {
        // Given
        tempDirectoryPath = Files.createDirectory(tempRootDir.resolve("IntegrationTestsTemporaryLogFiles"));
        setupKernel(tempDirectoryPath); // starts the LM with the smallSpaceManagementPeriodicIntervalConfig.yaml config

        long totalBytes = writeNLogFiles(tempDirectoryPath,15, "integTestRandomLogFiles.log_");
        long totalKb = totalBytes / FileSize.KB_COEFFICIENT;
        assertTrue(totalKb > 105); // 105 Kb is the amount on the configuration

        // Then - wait for the files to be deleted

        ComponentLogConfiguration compLogInfo = ComponentLogConfiguration.builder()
                .directoryPath(tempDirectoryPath)
                .fileNameRegex(Pattern.compile("^integTestRandomLogFiles.log\\w*"))
                .name("IntegrationTestsTemporaryLogFiles")
                .build();
        assertLogFileSizeEventuallyBelowBytes(compLogInfo, 105 * FileSize.KB_COEFFICIENT);

        // When

        Topics topics = kernel.locate(LogManagerService.LOGS_UPLOADER_SERVICE_TOPICS).getConfig();
        Map<String, Object> componentConfig = new HashMap<String, Object>(){{
            put(componentName, new HashMap<String, String>(){{
                put(DELETE_LOG_FILES_AFTER_UPLOAD_CONFIG_TOPIC_NAME, "false");
            }});
        }};
        UpdateBehaviorTree behaviour = new UpdateBehaviorTree(UpdateBehaviorTree.UpdateBehavior.REPLACE, System.currentTimeMillis());
        topics.lookupTopics(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC, COMPONENT_LOGS_CONFIG_MAP_TOPIC_NAME)
            .updateFromMap(componentConfig, behaviour);
        kernel.getContext().waitForPublishQueueToClear();

        // When

        writeNLogFiles(tempDirectoryPath,5, "integTestRandomLogFiles.log_");
        long kbInDisk = getTotalLogFilesBytesFor(compLogInfo) * FileSize.KB_COEFFICIENT;
        assertTrue(kbInDisk > 105); // 105 Kb is the amount on the configuration

        // Then
        assertLogFilesAreNotDeleted(compLogInfo);
    }
}
