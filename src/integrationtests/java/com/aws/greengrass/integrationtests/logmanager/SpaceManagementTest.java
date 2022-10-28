/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.integrationtests.logmanager;

import com.aws.greengrass.dependency.State;
import com.aws.greengrass.deployment.DeviceConfiguration;
import com.aws.greengrass.deployment.exceptions.DeviceConfigurationException;
import com.aws.greengrass.integrationtests.BaseITCase;
import com.aws.greengrass.lifecyclemanager.Kernel;
import com.aws.greengrass.logging.impl.GreengrassLogMessage;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.logmanager.LogManagerService;
import com.aws.greengrass.logmanager.model.LogFileGroup;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.util.exceptions.TLSAuthException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
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
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import static com.aws.greengrass.integrationtests.logmanager.util.LogFileHelper.createTempFileAndWriteData;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static com.aws.greengrass.testcommons.testutilities.TestUtils.createCloseableLogListener;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
@Disabled("fix it - it is flaky")
@ExtendWith({GGExtension.class, MockitoExtension.class})
class SpaceManagementTest extends BaseITCase {
    private static Kernel kernel;
    private static DeviceConfiguration deviceConfiguration;
    private LogManagerService logManagerService;
    private Path tempDirectoryPath;
    private static final Instant mockInstant = Instant.EPOCH;

    @Mock
    private CloudWatchLogsClient cloudWatchLogsClient;

    @BeforeEach
    void beforeEach(ExtensionContext context) {
        ignoreExceptionOfType(context, InterruptedException.class);
        ignoreExceptionOfType(context, TLSAuthException.class);
        ignoreExceptionOfType(context, NoSuchFileException.class);
        ignoreExceptionOfType(context, DateTimeParseException.class);
        ignoreExceptionOfType(context, CrtRuntimeException.class);
        LogManager.getRootLogConfiguration().setLevel(Level.DEBUG);
    }

    @AfterEach
    void afterEach() {
        kernel.shutdown();
        LogManager.getRootLogConfiguration().setLevel(Level.INFO);
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
        logManagerService.lastComponentUploadedLogFileInstantMap.put("UserComponentA",
                Instant.now().plus(10, ChronoUnit.MINUTES));
    }

    @Test
    void GIVEN_user_component_config_with_space_management_WHEN_space_exceeds_THEN_excess_log_files_are_deleted()
            throws Exception {
        tempDirectoryPath = Files.createDirectory(tempRootDir.resolve("IntegrationTestsTemporaryLogFiles"));

        setupKernel(tempDirectoryPath);
        TimeUnit.SECONDS.sleep(10);

        CountDownLatch cdl = new CountDownLatch(5);
        Consumer<GreengrassLogMessage> listener = (m) -> {
            if (m.getMessage().contains("Successfully deleted file")) {
                cdl.countDown();
            }
        };
        try (AutoCloseable l = createCloseableLogListener(listener)) {
            for (int i = 0; i < 15; i++) {
                createTempFileAndWriteData(tempDirectoryPath, "integTestRandomLogFiles.log_", "");
            }
            //Todo: we should not be checking for log messages to verify if a log file was deleted.
            assertTrue(cdl.await(60, TimeUnit.SECONDS), "5 files deleted");
        }

        Pattern logFileNamePattern = Pattern.compile("^integTestRandomLogFiles.log\\w*");
        LogFileGroup logFileGroup = LogFileGroup.create(logFileNamePattern, tempDirectoryPath.toUri(), mockInstant);
        assertEquals(10, logFileGroup.getLogFiles().size());
    }
}
