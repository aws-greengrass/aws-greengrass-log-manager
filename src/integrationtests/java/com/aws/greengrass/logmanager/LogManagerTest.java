/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.logmanager;

import com.aws.greengrass.dependency.State;
import com.aws.greengrass.deployment.DeviceConfiguration;
import com.aws.greengrass.deployment.exceptions.DeviceConfigurationException;
import com.aws.greengrass.integrationtests.BaseITCase;
import com.aws.greengrass.lifecyclemanager.Kernel;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.util.exceptions.TLSAuthException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsResponse;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static com.aws.greengrass.deployment.converter.DeploymentDocumentConverter.LOCAL_DEPLOYMENT_GROUP_NAME;
import static com.aws.greengrass.logmanager.CloudWatchAttemptLogsProcessor.DEFAULT_LOG_STREAM_NAME;
import static com.aws.greengrass.logmanager.LogManagerService.DEFAULT_FILE_REGEX;
import static com.aws.greengrass.logmanager.util.LogFileHelper.createTempFileAndWriteData;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith({GGExtension.class, MockitoExtension.class})
class LogManagerTest extends BaseITCase {
    private static final String THING_NAME = "ThingName";
    private static final String AWS_REGION = "us-east-1";
    private static Kernel kernel;
    private static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyyy/MM/dd", Locale.ENGLISH);
    private static DeviceConfiguration deviceConfiguration;
    private LogManagerService logManagerService;
    private Path tempDirectoryPath;

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
        when(cloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class)))
                .thenReturn(PutLogEventsResponse.builder().nextSequenceToken("nextToken").build());

        System.setProperty("root", tempRootDir.toAbsolutePath().toString());
        CountDownLatch logManagerRunning = new CountDownLatch(1);

        CompletableFuture cf = new CompletableFuture();
        cf.complete(null);
        kernel = new Kernel();

        Path testRecipePath = Paths.get(LogManagerTest.class.getResource(configFileName).toURI());
        String content = new String(Files.readAllBytes(testRecipePath), StandardCharsets.UTF_8);
        content = content.replaceAll("\\{\\{logFileDirectoryPath}}",
                storeDirectory.toAbsolutePath().toString());

        Path tempConfigPath = Files.createTempDirectory(tempRootDir, "config").resolve("smallPeriodicIntervalConfig.yaml");
        Files.write(tempConfigPath, content.getBytes(StandardCharsets.UTF_8));

        kernel.parseArgs("-i", tempConfigPath.toAbsolutePath().toString());
        kernel.getContext().addGlobalStateChangeListener((service, oldState, newState) -> {
            if (service.getName().equals(LogManagerService.LOGS_UPLOADER_SERVICE_TOPICS)
                    && newState.equals(State.RUNNING)) {
                logManagerRunning.countDown();
                logManagerService = (LogManagerService) service;
            }
        });
        deviceConfiguration = new DeviceConfiguration(kernel, "ThingName", "xxxxxx-ats.iot.us-east-1.amazonaws.com", "xxxxxx.credentials.iot.us-east-1.amazonaws.com", "privKeyFilePath",
                "certFilePath", "caFilePath", "us-east-1", "roleAliasName");

        kernel.getContext().put(DeviceConfiguration.class, deviceConfiguration);
        // set required instances from context
        kernel.launch();
        assertTrue(logManagerRunning.await(10, TimeUnit.SECONDS));

        logManagerService.getUploader().setCloudWatchLogsClient(cloudWatchLogsClient);
    }

    @BeforeEach
    void beforeEach(ExtensionContext context) {
        ignoreExceptionOfType(context, TLSAuthException.class);
        ignoreExceptionOfType(context, InterruptedException.class);
        ignoreExceptionOfType(context, DateTimeParseException.class);
    }

    @AfterEach
    void afterEach() {
        kernel.shutdown();
    }

    @Test
    void GIVEN_user_component_config_with_small_periodic_interval_WHEN_interval_elapses_THEN_logs_are_uploaded_to_cloud()
            throws Exception {
        tempDirectoryPath = Files.createTempDirectory(tempRootDir, "IntegrationTestsTemporaryLogFiles");

        for (int i = 0; i < 5; i++) {
            createTempFileAndWriteData(tempDirectoryPath, "integTestRandomLogFiles.log_",  "");
        }
        createTempFileAndWriteData(tempDirectoryPath, "integTestRandomLogFiles.log",  "");

        setupKernel(tempDirectoryPath, "smallPeriodicIntervalUserComponentConfig.yaml");
        TimeUnit.SECONDS.sleep(30);
        verify(cloudWatchLogsClient, atLeastOnce()).putLogEvents(captor.capture());

        List<PutLogEventsRequest> putLogEventsRequests = captor.getAllValues();
        assertEquals(1, putLogEventsRequests.size());
        for (PutLogEventsRequest request : putLogEventsRequests) {
            assertEquals(calculateLogStreamName(THING_NAME, LOCAL_DEPLOYMENT_GROUP_NAME), request.logStreamName());
            assertEquals("/aws/greengrass/UserComponent/" + AWS_REGION + "/UserComponentA",
                    request.logGroupName());
            assertNotNull(request.logEvents());
            assertEquals(50, request.logEvents().size());
            assertEquals(51200, request.logEvents().stream().mapToLong(value -> value.message().length())
                    .sum());
        }
        File folder = tempDirectoryPath.toFile();
        Pattern logFileNamePattern = Pattern.compile("^integTestRandomLogFiles.log\\w*");
        List<File> allFiles = new ArrayList<>();
        File[] files = folder.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isFile()
                        && logFileNamePattern.matcher(file.getName()).find()
                        && file.length() > 0) {
                    allFiles.add(file);
                }
            }
        }
        assertEquals(1, allFiles.size());
    }

    @Test
    void GIVEN_system_config_with_small_periodic_interval_WHEN_interval_elapses_THEN_logs_are_uploaded_to_cloud()
            throws Exception {
        LogManager.getRootLogConfiguration().setStoreDirectory(tempRootDir);
        tempDirectoryPath = LogManager.getRootLogConfiguration().getStoreDirectory().resolve("logs");
        String fileName = LogManager.getRootLogConfiguration().getFileName();
        Files.createDirectory(tempDirectoryPath);
        for (int i = 0; i < 5; i++) {
            createTempFileAndWriteData(tempDirectoryPath, fileName, ".log");
        }
        createTempFileAndWriteData(tempDirectoryPath, fileName + ".log", "");

        setupKernel(tempRootDir, "smallPeriodicIntervalSystemComponentConfig.yaml");
        TimeUnit.SECONDS.sleep(30);
        verify(cloudWatchLogsClient, atLeastOnce()).putLogEvents(captor.capture());

        List<PutLogEventsRequest> putLogEventsRequests = captor.getAllValues();
        assertEquals(1, putLogEventsRequests.size());

        for (PutLogEventsRequest request : putLogEventsRequests) {
            assertEquals(calculateLogStreamName(THING_NAME, LOCAL_DEPLOYMENT_GROUP_NAME), request.logStreamName());
            assertEquals("/aws/greengrass/GreengrassSystemComponent/" + AWS_REGION + "/System",
                    request.logGroupName());
            assertNotNull(request.logEvents());
            assertTrue(request.logEvents().size() >= 50);
            assertTrue(request.logEvents().stream().mapToLong(value -> value.message().length()).sum()
                    >= 51200);
        }
        File folder = tempDirectoryPath.toFile();
        Pattern logFileNamePattern = Pattern.compile(String.format(DEFAULT_FILE_REGEX, fileName));
        List<File> allFiles = new ArrayList<>();
        File[] files = folder.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isFile()
                        && logFileNamePattern.matcher(file.getName()).find()
                        && file.length() > 0) {
                    allFiles.add(file);
                }
            }
        }
        assertEquals(1, allFiles.size());
    }
}
