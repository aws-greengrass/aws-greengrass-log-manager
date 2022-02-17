/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.integrationtests.logmanager;

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
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.util.exceptions.TLSAuthException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.event.Level;
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
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
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
import static com.aws.greengrass.integrationtests.logmanager.util.LogFileHelper.createFileAndWriteData;
import static com.aws.greengrass.integrationtests.logmanager.util.LogFileHelper.createTempFileAndWriteData;
import static com.aws.greengrass.logging.impl.config.LogConfig.newLogConfigFromRootConfig;
import static com.aws.greengrass.logmanager.CloudWatchAttemptLogsProcessor.DEFAULT_LOG_STREAM_NAME;
import static com.aws.greengrass.logmanager.LogManagerService.COMPONENT_LOGS_CONFIG_MAP_TOPIC_NAME;
import static com.aws.greengrass.logmanager.LogManagerService.DEFAULT_FILE_REGEX;
import static com.aws.greengrass.logmanager.LogManagerService.DELETE_LOG_FILES_AFTER_UPLOAD_CONFIG_TOPIC_NAME;
import static com.aws.greengrass.logmanager.LogManagerService.DISK_SPACE_LIMIT_CONFIG_TOPIC_NAME;
import static com.aws.greengrass.logmanager.LogManagerService.DISK_SPACE_LIMIT_UNIT_CONFIG_TOPIC_NAME;
import static com.aws.greengrass.logmanager.LogManagerService.FILE_DIRECTORY_PATH_CONFIG_TOPIC_NAME;
import static com.aws.greengrass.logmanager.LogManagerService.FILE_REGEX_CONFIG_TOPIC_NAME;
import static com.aws.greengrass.logmanager.LogManagerService.LOGS_UPLOADER_CONFIGURATION_TOPIC;
import static com.aws.greengrass.logmanager.LogManagerService.LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC;
import static com.aws.greengrass.logmanager.LogManagerService.MIN_LOG_LEVEL_CONFIG_TOPIC_NAME;
import static com.aws.greengrass.logmanager.LogManagerService.SYSTEM_LOGS_COMPONENT_NAME;
import static com.aws.greengrass.logmanager.LogManagerService.SYSTEM_LOGS_CONFIG_TOPIC_NAME;
import static com.aws.greengrass.logmanager.LogManagerService.UPLOAD_TO_CW_CONFIG_TOPIC_NAME;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static com.github.grantwest.eventually.EventuallyLambdaMatcher.eventuallyEval;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.TestInstance.Lifecycle;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;

@SuppressWarnings("PMD.UnsynchronizedStaticFormatter")
@ExtendWith({GGExtension.class, MockitoExtension.class})
@TestInstance(Lifecycle.PER_CLASS)
class LogManagerConfigTest extends BaseITCase {
    private static final String THING_NAME = "ThingName";
    private static final String AWS_REGION = "us-east-1";
    private static Kernel kernel;
    private static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyyy/MM/dd", Locale.ENGLISH);
    private static DeviceConfiguration deviceConfiguration;
    private static LogManagerService logManagerService;
    private static Path tempDirectoryPath;
    private final static ObjectMapper YAML_OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());


    static {
        DATE_FORMATTER.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    @Mock
    private CloudWatchLogsClient cloudWatchLogsClient;


    void setupKernel(String configFileName) throws InterruptedException,
            URISyntaxException, IOException, DeviceConfigurationException {
        //        //From
        Path tempRootDir = Paths.get(System.getProperty("root"));
        tempDirectoryPath = Files.createTempDirectory(tempRootDir, "IntegrationTestsTemporaryLogFiles");

//        lenient().when(cloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class)))
//                .thenReturn(PutLogEventsResponse.builder().nextSequenceToken("nextToken").build());

        System.setProperty("root", tempRootDir.toAbsolutePath().toString());
        CountDownLatch logManagerRunning = new CountDownLatch(1);

        CompletableFuture<Void> cf = new CompletableFuture<>();
        cf.complete(null);
        kernel = new Kernel();

        Path testRecipePath = Paths.get(LogManagerTest.class.getResource(configFileName).toURI());
        String content = new String(Files.readAllBytes(testRecipePath), StandardCharsets.UTF_8);
        content = content.replaceAll("\\{\\{logFileDirectoryPath}}",
                tempDirectoryPath.toAbsolutePath().toString());

        Map<String, Object> objectMap = YAML_OBJECT_MAPPER.readValue(content, Map.class);
        kernel.parseArgs();
        kernel.getConfig().mergeMap(System.currentTimeMillis(), ConfigPlatformResolver.resolvePlatformMap(objectMap));

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

    @BeforeAll
    void beforeAll() throws Exception {

////        //From
//        Path tempRootDir = Paths.get(System.getProperty("root"));
//        tempDirectoryPath = Files.createTempDirectory(tempRootDir, "IntegrationTestsTemporaryLogFiles");
        setupKernel("configsDifferentFromDefaults.yaml");
    }


    @BeforeEach
    void beforeEach(ExtensionContext context) {
        ignoreExceptionOfType(context, TLSAuthException.class);
        ignoreExceptionOfType(context, InterruptedException.class);
        ignoreExceptionOfType(context, DateTimeParseException.class);
        ignoreExceptionOfType(context, CrtRuntimeException.class);

//        lenient().when(cloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class)))
//                .thenReturn(PutLogEventsResponse.builder().nextSequenceToken("nextToken").build());
    }

    @AfterEach
    void afterEach() {
        kernel.shutdown();
    }

    @Test
    void GIVEN_system_and_component_configs_WHEN_individual_configs_are_reset_and_replaced_THEN_correct_values_are_used()
            throws Exception {
        String componentName = "UserComponentA";
        Level minimumLogLevelDefault =  Level.INFO;

//        tempDirectoryPath = Files.createTempDirectory(tempRootDir, "IntegrationTestsTemporaryLogFiles");
//        setupKernel(tempDirectoryPath, "configsDifferentFromDefaults.yaml");

        // Verify correct reset and replacement for periodicUploadIntervalSec
        assertThat(()-> logManagerService.getPeriodicUpdateIntervalSec(), eventuallyEval(is(60),
                Duration.ofSeconds(30)));
        logManagerService.getConfig().find(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC).remove();
        assertThat(()-> logManagerService.getPeriodicUpdateIntervalSec(),
                eventuallyEval(is(LogManagerService.DEFAULT_PERIODIC_UPDATE_INTERVAL_SEC), Duration.ofSeconds(30)));
        logManagerService.getConfig().lookup(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC).withValue(600);
        assertThat(()-> logManagerService.getPeriodicUpdateIntervalSec(), eventuallyEval(is(600),
                Duration.ofSeconds(30)));

        // Verifications for system configs:
        // Verify correct reset and replacement for uploadToCloudWatch
        assertThat(()-> logManagerService.getComponentLogConfigurations().get(SYSTEM_LOGS_COMPONENT_NAME).isUploadToCloudWatch(),
                eventuallyEval(is(true), Duration.ofSeconds(30)));
        logManagerService.getConfig().lookup(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC,
                SYSTEM_LOGS_CONFIG_TOPIC_NAME, UPLOAD_TO_CW_CONFIG_TOPIC_NAME).remove();
        assertThat(()-> logManagerService.getComponentLogConfigurations().get(SYSTEM_LOGS_COMPONENT_NAME).isUploadToCloudWatch(),
                eventuallyEval(is(false), Duration.ofSeconds(30)));
        logManagerService.getConfig().lookup(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC,
                SYSTEM_LOGS_CONFIG_TOPIC_NAME, UPLOAD_TO_CW_CONFIG_TOPIC_NAME).withValue(true);
        assertThat(()-> logManagerService.getComponentLogConfigurations().get(SYSTEM_LOGS_COMPONENT_NAME).isUploadToCloudWatch(),
                eventuallyEval(is(true), Duration.ofSeconds(30)));

        // Verify correct reset and replacement for system minimumLogLevel
        assertThat(()-> logManagerService.getComponentLogConfigurations().get(SYSTEM_LOGS_COMPONENT_NAME).getMinimumLogLevel(),
                eventuallyEval(is(Level.TRACE), Duration.ofSeconds(30)));
        logManagerService.getConfig().lookup(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC,
                SYSTEM_LOGS_CONFIG_TOPIC_NAME, MIN_LOG_LEVEL_CONFIG_TOPIC_NAME).remove();
        assertThat(()-> logManagerService.getComponentLogConfigurations().get(SYSTEM_LOGS_COMPONENT_NAME).getMinimumLogLevel(),
                eventuallyEval(is(minimumLogLevelDefault), Duration.ofSeconds(30)));
        logManagerService.getConfig().lookup(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC,
                SYSTEM_LOGS_CONFIG_TOPIC_NAME, MIN_LOG_LEVEL_CONFIG_TOPIC_NAME).withValue("WARN");
        assertThat(()-> logManagerService.getComponentLogConfigurations().get(SYSTEM_LOGS_COMPONENT_NAME).getMinimumLogLevel(),
                eventuallyEval(is(Level.WARN), Duration.ofSeconds(30)));

        // Verify correct reset and replacement for system deleteLogFileAfterCloudUpload
        assertThat(()-> logManagerService.getComponentLogConfigurations().get(SYSTEM_LOGS_COMPONENT_NAME).isDeleteLogFileAfterCloudUpload(),
                eventuallyEval(is(true), Duration.ofSeconds(30)));
        logManagerService.getConfig().lookup(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC,
                SYSTEM_LOGS_CONFIG_TOPIC_NAME, DELETE_LOG_FILES_AFTER_UPLOAD_CONFIG_TOPIC_NAME).remove();
        assertThat(()-> logManagerService.getComponentLogConfigurations().get(SYSTEM_LOGS_COMPONENT_NAME).isDeleteLogFileAfterCloudUpload(),
                eventuallyEval(is(false), Duration.ofSeconds(30)));
        logManagerService.getConfig().lookup(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC,
                SYSTEM_LOGS_CONFIG_TOPIC_NAME, DELETE_LOG_FILES_AFTER_UPLOAD_CONFIG_TOPIC_NAME).withValue(true);
        assertThat(()-> logManagerService.getComponentLogConfigurations().get(SYSTEM_LOGS_COMPONENT_NAME).isDeleteLogFileAfterCloudUpload(),
                eventuallyEval(is(true), Duration.ofSeconds(30)));

        // Verify correct reset and replacement for system diskSpaceLimit (expected to be null)
        assertThat(()-> logManagerService.getComponentLogConfigurations().get(SYSTEM_LOGS_COMPONENT_NAME).getDiskSpaceLimit(),
                eventuallyEval(is(26214400L), Duration.ofSeconds(30)));
        logManagerService.getConfig().find(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC,
                SYSTEM_LOGS_CONFIG_TOPIC_NAME, DISK_SPACE_LIMIT_CONFIG_TOPIC_NAME).remove();
        assertThat(()-> logManagerService.getComponentLogConfigurations().get(SYSTEM_LOGS_COMPONENT_NAME).getDiskSpaceLimit() == null,
                eventuallyEval(equalTo(true), Duration.ofSeconds(30)));
        logManagerService.getConfig().lookup(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC,
                SYSTEM_LOGS_CONFIG_TOPIC_NAME, DISK_SPACE_LIMIT_CONFIG_TOPIC_NAME).withValue(5);
        assertThat(()-> logManagerService.getComponentLogConfigurations().get(SYSTEM_LOGS_COMPONENT_NAME).getDiskSpaceLimit(),
                eventuallyEval(is(5242880L), Duration.ofSeconds(30)));

        // Verify correct reset and replacement for system diskSpaceLimitUnit (expected to be "KB", so
        // with diskSpaceLimit=10, expected value in bytes is 10240)
        logManagerService.getConfig().lookup(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC,
                SYSTEM_LOGS_CONFIG_TOPIC_NAME, DISK_SPACE_LIMIT_CONFIG_TOPIC_NAME).withValue(10);
        logManagerService.getConfig().lookup(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC,
                SYSTEM_LOGS_CONFIG_TOPIC_NAME, DISK_SPACE_LIMIT_UNIT_CONFIG_TOPIC_NAME).remove();
        assertThat(()-> logManagerService.getComponentLogConfigurations().get(SYSTEM_LOGS_COMPONENT_NAME).getDiskSpaceLimit(),
                eventuallyEval(is(10240L), Duration.ofSeconds(30)));
        logManagerService.getConfig().lookup(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC,
                SYSTEM_LOGS_CONFIG_TOPIC_NAME, DISK_SPACE_LIMIT_UNIT_CONFIG_TOPIC_NAME).withValue("MB");
        assertThat(()-> logManagerService.getComponentLogConfigurations().get(SYSTEM_LOGS_COMPONENT_NAME).getDiskSpaceLimit(),
                eventuallyEval(is(10485760L), Duration.ofSeconds(30)));


        // Verifications for component configs:
        // Verify correct reset and replacement for fileNameRegex
        String fileNameRegexDefault = "^\\QUserComponentA\\E\\w*.log";
        String fileNameRegexNew = "RandomLogFileName\\w*.log";
        assertThat(()-> logManagerService.getComponentLogConfigurations().get(componentName).getFileNameRegex().pattern(),
                eventuallyEval(is("^integTestRandomLogFiles.log\\w*"), Duration.ofSeconds(30)));
        logManagerService.getConfig().find(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC,
                COMPONENT_LOGS_CONFIG_MAP_TOPIC_NAME, componentName, FILE_REGEX_CONFIG_TOPIC_NAME).remove();
        assertThat(()-> logManagerService.getComponentLogConfigurations().get(componentName).getFileNameRegex().pattern(),
                eventuallyEval(is(fileNameRegexDefault), Duration.ofSeconds(30)));
        logManagerService.getConfig().lookup(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC,
                COMPONENT_LOGS_CONFIG_MAP_TOPIC_NAME, componentName, FILE_REGEX_CONFIG_TOPIC_NAME).withValue(fileNameRegexNew);
        assertThat(()-> logManagerService.getComponentLogConfigurations().get(componentName).getFileNameRegex().pattern(),
                eventuallyEval(is(fileNameRegexNew), Duration.ofSeconds(30)));

        // Verify correct reset and replacement for directoryPath
        Path logFileDirectoryPathDefault = tempRootDir.resolve("logs");
        Path logFileDirectoryPathNew = tempRootDir.resolve("newLogDir");
        Files.createDirectory(logFileDirectoryPathNew);
        assertThat(()-> logManagerService.getComponentLogConfigurations().get(componentName).getDirectoryPath(),
                eventuallyEval(is(tempDirectoryPath), Duration.ofSeconds(30)));
        logManagerService.getConfig().find(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC,
                COMPONENT_LOGS_CONFIG_MAP_TOPIC_NAME, componentName, FILE_DIRECTORY_PATH_CONFIG_TOPIC_NAME).remove();
        assertThat(()-> logManagerService.getComponentLogConfigurations().get(componentName).getDirectoryPath(),
                eventuallyEval(is(logFileDirectoryPathDefault), Duration.ofSeconds(30)));
        logManagerService.getConfig().lookup(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC,
                        COMPONENT_LOGS_CONFIG_MAP_TOPIC_NAME, componentName, FILE_DIRECTORY_PATH_CONFIG_TOPIC_NAME)
                .withValue(logFileDirectoryPathNew.toString());
        assertThat(()-> logManagerService.getComponentLogConfigurations().get(componentName).getDirectoryPath(),
                eventuallyEval(is(logFileDirectoryPathNew), Duration.ofSeconds(30)));

        // Verify correct reset and replacement for component minimumLogLevel
        assertThat(()-> logManagerService.getComponentLogConfigurations().get(componentName).getMinimumLogLevel(),
                eventuallyEval(is(Level.TRACE), Duration.ofSeconds(30)));
        logManagerService.getConfig().find(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC,
                COMPONENT_LOGS_CONFIG_MAP_TOPIC_NAME, componentName, MIN_LOG_LEVEL_CONFIG_TOPIC_NAME).remove();
        assertThat(()-> logManagerService.getComponentLogConfigurations().get(componentName).getMinimumLogLevel(),
                eventuallyEval(is(minimumLogLevelDefault), Duration.ofSeconds(30)));
        logManagerService.getConfig().lookup(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC,
                COMPONENT_LOGS_CONFIG_MAP_TOPIC_NAME, componentName, MIN_LOG_LEVEL_CONFIG_TOPIC_NAME).withValue("WARN");
        assertThat(()-> logManagerService.getComponentLogConfigurations().get(componentName).getMinimumLogLevel(),
                eventuallyEval(is(Level.WARN), Duration.ofSeconds(30)));

        // Verify correct reset and replacement for component deleteLogFileAfterCloudUpload
        boolean deleteLogFileAfterCloudUploadDefault = false;
        assertThat(()-> logManagerService.getComponentLogConfigurations().get(componentName).isDeleteLogFileAfterCloudUpload(),
                eventuallyEval(is(true), Duration.ofSeconds(30)));
        logManagerService.getConfig().find(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC,
                COMPONENT_LOGS_CONFIG_MAP_TOPIC_NAME, componentName, DELETE_LOG_FILES_AFTER_UPLOAD_CONFIG_TOPIC_NAME).remove();
        assertThat(()-> logManagerService.getComponentLogConfigurations().get(componentName).isDeleteLogFileAfterCloudUpload(),
                eventuallyEval(is(deleteLogFileAfterCloudUploadDefault), Duration.ofSeconds(30)));
        logManagerService.getConfig().lookup(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC,
                COMPONENT_LOGS_CONFIG_MAP_TOPIC_NAME, componentName, DELETE_LOG_FILES_AFTER_UPLOAD_CONFIG_TOPIC_NAME).withValue(true);
        assertThat(()-> logManagerService.getComponentLogConfigurations().get(componentName).isDeleteLogFileAfterCloudUpload(),
                eventuallyEval(is(true), Duration.ofSeconds(30)));

        // Verify correct reset and replacement for component diskSpaceLimit (expected to be null)
        assertThat(()-> logManagerService.getComponentLogConfigurations().get(componentName).getDiskSpaceLimit(),
                eventuallyEval(is(10737418240L), Duration.ofSeconds(30)));
        logManagerService.getConfig().find(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC,
                COMPONENT_LOGS_CONFIG_MAP_TOPIC_NAME, componentName, DISK_SPACE_LIMIT_CONFIG_TOPIC_NAME).remove();
        assertThat(()-> logManagerService.getComponentLogConfigurations().get(componentName).getDiskSpaceLimit() == null,
                eventuallyEval(equalTo(true), Duration.ofSeconds(30)));
        logManagerService.getConfig().lookup(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC,
                COMPONENT_LOGS_CONFIG_MAP_TOPIC_NAME, componentName, DISK_SPACE_LIMIT_CONFIG_TOPIC_NAME).withValue(5);
        assertThat(()-> logManagerService.getComponentLogConfigurations().get(componentName).getDiskSpaceLimit(),
                eventuallyEval(is(5368709120L), Duration.ofSeconds(30)));

        // Verify correct reset and replacement for component diskSpaceLimitUnit (expected to be "KB", so
        // with diskSpaceLimit=10, expected value in bytes is 10240)
        logManagerService.getConfig().lookup(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC,
                COMPONENT_LOGS_CONFIG_MAP_TOPIC_NAME, componentName, DISK_SPACE_LIMIT_CONFIG_TOPIC_NAME).withValue(10);
        logManagerService.getConfig().lookup(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC,
                COMPONENT_LOGS_CONFIG_MAP_TOPIC_NAME, componentName, DISK_SPACE_LIMIT_UNIT_CONFIG_TOPIC_NAME).remove();
        assertThat(()-> logManagerService.getComponentLogConfigurations().get(componentName).getDiskSpaceLimit(),
                eventuallyEval(is(10240L), Duration.ofSeconds(30)));
        logManagerService.getConfig().lookup(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC,
                COMPONENT_LOGS_CONFIG_MAP_TOPIC_NAME, componentName, DISK_SPACE_LIMIT_UNIT_CONFIG_TOPIC_NAME).withValue("MB");
        assertThat(()-> logManagerService.getComponentLogConfigurations().get(componentName).getDiskSpaceLimit(),
                eventuallyEval(is(10485760L), Duration.ofSeconds(30)));
    }
}