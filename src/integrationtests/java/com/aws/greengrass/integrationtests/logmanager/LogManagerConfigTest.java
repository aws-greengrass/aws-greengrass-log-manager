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
import com.aws.greengrass.logmanager.LogManagerService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.event.Level;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.aws.greengrass.componentmanager.KernelConfigResolver.CONFIGURATION_CONFIG_KEY;
import static com.aws.greengrass.logmanager.LogManagerService.COMPONENT_LOGS_CONFIG_MAP_TOPIC_NAME;
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
import static com.github.grantwest.eventually.EventuallyLambdaMatcher.eventuallyEval;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SuppressWarnings("PMD.UnsynchronizedStaticFormatter")
class LogManagerConfigTest extends BaseITCase {
    private static Kernel kernel;
    private static DeviceConfiguration deviceConfiguration;
    private static LogManagerService logManagerService;
    private static Path tempDirectoryPath;
    private static Path tempRootDir;
    private static final  ObjectMapper YAML_OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());
    private static final String componentName = "UserComponentA";
    private static final Level minimumLogLevelDefault = Level.INFO;

    @BeforeAll
    static void setupKernel() throws InterruptedException,
            URISyntaxException, IOException, DeviceConfigurationException {

        tempRootDir = Paths.get(System.getProperty("root"));
        tempDirectoryPath = Files.createTempDirectory(tempRootDir, "IntegrationTestsTemporaryLogFiles");

        CountDownLatch logManagerRunning = new CountDownLatch(1);
        kernel = new Kernel();

        Path testRecipePath = Paths.get(LogManagerTest.class.getResource("configsDifferentFromDefaults.yaml").toURI());
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
        kernel.launch();
        assertTrue(logManagerRunning.await(10, TimeUnit.SECONDS));
    }

    @AfterAll
    static void afterAll() {
        kernel.shutdown();
    }

    @Test
    void GIVEN_periodicUploadIntervalSec_config_WHEN_value_is_reset_and_replaced_THEN_correct_values_are_used() {
            assertThat(()-> logManagerService.getPeriodicUpdateIntervalSec(), eventuallyEval(is(60),
                    Duration.ofSeconds(30)));

            logManagerService.getConfig().find(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC).remove();
            assertThat(()-> logManagerService.getPeriodicUpdateIntervalSec(),
                    eventuallyEval(is(LogManagerService.DEFAULT_PERIODIC_UPDATE_INTERVAL_SEC), Duration.ofSeconds(30)));

            logManagerService.getConfig().lookup(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC).withValue(600);
            assertThat(()-> logManagerService.getPeriodicUpdateIntervalSec(), eventuallyEval(is(600),
                    Duration.ofSeconds(30)));
    }

    @Test
    void GIVEN_uploadToCloudWatch_config_WHEN_value_is_reset_and_replaced_THEN_correct_values_are_used() {
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
    }

    @Test
    void GIVEN_system_minimumLogLevel_config_WHEN_value_is_reset_and_replaced_THEN_correct_values_are_used() {
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
    }

    @Test
    void GIVEN_system_deleteLogFileAfterCloudUpload_config_WHEN_value_is_reset_and_replaced_THEN_correct_values_are_used() {
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
    }

    @Test
    void GIVEN_system_diskSpaceLimit_and_diskSpaceLimitUnit_config_WHEN_values_are_reset_and_replaced_THEN_correct_values_are_used(){
        // diskSpaceLimit=25 and diskSpaceLimitUnit=MB in config file, so expected value is 26214400 bytes
        assertThat(()-> logManagerService.getComponentLogConfigurations().get(SYSTEM_LOGS_COMPONENT_NAME).getDiskSpaceLimit(),
                eventuallyEval(is(26214400L), Duration.ofSeconds(30)));

        logManagerService.getConfig().find(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC,
                SYSTEM_LOGS_CONFIG_TOPIC_NAME, DISK_SPACE_LIMIT_CONFIG_TOPIC_NAME).remove();
        assertThat(()-> logManagerService.getComponentLogConfigurations().get(SYSTEM_LOGS_COMPONENT_NAME).getDiskSpaceLimit() == null,
                eventuallyEval(equalTo(true), Duration.ofSeconds(30)));

        logManagerService.getConfig().lookup(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC,
                SYSTEM_LOGS_CONFIG_TOPIC_NAME, DISK_SPACE_LIMIT_CONFIG_TOPIC_NAME).withValue(5);
        // diskSpaceLimitUnit in config file is MB, so diskSpaceLimit=5 gives expected value of 5242880 bytes
        assertThat(()-> logManagerService.getComponentLogConfigurations().get(SYSTEM_LOGS_COMPONENT_NAME).getDiskSpaceLimit(),
                eventuallyEval(is(5242880L), Duration.ofSeconds(30)));

        // verifcation for diskSpaceLimitUnit
        // diskSpaceLimitUnit default is KB, so with diskSpaceLimit=5, expected value is 5120 bytes
        logManagerService.getConfig().lookup(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC,
                SYSTEM_LOGS_CONFIG_TOPIC_NAME, DISK_SPACE_LIMIT_UNIT_CONFIG_TOPIC_NAME).remove();
        assertThat(()-> logManagerService.getComponentLogConfigurations().get(SYSTEM_LOGS_COMPONENT_NAME).getDiskSpaceLimit(),
                eventuallyEval(is(5120L), Duration.ofSeconds(30)));

        // diskSpaceLimit=5 and diskSpaceLimitUnit=MB, so expected value is 5242880 bytes
        logManagerService.getConfig().lookup(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC,
                SYSTEM_LOGS_CONFIG_TOPIC_NAME, DISK_SPACE_LIMIT_UNIT_CONFIG_TOPIC_NAME).withValue("MB");
        assertThat(()-> logManagerService.getComponentLogConfigurations().get(SYSTEM_LOGS_COMPONENT_NAME).getDiskSpaceLimit(),
                eventuallyEval(is(5242880L), Duration.ofSeconds(30)));
    }

    @Test
    void GIVEN_component_fileNameRegex_config_WHEN_value_is_reset_and_replaced_THEN_correct_values_are_used() {
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
    }

    @Test
    void GIVEN_component_directoryPath_config_WHEN_value_is_reset_and_replaced_THEN_correct_values_are_used() throws IOException{
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
    }

    @Test
    void GIVEN_component_minimumLogLevel_config_WHEN_value_is_reset_and_replaced_THEN_correct_values_are_used() {
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
    }

    @Test
    void GIVEN_component_deleteLogFileAfterCloudUpload_config_WHEN_value_is_reset_and_replaced_THEN_correct_values_are_used() {
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
    }

    @Test
    void GIVEN_component_diskSpaceLimit_anddiskSpaceLimitUnit_config_WHEN_values_are_reset_and_replaced_THEN_correct_values_are_used() {
        // diskSpaceLimit=10 diskSpaceLimitUnit=GB in config file, so expected value is 10737418240 bytes
        assertThat(() -> logManagerService.getComponentLogConfigurations().get(componentName).getDiskSpaceLimit(),
                eventuallyEval(is(10737418240L), Duration.ofSeconds(30)));

        logManagerService.getConfig()
                .find(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC, COMPONENT_LOGS_CONFIG_MAP_TOPIC_NAME,
                        componentName, DISK_SPACE_LIMIT_CONFIG_TOPIC_NAME).remove();
        assertThat(() -> logManagerService.getComponentLogConfigurations().get(componentName).getDiskSpaceLimit() == null,
                eventuallyEval(equalTo(true), Duration.ofSeconds(30)));

        // diskSpaceLimitUnit in config file is GB, so diskSpaceLimit=5 gives expected value of 5368709120 bytes
        logManagerService.getConfig().lookup(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC,
                COMPONENT_LOGS_CONFIG_MAP_TOPIC_NAME, componentName, DISK_SPACE_LIMIT_CONFIG_TOPIC_NAME).withValue(5);
        assertThat(() -> logManagerService.getComponentLogConfigurations().get(componentName).getDiskSpaceLimit(),
                eventuallyEval(is(5368709120L), Duration.ofSeconds(30)));

        // verifcation for diskSpaceLimitUnit
        // diskSpaceLimitUnit default is KB, so with diskSpaceLimit=5, expected value is 5120 bytes
        logManagerService.getConfig().lookup(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC,
                COMPONENT_LOGS_CONFIG_MAP_TOPIC_NAME, componentName, DISK_SPACE_LIMIT_UNIT_CONFIG_TOPIC_NAME).remove();
        assertThat(()-> logManagerService.getComponentLogConfigurations().get(componentName).getDiskSpaceLimit(),
                eventuallyEval(is(5120L), Duration.ofSeconds(30)));

        // diskSpaceLimit=5 and diskSpaceLimitUnit=MB, so expected value is 5242880 bytes
        logManagerService.getConfig().lookup(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC,
                COMPONENT_LOGS_CONFIG_MAP_TOPIC_NAME, componentName, DISK_SPACE_LIMIT_UNIT_CONFIG_TOPIC_NAME).withValue("MB");
        assertThat(()-> logManagerService.getComponentLogConfigurations().get(componentName).getDiskSpaceLimit(),
                eventuallyEval(is(5242880L), Duration.ofSeconds(30)));
    }
}