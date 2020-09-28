package com.aws.greengrass.logmanager;

import com.aws.greengrass.dependency.State;
import com.aws.greengrass.deployment.DeviceConfiguration;
import com.aws.greengrass.deployment.exceptions.DeviceConfigurationException;
import com.aws.greengrass.integrationtests.BaseITCase;
import com.aws.greengrass.lifecyclemanager.Kernel;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.logmanager.model.configuration.ComponentsLogConfiguration;
import com.aws.greengrass.logmanager.model.configuration.LogsUploaderConfiguration;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.event.Level;
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
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static com.aws.greengrass.logmanager.util.LogFileHelper.createTempFileAndWriteData;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;

@ExtendWith({GGExtension.class, MockitoExtension.class})
class SpaceManagementTest extends BaseITCase {
    private static final String THING_NAME = "ThingName";
    private static final String AWS_REGION = "awsRegion";
    private static final String FILE_NAME_REGEX_STRING = "^integTestRandomLogFiles.log\\\\w*";
    private static Kernel kernel;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .setSerializationInclusion(JsonInclude.Include.NON_NULL);
    private static DeviceConfiguration deviceConfiguration;
    private LogManagerService logManagerService;
    private Path tempDirectoryPath;

    @Mock
    private CloudWatchLogsClient cloudWatchLogsClient;

    @BeforeEach
    void beforeEach(ExtensionContext context) {
        ignoreExceptionOfType(context, InterruptedException.class);
        LogManager.getConfig().setLevel(Level.DEBUG);
    }

    @AfterEach
    void afterEach() {
        kernel.shutdown();
        LogManager.getConfig().setLevel(Level.INFO);
    }


    void setupKernel(LogsUploaderConfiguration logsUploaderConfiguration) throws InterruptedException,
            URISyntaxException, IOException, DeviceConfigurationException {
        lenient().when(cloudWatchLogsClient.putLogEvents(any(PutLogEventsRequest.class)))
                .thenReturn(PutLogEventsResponse.builder().nextSequenceToken("nextToken").build());
        System.setProperty("root", tempRootDir.toAbsolutePath().toString());
        CountDownLatch logManagerRunning = new CountDownLatch(1);

        CompletableFuture cf = new CompletableFuture();
        cf.complete(null);
        kernel = new Kernel();

        Path testRecipePath = Paths.get(LogManagerTest.class
                .getResource("smallSpaceManagementPeriodicIntervalConfig.yaml").toURI());
        String content = new String(Files.readAllBytes(testRecipePath), StandardCharsets.UTF_8);
        content = content.replaceAll("\\{\\{logsUploaderConfiguration}}",
                OBJECT_MAPPER.writeValueAsString(logsUploaderConfiguration));

        Path tempConfigPath = Files.createTempDirectory(tempRootDir, "config").resolve("smallPeriodicIntervalConfig.yaml");
        Files.write(tempConfigPath, content.getBytes(StandardCharsets.UTF_8));

        kernel.parseArgs("-i", tempConfigPath.toAbsolutePath().toString());
        kernel.getContext().addGlobalStateChangeListener((service, oldState, newState) -> {
            if (service.getName().equals(LogManagerService.LOGS_MANAGER_SERVICE_TOPICS)
                    && newState.equals(State.RUNNING)) {
                logManagerRunning.countDown();
                logManagerService = (LogManagerService) service;
            }
        });
        deviceConfiguration = new DeviceConfiguration(kernel, THING_NAME, "dataEndpoint", "credEndpoint",
                "privKeyFilePath", "certFilePath", "caFilePath", AWS_REGION);
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
        tempDirectoryPath = Files.createTempDirectory(tempRootDir, "IntegrationTestsTemporaryLogFiles");
        LogsUploaderConfiguration logsUploaderConfiguration = new LogsUploaderConfiguration();
        List<ComponentsLogConfiguration> componentLogInformation = new ArrayList<>();
        ComponentsLogConfiguration componentsLogConfiguration = new ComponentsLogConfiguration();
        componentsLogConfiguration.setComponentName("UserComponentA");
        componentsLogConfiguration.setLogFileRegex(FILE_NAME_REGEX_STRING);
        componentsLogConfiguration.setDiskSpaceLimit("105");
        componentsLogConfiguration.setDiskSpaceLimitUnit("KB");
        componentsLogConfiguration.setLogFileDirectoryPath(tempDirectoryPath.toAbsolutePath().toString());
        componentLogInformation.add(componentsLogConfiguration);
        logsUploaderConfiguration.setComponentLogInformation(componentLogInformation);

        setupKernel(logsUploaderConfiguration);
        TimeUnit.SECONDS.sleep(10);
        for (int i = 0; i < 15; i++) {
            createTempFileAndWriteData(tempDirectoryPath, "integTestRandomLogFiles.log_",  "");
        }
        TimeUnit.SECONDS.sleep(30);

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

        assertEquals(10, allFiles.size());
    }
}
