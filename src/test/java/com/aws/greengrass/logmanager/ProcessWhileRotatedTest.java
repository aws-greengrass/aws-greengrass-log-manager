package com.aws.greengrass.logmanager;

import com.aws.greengrass.config.Topic;
import com.aws.greengrass.config.Topics;
import com.aws.greengrass.config.UpdateBehaviorTree;
import com.aws.greengrass.dependency.Context;
import com.aws.greengrass.deployment.DeviceConfiguration;
import com.aws.greengrass.logmanager.model.ComponentLogFileInformation;
import com.aws.greengrass.logmanager.model.LogFile;
import com.aws.greengrass.logmanager.util.CloudWatchClientFactory;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.testcommons.testutilities.GGServiceTestUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsResponse;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.aws.greengrass.componentmanager.KernelConfigResolver.CONFIGURATION_CONFIG_KEY;
import static com.aws.greengrass.logmanager.LogManagerService.COMPONENT_LOGS_CONFIG_MAP_TOPIC_NAME;
import static com.aws.greengrass.logmanager.LogManagerService.LOGS_UPLOADER_CONFIGURATION_TOPIC;
import static com.aws.greengrass.logmanager.LogManagerService.LOGS_UPLOADER_SERVICE_TOPICS;
import static com.aws.greengrass.logmanager.util.TestUtils.createLogFileWithSize;
import static com.aws.greengrass.logmanager.util.TestUtils.rotateFilesByRenamingThem;
import static com.aws.greengrass.logmanager.util.TestUtils.writeLogs;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, GGExtension.class})
class ProcessWhileRotatedTest extends GGServiceTestUtil {

    @Mock
    private CloudWatchClientFactory cloudWatchClientFactory;
    @Mock
    private DeviceConfiguration deviceConfiguration;
    @Mock
    private ExecutorService executorService;
    @Mock
    private CloudWatchLogsClient cloudWatchLogsClient;
    @TempDir
    private Path testDir;

    private CloudWatchLogsUploader uploader;
    private CloudWatchAttemptLogsProcessor processor;

    @BeforeEach
    public void startup() {
        lenient().when(cloudWatchClientFactory.getCloudWatchLogsClient()).thenReturn(cloudWatchLogsClient);

        context = new Context();
        Clock clock = Clock.systemUTC();
        processor = new CloudWatchAttemptLogsProcessor(deviceConfiguration, clock);
        uploader = spy(new CloudWatchLogsUploader(cloudWatchClientFactory));
    }

    private LogManagerService createService() {
        Topics topics = Topics.of(new Context(), LOGS_UPLOADER_SERVICE_TOPICS, null);
        Topics customComponentTopic = topics.lookupTopics(
                CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC,
                COMPONENT_LOGS_CONFIG_MAP_TOPIC_NAME, "TestComponent"
        );
        customComponentTopic.updateFromMap(new HashMap<String, Object>(){{
            put("logFileDirectoryPath", testDir.toAbsolutePath().toString());
            put("logFileRegex", "test.log\\w*");
        }},  new UpdateBehaviorTree(UpdateBehaviorTree.UpdateBehavior.REPLACE, System.currentTimeMillis()));

        return new LogManagerService(topics, uploader, processor, executorService);
    }


    @Test
    void foo() throws IOException {
        when(deviceConfiguration.getThingName()).thenReturn(Topic.of(new Context(), "thingName", "thing"));
        when(deviceConfiguration.getAWSRegion()).thenReturn(Topic.of(new Context(), "awsRegion", "us-west-2"));
        when(cloudWatchLogsClient.putLogEvents((PutLogEventsRequest) any())).thenReturn(PutLogEventsResponse.builder().nextSequenceToken("1").build());

        LogManagerService service = createService();

        // Some a rotated file with a partially written active file 5kb per file
        LogFile rotatedFile1 = createLogFileWithSize(testDir.resolve("test.log.1").toUri(), 1024 * 5);
        LogFile activeFile = createLogFileWithSize(testDir.resolve("test.log").toUri(), 1024 * 4);

        //  ---> cycle <-----
        service.getComponentLogFileInformation().stream()
                .map(processor::processLogFiles)
                .forEach((attempt -> uploader.upload(attempt, 0)));

        assertEquals( service.componentCurrentProcessingLogFile.get("TestComponent").getFileName(), activeFile.toPath().toAbsolutePath().toString());
        assertEquals(service.componentCurrentProcessingLogFile.get("TestComponent").getStartPosition(), 1024 * 4);

        // ----> cycle <-----
        writeLogs(activeFile, 500);
        // Get the files to process
        List<ComponentLogFileInformation> toProcess = service.getComponentLogFileInformation();
        toProcess = service.getComponentLogFileInformation();
        assertEquals(toProcess.size(), 1);
        assertEquals(toProcess.get(0).getLogFileInformationList().get(0).getStartPosition(), 1024 * 4);


        writeLogs(activeFile, 524); // Active file gets full
        File newActiveFile = rotateFilesByRenamingThem(new File[]{activeFile, rotatedFile1}); // Files get rotated
        writeLogs(newActiveFile, 1024); // Write 1kb to the new active file


        // Process the files
        processor.processLogFiles(toProcess.get(0));
    }

}
