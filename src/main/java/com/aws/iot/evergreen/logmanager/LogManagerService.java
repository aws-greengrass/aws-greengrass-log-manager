/*
 *  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *  SPDX-License-Identifier: Apache-2.0
 */

package com.aws.iot.evergreen.logmanager;

import com.aws.iot.evergreen.config.Topic;
import com.aws.iot.evergreen.config.Topics;
import com.aws.iot.evergreen.dependency.ImplementsService;
import com.aws.iot.evergreen.kernel.PluginService;
import com.aws.iot.evergreen.logging.impl.LogManager;
import com.aws.iot.evergreen.logmanager.model.CloudWatchAttempt;
import com.aws.iot.evergreen.logmanager.model.CloudWatchAttemptLogFileInformation;
import com.aws.iot.evergreen.logmanager.model.CloudWatchAttemptLogInformation;
import com.aws.iot.evergreen.logmanager.model.ComponentLogConfiguration;
import com.aws.iot.evergreen.logmanager.model.ComponentLogFileInformation;
import com.aws.iot.evergreen.logmanager.model.ComponentType;
import com.aws.iot.evergreen.logmanager.model.LogFileInformation;
import com.aws.iot.evergreen.logmanager.model.configuration.LogsUploaderConfiguration;
import com.aws.iot.evergreen.util.Coerce;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import javax.inject.Inject;

import static com.aws.iot.evergreen.logmanager.LogManagerService.LOGS_UPLOADER_SERVICE_TOPICS;
import static com.aws.iot.evergreen.packagemanager.KernelConfigResolver.PARAMETERS_CONFIG_KEY;

@ImplementsService(name = LOGS_UPLOADER_SERVICE_TOPICS, version = "1.0.0")
public class LogManagerService extends PluginService {
    public static final String LOGS_UPLOADER_SERVICE_TOPICS = "aws.greengrass.logmanager";
    public static final String LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC = "periodicUploadIntervalSec";
    public static final String LOGS_UPLOADER_CONFIGURATION_TOPIC = "logsUploaderConfiguration";
    public static final String SYSTEM_LOGS_COMPONENT_NAME = "System";
    public static final String PERSISTED_CURRENT_PROCESSING_FILE_NAME = "currentProcessingFileName";
    public static final String PERSISTED_CURRENT_PROCESSING_FILE_START_POSITION = "currentProcessingFileStartPosition";
    public static final String PERSISTED_COMPONENT_CURRENT_PROCESSING_FILE_INFORMATION =
            "currentComponentFileProcessingInformation";
    public static final String PERSISTED_COMPONENT_LAST_FILE_PROCESSED_TIMESTAMP =
            "componentLastFileProcessedTimeStamp";
    public static final String PERSISTED_LAST_FILE_PROCESSED_TIMESTAMP =
            "lastFileProcessedTimeStamp";
    public static final String PERSISTED_CURRENT_PROCESSING_FILE_LAST_MODIFIED_TIME =
            "currentProcessingFileLastModified";
    private static final int DEFAULT_PERIODIC_UPDATE_INTERVAL_SEC = 300;
    private static final String DEFAULT_FILE_REGEX = "^%s\\w*.log";
    private static final ObjectMapper DESERIALIZER = new ObjectMapper()
            .configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);

    final Map<String, Instant> lastComponentUploadedLogFileInstantMap =
            Collections.synchronizedMap(new LinkedHashMap<>());
    final Map<String, CurrentProcessingFileInformation> componentCurrentProcessingLogFile =
            new ConcurrentHashMap<>();
    private final CloudWatchLogsUploader uploader;
    private final CloudWatchAttemptLogsProcessor logsProcessor;
    private Set<ComponentLogConfiguration> componentLogConfigurations = new HashSet<>();
    private final AtomicBoolean isCurrentlyUploading = new AtomicBoolean(false);
    private int periodicUpdateIntervalSec;

    /**
     * Constructor.
     *
     * @param topics              The configuration coming from the kernel.
     * @param uploader            {@link CloudWatchLogsUploader}
     * @param logProcessor        {@link CloudWatchAttemptLogsProcessor}
     */
    @Inject
    LogManagerService(Topics topics, CloudWatchLogsUploader uploader, CloudWatchAttemptLogsProcessor logProcessor) {
        super(topics);
        this.uploader = uploader;
        this.logsProcessor = logProcessor;

        topics.lookup(PARAMETERS_CONFIG_KEY, LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC)
                .dflt(DEFAULT_PERIODIC_UPDATE_INTERVAL_SEC)
                .subscribe((why, newv) -> {
                    periodicUpdateIntervalSec = Coerce.toInt(newv);
                });
        this.uploader.registerAttemptStatus(LOGS_UPLOADER_SERVICE_TOPICS, this::handleCloudWatchAttemptStatus);

        topics.lookup(PARAMETERS_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC)
                .subscribe((why, newv) -> {
                    try {
                        if (newv == null || Coerce.toString(newv) == null) {
                            //TODO: fail the deployment.
                            return;
                        }
                        LogsUploaderConfiguration config = DESERIALIZER.readValue(Coerce.toString(newv),
                                LogsUploaderConfiguration.class);
                        processConfiguration(config);
                    } catch (JsonProcessingException e) {
                        //TODO: Add validation step and fail the deployment here.
                        logger.atError().cause(e).log("Unable to parse configuration.");
                    }
                });
    }

    private void processConfiguration(LogsUploaderConfiguration config) {
        Set<ComponentLogConfiguration> newComponentLogConfigurations = new HashSet<>();
        if (config.getSystemLogsConfiguration().isUploadToCloudWatch()) {
            addSystemLogsConfiguration(newComponentLogConfigurations);
        }
        config.getComponentLogInformation().forEach(componentConfiguration -> {
            ComponentLogConfiguration componentLogConfiguration = ComponentLogConfiguration.builder()
                    .name(componentConfiguration.getComponentName())
                    .fileNameRegex(Pattern.compile(componentConfiguration.getLogFileRegex()))
                    .directoryPath(Paths.get(componentConfiguration.getLogFileDirectoryPath()))
                    .componentType(ComponentType.UserComponent)
                    .build();
            // TODO: handle the different optional cases.
            newComponentLogConfigurations.add(componentLogConfiguration);
        });

        this.componentLogConfigurations = newComponentLogConfigurations;
    }

    private void addSystemLogsConfiguration(Set<ComponentLogConfiguration> newComponentLogConfigurations) {
        Path logsDirectoryPath = Paths.get(LogManager.getConfig().getStoreName()).getParent();
        newComponentLogConfigurations.add(ComponentLogConfiguration.builder()
                .fileNameRegex(Pattern.compile(String.format(DEFAULT_FILE_REGEX, LogManager.getConfig().getFileName())))
                .directoryPath(logsDirectoryPath)
                .name(SYSTEM_LOGS_COMPONENT_NAME)
                .componentType(ComponentType.GreengrassSystemComponent)
                .build());
    }

    /**
     * Handle the attempt to upload logs to CloudWatch.
     *
     * @param cloudWatchAttempt The cloud watch attempt.
     * @implSpec : The method gets the attempt from the uploader which has information about which log groups and
     *     log streams have been successfully uploaded to CloudWatch. Based on that information, it will update
     *     the appropriate information about each component.
     *     If a log group/stream has been successfully uploaded, there will be appropriate information about which
     *     log files did the logs belong to and what was the starting position for the logs in the stream and how many
     *     bytes worth of logs were part of the upload.
     *     If the file has been completely read and uploaded, the method will update the component information about the
     *     latest log file's information.
     *     If the file was partially read, then the method will update that information about what is the current
     *     processing log file for the component and what is the starting position of the next log line.
     */
    private void handleCloudWatchAttemptStatus(CloudWatchAttempt cloudWatchAttempt) {
        Map<String, Set<String>> completedLogFilePerComponent = new ConcurrentHashMap<>();
        Map<String, CurrentProcessingFileInformation> currentProcessingLogFilePerComponent = new HashMap<>();

        cloudWatchAttempt.getLogStreamUploadedSet().forEach((streamName) -> {
                    CloudWatchAttemptLogInformation attemptLogInformation =
                            cloudWatchAttempt.getLogStreamsToLogEventsMap().get(streamName);
                    attemptLogInformation.getAttemptLogFileInformationMap().forEach(
                            (fileName, cloudWatchAttemptLogFileInformation) ->
                                    processCloudWatchAttemptLogInformation(completedLogFilePerComponent,
                                            currentProcessingLogFilePerComponent, attemptLogInformation, fileName,
                                            cloudWatchAttemptLogFileInformation));
                });

        completedLogFilePerComponent.forEach((componentName, fileNames) ->
                fileNames.stream().map(File::new).forEach(file -> {
                    if (!lastComponentUploadedLogFileInstantMap.containsKey(componentName)
                            || lastComponentUploadedLogFileInstantMap.get(componentName)
                            .isBefore(Instant.ofEpochMilli(file.lastModified()))) {
                        lastComponentUploadedLogFileInstantMap.put(componentName,
                                Instant.ofEpochMilli(file.lastModified()));
                    }
                }));
        currentProcessingLogFilePerComponent.forEach(componentCurrentProcessingLogFile::put);

        //TODO: Persist this information to the disk.
        componentCurrentProcessingLogFile.forEach((componentName, currentProcessingFileInformation) -> {
            Topics componentTopics = config.lookupTopics(PERSISTED_COMPONENT_CURRENT_PROCESSING_FILE_INFORMATION,
                    componentName);
            componentTopics.replaceAndWait(currentProcessingFileInformation.convertToMapOfObjects());
        });
        lastComponentUploadedLogFileInstantMap.forEach((componentName, instant) -> {
            Topics componentTopics = config.lookupTopics(PERSISTED_COMPONENT_LAST_FILE_PROCESSED_TIMESTAMP,
                    componentName);
            Topic lastFileProcessedTimeStamp = componentTopics.createLeafChild(PERSISTED_LAST_FILE_PROCESSED_TIMESTAMP);
            lastFileProcessedTimeStamp.withValue(instant.toEpochMilli());
        });
        isCurrentlyUploading.set(false);
    }

    private void processCloudWatchAttemptLogInformation(Map<String, Set<String>> completedLogFilePerComponent,
                                                        Map<String, CurrentProcessingFileInformation>
                                                                currentProcessingLogFilePerComponent,
                                                        CloudWatchAttemptLogInformation attemptLogInformation,
                                                        String fileName,
                                                        CloudWatchAttemptLogFileInformation
                                                                cloudWatchAttemptLogFileInformation) {
        File file = new File(fileName);
        String componentName = attemptLogInformation.getComponentName();
        // If we have completely read the file, then we need add it to the completed files list and remove it
        // it (if necessary) for the current processing list.
        if (file.length() == cloudWatchAttemptLogFileInformation.getBytesRead()
                + cloudWatchAttemptLogFileInformation.getStartPosition()) {
            Set<String> completedFileNames = completedLogFilePerComponent.getOrDefault(componentName, new HashSet<>());
            completedFileNames.add(fileName);
            completedLogFilePerComponent.put(componentName, completedFileNames);
            if (currentProcessingLogFilePerComponent.containsKey(componentName)) {
                CurrentProcessingFileInformation fileInformation = currentProcessingLogFilePerComponent
                        .get(componentName);
                if (fileInformation.fileName.equals(fileName)) {
                    currentProcessingLogFilePerComponent.remove(componentName);
                }
            }
        } else {
            // Add the file to the current processing list for the component.
            // Note: There should always be only 1 file which will be in progress at any given time.
            CurrentProcessingFileInformation processingFileInformation =
                    CurrentProcessingFileInformation.builder()
                            .fileName(fileName)
                            .startPosition(cloudWatchAttemptLogFileInformation.getStartPosition()
                                    + cloudWatchAttemptLogFileInformation.getBytesRead())
                            .lastModifiedTime(cloudWatchAttemptLogFileInformation.getLastModifiedTime())
                            .build();
            currentProcessingLogFilePerComponent.put(attemptLogInformation.getComponentName(),
                    processingFileInformation);
        }
    }

    /**
     * Long running process which will keep checking if there are any logs to be uploaded to the cloud.
     *
     * @implSpec : The service will first check if there was already a cloudwatch attempt in progess. If so, it will
     *     return.
     *     It will then go through the components log configuration map and check if there are any log files from any
     *     component that needs to be uploaded to the cloud.
     *     The service will first get all the files from the log file directory and then sort them by the last modified
     *     time.
     *     It will then get all the log files which have not yet been uploaded to the cloud. This is done by checking
     *     the last uploaded log file time for that component.
     */
    private void processLogsAndUpload() throws InterruptedException {
        while (true) {
            // If there is already an upload ongoing, don't do anything. Wait for the next schedule to trigger to
            // upload new logs.
            if (!isCurrentlyUploading.compareAndSet(false, true)) {
                //TODO: Use a CountDownLatch so that we could await here instead of sleep?
                TimeUnit.SECONDS.sleep(periodicUpdateIntervalSec);
                continue;
            }
            // TODO: Sleep here if we had received a throttling exception in the previous run.
            AtomicReference<Optional<ComponentLogFileInformation>> componentLogFileInformation =
                    new AtomicReference<>(Optional.empty());
            // Get the latest known configurations because the componentLogConfigurations can change if a new
            // configuration is received from the customer.
            final Set<ComponentLogConfiguration> currentComponentLogConfigurations = componentLogConfigurations;
            for (ComponentLogConfiguration componentLogConfiguration : currentComponentLogConfigurations) {
                if (componentLogFileInformation.get().isPresent()) {
                    break;
                }
                String componentName = componentLogConfiguration.getName();
                Instant lastUploadedLogFileTimeMs =
                        lastComponentUploadedLogFileInstantMap.getOrDefault(componentName,
                                Instant.EPOCH);
                File folder = new File(componentLogConfiguration.getDirectoryPath().toUri());
                List<File> allFiles = new ArrayList<>();
                try {
                    File[] files = folder.listFiles();
                    if (files != null) {
                        for (File file : files) {
                            if (file.isFile()
                                    && lastUploadedLogFileTimeMs.isBefore(Instant.ofEpochMilli(file.lastModified()))
                                    && componentLogConfiguration.getFileNameRegex().matcher(file.getName()).find()
                                    && file.length() > 0) {
                                allFiles.add(file);
                            }
                        }
                    }
                    // Sort the files by the last modified time.
                    allFiles.sort(Comparator.comparingLong(File::lastModified));
                    // If there are no rotated log files for the component, then return.
                    if (allFiles.size() - 1 <= 0) {
                        continue;
                    }
                    // Don't consider the active log file.
                    allFiles = allFiles.subList(0, allFiles.size() - 1);

                    componentLogFileInformation.set(Optional.of(
                            ComponentLogFileInformation.builder()
                                    .name(componentName)
                                    .multiLineStartPattern(componentLogConfiguration.getMultiLineStartPattern())
                                    .desiredLogLevel(componentLogConfiguration.getMinimumLogLevel())
                                    .componentType(componentLogConfiguration.getComponentType())
                                    .build()));
                    allFiles.forEach(file -> {
                        long startPosition = 0;
                        // If the file was paritially read in the previous run, then get the starting position for
                        // new log lines.
                        if (componentCurrentProcessingLogFile.containsKey(componentName)) {
                            CurrentProcessingFileInformation processingFileInformation =
                                    componentCurrentProcessingLogFile.get(componentName);
                            if (processingFileInformation.fileName.equals(file.getAbsolutePath())
                                    && processingFileInformation.lastModifiedTime == file.lastModified()) {
                                startPosition = processingFileInformation.startPosition;
                            }
                        }
                        LogFileInformation logFileInformation = LogFileInformation.builder()
                                .file(file)
                                .startPosition(startPosition)
                                .build();
                        componentLogFileInformation.get().get().getLogFileInformationList().add(logFileInformation);
                    });
                    break;
                } catch (SecurityException e) {
                    logger.atError().cause(e).log("Unable to get log files for {} from {}",
                            componentName, componentLogConfiguration.getDirectoryPath());
                }
            }
            if (componentLogFileInformation.get().isPresent()) {
                CloudWatchAttempt cloudWatchAttempt =
                        logsProcessor.processLogFiles(componentLogFileInformation.get().get());
                uploader.upload(cloudWatchAttempt, 1);
            } else {
                TimeUnit.SECONDS.sleep(periodicUpdateIntervalSec);
                isCurrentlyUploading.set(false);
            }
        }
    }

    @Override
    public void startup() throws InterruptedException {
        // Need to override the function for tests.
        super.startup();
        processLogsAndUpload();
    }

    @Override
    @SuppressWarnings("PMD.UselessOverridingMethod")
    public void shutdown() throws InterruptedException {
        // Need to override the function for tests.
        super.shutdown();
    }

    @Builder
    @Getter
    @Data
    static class CurrentProcessingFileInformation {
        private String fileName;
        private long startPosition;
        private long lastModifiedTime;

        public Map<Object, Object> convertToMapOfObjects() {
            Map<Object, Object> currentProcessingFileInformationMap = new HashMap<>();
            currentProcessingFileInformationMap.put(PERSISTED_CURRENT_PROCESSING_FILE_NAME, fileName);
            currentProcessingFileInformationMap.put(PERSISTED_CURRENT_PROCESSING_FILE_START_POSITION, startPosition);
            currentProcessingFileInformationMap.put(PERSISTED_CURRENT_PROCESSING_FILE_LAST_MODIFIED_TIME,
                    lastModifiedTime);
            return currentProcessingFileInformationMap;
        }

        public static CurrentProcessingFileInformation convertFromMapOfObjects(
                Map<Object, Object> currentProcessingFileInformationMap) {
            return CurrentProcessingFileInformation.builder()
                    .fileName(Coerce.toString(currentProcessingFileInformationMap
                            .get(PERSISTED_CURRENT_PROCESSING_FILE_NAME)))
                    .lastModifiedTime(Coerce.toLong(currentProcessingFileInformationMap
                            .get(PERSISTED_CURRENT_PROCESSING_FILE_LAST_MODIFIED_TIME)))
                    .startPosition(Coerce.toLong(currentProcessingFileInformationMap
                            .get(PERSISTED_CURRENT_PROCESSING_FILE_START_POSITION)))
                    .build();
        }
    }
}
