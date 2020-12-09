/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.logmanager;

import ch.qos.logback.core.util.FileSize;
import com.aws.greengrass.config.Topic;
import com.aws.greengrass.config.Topics;
import com.aws.greengrass.dependency.ImplementsService;
import com.aws.greengrass.lifecyclemanager.PluginService;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.logmanager.model.CloudWatchAttempt;
import com.aws.greengrass.logmanager.model.CloudWatchAttemptLogFileInformation;
import com.aws.greengrass.logmanager.model.CloudWatchAttemptLogInformation;
import com.aws.greengrass.logmanager.model.ComponentLogConfiguration;
import com.aws.greengrass.logmanager.model.ComponentLogFileInformation;
import com.aws.greengrass.logmanager.model.ComponentType;
import com.aws.greengrass.logmanager.model.LogFileInformation;
import com.aws.greengrass.util.Coerce;
import com.aws.greengrass.util.Utils;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.event.Level;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.Watchable;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import javax.inject.Inject;

import static com.aws.greengrass.componentmanager.KernelConfigResolver.CONFIGURATION_CONFIG_KEY;
import static com.aws.greengrass.logmanager.LogManagerService.LOGS_UPLOADER_SERVICE_TOPICS;

@ImplementsService(name = LOGS_UPLOADER_SERVICE_TOPICS, version = "2.0.0")
public class LogManagerService extends PluginService {
    public static final String LOGS_UPLOADER_SERVICE_TOPICS = "aws.greengrass.LogManager";
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
    public static final String DEFAULT_FILE_REGEX = "^%s\\w*.log";
    public static final String COMPONENT_LOGS_CONFIG_TOPIC_NAME = "componentLogsConfiguration";
    public static final String SYSTEM_LOGS_CONFIG_TOPIC_NAME = "systemLogsConfiguration";
    public static final String COMPONENT_NAME_CONFIG_TOPIC_NAME = "componentName";
    public static final String FILE_REGEX_CONFIG_TOPIC_NAME = "logFileRegex";
    public static final String FILE_DIRECTORY_PATH_CONFIG_TOPIC_NAME = "logFileDirectoryPath";
    public static final String MIN_LOG_LEVEL_CONFIG_TOPIC_NAME = "minimumLogLevel";
    public static final String DISK_SPACE_LIMIT_CONFIG_TOPIC_NAME = "diskSpaceLimit";
    public static final String DISK_SPACE_LIMIT_UNIT_CONFIG_TOPIC_NAME = "diskSpaceLimitUnit";
    public static final String DELETE_LOG_FILES_AFTER_UPLOAD_CONFIG_TOPIC_NAME = "deleteLogFileAfterCloudUpload";
    public static final String UPLOAD_TO_CW_CONFIG_TOPIC_NAME = "uploadToCloudWatch";
    public static final String MULTILINE_PATTERN_CONFIG_TOPIC_NAME = "multiLineStartPattern";
    private static final int DEFAULT_PERIODIC_UPDATE_INTERVAL_SEC = 300;
    private final Object spaceManagementLock = new Object();

    final Map<String, Instant> lastComponentUploadedLogFileInstantMap =
            Collections.synchronizedMap(new LinkedHashMap<>());
    final Map<String, CurrentProcessingFileInformation> componentCurrentProcessingLogFile =
            new ConcurrentHashMap<>();
    Map<String, ComponentLogConfiguration> componentLogConfigurations = new ConcurrentHashMap<>();
    @Getter
    private final CloudWatchLogsUploader uploader;
    private final CloudWatchAttemptLogsProcessor logsProcessor;
    private final ExecutorService executorService;
    private final AtomicBoolean isCurrentlyUploading = new AtomicBoolean(false);
    private int periodicUpdateIntervalSec;
    private Future<?> spaceManagementThread;

    /**
     * Constructor.
     *
     * @param topics          The configuration coming from the nucleus.
     * @param uploader        {@link CloudWatchLogsUploader}
     * @param logProcessor    {@link CloudWatchAttemptLogsProcessor}
     * @param executorService {@link ExecutorService}
     */
    @Inject
    LogManagerService(Topics topics, CloudWatchLogsUploader uploader, CloudWatchAttemptLogsProcessor logProcessor,
                      ExecutorService executorService) {
        super(topics);
        this.uploader = uploader;
        this.logsProcessor = logProcessor;
        this.executorService = executorService;

        topics.lookup(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC)
                .dflt(DEFAULT_PERIODIC_UPDATE_INTERVAL_SEC)
                .subscribe((why, newv) -> periodicUpdateIntervalSec = Coerce.toInt(newv));
        this.uploader.registerAttemptStatus(LOGS_UPLOADER_SERVICE_TOPICS, this::handleCloudWatchAttemptStatus);

        Topics configTopics = topics.lookupTopics(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC);
        configTopics.subscribe((why, newv) -> {
            Map<String, Object> configTopicsPojo = configTopics.toPOJO();
            if (configTopicsPojo == null) {
                //TODO: fail the deployment.
                return;
            }
            processConfiguration(configTopicsPojo);
        });
    }

    private void processConfiguration(Map<String, Object> configTopicsPojo) {
        Map<String, ComponentLogConfiguration> newComponentLogConfigurations = new ConcurrentHashMap<>();
        configTopicsPojo.computeIfPresent(COMPONENT_LOGS_CONFIG_TOPIC_NAME, (s, o) -> {
            if (o instanceof ArrayList) {
                List<Object> list = (ArrayList) o;
                list.forEach(componentConfigObject -> {
                    if (componentConfigObject instanceof Map) {
                        handleUserComponentConfiguration(componentConfigObject, newComponentLogConfigurations);
                    }
                });
            }
            return o;
        });
        configTopicsPojo.computeIfPresent(SYSTEM_LOGS_CONFIG_TOPIC_NAME, (s, o) -> {
            Map<String, Object> systemConfigMap = (Map) o;
            AtomicBoolean isUploadToCloudWatch = new AtomicBoolean(false);
            systemConfigMap.computeIfPresent(UPLOAD_TO_CW_CONFIG_TOPIC_NAME, (s1, o1) -> {
                isUploadToCloudWatch.set(Coerce.toBoolean(o1));
                return o1;
            });
            if (!isUploadToCloudWatch.get()) {
                return true;
            }
            Path logsDirectoryPath = LogManager.getRootLogConfiguration().getStoreDirectory();
            ComponentLogConfiguration systemConfiguration = ComponentLogConfiguration.builder()
                    .fileNameRegex(Pattern.compile(String.format(DEFAULT_FILE_REGEX,
                            LogManager.getRootLogConfiguration().getFileName())))
                    .directoryPath(logsDirectoryPath)
                    .name(SYSTEM_LOGS_COMPONENT_NAME)
                    .componentType(ComponentType.GreengrassSystemComponent)
                    .build();

            setCommonComponentConfiguration(systemConfigMap, systemConfiguration);
            newComponentLogConfigurations.put(systemConfiguration.getName(), systemConfiguration);
            loadStateFromConfiguration(systemConfiguration.getName());
            return o;
        });

        this.componentLogConfigurations = newComponentLogConfigurations;

        scheduleSpaceManagementThread();
    }

    private void handleUserComponentConfiguration(Object componentConfigObject,
                                                  Map<String, ComponentLogConfiguration>
                                                          newComponentLogConfigurations) {
        Map<String, Object> componentConfigMap = (Map) componentConfigObject;
        ComponentLogConfiguration componentLogConfiguration = ComponentLogConfiguration.builder()
                .componentType(ComponentType.UserComponent)
                .build();
        setUserComponentConfiguration(componentConfigMap, componentLogConfiguration);
        setCommonComponentConfiguration(componentConfigMap, componentLogConfiguration);
        newComponentLogConfigurations.put(componentLogConfiguration.getName(), componentLogConfiguration);
        loadStateFromConfiguration(componentLogConfiguration.getName());
    }

    private void setUserComponentConfiguration(Map<String, Object> componentConfigMap,
                                               ComponentLogConfiguration componentLogConfiguration) {
        AtomicReference<Pattern> fileNameRegex = new AtomicReference<>();
        AtomicReference<Path> directoryPath = new AtomicReference<>();
        componentConfigMap.forEach((key, val) -> {
            switch (key) {
                case COMPONENT_NAME_CONFIG_TOPIC_NAME:
                    componentLogConfiguration.setName(Coerce.toString(val));
                    break;
                case FILE_REGEX_CONFIG_TOPIC_NAME:
                    String logFileRegexString = Coerce.toString(val);
                    if (Utils.isNotEmpty(logFileRegexString)) {
                        fileNameRegex.set(Pattern.compile(logFileRegexString));
                    } else {
                        fileNameRegex.set(Pattern.compile(String.format(DEFAULT_FILE_REGEX,
                                LogManager.getRootLogConfiguration().getFileName())));
                    }
                    break;
                case FILE_DIRECTORY_PATH_CONFIG_TOPIC_NAME:
                    String logFileDirectoryPathString = Coerce.toString(val);
                    if (Utils.isNotEmpty(logFileDirectoryPathString)) {
                        directoryPath.set(Paths.get(logFileDirectoryPathString));
                    }
                    break;
                case MULTILINE_PATTERN_CONFIG_TOPIC_NAME:
                    String multiLineStartPatternString = Coerce.toString(val);
                    if (Utils.isNotEmpty(multiLineStartPatternString)) {
                        componentLogConfiguration.setMultiLineStartPattern(Pattern
                                .compile(multiLineStartPatternString));
                    }
                    break;
                default:
                    break;
            }

        });
        componentLogConfiguration.setFileNameRegex(fileNameRegex.get());
        if (directoryPath.get() == null) {
            LogManager.getLogConfigurations().computeIfPresent(componentLogConfiguration.getName(), (s, logConfig) -> {
                componentLogConfiguration.setDirectoryPath(logConfig.getStoreDirectory());
                return logConfig;
            });
        } else {
            componentLogConfiguration.setDirectoryPath(directoryPath.get());
        }

    }

    private void setCommonComponentConfiguration(Map<String, Object> componentConfigMap,
                                                 ComponentLogConfiguration componentLogConfiguration) {
        AtomicReference<String> diskSpaceLimitString = new AtomicReference<>();
        AtomicReference<String> diskSpaceLimitUnitString = new AtomicReference<>();
        componentConfigMap.forEach((key, val) -> {
            switch (key) {
                case MIN_LOG_LEVEL_CONFIG_TOPIC_NAME:
                    setMinimumLogLevel(Coerce.toString(val), componentLogConfiguration);
                    break;
                case DISK_SPACE_LIMIT_CONFIG_TOPIC_NAME:
                    diskSpaceLimitString.set(Coerce.toString(val));
                    break;
                case DISK_SPACE_LIMIT_UNIT_CONFIG_TOPIC_NAME:
                    diskSpaceLimitUnitString.set(Coerce.toString(val));
                    break;
                case DELETE_LOG_FILES_AFTER_UPLOAD_CONFIG_TOPIC_NAME:
                    setDeleteLogFileAfterCloudUpload(Coerce.toString(val), componentLogConfiguration);
                    break;
                default:
                    break;
            }
        });
        setDiskSpaceLimit(diskSpaceLimitString.get(), diskSpaceLimitUnitString.get(), componentLogConfiguration);
    }

    private void scheduleSpaceManagementThread() {
        synchronized (spaceManagementLock) {
            if (spaceManagementThread != null) {
                spaceManagementThread.cancel(true);
            }
            spaceManagementThread = this.executorService.submit(() -> {
                try {
                    startWatchServiceOnLogFilePaths();
                } catch (IOException e) {
                    //TODO: fail the deployment?
                    logger.atError().cause(e).log("Unable to start space management thread.");
                }
            });
        }
    }

    private void setMinimumLogLevel(String minimumLogLevel, ComponentLogConfiguration componentLogConfiguration) {
        if (!StringUtils.isEmpty(minimumLogLevel)) {
            componentLogConfiguration.setMinimumLogLevel(Level.valueOf(minimumLogLevel));
        }
    }

    private void setDiskSpaceLimit(String diskSpaceLimit, String diskSpaceLimitUnit,
                                   ComponentLogConfiguration componentLogConfiguration) {
        if (!StringUtils.isEmpty(diskSpaceLimit) && !StringUtils.isEmpty(diskSpaceLimitUnit)) {
            long coefficient;
            switch (diskSpaceLimitUnit) {
                case "MB":
                    coefficient = FileSize.MB_COEFFICIENT;
                    break;
                case "GB":
                    coefficient = FileSize.GB_COEFFICIENT;
                    break;
                case "KB":
                default:
                    coefficient = FileSize.KB_COEFFICIENT;
                    break;
            }
            componentLogConfiguration.setDiskSpaceLimit(Coerce.toLong(diskSpaceLimit) * coefficient);
        }
    }

    private void setDeleteLogFileAfterCloudUpload(String deleteLogFileAfterCloudUpload,
                                                  ComponentLogConfiguration componentLogConfiguration) {
        if (!StringUtils.isEmpty(deleteLogFileAfterCloudUpload)) {
            componentLogConfiguration.setDeleteLogFileAfterCloudUpload(Coerce.toBoolean(deleteLogFileAfterCloudUpload));
        }
    }

    private void loadStateFromConfiguration(String componentName) {
        Topics currentProcessingComponentTopics = getRuntimeConfig()
                .lookupTopics(PERSISTED_COMPONENT_CURRENT_PROCESSING_FILE_INFORMATION, componentName);
        if (!currentProcessingComponentTopics.isEmpty()) {
            CurrentProcessingFileInformation currentProcessingFileInformation =
                    CurrentProcessingFileInformation.builder().build();
            currentProcessingComponentTopics.iterator().forEachRemaining(node ->
                    currentProcessingFileInformation.updateFromTopic((Topic) node));
            componentCurrentProcessingLogFile.put(componentName, currentProcessingFileInformation);
        }
        Topics lastFileProcessedComponentTopics = getRuntimeConfig()
                .lookupTopics(PERSISTED_COMPONENT_LAST_FILE_PROCESSED_TIMESTAMP, componentName);
        if (!lastFileProcessedComponentTopics.isEmpty()) {
            Topic lastFileProcessedTimeStamp =
                    lastFileProcessedComponentTopics.lookup(PERSISTED_LAST_FILE_PROCESSED_TIMESTAMP);
            lastComponentUploadedLogFileInstantMap.put(componentName,
                    Instant.ofEpochMilli(Coerce.toLong(lastFileProcessedTimeStamp)));
        }
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

        completedLogFilePerComponent.forEach((componentName, fileNames) -> {
            fileNames.stream().map(File::new).forEach(file -> {
                if (!lastComponentUploadedLogFileInstantMap.containsKey(componentName)
                        || lastComponentUploadedLogFileInstantMap.get(componentName)
                        .isBefore(Instant.ofEpochMilli(file.lastModified()))) {
                    lastComponentUploadedLogFileInstantMap.put(componentName,
                            Instant.ofEpochMilli(file.lastModified()));
                }
            });
            if (!componentLogConfigurations.containsKey(componentName)) {
                return;
            }
            ComponentLogConfiguration componentLogConfiguration = componentLogConfigurations.get(componentName);
            if (!componentLogConfiguration.isDeleteLogFileAfterCloudUpload()) {
                return;
            }
            fileNames.forEach(fileName -> {
                try {
                    boolean successfullyDeleted = Files.deleteIfExists(Paths.get(fileName));
                    if (successfullyDeleted) {
                        logger.atDebug().log("Successfully deleted file with name {}", fileName);
                    } else {
                        logger.atWarn().log("Unable to delete file with name {}", fileName);
                    }
                } catch (IOException e) {
                    logger.atError().cause(e).log("Unable to delete file with name: {}", fileName);
                }
            });
        });
        currentProcessingLogFilePerComponent.forEach(componentCurrentProcessingLogFile::put);

        componentCurrentProcessingLogFile.forEach((componentName, currentProcessingFileInformation) -> {
            Topics componentTopics = getRuntimeConfig()
                    .lookupTopics(PERSISTED_COMPONENT_CURRENT_PROCESSING_FILE_INFORMATION, componentName);
            componentTopics.replaceAndWait(currentProcessingFileInformation.convertToMapOfObjects());
        });
        lastComponentUploadedLogFileInstantMap.forEach((componentName, instant) -> {
            Topics componentTopics = getRuntimeConfig()
                    .lookupTopics(PERSISTED_COMPONENT_LAST_FILE_PROCESSED_TIMESTAMP, componentName);
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
            AtomicReference<Optional<ComponentLogFileInformation>> componentLogFileInformation =
                    new AtomicReference<>(Optional.empty());
            // Get the latest known configurations because the componentLogConfigurations can change if a new
            // configuration is received from the customer.
            final Map<String, ComponentLogConfiguration> currentComponentLogConfigurations = componentLogConfigurations;
            for (ComponentLogConfiguration componentLogConfiguration : currentComponentLogConfigurations.values()) {
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

    /**
     * Starts a Watch Service on all the log directories where customer has specified disk space limit.
     * If any file is modified and created in any of those directories, we get an event containing that information.
     * The log manager will then verify that the size of all log files in that directory for the component does not
     * exceed the disk space limit set by the customer. If it does, the log manager will delete the log files until
     * the limit is met.
     *
     * @throws IOException if unable to initialise a new Watch Service.
     */
    private void startWatchServiceOnLogFilePaths() throws IOException {
        //TODO: Optimize this.
        // The optimization would be to have best of both worlds. The file watcher will mark the changed components
        // log directories. Another scheduled thread will look at that and clean up files if necessary.
        try (WatchService watchService = FileSystems.getDefault().newWatchService()) {
            try {
                componentLogConfigurations.forEach((componentName, componentLogConfiguration) -> {
                    // Only register the path of a component if the disk space limit is set.
                    if (componentLogConfiguration.getDiskSpaceLimit() > 0) {
                        Path path = componentLogConfiguration.getDirectoryPath();
                        try {
                            path.register(watchService, StandardWatchEventKinds.ENTRY_CREATE,
                                    StandardWatchEventKinds.ENTRY_MODIFY);
                        } catch (IOException e) {
                            logger.atError().cause(e).log("Unable to watch {} for log space management.",
                                    path);
                        }
                    }
                });

                while (true) {
                    final WatchKey watchKey = watchService.take();
                    final Watchable watchable = watchKey.watchable();

                    //Since we are registering only paths in thw watch service, the watchables must be paths
                    if (watchable instanceof Path) {
                        final Path directory = (Path) watchable;
                        Map<String, ComponentLogConfiguration> updatedComponentsConfiguration = new HashMap<>();
                        // Based on the directory and file name created/modified, the log manager will store the
                        // component information which need to be checked.
                        List<ComponentLogConfiguration> allComponentsConfiguration =
                                componentLogConfigurations.values().stream()
                                        .filter(componentLogConfiguration ->
                                                componentLogConfiguration.getDirectoryPath().equals(directory))
                                        .collect(Collectors.toList());

                        // The events can have multiple log files in the same directory. We need to make sure we get the
                        // correct component based on the file name pattern.
                        for (WatchEvent<?> event : watchKey.pollEvents()) {
                            String fileName = Coerce.toString(event.context());
                            List<ComponentLogConfiguration> list = allComponentsConfiguration.stream()
                                    .filter(componentLogConfiguration -> componentLogConfiguration
                                            .getMultiLineStartPattern().matcher(fileName).find())
                                    .collect(Collectors.toList());
                            list.forEach(componentLogConfiguration ->
                                    updatedComponentsConfiguration.putIfAbsent(componentLogConfiguration.getName(),
                                            componentLogConfiguration));
                        }

                        // Get the total log files size in the directories for all updates components. If the log files
                        // size exceed the limit set by the customer, the log manager will figure out the minimum bytes
                        // to be deleted inorder to meet the limit. It will then go ahead and delete the oldest files
                        // until the minimum bytes have been deleted.
                        for (ComponentLogConfiguration componentLogConfiguration :
                                updatedComponentsConfiguration.values()) {
                            try (LongStream fileSizes = Files.walk(componentLogConfiguration.getDirectoryPath())
                                         .filter(p -> {
                                             File file = p.toFile();
                                             return file.isFile() && componentLogConfiguration.getFileNameRegex()
                                                     .matcher(file.getName()).find();
                                         })
                                         .mapToLong(p -> p.toFile().length())) {
                                long totalDirectorySize = fileSizes.sum();

                                if (totalDirectorySize > componentLogConfiguration.getDiskSpaceLimit()) {
                                    long minimumBytesToBeDeleted =
                                            totalDirectorySize - componentLogConfiguration.getDiskSpaceLimit();
                                    deleteFiles(minimumBytesToBeDeleted, componentLogConfiguration.getDirectoryPath(),
                                            componentLogConfiguration.getFileNameRegex());
                                }
                            }
                        }
                    }

                    if (!watchKey.reset()) {
                        logger.atError().log("Log Space Management encountered an issue. Returning.");
                        scheduleSpaceManagementThread();
                        break;
                    }
                }
            } catch (IOException e) {
                // If there is any other IOException, then we should restart the thread.
                scheduleSpaceManagementThread();
            }
        } catch (InterruptedException e) {
            logger.atError().cause(e).log("Log Space management interrupted. Returning.");
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Deletes the oldest log files which matches the file name pattern. The log files will be deleted until the
     * minimum number of bytes have been deleted.
     *
     * @param minimumBytesToBeDeleted The minimum number of bytes that need to be deleted.
     * @param directoryPath           The directory path for the log files.
     * @param fileNameRegex           The file name regex.
     */
    private void deleteFiles(long minimumBytesToBeDeleted, Path directoryPath, Pattern fileNameRegex) {
        long bytesDeleted = 0;
        File folder = directoryPath.toFile();
        List<File> allFiles = new ArrayList<>();
        File[] files = folder.listFiles();
        if (files != null && files.length > 0) {
            for (File file : files) {
                if (file.isFile() && fileNameRegex.matcher(file.getName()).find()) {
                    allFiles.add(file);
                }
            }
            // Sort the files by the last modified time.
            allFiles.sort(Comparator.comparingLong(File::lastModified));
            int fileIndex = 0;
            while (bytesDeleted < minimumBytesToBeDeleted) {
                File fileToBeDeleted = allFiles.get(fileIndex++);
                long fileSize = fileToBeDeleted.length();
                boolean successfullyDeleted = fileToBeDeleted.delete();
                if (successfullyDeleted) {
                    logger.atDebug().log("Successfully deleted file with name {}",
                            fileToBeDeleted.getAbsolutePath());
                    bytesDeleted += fileSize;
                } else {
                    logger.atWarn().log("Unable to delete file with name {}",
                            fileToBeDeleted.getAbsolutePath());
                    break;
                }
            }
        }
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
        @JsonProperty(PERSISTED_CURRENT_PROCESSING_FILE_NAME)
        private String fileName;
        @JsonProperty(PERSISTED_CURRENT_PROCESSING_FILE_START_POSITION)
        private long startPosition;
        @JsonProperty(PERSISTED_CURRENT_PROCESSING_FILE_LAST_MODIFIED_TIME)
        private long lastModifiedTime;

        public Map<String, Object> convertToMapOfObjects() {
            Map<String, Object> currentProcessingFileInformationMap = new HashMap<>();
            currentProcessingFileInformationMap.put(PERSISTED_CURRENT_PROCESSING_FILE_NAME, fileName);
            currentProcessingFileInformationMap.put(PERSISTED_CURRENT_PROCESSING_FILE_START_POSITION, startPosition);
            currentProcessingFileInformationMap.put(PERSISTED_CURRENT_PROCESSING_FILE_LAST_MODIFIED_TIME,
                    lastModifiedTime);
            return currentProcessingFileInformationMap;
        }

        public void updateFromTopic(Topic topic) {
            switch (topic.getName()) {
                case PERSISTED_CURRENT_PROCESSING_FILE_NAME:
                    fileName = Coerce.toString(topic);
                    break;
                case PERSISTED_CURRENT_PROCESSING_FILE_START_POSITION:
                    startPosition = Coerce.toLong(topic);
                    break;
                case PERSISTED_CURRENT_PROCESSING_FILE_LAST_MODIFIED_TIME:
                    lastModifiedTime = Coerce.toLong(topic);
                    break;
                default:
                    break;
            }
        }

        public static CurrentProcessingFileInformation convertFromMapOfObjects(
                Map<String, Object> currentProcessingFileInformationMap) {
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
