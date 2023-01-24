/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.logmanager;

import ch.qos.logback.core.util.FileSize;
import com.aws.greengrass.config.Topic;
import com.aws.greengrass.config.Topics;
import com.aws.greengrass.config.UpdateBehaviorTree;
import com.aws.greengrass.config.WhatHappened;
import com.aws.greengrass.dependency.ImplementsService;
import com.aws.greengrass.lifecyclemanager.PluginService;
import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.logging.impl.config.LogConfig;
import com.aws.greengrass.logmanager.exceptions.InvalidLogGroupException;
import com.aws.greengrass.logmanager.model.CloudWatchAttempt;
import com.aws.greengrass.logmanager.model.CloudWatchAttemptLogFileInformation;
import com.aws.greengrass.logmanager.model.CloudWatchAttemptLogInformation;
import com.aws.greengrass.logmanager.model.ComponentLogConfiguration;
import com.aws.greengrass.logmanager.model.ComponentLogFileInformation;
import com.aws.greengrass.logmanager.model.ComponentType;
import com.aws.greengrass.logmanager.model.EventType;
import com.aws.greengrass.logmanager.model.LogFile;
import com.aws.greengrass.logmanager.model.LogFileGroup;
import com.aws.greengrass.logmanager.model.LogFileInformation;
import com.aws.greengrass.logmanager.model.ProcessingFileLRU;
import com.aws.greengrass.util.Coerce;
import com.aws.greengrass.util.NucleusPaths;
import com.aws.greengrass.util.Utils;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.event.Level;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
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
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.inject.Inject;

import static com.aws.greengrass.componentmanager.KernelConfigResolver.CONFIGURATION_CONFIG_KEY;
import static com.aws.greengrass.logmanager.LogManagerService.LOGS_UPLOADER_SERVICE_TOPICS;


@ImplementsService(name = LOGS_UPLOADER_SERVICE_TOPICS, version = "2.0.0")
public class LogManagerService extends PluginService {
    public static final String LOGS_UPLOADER_SERVICE_TOPICS = "aws.greengrass.LogManager";
    public static final String LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC = "periodicUploadIntervalSec";
    public static final String LOGS_UPLOADER_CONFIGURATION_TOPIC = "logsUploaderConfiguration";
    public static final String SYSTEM_LOGS_COMPONENT_NAME = "System";
    // @deprecated This is deprecated value in versions greater than 2.2, but keep it here to avoid
    // upgrade-downgrade issues.
    public static final String PERSISTED_CURRENT_PROCESSING_FILE_NAME = "currentProcessingFileName";
    public static final String PERSISTED_CURRENT_PROCESSING_FILE_HASH = "currentProcessingFileHash";
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
    public static final String COMPONENT_LOGS_CONFIG_MAP_TOPIC_NAME = "componentLogsConfigurationMap";
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
    public static final int DEFAULT_PERIODIC_UPDATE_INTERVAL_SEC = 300;
    public static final int DEFAULT_MAX_FILES_TO_TRACK_PER_COMPONENT = 5;
    private final Object spaceManagementLock = new Object();
    private final List<Consumer<EventType>> serviceStatusListeners = new ArrayList<>();

    // public only for integ tests
    public final Map<String, Instant> lastComponentUploadedLogFileInstantMap =
            Collections.synchronizedMap(new LinkedHashMap<>());

    // TODO: Remove - OLD
    final Map<String, CurrentProcessingFileInformation> componentCurrentProcessingLogFile =
            new ConcurrentHashMap<>();
    final Map<String, ProcessingFileLRU> processingFilesInformation =
            new ConcurrentHashMap<>();


    @Getter
    Map<String, ComponentLogConfiguration> componentLogConfigurations = new ConcurrentHashMap<>();
    @Getter
    private final CloudWatchLogsUploader uploader;
    private final CloudWatchAttemptLogsProcessor logsProcessor;
    private final ExecutorService executorService;
    private final AtomicBoolean isCurrentlyUploading = new AtomicBoolean(false);
    @Getter
    private int periodicUpdateIntervalSec;
    private Future<?> spaceManagementThread;
    private final Path workDir;

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
                      ExecutorService executorService, NucleusPaths nucleusPaths) throws IOException {
        super(topics);
        this.uploader = uploader;
        this.logsProcessor = logProcessor;
        this.executorService = executorService;
        this.workDir = nucleusPaths.workPath(LOGS_UPLOADER_SERVICE_TOPICS);

        topics.lookupTopics(CONFIGURATION_CONFIG_KEY).subscribe((why, newv) -> {
            if (why == WhatHappened.timestampUpdated) {
                return;
            }
            logger.atDebug().kv("why", why).kv("node", newv).log();
            handlePeriodicUploadIntervalSecConfig(topics);
            handleLogsUploaderConfig(topics);
        });

        this.uploader.registerAttemptStatus(LOGS_UPLOADER_SERVICE_TOPICS, this::handleCloudWatchAttemptStatus);
    }

    private void handlePeriodicUploadIntervalSecConfig(Topics topics) {
        int periodicUploadIntervalSecInput = Coerce.toInt(topics.lookup(CONFIGURATION_CONFIG_KEY,
                        LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC)
                .dflt(DEFAULT_PERIODIC_UPDATE_INTERVAL_SEC)
                .toPOJO());

        if (periodicUploadIntervalSecInput > 0) {
            periodicUpdateIntervalSec = periodicUploadIntervalSecInput;
        } else {
            logger.atWarn().log("Invalid config value, {}, for periodicUploadIntervalSec. Must be an "
                            + "integer greater than 0. Using default value of 300 (5 minutes)",
                    periodicUploadIntervalSecInput);
            periodicUpdateIntervalSec = DEFAULT_PERIODIC_UPDATE_INTERVAL_SEC;
        }
    }

    /**
     * Find the current 'logsUploaderConfiguration' configuration from runtime.
     * @param topics The topics to search.
     */
    private void handleLogsUploaderConfig(Topics topics) {
        Topics logsUploaderTopics = topics.lookupTopics(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC);
        Map<String, Object> logsUploaderConfigTopicsPojo = logsUploaderTopics.toPOJO();
        if (logsUploaderConfigTopicsPojo == null) {
            //TODO: fail the deployment.
            return;
        }
        processConfiguration(logsUploaderConfigTopicsPojo);
    }

    private synchronized void processConfiguration(Map<String, Object> logsUploaderConfigTopicsPojo) {
        Map<String, ComponentLogConfiguration> newComponentLogConfigurations = new ConcurrentHashMap<>();
        logsUploaderConfigTopicsPojo.computeIfPresent(COMPONENT_LOGS_CONFIG_MAP_TOPIC_NAME, (s, o) -> {
            if (o instanceof Map) {
                Map<String, Object> map = (Map) o;
                map.forEach((componentName, componentConfigObject) -> {
                    if (componentConfigObject instanceof Map) {
                        Map<String, Object> componentConfigObjectMap = (Map) componentConfigObject;
                        componentConfigObjectMap.put(COMPONENT_NAME_CONFIG_TOPIC_NAME, componentName);
                        handleUserComponentConfiguration(componentConfigObjectMap, newComponentLogConfigurations);
                    }
                });
            }
            return o;
        });
        logsUploaderConfigTopicsPojo.computeIfPresent(COMPONENT_LOGS_CONFIG_TOPIC_NAME, (s, o) -> {
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
        logsUploaderConfigTopicsPojo.computeIfPresent(SYSTEM_LOGS_CONFIG_TOPIC_NAME, (s, o) -> {
            logger.atInfo().log("Process LogManager configuration for Greengrass system logs");
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
                            Pattern.quote(LogManager.getRootLogConfiguration().getFileName()))))
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
        logger.atInfo().log("Finished processing LogManager configuration");

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
        logger.atInfo().kv("componentName", componentLogConfiguration.getName())
                .log("Process LogManager configuration for Greengrass user component");
        setCommonComponentConfiguration(componentConfigMap, componentLogConfiguration);
        newComponentLogConfigurations.put(componentLogConfiguration.getName(), componentLogConfiguration);
        loadStateFromConfiguration(componentLogConfiguration.getName());
    }

    @SuppressWarnings("PMD.ConfusingTernary")
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
        LogConfig logConfig = null;
        if (LogManager.getLogConfigurations().containsKey(componentLogConfiguration.getName())) {
            logConfig = LogManager.getLogConfigurations().get(componentLogConfiguration.getName());
        }

        if (fileNameRegex.get() != null) {
            componentLogConfiguration.setFileNameRegex(fileNameRegex.get());
        } else if (logConfig != null) {
            // If details missing in log manager configuration, get component log file name from its logger config
            componentLogConfiguration.setFileNameRegex(Pattern.compile(String.format(DEFAULT_FILE_REGEX,
                    Pattern.quote(logConfig.getFileName()))));
        } else {
            // If logger config is missing, default to <componentName>_*.log
            componentLogConfiguration.setFileNameRegex(Pattern.compile(String.format(DEFAULT_FILE_REGEX,
                    Pattern.quote(componentLogConfiguration.getName()))));
        }

        if (directoryPath.get() != null) {
            componentLogConfiguration.setDirectoryPath(directoryPath.get());
        } else if (logConfig != null) {
            // If details missing in log manager configuration, get component log directory from its logger config
            componentLogConfiguration.setDirectoryPath(logConfig.getStoreDirectory());
        } else {
            // If log config is missing, default to root log directory
            componentLogConfiguration.setDirectoryPath(LogManager.getRootLogConfiguration().getStoreDirectory());
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

    @SuppressWarnings("PMD.AvoidCatchingThrowable")
    private void scheduleSpaceManagementThread() {
        synchronized (spaceManagementLock) {
            if (spaceManagementThread != null) {
                spaceManagementThread.cancel(true);
            }
            spaceManagementThread = this.executorService.submit(() -> {
                try {
                    logger.atInfo().log("Starting space management thread");
                    startWatchServiceOnLogFilePaths();
                } catch (IOException e) {
                    //TODO: fail the deployment?
                    logger.atError().cause(e).log("Unable to start space management thread.");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Throwable e) {
                    logger.atError().log("Failure in log manager space management", e);
                    scheduleSpaceManagementThread(); // restart space management
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
        if (!StringUtils.isEmpty(diskSpaceLimit)) {
             if (StringUtils.isEmpty(diskSpaceLimitUnit)) {
                 diskSpaceLimitUnit = "KB";
             }
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
//            ProcessingFileLRU lru =  new ProcessingFileLRU(DEFAULT_MAX_FILES_TO_TRACK_PER_COMPONENT);
//
//            currentProcessingComponentTopics.iterator().forEachRemaining(node -> {
//                CurrentProcessingFileInformation fileInformation = CurrentProcessingFileInformation.builder().build();
//
//                // Handle legacy format of configuration before 2.3.0 and below
//
//                fileInformation.updateFromTopic((Topic) node);
//
//                // Handle new configuration 2.3.1 and above
//
//                currentProcessingComponentTopics.lookupTopics(node.getName()).iterator().forEachRemaining(subNode -> {
//                    fileInformation.updateFromTopic((Topic) node);
//                });
//
//                if (Utils.isNotEmpty(fileInformation.getFileHash())) {
//                    lru.put(fileInformation);
//                }
//            });
//
//            processingFilesInformation.put(componentName, lru);
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
        Map<String, Set<LogFile>> completedLogFilePerComponent = new ConcurrentHashMap<>();

        cloudWatchAttempt.getLogStreamUploadedSet().forEach((streamName) -> {
            CloudWatchAttemptLogInformation attemptLogInformation =
                    cloudWatchAttempt.getLogStreamsToLogEventsMap().get(streamName);
            attemptLogInformation.getAttemptLogFileInformationMap().forEach(
                    (fileHash, cloudWatchAttemptLogFileInformation) ->
                            processCloudWatchAttemptLogInformation(completedLogFilePerComponent,
                                    attemptLogInformation, fileHash,
                                    cloudWatchAttemptLogFileInformation));
        });

        completedLogFilePerComponent.forEach((componentName, completedFiles) -> {
            completedFiles.forEach(file -> {
                updatelastComponentUploadedLogFile(lastComponentUploadedLogFileInstantMap, componentName,
                        file);
            });

            if (!componentLogConfigurations.containsKey(componentName)) {
                return;
            }

            ComponentLogConfiguration componentLogConfiguration = componentLogConfigurations.get(componentName);
            completedFiles.forEach(file -> this.deleteFile(componentLogConfiguration, file));
        });


        // Update the runtime configuration and store the last processed file information

        context.runOnPublishQueueAndWait(() -> {
            processingFilesInformation.forEach((componentName, lru) -> {
                Topics componentTopics =
                        getRuntimeConfig().lookupTopics(PERSISTED_COMPONENT_CURRENT_PROCESSING_FILE_INFORMATION,
                                componentName);

                logger.info("LRU {}", lru.toMap());

                componentTopics.updateFromMap(lru.toMap(),
                        new UpdateBehaviorTree(UpdateBehaviorTree.UpdateBehavior.REPLACE, System.currentTimeMillis()));
            });
        });
        context.waitForPublishQueueToClear();

        lastComponentUploadedLogFileInstantMap.forEach((componentName, instant) -> {
            Topics componentTopics = getRuntimeConfig()
                    .lookupTopics(PERSISTED_COMPONENT_LAST_FILE_PROCESSED_TIMESTAMP, componentName);
            Topic lastFileProcessedTimeStamp = componentTopics.createLeafChild(PERSISTED_LAST_FILE_PROCESSED_TIMESTAMP);
            lastFileProcessedTimeStamp.withValue(instant.toEpochMilli());
        });
        isCurrentlyUploading.set(false);
    }

    private void deleteFile(ComponentLogConfiguration config, LogFile file) {
        if (!config.isDeleteLogFileAfterCloudUpload()) {
            return;
        }

        boolean successfullyDeleted = file.delete();
        if (successfullyDeleted) {
            logger.atDebug().log("Successfully deleted file with name {}", file.getName());
        } else {
            logger.atWarn().log("Unable to delete file with name {}", file.getName());
        }
    }

    private void processCloudWatchAttemptLogInformation(Map<String, Set<LogFile>> completedLogFilePerComponent,
                                                        CloudWatchAttemptLogInformation attemptLogInformation,
                                                        String fileHash,
                                                        CloudWatchAttemptLogFileInformation
                                                                cloudWatchAttemptLogFileInformation) {
        LogFileGroup logFileGroup = attemptLogInformation.getLogFileGroup();
        LogFile file = logFileGroup.getFile(fileHash);

        if (file == null) {
            logger.atTrace().kv("fileHash", fileHash).log("component",
                    logFileGroup.getFilePattern(), "File not found in directory");
            return;
        }

        // If we have completely read the file, then we need add it to the completed files list and remove it
        // it (if necessary) for the current processing list.
        String componentName = attemptLogInformation.getComponentName();
        long bytesUploaded = cloudWatchAttemptLogFileInformation.getBytesRead()
                + cloudWatchAttemptLogFileInformation.getStartPosition();

        if (!logFileGroup.isActiveFile(file) && file.length() == bytesUploaded) {
            Set<LogFile> completedFiles = completedLogFilePerComponent.getOrDefault(componentName,
                    new HashSet<>());
            completedFiles.add(file);
            completedLogFilePerComponent.put(componentName, completedFiles);
        }


        // Track each file that got uploaded. So that if modified in the future we can still track upload
        // the new contents starting from the last upload position.

        CurrentProcessingFileInformation processingFileInformation =
                CurrentProcessingFileInformation.builder()
                        .fileName(file.getSourcePath()) // @deprecated
                        .startPosition(bytesUploaded)
                        .lastModifiedTime(cloudWatchAttemptLogFileInformation.getLastModifiedTime())
                        .fileHash(fileHash)
                        .build();

        if (!processingFilesInformation.containsKey(componentName)) {
            // TODO: need to figure a smarter mechanism to adjust the LRU capacity. Add an adjust capacity method
            processingFilesInformation.put(componentName, new ProcessingFileLRU(
                    Math.max(logFileGroup.getLogFiles().size(), DEFAULT_MAX_FILES_TO_TRACK_PER_COMPONENT)));
        }

        ProcessingFileLRU lru  = processingFilesInformation.get(componentName);
        lru.put(processingFileInformation);
    }

    /**
     * This updates the lastComponentUploadedLogFileInstantMap if the current component has a newly uploaded file,
     * which the lastModified time is larger than saved value of lastModified time.
     * @param lastComponentUploadedLogFileInstantMap The instant map of all components.
     * @param componentName componentName.
     * @param logFile the logFile that is going to be recorded.
     */
    private void updatelastComponentUploadedLogFile(Map<String, Instant> lastComponentUploadedLogFileInstantMap,
                                                    String componentName,
                                                    LogFile logFile) {
        if (!lastComponentUploadedLogFileInstantMap.containsKey(componentName)
                || lastComponentUploadedLogFileInstantMap.get(componentName)
                .isBefore(Instant.ofEpochMilli(logFile.lastModified()))) {
            lastComponentUploadedLogFileInstantMap.put(componentName,
                    Instant.ofEpochMilli(logFile.lastModified()));
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
    @SuppressWarnings("PMD.CollapsibleIfStatements")
    private void processLogsAndUpload() throws InterruptedException {
        while (true) {
            //TODO: this is only done for passing the current text. But in practise, we don`t need to intentionally
            // sleep here.
            if (!isCurrentlyUploading.compareAndSet(false, true)) {
                TimeUnit.SECONDS.sleep(periodicUpdateIntervalSec);
                continue;
            }
            List<ComponentLogFileInformation> componentMetadata = new ArrayList<>();
            // Get the latest known configurations because the componentLogConfigurations can change if a new
            // configuration is received from the customer.
            for (ComponentLogConfiguration componentLogConfiguration : componentLogConfigurations.values()) {
                String componentName = componentLogConfiguration.getName();
                Instant lastUploadedLogFileTimeMs =
                        lastComponentUploadedLogFileInstantMap.getOrDefault(componentName,
                                Instant.EPOCH);

                try {
                    LogFileGroup logFileGroup = LogFileGroup.create(componentLogConfiguration,
                            lastUploadedLogFileTimeMs, workDir);

                    if (logFileGroup.isEmpty()) {
                        continue;
                    }

                    ComponentLogFileInformation logFileInfo = ComponentLogFileInformation.builder()
                            .name(componentName)
                            .multiLineStartPattern(componentLogConfiguration.getMultiLineStartPattern())
                            .desiredLogLevel(componentLogConfiguration.getMinimumLogLevel())
                            .componentType(componentLogConfiguration.getComponentType())
                            .logFileGroup(logFileGroup)
                            .build();

                    componentMetadata.add(logFileInfo);

                    logFileGroup.forEach(file -> {
                        long startPosition = 0;
                        String fileHash = file.hashString();

                        // If the file was partially read in the previous run, then get the starting position for
                        // new log lines.
                        if (processingFilesInformation.containsKey(componentName)) {
                            ProcessingFileLRU lru = processingFilesInformation.get(componentName);
                            Optional<CurrentProcessingFileInformation> info = lru.get(fileHash);

                            if (info.isPresent()) {
                                startPosition = info.get().getStartPosition();
                            }
                        }

                        LogFileInformation logFileInformation = LogFileInformation.builder()
                                .logFile(file)
                                .startPosition(startPosition)
                                .fileHash(fileHash)
                                .build();

                        if (startPosition < file.length()) {
                            logFileInfo.getLogFileInformationList().add(logFileInformation);
                        } else if (startPosition == file.length() && !logFileGroup.isActiveFile(file)) {
                            updatelastComponentUploadedLogFile(lastComponentUploadedLogFileInstantMap,
                                    componentName, file);

                            // NOTE: This handles the scenario where we are uploading the active file constantly and
                            // upload all its contents and then rotates. We would pick it again on the next cycle, and
                            // it will fall under this condition but since it was the active file on the previous
                            // cycle it didn't get deleted
                            deleteFile(componentLogConfiguration, file);
                        }
                    });
                } catch (SecurityException e) {
                    logger.atError().cause(e).log("Unable to get log files for {} from {}",
                            componentName, componentLogConfiguration.getDirectoryPath());
                } catch (InvalidLogGroupException e) {
                    logger.atDebug().cause(e).log("Unable to read the directory");
                }
            }

            //TODO: need to refactor. This is for the case componentMetadata may be empty.
            // This will get refactored when the logFileGroup won't return files that should not be processed.
            componentMetadata = componentMetadata.stream()
                    .filter(metaData -> !metaData.getLogFileInformationList().isEmpty())
                    .collect(Collectors.toList());

            componentMetadata.forEach((unit) -> {
                CloudWatchAttempt cloudWatchAttempt = logsProcessor.processLogFiles(unit);
                uploader.upload(cloudWatchAttempt, 1);
            });
            isCurrentlyUploading.set(false);

            // TODO: Change this. It is only added for testing
            emitEventStatus(EventType.ALL_COMPONENTS_PROCESSED);
            // after handle one cycle, we sleep for interval to avoid seamless scanning and processing next cycle.
            // TODO, do not use lazy sleep. Use scheduler to unblock the thread.
            TimeUnit.SECONDS.sleep(periodicUpdateIntervalSec);
        }
    }

    public void registerEventStatusListener(Consumer<EventType> callback) {
        serviceStatusListeners.add(callback);
    }

    private void emitEventStatus(EventType eventStatus) {
        serviceStatusListeners.forEach(callback -> callback.accept(eventStatus));
    }

    @Override
    public void startup() throws InterruptedException {
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
     * @throws IOException          if unable to initialise a new Watch Service.
     * @throws InterruptedException if thread is shutdown
     */
    private void startWatchServiceOnLogFilePaths() throws IOException, InterruptedException {
        //TODO: Optimize this.
        // The optimization would be to have best of both worlds. The file watcher will mark the changed components
        // log directories. Another scheduled thread will look at that and clean up files if necessary.
        try (WatchService watchService = FileSystems.getDefault().newWatchService()) {
            try {
                componentLogConfigurations.forEach((componentName, componentLogConfiguration) -> {
                    // Only register the path of a component if the disk space limit is set.
                    if (componentLogConfiguration != null && componentLogConfiguration.getDiskSpaceLimit() != null
                            && componentLogConfiguration.getDiskSpaceLimit() > 0) {
                        Path path = componentLogConfiguration.getDirectoryPath();
                        try {
                            path.register(watchService, StandardWatchEventKinds.ENTRY_CREATE,
                                    StandardWatchEventKinds.ENTRY_MODIFY);
                            logger.atDebug().kv("filePath", path).log("Start watching file for log space management.");
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
                            if (fileName == null) {
                                continue;
                            }
                            List<ComponentLogConfiguration> list = allComponentsConfiguration.stream()
                                    .filter(componentLogConfiguration -> componentLogConfiguration
                                            .getFileNameRegex().matcher(fileName).find())
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
                            try {
                                deleteFilesIfNecessary(componentLogConfiguration);
                            } catch (UncheckedIOException e) {
                                logger.atWarn().log("Unchecked error thrown when collecting files to be deleted",
                                        Utils.getUltimateCause(e));
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
        }
    }

    /**
     * Deletes the oldest log files which matches the file name pattern. The log files will be deleted until the
     * minimum number of bytes have been deleted.
     *
     * @param componentLogConfiguration Log configuration to investigate for deletion
     * @throws IOException for errors during directory walking
     */
    private void deleteFilesIfNecessary(ComponentLogConfiguration componentLogConfiguration) throws IOException {
        if (componentLogConfiguration.getDiskSpaceLimit() == null) {
            return;
        }
        try (Stream<Path> fileStream = Files.walk(componentLogConfiguration.getDirectoryPath())
                .filter(p -> {
                    File file = p.toFile();
                    return file.isFile() && componentLogConfiguration.getFileNameRegex()
                            .matcher(file.getName()).find();
                })) {
            List<Path> paths = fileStream.collect(Collectors.toList());
            long totalBytes = paths.stream().mapToLong(p -> p.toFile().length()).sum();
            long minimumBytesToBeDeleted = totalBytes - componentLogConfiguration.getDiskSpaceLimit();

            // If we don't need to remove any bytes, or if the file count is only 1 (or less), then there's nothing
            // to do.
            if (minimumBytesToBeDeleted <= 0 || paths.size() < 2) {
                return;
            }

            long bytesDeleted = 0;
            // Sort the files by the last modified time.
            paths.sort(Comparator.comparingLong((p) -> p.toFile().lastModified()));
            int fileIndex = 0;
            // stop before the end to skip the active file which should have the newest modified time
            while (bytesDeleted < minimumBytesToBeDeleted && fileIndex < paths.size() - 1) {
                Path fileToBeDeleted = paths.get(fileIndex++);
                long fileSize = fileToBeDeleted.toFile().length();
                try {
                    Files.deleteIfExists(fileToBeDeleted);
                } catch (IOException e) {
                    logger.atWarn().log("Unable to delete file: {}", fileToBeDeleted.toAbsolutePath(), e);
                    break;
                }
                logger.atInfo().log("Successfully deleted file: {}", fileToBeDeleted.toAbsolutePath());
                bytesDeleted += fileSize;
            }
        }
    }

    @Override
    @SuppressWarnings("PMD.UselessOverridingMethod")
    public void shutdown() throws InterruptedException {
        super.shutdown();
        isCurrentlyUploading.set(false);
    }

    @Builder
    @Getter
    @Data
    public static class CurrentProcessingFileInformation {
        //This is deprecated value in versions greater than 2.2, but keep it here to avoid
        // upgrade-downgrade issues.
        @JsonProperty(PERSISTED_CURRENT_PROCESSING_FILE_NAME)
        private String fileName;
        @JsonProperty(PERSISTED_CURRENT_PROCESSING_FILE_START_POSITION)
        private long startPosition;
        @JsonProperty(PERSISTED_CURRENT_PROCESSING_FILE_LAST_MODIFIED_TIME)
        private long lastModifiedTime;
        @JsonProperty(PERSISTED_CURRENT_PROCESSING_FILE_HASH)
        private String fileHash;
        private static final Logger logger = LogManager.getLogger(CurrentProcessingFileInformation.class);

        public Map<String, Object> convertToMapOfObjects() {
            Map<String, Object> currentProcessingFileInformationMap = new HashMap<>();
            // @deprecated  This is deprecated value in versions greater than 2.2, but keep it here to avoid
            // upgrade-downgrade issues.
            currentProcessingFileInformationMap.put(PERSISTED_CURRENT_PROCESSING_FILE_NAME, fileName);
            currentProcessingFileInformationMap.put(PERSISTED_CURRENT_PROCESSING_FILE_START_POSITION, startPosition);
            currentProcessingFileInformationMap.put(PERSISTED_CURRENT_PROCESSING_FILE_LAST_MODIFIED_TIME,
                    lastModifiedTime);
            currentProcessingFileInformationMap.put(PERSISTED_CURRENT_PROCESSING_FILE_HASH, fileHash);
            return currentProcessingFileInformationMap;
        }

        public void updateFromTopic(Topic topic) {
            switch (topic.getName()) {
                //  @deprecated  This is deprecated value in versions greater than 2.2, but keep it here to avoid
                // upgrade-downgrade issues.
                case PERSISTED_CURRENT_PROCESSING_FILE_NAME:
                    fileName = Coerce.toString(topic);
                    fileHash = getFileHashFromTopic(topic);
                    break;
                case PERSISTED_CURRENT_PROCESSING_FILE_START_POSITION:
                    startPosition = Coerce.toLong(topic);
                    break;
                case PERSISTED_CURRENT_PROCESSING_FILE_LAST_MODIFIED_TIME:
                    lastModifiedTime = Coerce.toLong(topic);
                    break;
                case PERSISTED_CURRENT_PROCESSING_FILE_HASH:
                    fileHash = getFileHashFromTopic(topic);
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
                    .fileHash(Coerce.toString(currentProcessingFileInformationMap
                            .get(PERSISTED_CURRENT_PROCESSING_FILE_HASH)))
                    .build();
        }

        private String getFileHashFromTopic(Topic topic) {
            Topics topics = topic.parent;
            Topic hashTopic = topics.find(PERSISTED_CURRENT_PROCESSING_FILE_HASH);

            if (hashTopic != null) {
                return Coerce.toString(hashTopic);
            }

            Topic nameTopic = topics.find(PERSISTED_CURRENT_PROCESSING_FILE_NAME);

            if (nameTopic == null || Coerce.toString(nameTopic) == null) {
                return null;
            }

            try {
                Path filePath = Paths.get(Coerce.toString(nameTopic));
                File file = filePath.toFile();

                if (!file.exists() || !file.isFile()) {
                    return null;
                }

                LogFile logFile = LogFile.of(file);
                return logFile.hashString();
            } catch (InvalidPathException e) {
                logger.atWarn().cause(e).log("File name is not a valid path");
            }

            return null;
        }
    }
}
