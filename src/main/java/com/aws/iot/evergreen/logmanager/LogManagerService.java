/*
 *  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *  SPDX-License-Identifier: Apache-2.0
 */

package com.aws.iot.evergreen.logmanager;

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
import java.util.Arrays;
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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import javax.inject.Inject;
import javax.naming.LimitExceededException;

import static com.aws.iot.evergreen.logmanager.LogManagerService.LOGS_UPLOADER_SERVICE_TOPICS;
import static com.aws.iot.evergreen.packagemanager.KernelConfigResolver.PARAMETERS_CONFIG_KEY;

@ImplementsService(name = LOGS_UPLOADER_SERVICE_TOPICS, version = "1.0.0")
public class LogManagerService extends PluginService {
    public static final String LOGS_UPLOADER_SERVICE_TOPICS = "aws.greengrass.logmanager";
    public static final String LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC = "logsUploaderPeriodicUpdateIntervalSec";
    public static final String LOGS_UPLOADER_CONFIGURATION_TOPIC = "logsUploaderConfigurationJson";
    public static final String SYSTEM_LOGS_COMPONENT_NAME = "System";
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
    private final ScheduledExecutorService scheduledExecutorService;
    private Set<ComponentLogConfiguration> componentLogConfigurations = new HashSet<>();
    private final AtomicBoolean isCurrentlyUploading = new AtomicBoolean(false);
    private ScheduledFuture<?> periodicUpdateFuture;
    private int periodicUpdateIntervalSec;

    /**
     * Constructor.
     *
     * @param topics              The configuration coming from  kernel
     * @param uploader            {@link CloudWatchLogsUploader}
     * @param deviceConfiguration {@link DeviceConfiguration}
     */
    @Inject
    LogManagerService(Topics topics, CloudWatchLogsUploader uploader, CloudWatchAttemptLogsProcessor merger,
                      ScheduledExecutorService scheduledExecutorService) {
        super(topics);
        this.uploader = uploader;
        this.logsProcessor = merger;
        this.scheduledExecutorService = scheduledExecutorService;

        topics.lookup(PARAMETERS_CONFIG_KEY, LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC)
                .dflt(DEFAULT_PERIODIC_UPDATE_INTERVAL_SEC)
                .subscribe((why, newv) -> {
                    periodicUpdateIntervalSec = Coerce.toInt(newv);
                    schedulePeriodicLogsUploaderUpdate();
                });
        this.uploader.registerAttemptStatus(LOGS_UPLOADER_SERVICE_TOPICS, this::handleCloudWatchAttemptStatus);

        //TODO: read configuration of components.
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
        componentLogConfigurations = newComponentLogConfigurations;
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
        if (cloudWatchAttempt.getLastException() != null
                && cloudWatchAttempt.getLastException() instanceof LimitExceededException) {
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException ignored) { }
        }
        Map<String, Set<String>> completedLogFilePerComponent = new ConcurrentHashMap<>();
        Map<String, CurrentProcessingFileInformation> currentProcessingLogFilePerComponent = new HashMap<>();

        cloudWatchAttempt.getLogStreamUploadedSet().forEach((streamName) -> {
                    CloudWatchAttemptLogInformation attemptLogInformation =
                            cloudWatchAttempt.getLogStreamsToLogEventsMap().get(streamName);
                    attemptLogInformation.getAttemptLogFileInformationList().forEach(
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
        isCurrentlyUploading.set(false);

        //TODO: Persist this information to the disk.
    }

    private void processCloudWatchAttemptLogInformation(Map<String, Set<String>> completedLogFilePerComponent,
                                                        Map<String, CurrentProcessingFileInformation>
                                                                currentProcessingLogFilePerComponent,
                                                        CloudWatchAttemptLogInformation attemptLogInformation,
                                                        String fileName,
                                                        CloudWatchAttemptLogFileInformation
                                                                cloudWatchAttemptLogFileInformation) {
        File file = new File(fileName);
        // If we have completely read the file, then we need add it to the completed files list and remove it
        // it (if necessary) for the current processing list.
        if (file.length() == cloudWatchAttemptLogFileInformation.getBytesRead()
                + cloudWatchAttemptLogFileInformation.getStartPosition()) {
            Set<String> completedFileNames = completedLogFilePerComponent
                    .getOrDefault(attemptLogInformation.getComponentName(), new HashSet<>());
            completedFileNames.add(fileName);
            completedLogFilePerComponent.put(attemptLogInformation.getComponentName(), completedFileNames);
            if (currentProcessingLogFilePerComponent.containsKey(attemptLogInformation.getComponentName())) {
                CurrentProcessingFileInformation fileInformation = currentProcessingLogFilePerComponent
                        .get(attemptLogInformation.getComponentName());
                if (fileInformation.fileName.equals(fileName)) {
                    currentProcessingLogFilePerComponent.remove(attemptLogInformation.getComponentName());
                }
            }
        } else {
            if (completedLogFilePerComponent.containsKey(attemptLogInformation.getComponentName())
                    && completedLogFilePerComponent.get(attemptLogInformation.getComponentName()).contains(fileName)) {
                return;
            }
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

    private void schedulePeriodicLogsUploaderUpdate() {
        if (periodicUpdateFuture != null) {
            periodicUpdateFuture.cancel(true);
        }

        this.periodicUpdateFuture = scheduledExecutorService.schedule(this::processLogsAndUpload,
                periodicUpdateIntervalSec, TimeUnit.SECONDS);
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
    private void processLogsAndUpload() {
        try {
            while (!Thread.currentThread().isInterrupted()) {
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
                for (ComponentLogConfiguration componentLogConfiguration : componentLogConfigurations) {
                    if (componentLogFileInformation.get().isPresent()) {
                        continue;
                    }
                    Instant lastUploadedLogFileTimeMs =
                            lastComponentUploadedLogFileInstantMap.getOrDefault(componentLogConfiguration.getName(),
                                    Instant.EPOCH);
                    File folder = new File(componentLogConfiguration.getDirectoryPath().toUri());
                    List<File> allFiles = new ArrayList<>();
                    try {
                        File[] files = folder.listFiles();
                        if (files != null) {
                            // Sort the files by the last modified time.
                            Arrays.sort(files, Comparator.comparingLong(File::lastModified));
                            for (File file : files) {
                                if (file.isFile()
                                        && lastUploadedLogFileTimeMs.isBefore(Instant.ofEpochMilli(file.lastModified()))
                                        && componentLogConfiguration.getFileNameRegex().matcher(file.getName()).find()
                                        && file.length() > 0) {
                                    allFiles.add(file);
                                }
                            }
                        }
                        // If there are no rotated log files for the component, then return.
                        if (allFiles.size() - 1 <= 0) {
                            continue;
                        }
                        // Don't consider the active log file.
                        allFiles = allFiles.subList(0, allFiles.size() - 1);

                        componentLogFileInformation.set(Optional.of(
                                ComponentLogFileInformation.builder()
                                        .name(componentLogConfiguration.getName())
                                        .multiLineStartPattern(componentLogConfiguration.getMultiLineStartPattern())
                                        .desiredLogLevel(componentLogConfiguration.getMinimumLogLevel())
                                        .componentType(componentLogConfiguration.getComponentType())
                                        .logFileInformationList(new ArrayList<>())
                                        .build()));
                        allFiles.forEach(file -> {
                            long startPosition = 0;
                            // If the file was paritially read in the previous run, then get the starting position for
                            // new log lines.
                            // TODO: Add file last modified time information to make sure it is the same file.
                            if (componentCurrentProcessingLogFile.containsKey(componentLogConfiguration.getName())) {
                                CurrentProcessingFileInformation processingFileInformation =
                                        componentCurrentProcessingLogFile
                                                .get(componentLogConfiguration.getName());
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
                                componentLogConfiguration.getName(), componentLogConfiguration.getDirectoryPath());
                    }
                }
                if (componentLogFileInformation.get().isPresent()) {
                    CloudWatchAttempt cloudWatchAttempt =
                            logsProcessor.processLogFiles(componentLogFileInformation.get().get());
                    uploader.upload(cloudWatchAttempt);
                } else {
                    TimeUnit.SECONDS.sleep(periodicUpdateIntervalSec);
                }
            }
        } catch (InterruptedException e) {
            logger.atDebug().log("Interrupted while running upload loop, exiting");
        }
    }

    @Override
    @SuppressWarnings("PMD.UselessOverridingMethod")
    public void startup() throws InterruptedException {
        // Need to override the function for tests.
        super.startup();
    }

    @Override
    public void shutdown() {
        if (this.periodicUpdateFuture != null) {
            this.periodicUpdateFuture.cancel(true);
        }
    }

    @Builder
    @Getter
    @Data
    static class CurrentProcessingFileInformation {
        private String fileName;
        private long startPosition;
        private long lastModifiedTime;
    }
}
