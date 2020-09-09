/*
 *  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *  SPDX-License-Identifier: Apache-2.0
 */

package com.aws.iot.evergreen.logmanager;

import com.aws.iot.evergreen.deployment.DeploymentService;
import com.aws.iot.evergreen.deployment.DeviceConfiguration;
import com.aws.iot.evergreen.kernel.EvergreenService;
import com.aws.iot.evergreen.kernel.Kernel;
import com.aws.iot.evergreen.kernel.exceptions.ServiceLoadException;
import com.aws.iot.evergreen.logging.api.Logger;
import com.aws.iot.evergreen.logging.impl.EvergreenStructuredLogMessage;
import com.aws.iot.evergreen.logging.impl.LogManager;
import com.aws.iot.evergreen.logmanager.model.CloudWatchAttempt;
import com.aws.iot.evergreen.logmanager.model.CloudWatchAttemptLogFileInformation;
import com.aws.iot.evergreen.logmanager.model.CloudWatchAttemptLogInformation;
import com.aws.iot.evergreen.logmanager.model.ComponentLogFileInformation;
import com.aws.iot.evergreen.logmanager.model.ComponentType;
import com.aws.iot.evergreen.util.Coerce;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.event.Level;
import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.inject.Inject;

import static com.aws.iot.evergreen.deployment.converter.DeploymentDocumentConverter.DEFAULT_GROUP_NAME;

public class CloudWatchAttemptLogsProcessor {
    public static final String DEFAULT_LOG_GROUP_NAME = "/aws/greengrass/{componentType}/{region}/{componentName}";
    public static final String DEFAULT_LOG_STREAM_NAME = "/{date}/{ggFleetId}/{thingName}";
    // https://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_PutLogEvents.html
    // The maximum batch size is 1,048,576 bytes. This size is calculated as the sum of all event messages in UTF-8,
    // plus 26 bytes for each log event which is defined in the API definition.
    private static final int EVENT_STORAGE_OVERHEAD = 26;
    private static final int TIMESTAMP_BYTES = 8;
    private static final int MAX_BATCH_SIZE = 1024 * 1024;
    private static final int MAX_LOG_STREAM_NAME = 512;
    private static final ObjectMapper DESERIALIZER = new ObjectMapper()
            .configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);
    private final SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy/MM/dd", Locale.ENGLISH);
    private final Kernel kernel;
    private final DeviceConfiguration deviceConfiguration;
    private static final Logger logger = LogManager.getLogger(CloudWatchAttemptLogsProcessor.class);

    /**
     * Constructor.
     *
     * @param deviceConfiguration {@link DeviceConfiguration}
     * @param kernel              {@link Kernel}
     */
    @Inject
    public CloudWatchAttemptLogsProcessor(DeviceConfiguration deviceConfiguration, Kernel kernel) {
        this.kernel = kernel;
        this.deviceConfiguration = deviceConfiguration;
    }

    /**
     * Gets CW input log events from the component which processLogFiles need to be uploaded to CloudWatch.
     *
     * @param componentLogFileInformation log files information for a component to read logs from.
     * @return CloudWatch attempt containing information needed to upload logs from the component to the cloud.
     */
    public CloudWatchAttempt processLogFiles(ComponentLogFileInformation componentLogFileInformation) {
        AtomicInteger totalBytesRead = new AtomicInteger();
        AtomicInteger totalCompletelyReadAllComponentsCount = new AtomicInteger();
        CloudWatchAttempt attempt = new CloudWatchAttempt();
        Map<String, CloudWatchAttemptLogInformation> logStreamsMap = new ConcurrentHashMap<>();
        AtomicBoolean reachedMaxSize = new AtomicBoolean(false);
        String thingName = Coerce.toString(deviceConfiguration.getThingName());
        String awsRegion = Coerce.toString(deviceConfiguration.getAWSRegion());

        String logGroupName = DEFAULT_LOG_GROUP_NAME
                .replace("{componentType}", componentLogFileInformation.getComponentType().toString())
                .replace("{region}", awsRegion)
                .replace("{componentName}", componentLogFileInformation.getName());
        attempt.setLogGroupName(logGroupName);

        // Run the loop until all the log files from the component have been read or the max message
        // size has been reached.
        while (!componentLogFileInformation.getLogFileInformationList().isEmpty() && !reachedMaxSize.get()) {
            File file = componentLogFileInformation.getLogFileInformationList().get(0).getFile();
            long startPosition = componentLogFileInformation.getLogFileInformationList().get(0).getStartPosition();
            String fileName = file.getAbsolutePath();
            long lastModified = file.lastModified();
            String logStreamName = DEFAULT_LOG_STREAM_NAME.replace("{thingName}", thingName);
            Set<String> groups = getGroupsForComponent(componentLogFileInformation);

            if (groups.isEmpty()) {
                logStreamName = logStreamName.replace("{ggFleetId}", DEFAULT_GROUP_NAME);
            } else {
                StringJoiner stringJoiner = new StringJoiner(":");
                groups.forEach(groupName -> {
                    if (stringJoiner.length() + groupName.length() <= MAX_LOG_STREAM_NAME) {
                        stringJoiner.add(groupName);
                    }
                });
                logStreamName = logStreamName.replace("{ggFleetId}", stringJoiner.toString());
            }

            // If we have read the file already, we are at the correct offset in the file to start reading from
            // Let's get that file handle to read the new log line.
            //TODO: This does not support the full Unicode character set. May need to rethink?
            try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
                raf.seek(startPosition);
                StringBuilder data = new StringBuilder(raf.readLine());

                // Run the loop until we detect that the log file is completely read, or that we have reached the max
                // message size or if we detect any IOException while reading from the file.
                while (!reachedMaxSize.get()) {
                    try {
                        long tempStartPosition = raf.getFilePointer();
                        String partialLogLine = raf.readLine();
                        // If we do not get any data from the file, we have reached the end of the file.
                        // and we add the log line into our input logs event list since we are currently only
                        // working on rotated files, this will be guaranteed to be a complete log line.
                        if (partialLogLine == null) {
                            reachedMaxSize.set(processLogLine(totalBytesRead,
                                    componentLogFileInformation.getDesiredLogLevel(), logStreamName,
                                    logStreamsMap, data, fileName, startPosition, componentLogFileInformation.getName(),
                                    tempStartPosition, lastModified));
                            componentLogFileInformation.getLogFileInformationList().remove(0);
                            if (componentLogFileInformation.getLogFileInformationList().isEmpty()) {
                                totalCompletelyReadAllComponentsCount.getAndIncrement();
                            }
                            break;
                        }

                        // If the new log line read from the file has the multiline separator, that means that
                        // the string builder we have appended data to until now, has a complete log line.
                        // Let's add that in the input logs event list.
                        if (componentLogFileInformation.getMultiLineStartPattern().matcher(partialLogLine).find()) {
                            reachedMaxSize.set(processLogLine(totalBytesRead,
                                    componentLogFileInformation.getDesiredLogLevel(), logStreamName,
                                    logStreamsMap, data, fileName, startPosition, componentLogFileInformation.getName(),
                                    tempStartPosition, lastModified));
                            data = new StringBuilder();
                        }

                        // Need to read more lines until we get a complete log line. Let's add this to the SB.
                        data.append(partialLogLine);
                    } catch (IOException e) {
                        logger.atError().cause(e).log("Unable to read file {}", file.getAbsolutePath());
                        componentLogFileInformation.getLogFileInformationList().remove(0);
                        if (componentLogFileInformation.getLogFileInformationList().isEmpty()) {
                            totalCompletelyReadAllComponentsCount.getAndIncrement();
                        }
                        break;
                    }
                }
            } catch (IOException e) {
                // File probably does not exist.
                logger.atError().cause(e).log("Unable to read file {}", file.getAbsolutePath());
                componentLogFileInformation.getLogFileInformationList().remove(0);
                if (componentLogFileInformation.getLogFileInformationList().isEmpty()) {
                    totalCompletelyReadAllComponentsCount.getAndIncrement();
                }
            }
        }
        attempt.setLogStreamsToLogEventsMap(logStreamsMap);
        return attempt;
    }

    /**
     * Gets the groups associated with the component. If it is for System logs, we get all the groups
     * the device is a part of.
     *
     * @param componentLogFileInformation   The information about the component we are currently processing.
     * @return  The list of groups associated to the component.
     */
    private Set<String> getGroupsForComponent(ComponentLogFileInformation componentLogFileInformation) {
        Set<String> groups = new HashSet<>();
        try {
            EvergreenService deploymentServiceLocateResult = this.kernel
                    .locate(DeploymentService.DEPLOYMENT_SERVICE_TOPICS);
            if (deploymentServiceLocateResult instanceof DeploymentService) {
                DeploymentService deploymentService = (DeploymentService) deploymentServiceLocateResult;
                if (ComponentType.GreengrassSystemComponent
                        .equals(componentLogFileInformation.getComponentType())) {
                    groups = deploymentService.getAllGroupConfigs();
                } else {
                    groups = deploymentService.getGroupConfigsForUserComponent(componentLogFileInformation.getName());
                }
            }
        } catch (ServiceLoadException e) {
            logger.atError().cause(e).log("Unable to locate {} service while uploading FSS data",
                    DeploymentService.DEPLOYMENT_SERVICE_TOPICS);
        }
        return groups;
    }

    /**
     * Processes the log line by trying to deserialize the log line as a {@link EvergreenStructuredLogMessage}.
     * If log line is in the correct format, add the minimum log level filter and add the log event if the filter
     * passes.
     * If the log line is not in the {@link EvergreenStructuredLogMessage} format, we will add the log event to be
     * uploaded to CloudWatch.
     * Also creates the log stream name based on the timestamp value of the log line if it is in the
     * {@link EvergreenStructuredLogMessage} format.
     * Else, it will use the current date for the formatter.
     *
     * @param totalBytesRead  Total bytes read/added to the log events list.
     * @param desiredLogLevel The minimum desired log level.
     * @param logStreamName   The log stream name.
     * @param logStreamsMap   The log stream name map for the group.
     * @param data            The raw string data of the log line.
     * @param lastModified    The last modified time of the file.
     */
    @SuppressWarnings("PMD.ExcessiveParameterList")
    private boolean processLogLine(AtomicInteger totalBytesRead,
                                   Level desiredLogLevel,
                                   String logStreamName,
                                   Map<String, CloudWatchAttemptLogInformation> logStreamsMap,
                                   StringBuilder data,
                                   String fileName,
                                   long startPosition,
                                   String componentName,
                                   long currentPosition,
                                   long lastModified) {
        boolean reachedMaxSize;
        CloudWatchAttemptLogInformation attemptLogInformation;
        Optional<EvergreenStructuredLogMessage> logMessage = tryGetEvergreenStructuredLogMessage(data);
        if (logMessage.isPresent()) {
            logStreamName = logStreamName.replace("{date}",
                    dateFormatter.format(new Date(logMessage.get().getTimestamp())));
            attemptLogInformation = logStreamsMap.getOrDefault(logStreamName,
                    CloudWatchAttemptLogInformation.builder()
                            .componentName(componentName)
                            .logEvents(new ArrayList<>())
                            .attemptLogFileInformationMap(new HashMap<>())
                            .build());
            reachedMaxSize = checkAndAddNewLogEvent(totalBytesRead, attemptLogInformation, data,
                    desiredLogLevel, logMessage.get(), data.toString().getBytes(StandardCharsets.UTF_8).length);

        } else {
            logStreamName = logStreamName.replace("{date}", dateFormatter.format(new Date()));
            attemptLogInformation = logStreamsMap.getOrDefault(logStreamName,
                    CloudWatchAttemptLogInformation.builder()
                            .componentName(componentName)
                            .logEvents(new ArrayList<>())
                            .build());
            reachedMaxSize = addNewLogEvent(totalBytesRead, attemptLogInformation, data.toString(),
                    data.toString().getBytes(StandardCharsets.UTF_8).length);
        }
        if (!reachedMaxSize) {
            updateCloudWatchAttemptLogInformation(logStreamName, logStreamsMap, fileName, startPosition,
                    currentPosition, attemptLogInformation, lastModified);
        }
        return reachedMaxSize;
    }

    /**
     * Updates the number of bytes read for the current CloudWatchAttempt.
     *
     * @param logStreamName         The log stream name.
     * @param logStreamsMap         The log stream to CloudWatchAttemptLogInformation map.
     * @param fileName              The name of the file we are currently processing.
     * @param startPosition         The initial start offset of the file.
     * @param currentPosition       The current offset in the file.
     * @param attemptLogInformation The attempt information containing the log file information.
     * @param lastModified          The last modified time for the file we are processing.
     */
    private void updateCloudWatchAttemptLogInformation(String logStreamName,
                                                       Map<String, CloudWatchAttemptLogInformation> logStreamsMap,
                                                       String fileName,
                                                       long startPosition,
                                                       long currentPosition,
                                                       CloudWatchAttemptLogInformation attemptLogInformation,
                                                       long lastModified) {
        CloudWatchAttemptLogFileInformation attemptLogFileInformation =
                attemptLogInformation.getAttemptLogFileInformationMap().getOrDefault(fileName,
                        CloudWatchAttemptLogFileInformation.builder()
                                .startPosition(startPosition)
                                .lastModifiedTime(lastModified)
                                .build());
        attemptLogFileInformation.setBytesRead(currentPosition - attemptLogFileInformation.getStartPosition());
        attemptLogInformation.getAttemptLogFileInformationMap().put(fileName, attemptLogFileInformation);
        logStreamsMap.put(logStreamName, attemptLogInformation);
    }

    /**
     * Verify we can deserialize the log line as a EvergreenStructuredLogMessage. If not, return an empty optional
     * value.
     *
     * @param data  The log line read from the file.
     * @return  a EvergreenStructuredLogMessage if the deserialization is successful, else an empty optional object.
     */
    private Optional<EvergreenStructuredLogMessage> tryGetEvergreenStructuredLogMessage(StringBuilder data) {
        try {
            return Optional.of(DESERIALIZER.readValue(data.toString(), EvergreenStructuredLogMessage.class));
        } catch (JsonProcessingException ignored) {
            // If unable to deserialize, then we treat it as a normal log line and do not need to smartly upload.
            return Optional.empty();
        }
    }

    /**
     * Verify the {@link EvergreenStructuredLogMessage}'s log level is greater than the desired log level to be uploaded
     * to CloudWatch.
     *
     * @param totalBytesRead        The total number of bytes read till now.
     * @param attemptLogInformation The attempt information containing the log file information.
     * @param data                  The log line read from the file.
     * @param desiredLogLevel       The minimum desired log level to be uploaded to CloudWatch.
     * @param logMessage            The structured log message.
     * @param dataSize              The size of the log line.
     * @return whether or not the maximum message size has reached or not.
     */
    private boolean checkAndAddNewLogEvent(AtomicInteger totalBytesRead,
                                           CloudWatchAttemptLogInformation attemptLogInformation,
                                           StringBuilder data,
                                           Level desiredLogLevel,
                                           EvergreenStructuredLogMessage logMessage,
                                           int dataSize) {
        Level currentLogLevel = Level.valueOf(logMessage.getLevel());
        if (currentLogLevel.toInt() < desiredLogLevel.toInt()) {
            return false;
        }
        return addNewLogEvent(totalBytesRead, attemptLogInformation, data.toString(), dataSize);
    }

    /**
     * Adds a new log event to the CloudWatchAttempt provided the maximum message size is not reached after adding the
     * input event.
     * @implNote We need to add extra bytes size for every input message as well as the timestamp byte size alongwith
     *     the log line data size to get the exact size of the input log events.
     * @param totalBytesRead        The total bytes read till now.
     * @param attemptLogInformation The attempt information containing the log file information.
     * @param data                  The log line read from the file.
     * @param dataSize              The size of the log line.
     * @return whether or not the maximum message size has reached or not.
     */
    private boolean addNewLogEvent(AtomicInteger totalBytesRead, CloudWatchAttemptLogInformation attemptLogInformation,
                                   String data, int dataSize) {
        // Total bytes equal the number of bytes of the data plus 8 bytes for the timestamp since its a long
        // and there is an overhead for each log event on the cloud watch side which needs to be added.
        if (totalBytesRead.get() + dataSize + TIMESTAMP_BYTES + EVENT_STORAGE_OVERHEAD > MAX_BATCH_SIZE) {
            return true;
        }
        totalBytesRead.addAndGet(dataSize + TIMESTAMP_BYTES + EVENT_STORAGE_OVERHEAD);

        InputLogEvent inputLogEvent = InputLogEvent.builder()
                .message(data)
                .timestamp(Instant.now().toEpochMilli()).build();
        attemptLogInformation.getLogEvents().add(inputLogEvent);
        return false;
    }
}
