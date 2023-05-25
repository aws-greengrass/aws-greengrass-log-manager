/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.logmanager;

import com.aws.greengrass.deployment.DeviceConfiguration;
import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.GreengrassLogMessage;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.logmanager.model.CloudWatchAttempt;
import com.aws.greengrass.logmanager.model.CloudWatchAttemptLogFileInformation;
import com.aws.greengrass.logmanager.model.CloudWatchAttemptLogInformation;
import com.aws.greengrass.logmanager.model.ComponentLogFileInformation;
import com.aws.greengrass.logmanager.model.LogFile;
import com.aws.greengrass.logmanager.model.LogFileGroup;
import com.aws.greengrass.logmanager.model.LogFileInformation;
import com.aws.greengrass.util.Coerce;
import com.aws.greengrass.util.Pair;
import com.aws.greengrass.util.Utils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.event.Level;
import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.time.Clock;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAccessor;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.inject.Inject;

public class CloudWatchAttemptLogsProcessor {
    public static final String DEFAULT_LOG_GROUP_NAME = "/aws/greengrass/{componentType}/{region}/{componentName}";
    // Log stream names can be 1-512 chars long, thing name 1-128 chars.
    // Constant lengths are date of 10 characters and divider "/" of 3 characters.
    // Example format: /2020/12/15/thing/thing-name
    public static final String DEFAULT_LOG_STREAM_NAME = "/{date}/thing/{thingName}";

    // https://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_PutLogEvents.html
    // The maximum batch size is 1,048,576 bytes. This size is calculated as the sum of all event messages in UTF-8,
    // plus 26 bytes for each log event which is defined in the API definition.
    private static final int EVENT_STORAGE_OVERHEAD = 26;
    private static final int TIMESTAMP_BYTES = 8;
    private static final int MAX_BATCH_SIZE = 1024 * 1024;
    private static final int MAX_EVENT_LENGTH = 1024 * 256 - TIMESTAMP_BYTES - EVENT_STORAGE_OVERHEAD;
    private static final int MAX_NUM_OF_LOG_EVENTS = 10_000;
    private static final ObjectMapper DESERIALIZER = new ObjectMapper()
            .configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);
    private static final ThreadLocal<SimpleDateFormat> DATE_FORMATTER =
            ThreadLocal.withInitial(() -> {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd", Locale.ENGLISH);
                // Set timezone of the formatter to UTC since we always pass in UTC dates. We don't want it to think
                // that it should use the local timezone.
                sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
                return sdf;
            });
    private final DeviceConfiguration deviceConfiguration;
    private static final Logger logger = LogManager.getLogger(CloudWatchAttemptLogsProcessor.class);
    private static final Pattern textTimestampPattern = Pattern.compile("([\\w-:.+]+)");
    private final Clock clock;

    /**
     * Constructor.
     *
     * @param deviceConfiguration {@link DeviceConfiguration}
     */
    @Inject
    public CloudWatchAttemptLogsProcessor(DeviceConfiguration deviceConfiguration) {
        this(deviceConfiguration, Clock.systemUTC());
    }

    CloudWatchAttemptLogsProcessor(DeviceConfiguration deviceConfiguration, Clock clock) {
        this.deviceConfiguration = deviceConfiguration;
        this.clock = clock;
    }

    String getLogStreamName(String thingName) {
        String logStreamName = DEFAULT_LOG_STREAM_NAME.replace("{thingName}", thingName);
        // Thing name can be [a-zA-Z0-9:_-]+
        // while log stream name has to be [^:*]*
        return logStreamName.replace(":", "+");
    }

    /**
     * Gets CW input log events from the component which processLogFiles need to be uploaded to CloudWatch.
     *
     * @param componentLogFileInformation log files information for a component to read logs from.
     * @return CloudWatch attempt containing information needed to upload logs from the component to the cloud.
     */
    public CloudWatchAttempt processLogFiles(ComponentLogFileInformation componentLogFileInformation) {
        AtomicInteger totalBytesRead = new AtomicInteger();
        CloudWatchAttempt attempt = new CloudWatchAttempt();
        Map<String, CloudWatchAttemptLogInformation> logStreamsMap = new ConcurrentHashMap<>();
        AtomicBoolean reachedMaxSize = new AtomicBoolean(false);
        String thingName = Coerce.toString(deviceConfiguration.getThingName());
        String awsRegion = Coerce.toString(deviceConfiguration.getAWSRegion());

        String logGroupName = DEFAULT_LOG_GROUP_NAME.replace("{componentType}",
                        componentLogFileInformation.getComponentType().toString()).replace("{region}", awsRegion)
                .replace("{componentName}", componentLogFileInformation.getName());
        attempt.setLogGroupName(logGroupName);
        String logStreamName = getLogStreamName(thingName);
        LogFileGroup logFileGroup = componentLogFileInformation.getLogFileGroup();
        // Run the loop until all the log files from the component have been read or the max message
        // size has been reached.
        while (!componentLogFileInformation.getLogFileInformationList().isEmpty() && !reachedMaxSize.get()) {
            LogFileInformation logFileInformation = componentLogFileInformation.getLogFileInformationList().get(0);
            LogFile logFile = logFileInformation.getLogFile();
            long startPosition = logFileInformation.getStartPosition();
            String fileHash = logFileInformation.getFileHash();
            //This has been handled in the service, but leave here to prevent processor crash
            if (logFile.isEmpty() || startPosition == logFile.length()) {
                componentLogFileInformation.getLogFileInformationList().remove(0);
                continue;
            }

            long lastModified = logFile.lastModified();

            // If we have read the file already, we are at the correct offset in the file to start reading from
            // Let's get that file handle to read the new log line.
            try (SeekableByteChannel chan = Files.newByteChannel(logFile.toPath(), StandardOpenOption.READ);
                 PositionTrackingBufferedReader r = new PositionTrackingBufferedReader(
                         new InputStreamReader(Channels.newInputStream(chan), StandardCharsets.UTF_8))) {
                r.setInitialPosition(startPosition);
                chan.position(startPosition);
                StringBuilder data = new StringBuilder(r.readLine());

                // Run the loop until we detect that the log file is completely read, or that we have reached the max
                // message size or if we detect any IOException while reading from the file.
                while (!reachedMaxSize.get()) {
                    try {
                        long tempStartPosition = r.position();
                        String partialLogLine = r.readLine();
                        // If we do not get any data from the file, we have reached the end of the file.
                        // and we add the log line into our input logs event list since we are currently only
                        // working on rotated files, this will be guaranteed to be a complete log line.
                        if (partialLogLine == null) {
                            reachedMaxSize.set(
                                    processLogLine(totalBytesRead, componentLogFileInformation.getDesiredLogLevel(),
                                            logStreamName, logStreamsMap, data, fileHash, startPosition,
                                            componentLogFileInformation.getName(), tempStartPosition, lastModified,
                                            logFileGroup));
                            componentLogFileInformation.getLogFileInformationList().remove(0);
                            break;
                        }

                        // If the new log line read from the file matches the start pattern, that means
                        // the string builder we have appended data to until now, has a complete log line.
                        // Let's add that in the input logs event list.
                        // The default pattern is checking if the line starts with non-white space
                        if (checkLogStartPattern(componentLogFileInformation, partialLogLine)) {
                            reachedMaxSize.set(
                                    processLogLine(totalBytesRead, componentLogFileInformation.getDesiredLogLevel(),
                                            logStreamName, logStreamsMap, data, fileHash, startPosition,
                                            componentLogFileInformation.getName(), tempStartPosition, lastModified,
                                            logFileGroup));
                            data = new StringBuilder();
                        }

                        // Need to read more lines until we get a complete log line. Let's add this to the SB.
                        data.append(partialLogLine);
                    } catch (IOException e) {
                        logger.atError().cause(e).log("Unable to read file {}", logFile.getAbsolutePath());
                        componentLogFileInformation.getLogFileInformationList().remove(0);
                        break;
                    }
                }
            } catch (IOException e) {
                // File probably does not exist.
                logger.atError().cause(e).log("Unable to read file {}", logFile.getAbsolutePath());
                componentLogFileInformation.getLogFileInformationList().remove(0);
            }
        }
        attempt.setLogStreamsToLogEventsMap(logStreamsMap);
        return attempt;
    }

    /**
     * Processes the log line by trying to deserialize the log line as a {@link GreengrassLogMessage}.
     * If log line is in the correct format, add the minimum log level filter and add the log event if the filter
     * passes.
     * If the log line is not in the {@link GreengrassLogMessage} format, we will add the log event to be
     * uploaded to CloudWatch.
     * Also creates the log stream name based on the timestamp value of the log line if it is in the
     * {@link GreengrassLogMessage} format.
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
                                   String fileHash,
                                   long startPosition,
                                   String componentName,
                                   long currentPosition,
                                   long lastModified,
                                   LogFileGroup logFileGroup) {
        String dataStr = data.toString();
        int dataSize = dataStr.getBytes(StandardCharsets.UTF_8).length;
        CloudWatchAttemptLogInformation attemptLogInformation;
        Optional<GreengrassLogMessage> logMessage = tryGetStructuredLogMessage(dataStr);
        Pair<Boolean, AtomicInteger> addEventResult;
        // if the message is in JSON format, directly grab the message timestamp as logStreamName
        // otherwise, try to find if any timestamp matched in text. If not, set the local (where LM is) time as the
        // logStreamName
        if (logMessage.isPresent()) {
            logStreamName = logStreamName.replace("{date}",
                    DATE_FORMATTER.get().format(new Date(logMessage.get().getTimestamp())));
            attemptLogInformation = logStreamsMap.computeIfAbsent(logStreamName,
                    key -> CloudWatchAttemptLogInformation.builder()
                            .componentName(componentName)
                            .logFileGroup(logFileGroup)
                            .build());
            addEventResult = checkAndAddNewLogEvent(totalBytesRead, attemptLogInformation,
                    dataStr, dataSize, desiredLogLevel, logMessage.get());

        } else {
            Matcher matcher = textTimestampPattern.matcher(data);
            Instant logTimestamp = Instant.now();
            if (matcher.find()) {
                String match = matcher.group(1);
                Optional<Instant> parsedInstant = tryParseInstant(match, DateTimeFormatter.ISO_INSTANT);
                if (parsedInstant.isPresent()) {
                    logTimestamp = parsedInstant.get();
                } else {
                    parsedInstant = tryParseInstant(match, DateTimeFormatter.ISO_OFFSET_DATE_TIME);
                    if (parsedInstant.isPresent()) {
                        logTimestamp = parsedInstant.get();
                    }
                }
            }
            logStreamName = logStreamName.replace("{date}", DATE_FORMATTER.get().format(Date.from(logTimestamp)));
            attemptLogInformation = logStreamsMap.computeIfAbsent(logStreamName,
                    key -> CloudWatchAttemptLogInformation.builder()
                            .componentName(componentName)
                            .logFileGroup(logFileGroup)
                            .build());
            addEventResult =
                    addNewLogEvent(totalBytesRead, attemptLogInformation, dataStr, dataSize, logTimestamp);
        }
        boolean reachedMaxSize = addEventResult.getLeft();
        int actualAddedSize = addEventResult.getRight().get();
        if (actualAddedSize > 0) {
            updateCloudWatchAttemptLogInformation(fileHash, startPosition,
                    currentPosition - (dataSize - actualAddedSize), attemptLogInformation, lastModified);
        }
        return reachedMaxSize;
    }

    /**
     * Updates the number of bytes read for the current CloudWatchAttempt.
     *
     * @param fileHash              The hash of the file we are currently processing, as the file identifier.
     * @param startPosition         The initial start offset of the file.
     * @param currentPosition       The current offset in the file.
     * @param attemptLogInformation The attempt information containing the log file information.
     * @param lastModified          The last modified time for the file we are processing.
     */
    private void updateCloudWatchAttemptLogInformation(String fileHash,
                                                       long startPosition,
                                                       long currentPosition,
                                                       CloudWatchAttemptLogInformation attemptLogInformation,
                                                       long lastModified) {
        CloudWatchAttemptLogFileInformation attemptLogFileInformation =
                attemptLogInformation.getAttemptLogFileInformationMap().computeIfAbsent(fileHash,
                        key -> CloudWatchAttemptLogFileInformation.builder()
                                .startPosition(startPosition)
                                .lastModifiedTime(lastModified)
                                .fileHash(fileHash)
                                .build());
        attemptLogFileInformation.setBytesRead(currentPosition - attemptLogFileInformation.getStartPosition());
    }

    /**
     * Verify we can deserialize the log line as a structuredLogMessage. If not, return an empty optional
     * value.
     *
     * @param data The log line read from the file.
     * @return a structuredLogMessage if the deserialization is successful, else an empty optional object.
     */
    private Optional<GreengrassLogMessage> tryGetStructuredLogMessage(String data) {
        try {
            return Optional.ofNullable(DESERIALIZER.readValue(data, GreengrassLogMessage.class));
        } catch (JsonProcessingException ignored) {
            // If unable to deserialize, then we treat it as a normal log line and do not need to smartly upload.
            return Optional.empty();
        }
    }

    /**
     * Verify the {@link GreengrassLogMessage}'s log level is greater than the desired log level to be uploaded to
     * CloudWatch.
     *
     * @param totalBytesRead        The total number of bytes read till now.
     * @param attemptLogInformation The attempt information containing the log file information.
     * @param data                  The log line read from the file.
     * @param dataSize              No of bytes for data string
     * @param desiredLogLevel       The minimum desired log level to be uploaded to CloudWatch.
     * @param logMessage            The structured log message.
     * @return a pair of reachedMaxSize(boolean) indicating whether maximum message size has reached, and
     *         noOfBytesAdded(AtomicInteger) indicating no of bytes that were successfully added.
     */
    private Pair<Boolean, AtomicInteger> checkAndAddNewLogEvent(AtomicInteger totalBytesRead,
                                                      CloudWatchAttemptLogInformation attemptLogInformation,
                                                      String data,
                                                      int dataSize,
                                                      Level desiredLogLevel,
                                                      GreengrassLogMessage logMessage) {
        Level currentLogLevel = Level.valueOf(logMessage.getLevel());
        if (currentLogLevel.toInt() < desiredLogLevel.toInt()) {
            return new Pair(false, new AtomicInteger());
        }
        return addNewLogEvent(totalBytesRead, attemptLogInformation, data, dataSize,
                Instant.ofEpochMilli(logMessage.getTimestamp()));
    }

    /**
     * Adds a new log event to the CloudWatchAttempt provided the maximum message size is not reached after adding the
     * input event.
     *
     * @param totalBytesRead        The total bytes read till now.
     * @param attemptLogInformation The attempt information containing the log file information.
     * @param data                  The log line read from the file.
     * @param dataSize              No of bytes for data string
     * @param timestamp             Timestamp for the log line
     * @return a pair of reachedMaxSize(boolean) indicating whether maximum message size has reached, and
     *         noOfBytesAdded(AtomicInteger) indicating no of bytes that were successfully added.
     * @implNote We need to add extra bytes size for every input message as well as the timestamp byte size
     *         along with the log line data size to get the exact size of the input log events.
     */
    private Pair<Boolean, AtomicInteger> addNewLogEvent(AtomicInteger totalBytesRead,
                                                        CloudWatchAttemptLogInformation attemptLogInformation,
                                                        String data,
                                                        int dataSize,
                                                        Instant timestamp) {
        // Cloudwatch does not allow uploading data older than 14 days, so we want to skip
        if (timestamp.isBefore(Instant.now(clock).minus(14, ChronoUnit.DAYS))) {
            return new Pair<>(false, new AtomicInteger(dataSize));
        }
        // If the earliest time + 24 hours is before the new time then we cannot insert it because the gap
        // from earliest to latest is greater than 24 hours. Otherwise we can add it.
        // Using 23 instead of 24 hours to have a bit of leeway.
        Optional<InputLogEvent> earliestTime =
                attemptLogInformation.getLogEvents().stream().min(Comparator.comparingLong(InputLogEvent::timestamp));
        if (earliestTime.isPresent() && Instant.ofEpochMilli(earliestTime.get().timestamp()).plus(23, ChronoUnit.HOURS)
                .isBefore(timestamp)) {
            return new Pair<>(true, new AtomicInteger());
        }

        if (attemptLogInformation.getLogEvents().size() >= MAX_NUM_OF_LOG_EVENTS) {
            return new Pair<>(true, new AtomicInteger());
        }

        // If log line is over maximum allowed log event size, break it into chunks of allowed size before constructing
        // log events.
        if (dataSize > MAX_EVENT_LENGTH) {
            logger.atTrace().kv("log-line-size-bytes", dataSize)
                    .log("Log line larger than maximum event size 256KB, " + "will be split into multiple log events");
        }
        int totalChunks = (dataSize - 1) / MAX_EVENT_LENGTH + 1;
        int currChunk = 1;
        int currChunkSize;
        boolean reachedMaxBatchSize = false;
        AtomicInteger currBytesRead = new AtomicInteger();

        // Keep adding events until there are no more chunks left or max batch size limit is reached.
        while (currChunk <= totalChunks) {
            currChunkSize = currChunk == totalChunks ? (dataSize - 1) % MAX_EVENT_LENGTH + 1 : MAX_EVENT_LENGTH;

            reachedMaxBatchSize = reachedMaxBatchSize(totalBytesRead, currChunkSize);
            if (reachedMaxBatchSize) {
                break;
            }

            int startFromByte = MAX_EVENT_LENGTH * (currChunk - 1);
            String partialData =
                    new String(Arrays.copyOfRange(data.getBytes(StandardCharsets.UTF_8), startFromByte,
                            startFromByte + currChunkSize),
                            StandardCharsets.UTF_8);
            totalBytesRead.addAndGet(currChunkSize + TIMESTAMP_BYTES + EVENT_STORAGE_OVERHEAD);
            currBytesRead.addAndGet(currChunkSize);

            // Leave out empty messages since CloudWatch doesn't accept them
            if (Utils.isNotEmpty(partialData)) {
                InputLogEvent inputLogEvent =
                        InputLogEvent.builder().message(partialData).timestamp(timestamp.toEpochMilli()).build();
                attemptLogInformation.getLogEvents().add(inputLogEvent);
            }

            currChunk++;
        }
        return new Pair(reachedMaxBatchSize, currBytesRead);
    }

    /**
     * Check if the log line is the start of a new log event.
     *
     * @param componentLogFileInformation   Component info of log line processed
     * @param logLine                       The processed log line
     * @return Whether the log line is the start of a new log event.
     */
    private boolean checkLogStartPattern(ComponentLogFileInformation componentLogFileInformation, String logLine) {
        Pattern multiLineStartPattern = componentLogFileInformation.getMultiLineStartPattern();
        if (multiLineStartPattern != null) {
            try {
                return multiLineStartPattern.matcher(logLine).find();
            } catch (StackOverflowError e) {
                // Not logging error cause because it's a huge recursive stack trace
                logger.atWarn().kv("componentName", componentLogFileInformation.getName())
                        .log("StackOverflowError thrown when matching log against pattern {}. "
                                + "Check for leading non-whitespace instead", multiLineStartPattern.pattern());
            }
        }
        // The default pattern is to check if the line starts with non-whitespace. If so, the line is considered
        // the start of a new log event.
        return !logLine.isEmpty() && !Character.isWhitespace(logLine.charAt(0));
    }

    private boolean reachedMaxBatchSize(AtomicInteger totalBytesRead, int dataSize) {
        // Total bytes equal the number of bytes of the data plus 8 bytes for the timestamp since its a long
        // and there is an overhead for each log event on the cloud watch side which needs to be added.
        return totalBytesRead.get() + dataSize + TIMESTAMP_BYTES + EVENT_STORAGE_OVERHEAD > MAX_BATCH_SIZE;
    }

    private Optional<Instant> tryParseInstant(String instantString, DateTimeFormatter formatter) {
        try {
            TemporalAccessor ta = formatter.parse(instantString);
            return Optional.of(Instant.from(ta));
        } catch (DateTimeParseException e) {
            logger.atTrace().cause(e).log("Unable to parse timestamp: {}", instantString);
            return Optional.empty();
        }
    }
}
