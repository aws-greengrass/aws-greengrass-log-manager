/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.logmanager;

import com.aws.greengrass.config.Configuration;
import com.aws.greengrass.config.Topic;
import com.aws.greengrass.config.Topics;
import com.aws.greengrass.dependency.Context;
import com.aws.greengrass.deployment.DeviceConfiguration;
import com.aws.greengrass.logmanager.model.EventType;
import com.aws.greengrass.logmanager.model.LogFile;
import com.aws.greengrass.logmanager.util.CloudWatchClientFactory;
import com.aws.greengrass.logmanager.util.SdkClientWrapper;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.util.NucleusPaths;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.ServiceUnavailableException;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static com.aws.greengrass.componentmanager.KernelConfigResolver.CONFIGURATION_CONFIG_KEY;
import static com.aws.greengrass.deployment.DeviceConfiguration.DEVICE_PARAM_AWS_REGION;
import static com.aws.greengrass.deployment.DeviceConfiguration.DEVICE_PARAM_THING_NAME;
import static com.aws.greengrass.logmanager.LogManagerService.COMPONENT_LOGS_CONFIG_MAP_TOPIC_NAME;
import static com.aws.greengrass.logmanager.LogManagerService.DELETE_LOG_FILES_AFTER_UPLOAD_CONFIG_TOPIC_NAME;
import static com.aws.greengrass.logmanager.LogManagerService.DISK_SPACE_LIMIT_CONFIG_TOPIC_NAME;
import static com.aws.greengrass.logmanager.LogManagerService.FILE_DIRECTORY_PATH_CONFIG_TOPIC_NAME;
import static com.aws.greengrass.logmanager.LogManagerService.FILE_REGEX_CONFIG_TOPIC_NAME;
import static com.aws.greengrass.logmanager.LogManagerService.LOGS_UPLOADER_CONFIGURATION_TOPIC;
import static com.aws.greengrass.logmanager.LogManagerService.LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC;
import static com.aws.greengrass.logmanager.LogManagerService.MULTILINE_PATTERN_CONFIG_TOPIC_NAME;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

@ExtendWith({MockitoExtension.class, GGExtension.class})
class PartialUploadTest {
    private static final String COMPONENT = "UserComponentA";
    private static final String THING = "T";
    // CloudWatch rejects events older than 14 days, so the processor skips them
    private static final String DAY_D = utcDay(Instant.now().minus(3, ChronoUnit.DAYS));
    private static final String DAY_D1 = utcDay(Instant.now().minus(2, ChronoUnit.DAYS));

    enum Layout {
        ROTATED_ACROSS_MIDNIGHT,
        ACTIVE_ACROSS_MIDNIGHT,
        ROTATED_INTERLEAVED,
        TWO_ROTATED_FILES
    }

    enum Failure {
        SERVICE_ERROR_CONTINUES_BATCH,
        CLIENT_ERROR_ABORTS_BATCH
    }

    enum FailingStream {
        EARLIER,
        LATER,
        FIRST_ATTEMPTED,
        AFTER_FIRST_ATTEMPTED
    }

    @TempDir
    Path tempDir;

    private final Context context = new Context();
    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final DeviceConfiguration deviceConfiguration = mock(DeviceConfiguration.class);

    @BeforeEach
    void setup(ExtensionContext extensionContext) {
        ignoreExceptionOfType(extensionContext, ServiceUnavailableException.class);
        ignoreExceptionOfType(extensionContext, SdkClientException.class);
        lenient().when(deviceConfiguration.getThingName()).thenReturn(Topic.of(context, DEVICE_PARAM_THING_NAME,
                THING));
        lenient().when(deviceConfiguration.getAWSRegion()).thenReturn(Topic.of(context, DEVICE_PARAM_AWS_REGION,
                "us-east-1"));
    }

    @AfterEach
    void cleanup() {
        executor.shutdownNow();
        context.shutdown();
    }

    @TestFactory
    Stream<DynamicTest> GIVEN_lines_of_a_read_split_across_streams_WHEN_one_stream_fails_THEN_no_line_is_lost() {
        Stream.Builder<DynamicTest> cases = Stream.builder();
        for (Layout layout : Layout.values()) {
            for (FailingStream failingStream : FailingStream.values()) {
                for (Failure failure : Failure.values()) {
                    for (boolean deleteAfterUpload : new boolean[]{true, false}) {
                        for (boolean restart : new boolean[]{false, true}) {
                            String name = String.format("%s fail=%s %s delete=%s restart=%s", layout, failingStream,
                                    failure, deleteAfterUpload, restart);
                            cases.add(DynamicTest.dynamicTest(name, () -> new Scenario().run(name, scenario ->
                                    scenario.partialFailure(layout, failingStream, failure, deleteAfterUpload,
                                            restart))));
                        }
                    }
                }
            }
        }
        return cases.build();
    }

    @TestFactory
    Stream<DynamicTest> GIVEN_every_stream_succeeds_WHEN_files_are_uploaded_THEN_each_line_is_delivered_once() {
        Stream.Builder<DynamicTest> cases = Stream.builder();
        for (Layout layout : Layout.values()) {
            for (boolean deleteAfterUpload : new boolean[]{true, false}) {
                cases.add(DynamicTest.dynamicTest(layout + " delete=" + deleteAfterUpload,
                        () -> new Scenario().run(layout + " delete=" + deleteAfterUpload,
                                scenario -> scenario.happyPath(layout, deleteAfterUpload))));
            }
        }
        return cases.build();
    }

    @TestFactory
    Stream<DynamicTest> GIVEN_active_file_crosses_midnight_WHEN_new_day_stream_fails_THEN_old_day_lines_are_sent_once() {
        return Stream.of(Failure.values()).map(failure -> DynamicTest.dynamicTest(failure.name(),
                () -> new Scenario().run("new day fails " + failure, scenario -> scenario.newDayFails(failure))));
    }

    @Test
    void GIVEN_stream_fails_every_cycle_WHEN_other_streams_succeed_THEN_rotated_file_is_never_completed()
            throws Exception {
        new Scenario().run("rotated never completed", scenario -> {
            scenario.writeFiles(Layout.ROTATED_ACROSS_MIDNIGHT, FailingStream.EARLIER);
            scenario.stubCloudWatch(FailingStream.EARLIER, streamFor(DAY_D), Failure.SERVICE_ERROR_CONTINUES_BATCH);
            scenario.configure(true);

            scenario.startService();
            scenario.awaitCycles(4);

            assertTrue(Files.exists(scenario.files.get("rotated")),
                    "rotated file kept while its earlier lines never uploaded");
        });
    }

    @Test
    void GIVEN_disk_space_limit_WHEN_one_stream_fails_THEN_already_uploaded_files_are_still_freed() throws Exception {
        new Scenario().run("disk space freed", scenario -> {
            scenario.writeFile("older", "test.log.1", Instant.now().minusSeconds(600),
                    line(DAY_D, "20:00:00", "a"), line(DAY_D, "20:00:01", "b"));
            scenario.writeFile("active", "test.log", Instant.now().minusSeconds(300),
                    line(DAY_D, "21:00:00", "c"), line(DAY_D, "21:00:01", "d"));
            scenario.failing.set(false);
            scenario.stubCloudWatch(FailingStream.LATER, streamFor(DAY_D1), Failure.SERVICE_ERROR_CONTINUES_BATCH);
            scenario.configure(false);
            scenario.component().createLeafChild(DISK_SPACE_LIMIT_CONFIG_TOPIC_NAME).withValue("1");

            scenario.startService();
            scenario.awaitCycles(1);
            scenario.failing.set(true);
            Files.write(scenario.files.get("active"), (line(DAY_D, "23:59:59", "e") + "\n"
                    + line(DAY_D1, "00:00:01", "f") + "\n").getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.APPEND);
            scenario.awaitCycles(3);

            assertFalse(Files.exists(scenario.files.get("older")),
                    "uploaded file over the disk space limit deleted while another stream fails");
        });
    }

    @Test
    void GIVEN_rotated_files_share_a_modification_time_WHEN_stream_of_the_second_fails_THEN_no_line_is_lost()
            throws Exception {
        new Scenario().run("tied modification times", Scenario::tiedRotatedFilesSecondFails);
    }

    @TestFactory
    Stream<DynamicTest> GIVEN_entry_larger_than_a_batch_WHEN_it_is_uploaded_over_several_cycles_THEN_no_byte_is_skipped() {
        Stream.Builder<DynamicTest> cases = Stream.builder();
        for (String ending : new String[]{"\n", "\r\n"}) {
            for (int lineCount : new int[]{1, 120}) {
                String name = "lines=" + lineCount + " ending=" + ending.replace("\r", "CR").replace("\n", "LF");
                cases.add(DynamicTest.dynamicTest(name, () -> new Scenario().run(name,
                        scenario -> scenario.entryLargerThanABatch(lineCount, ending))));
            }
        }
        return cases.build();
    }

    interface ScenarioBody {
        void accept(Scenario scenario) throws Exception;
    }

    private class Scenario {
        private final Path logDir;
        private final Path workDir;
        private final Topics config = new Configuration(context).lookupTopics("a");
        private final CloudWatchLogsClient client = mock(CloudWatchLogsClient.class);
        private final NucleusPaths nucleusPaths = mock(NucleusPaths.class);
        private final AtomicBoolean failing = new AtomicBoolean(true);
        private String lineEnding = "\n";
        private final List<String> delivered = Collections.synchronizedList(new ArrayList<>());
        private final AtomicInteger cycles = new AtomicInteger();
        private final Set<String> expectedLines = new LinkedHashSet<>();
        private final Map<String, Path> files = new HashMap<>();
        private final Map<String, Set<String>> streamsReadingFile = new HashMap<>();
        private LogManagerService service;
        private Future<?> serviceRun;
        private CountDownLatch serviceStopped;
        private final AtomicInteger callsThisCycle = new AtomicInteger();
        private final Set<String> failedStreams = ConcurrentHashMap.newKeySet();

        Scenario() throws IOException {
            logDir = Files.createTempDirectory(tempDir, "logs");
            workDir = Files.createTempDirectory(tempDir, "work");
            lenient().when(nucleusPaths.workPath(LogManagerService.LOGS_UPLOADER_SERVICE_TOPICS)).thenReturn(workDir);
        }

        void run(String name, ScenarioBody body) throws Exception {
            try {
                body.accept(this);
            } catch (AssertionError e) {
                throw new AssertionError(name + ": " + e.getMessage(), e);
            } finally {
                stopService();
            }
        }

        void entryLargerThanABatch(int lineCount, String ending) throws Exception {
            StringBuilder entry = new StringBuilder();
            StringBuilder text = new StringBuilder();
            for (int i = 0; i < lineCount; i++) {
                StringBuilder physicalLine = new StringBuilder(i == 0 ? line(DAY_D, "10:00:00", "big") : "\tat frame");
                while (physicalLine.length() < 1_200_000 / lineCount) {
                    physicalLine.append((char) ('0' + physicalLine.length() % 10));
                }
                entry.append(i == 0 ? "" : ending).append(physicalLine);
                text.append(physicalLine);
            }
            String small = line(DAY_D1, "01:00:00", "small");
            lineEnding = ending;
            writeFile("rotated", "test.log.1", Instant.now().minusSeconds(600), entry.toString());
            writeFile("active", "test.log", Instant.now().minusSeconds(300), small);
            failing.set(false);
            stubCloudWatch(FailingStream.EARLIER, "", Failure.SERVICE_ERROR_CONTINUES_BATCH);
            configure(false);

            startService();
            awaitCycles(4);

            StringBuilder deliveredText = new StringBuilder();
            synchronized (delivered) {
                delivered.stream().filter(message -> !message.equals(small)).forEach(deliveredText::append);
            }
            int at = 0;
            while (at < text.length() && at < deliveredText.length() && text.charAt(at) == deliveredText.charAt(at)) {
                at++;
            }
            assertEquals(window(text, at), window(deliveredText, at),
                    "text of the split entry without line endings, first difference at char " + at);
        }

        void partialFailure(Layout layout, FailingStream failingStream, Failure failure, boolean deleteAfterUpload,
                            boolean restart) throws Exception {
            String failingStreamName = writeFiles(layout, failingStream);
            stubCloudWatch(failingStream, failingStreamName, failure);
            configure(deleteAfterUpload);

            startService();
            awaitCycles(1);
            for (Map.Entry<String, Path> file : files.entrySet()) {
                if (!Collections.disjoint(streamsReadingFile.get(file.getKey()), failedStreams)) {
                    assertTrue(Files.exists(file.getValue()),
                            file.getKey() + " kept while a stream that read it failed");
                }
            }
            if (restart) {
                stopService();
            }
            failing.set(false);
            if (restart) {
                startService();
            }
            awaitCycles(3);

            Set<String> missing = new LinkedHashSet<>(expectedLines);
            delivered.forEach(missing::remove);
            assertEquals(Collections.emptySet(), missing, "lines never delivered to CloudWatch");
            if (layout != Layout.ACTIVE_ACROSS_MIDNIGHT) {
                assertEquals(!deleteAfterUpload, Files.exists(files.get("rotated")),
                        "rotated file deleted once fully uploaded only when delete-after-upload is on");
            }
        }

        void tiedRotatedFilesSecondFails() throws Exception {
            Instant tied = Instant.now().minusSeconds(600);
            writeFile("older", "test.log.2", tied, line(DAY_D, "22:00:00", "a"), line(DAY_D, "22:00:01", "b"));
            writeFile("rotated", "test.log.1", tied, line(DAY_D1, "02:00:00", "c"), line(DAY_D1, "02:00:01", "d"));
            writeFile("active", "test.log", tied.plusSeconds(60), line(DAY_D1, "03:00:00", "e"),
                    line(DAY_D1, "03:00:01", "f"));
            // Equal modification times leave the read order to the directory listing, which the service also uses
            String secondTiedFile = Stream.of(logDir.toFile().listFiles())
                    .map(File::getName)
                    .filter(name -> name.startsWith("test.log.")).reduce((first, second) -> second).get();
            String failingStream = streamFor("test.log.2".equals(secondTiedFile) ? DAY_D : DAY_D1);
            stubCloudWatch(FailingStream.EARLIER, failingStream, Failure.SERVICE_ERROR_CONTINUES_BATCH);
            configure(true);

            startService();
            awaitCycles(4);
            failing.set(false);
            awaitCycles(3);

            Set<String> missing = new LinkedHashSet<>(expectedLines);
            delivered.forEach(missing::remove);
            assertEquals(Collections.emptySet(), missing, "lines never delivered to CloudWatch");
            assertFalse(Files.exists(files.get("older")), "tied rotated file deleted once uploaded");
            assertFalse(Files.exists(files.get("rotated")), "tied rotated file deleted once uploaded");
        }

        void happyPath(Layout layout, boolean deleteAfterUpload) throws Exception {
            writeFiles(layout, FailingStream.EARLIER);
            failing.set(false);
            stubCloudWatch(FailingStream.EARLIER, "", Failure.SERVICE_ERROR_CONTINUES_BATCH);
            configure(deleteAfterUpload);

            startService();
            awaitCycles(1);
            String appended = line(DAY_D1, "23:00:00", "appended");
            Files.write(files.get("active"), (appended + "\n").getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.APPEND);
            expectedLines.add(appended);
            awaitCycles(3);

            List<String> expected = new ArrayList<>(expectedLines);
            List<String> actual = new ArrayList<>(delivered);
            Collections.sort(expected);
            Collections.sort(actual);
            assertEquals(expected, actual, "each line delivered exactly once");
            if (layout != Layout.ACTIVE_ACROSS_MIDNIGHT) {
                assertEquals(!deleteAfterUpload, Files.exists(files.get("rotated")));
            }
        }

        void newDayFails(Failure failure) throws Exception {
            String newDayStream = writeFiles(Layout.ACTIVE_ACROSS_MIDNIGHT, FailingStream.LATER);
            stubCloudWatch(FailingStream.LATER, newDayStream, failure);
            configure(false);

            startService();
            awaitCycles(4);
            failing.set(false);
            awaitCycles(3);

            Map<String, Long> expectedOldDay = new LinkedHashMap<>();
            Map<String, Long> deliveredOldDay = new LinkedHashMap<>();
            Set<String> missing = new LinkedHashSet<>();
            for (String line : expectedLines) {
                long sends = delivered.stream().filter(line::equals).count();
                if (line.startsWith(DAY_D)) {
                    expectedOldDay.put(line, 1L);
                    deliveredOldDay.put(line, sends);
                } else if (sends == 0) {
                    missing.add(line);
                }
            }
            assertEquals(expectedOldDay, deliveredOldDay, "old-day lines delivered exactly once");
            assertEquals(Collections.emptySet(), missing, "new-day lines never delivered to CloudWatch");
        }

        private String writeFiles(Layout layout, FailingStream failingStream) throws IOException {
            Instant base = Instant.now().minusSeconds(600);
            String earlier;
            String later;
            switch (layout) {
                case ROTATED_ACROSS_MIDNIGHT:
                    writeFile("rotated", "test.log.1", base, line(DAY_D, "23:59:58", "a"), line(DAY_D, "23:59:59", "b"),
                            line(DAY_D1, "00:00:01", "c"), line(DAY_D1, "00:00:02", "d"));
                    writeFile("active", "test.log", base.plusSeconds(60), line(DAY_D1, "01:00:00", "e"),
                            line(DAY_D1, "01:00:01", "f"));
                    earlier = streamFor(DAY_D);
                    later = streamFor(DAY_D1);
                    break;
                case ACTIVE_ACROSS_MIDNIGHT:
                    writeFile("active", "test.log", base, line(DAY_D, "23:59:58", "a"), line(DAY_D, "23:59:59", "b"),
                            line(DAY_D1, "00:00:01", "c"), line(DAY_D1, "00:00:02", "d"));
                    earlier = streamFor(DAY_D);
                    later = streamFor(DAY_D1);
                    break;
                case ROTATED_INTERLEAVED:
                    writeFile("rotated", "test.log.1", base, line(DAY_D, "10:00:00", "a"), untimestamped("b"),
                            line(DAY_D, "10:00:01", "c"), untimestamped("d"), line(DAY_D, "10:00:02", "e"),
                            untimestamped("f"));
                    writeFile("active", "test.log", base.plusSeconds(60), line(DAY_D1, "01:00:00", "g"),
                            line(DAY_D1, "01:00:01", "h"));
                    earlier = streamFor(DAY_D);
                    later = streamFor(today());
                    break;
                case TWO_ROTATED_FILES:
                    writeFile("older", "test.log.2", base, line(DAY_D, "22:00:00", "a"), line(DAY_D, "22:00:01", "b"));
                    writeFile("rotated", "test.log.1", base.plusSeconds(60), line(DAY_D1, "02:00:00", "c"),
                            line(DAY_D1, "02:00:01", "d"));
                    writeFile("active", "test.log", base.plusSeconds(120), line(DAY_D1, "03:00:00", "e"),
                            line(DAY_D1, "03:00:01", "f"));
                    earlier = streamFor(DAY_D);
                    later = streamFor(DAY_D1);
                    break;
                default:
                    throw new IllegalArgumentException(layout.name());
            }
            return failingStream == FailingStream.EARLIER ? earlier : later;
        }

        private void writeFile(String role, String name, Instant modified, String... lines) throws IOException {
            Path path = logDir.resolve(name);
            StringBuilder content = new StringBuilder();
            Set<String> streams = new HashSet<>();
            for (String line : lines) {
                content.append(line).append(lineEnding);
                expectedLines.add(line);
                streams.add(streamFor(Character.isDigit(line.charAt(0)) ? line.substring(0, 10) : today()));
            }
            streamsReadingFile.put(role, streams);
            Files.write(path, content.toString().getBytes(StandardCharsets.UTF_8));
            Files.setLastModifiedTime(path, FileTime.from(modified));
            files.put(role, path);
        }

        private void stubCloudWatch(FailingStream failingStream, String failingStreamName, Failure failure) {
            lenient().when(client.putLogEvents(any(PutLogEventsRequest.class))).thenAnswer(invocation -> {
                PutLogEventsRequest request = invocation.getArgument(0);
                int position = callsThisCycle.getAndIncrement();
                boolean fails;
                if (failingStream == FailingStream.FIRST_ATTEMPTED) {
                    fails = position == 0;
                } else if (failingStream == FailingStream.AFTER_FIRST_ATTEMPTED) {
                    fails = position > 0;
                } else {
                    fails = request.logStreamName().equals(failingStreamName);
                }
                if (failing.get() && fails) {
                    failedStreams.add(request.logStreamName());
                    if (failure == Failure.CLIENT_ERROR_ABORTS_BATCH) {
                        throw SdkClientException.create("simulated network failure");
                    }
                    throw ServiceUnavailableException.builder().message("simulated 5xx").build();
                }
                for (InputLogEvent event : request.logEvents()) {
                    delivered.add(event.message());
                }
                return PutLogEventsResponse.builder().nextSequenceToken("token").build();
            });
        }

        private Topics component() {
            return config.lookupTopics(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_CONFIGURATION_TOPIC,
                    COMPONENT_LOGS_CONFIG_MAP_TOPIC_NAME, COMPONENT);
        }

        private void configure(boolean deleteAfterUpload) {
            config.lookup(CONFIGURATION_CONFIG_KEY, LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC).withValue("0.1");
            Topics component = component();
            component.createLeafChild(FILE_REGEX_CONFIG_TOPIC_NAME).withValue("^test.log\\w*");
            component.createLeafChild(FILE_DIRECTORY_PATH_CONFIG_TOPIC_NAME).withValue(logDir.toString());
            component.createLeafChild(MULTILINE_PATTERN_CONFIG_TOPIC_NAME).withValue("^\\S");
            component.createLeafChild(DELETE_LOG_FILES_AFTER_UPLOAD_CONFIG_TOPIC_NAME)
                    .withValue(Boolean.toString(deleteAfterUpload));
        }

        private void startService() throws IOException {
            CloudWatchClientFactory factory = mock(CloudWatchClientFactory.class);
            lenient().when(factory.getWrapper()).thenReturn(new SdkClientWrapper<>(() -> client));
            service = new LogManagerService(config, new CloudWatchLogsUploader(factory),
                    new CloudWatchAttemptLogsProcessor(deviceConfiguration), nucleusPaths);
            cycles.set(0);
            service.registerEventStatusListener(event -> {
                if (event == EventType.ALL_COMPONENTS_PROCESSED) {
                    callsThisCycle.set(0);
                    cycles.incrementAndGet();
                }
            });
            CountDownLatch stopped = new CountDownLatch(1);
            serviceStopped = stopped;
            serviceRun = executor.submit(() -> {
                try {
                    service.startup();
                } catch (InterruptedException ignored) {
                } finally {
                    stopped.countDown();
                }
            });
        }

        private void stopService() throws InterruptedException {
            if (serviceRun == null || serviceRun.isDone()) {
                return;
            }
            serviceRun.cancel(true);
            assertTrue(serviceStopped.await(20, TimeUnit.SECONDS), "upload thread stopped");
            service.shutdown();
        }

        private void awaitCycles(int count) throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            int target = cycles.get() + count;
            while (cycles.get() < target) {
                assertTrue(System.nanoTime() < deadline, "upload cycles did not run");
                TimeUnit.MILLISECONDS.sleep(20);
            }
        }
    }

    private static String line(String day, String time, String id) {
        return padToHashableLength(day + "T" + time + "Z [INFO] line-" + id);
    }

    private static String untimestamped(String id) {
        return padToHashableLength("untimestamped [INFO] line-" + id);
    }

    private static String padToHashableLength(String prefix) {
        StringBuilder sb = new StringBuilder(prefix).append(' ');
        while (sb.length() < LogFile.bytesNeeded) {
            sb.append('x');
        }
        return sb.toString();
    }

    private static String streamFor(String isoDay) {
        return "/" + isoDay.replace('-', '/') + "/thing/" + THING;
    }

    private static String today() {
        return utcDay(Instant.now());
    }

    private static String utcDay(Instant instant) {
        return DateTimeFormatter.ISO_LOCAL_DATE.withZone(ZoneOffset.UTC).format(instant);
    }

    private static String window(CharSequence text, int at) {
        return text.length() + " chars, " + text.subSequence(Math.max(0, at - 12), Math.min(text.length(), at + 12));
    }
}

