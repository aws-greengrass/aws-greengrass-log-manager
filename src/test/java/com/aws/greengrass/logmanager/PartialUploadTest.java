/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.logmanager;

import com.aws.greengrass.config.Configuration;
import com.aws.greengrass.config.Topic;
import com.aws.greengrass.config.Topics;
import com.aws.greengrass.config.UpdateBehaviorTree;
import com.aws.greengrass.dependency.Context;
import com.aws.greengrass.deployment.DeviceConfiguration;
import com.aws.greengrass.logging.impl.GreengrassLogMessage;
import com.aws.greengrass.logging.impl.Slf4jLogAdapter;
import com.aws.greengrass.logmanager.model.EventType;
import com.aws.greengrass.logmanager.model.LogFile;
import com.aws.greengrass.logmanager.util.CloudWatchClientFactory;
import com.aws.greengrass.logmanager.util.SdkClientWrapper;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.util.Digest;
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
import java.util.Arrays;
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
import java.util.function.Consumer;
import java.util.stream.Stream;

import static com.aws.greengrass.componentmanager.KernelConfigResolver.CONFIGURATION_CONFIG_KEY;
import static com.aws.greengrass.deployment.DeviceConfiguration.DEVICE_PARAM_AWS_REGION;
import static com.aws.greengrass.deployment.DeviceConfiguration.DEVICE_PARAM_THING_NAME;
import static com.aws.greengrass.lifecyclemanager.GreengrassService.RUNTIME_STORE_NAMESPACE_TOPIC;
import static com.aws.greengrass.logmanager.LogManagerService.COMPONENT_LOGS_CONFIG_MAP_TOPIC_NAME;
import static com.aws.greengrass.logmanager.LogManagerService.DELETE_LOG_FILES_AFTER_UPLOAD_CONFIG_TOPIC_NAME;
import static com.aws.greengrass.logmanager.LogManagerService.DISK_SPACE_LIMIT_CONFIG_TOPIC_NAME;
import static com.aws.greengrass.logmanager.LogManagerService.FILE_DIRECTORY_PATH_CONFIG_TOPIC_NAME;
import static com.aws.greengrass.logmanager.LogManagerService.FILE_REGEX_CONFIG_TOPIC_NAME;
import static com.aws.greengrass.logmanager.LogManagerService.LOGS_UPLOADER_CONFIGURATION_TOPIC;
import static com.aws.greengrass.logmanager.LogManagerService.LOGS_UPLOADER_PERIODIC_UPDATE_INTERVAL_SEC;
import static com.aws.greengrass.logmanager.LogManagerService.MULTILINE_PATTERN_CONFIG_TOPIC_NAME;
import static com.aws.greengrass.logmanager.LogManagerService.PERSISTED_COMPONENT_CURRENT_PROCESSING_FILE_INFORMATION_V2;
import static com.aws.greengrass.logmanager.LogManagerService.PERSISTED_CURRENT_PROCESSING_FILE_HASH;
import static com.aws.greengrass.logmanager.LogManagerService.PERSISTED_CURRENT_PROCESSING_FILE_LAST_ACCESSED_TIME;
import static com.aws.greengrass.logmanager.LogManagerService.PERSISTED_CURRENT_PROCESSING_FILE_LAST_MODIFIED_TIME;
import static com.aws.greengrass.logmanager.LogManagerService.PERSISTED_CURRENT_PROCESSING_FILE_NAME;
import static com.aws.greengrass.logmanager.LogManagerService.PERSISTED_CURRENT_PROCESSING_FILE_START_POSITION;
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

    enum FilePair {
        TWO_ROTATED,
        ROTATED_AND_ACTIVE,
        ACTIVE_ROTATES_BETWEEN_CYCLES
    }

    @TestFactory
    Stream<DynamicTest> GIVEN_files_of_one_component_WHEN_first_lines_match_THEN_each_line_is_delivered_once() {
        Stream.Builder<DynamicTest> cases = Stream.builder();
        for (FilePair pair : FilePair.values()) {
            for (boolean sameFirstLine : new boolean[]{true, false}) {
                for (boolean deleteAfterUpload : new boolean[]{true, false}) {
                    String name = String.format("%s sameFirstLine=%s delete=%s", pair, sameFirstLine,
                            deleteAfterUpload);
                    cases.add(DynamicTest.dynamicTest(name, () -> new Scenario().run(name, scenario ->
                            scenario.sharedFirstLine(pair, sameFirstLine, deleteAfterUpload))));
                }
            }
        }
        return cases.build();
    }

    @TestFactory
    Stream<DynamicTest> GIVEN_active_file_rotated_by_rename_WHEN_cycles_run_THEN_each_file_resumes_at_its_own_position() {
        return Stream.of(true, false).map(deleteAfterUpload -> DynamicTest.dynamicTest("delete=" + deleteAfterUpload,
                () -> new Scenario().run("rename rotation delete=" + deleteAfterUpload,
                        scenario -> scenario.renameRotation(deleteAfterUpload))));
    }

    @TestFactory
    Stream<DynamicTest> GIVEN_file_truncated_and_regrown_with_same_first_line_WHEN_cycle_runs_THEN_read_from_start() {
        Stream.Builder<DynamicTest> cases = Stream.builder();
        for (boolean copyInLogDirectory : new boolean[]{true, false}) {
            for (boolean deleteAfterUpload : new boolean[]{true, false}) {
                String name = String.format("copyInLogDirectory=%s delete=%s", copyInLogDirectory,
                        deleteAfterUpload);
                cases.add(DynamicTest.dynamicTest(name, () -> new Scenario().run(name,
                        scenario -> scenario.copyTruncate(copyInLogDirectory, deleteAfterUpload))));
            }
        }
        return cases.build();
    }

    @Test
    void GIVEN_position_saved_by_an_older_version_WHEN_upgraded_THEN_resumes_there_and_tells_later_files_apart()
            throws Exception {
        new Scenario().run("legacy record", Scenario::legacyRecord);
    }

    @Test
    void GIVEN_older_version_position_matching_two_files_WHEN_upgraded_THEN_both_read_from_start_with_a_warning()
            throws Exception {
        new Scenario().run("ambiguous legacy record", Scenario::ambiguousLegacyRecord);
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

        void sharedFirstLine(FilePair pair, boolean sameFirstLine, boolean deleteAfterUpload) throws Exception {
            Instant base = Instant.now().minusSeconds(600);
            List<String> rotatedRoles = new ArrayList<>();
            if (pair == FilePair.TWO_ROTATED) {
                writeFile("older", "test.log.2", base, banner("older", sameFirstLine), shortLine("20:00:00", "a"),
                        shortLine("20:00:01", "b"));
                rotatedRoles.add("older");
            }
            if (pair != FilePair.ACTIVE_ROTATES_BETWEEN_CYCLES) {
                writeFile("rotated", "test.log.1", base.plusSeconds(60), banner("rotated", sameFirstLine),
                        shortLine("21:00:00", "c"), shortLine("21:00:01", "d"));
                rotatedRoles.add("rotated");
            }
            writeFile("active", "test.log", base.plusSeconds(120), pair == FilePair.TWO_ROTATED
                    ? shortLine("22:00:00", "e") : banner("active", sameFirstLine), shortLine("22:00:01", "f"));
            List<String> expected = new ArrayList<>();
            for (Path file : files.values()) {
                expected.addAll(Files.readAllLines(file, StandardCharsets.UTF_8));
            }
            failing.set(false);
            stubCloudWatch(FailingStream.EARLIER, "", Failure.SERVICE_ERROR_CONTINUES_BATCH);
            configure(deleteAfterUpload);

            startService();
            awaitCycles(2);
            if (pair == FilePair.ACTIVE_ROTATES_BETWEEN_CYCLES) {
                Path rotated = logDir.resolve("test.log.1");
                Files.move(files.get("active"), rotated);
                files.put("rotated", rotated);
                rotatedRoles.add("rotated");
                writeFile("active", "test.log", base.plusSeconds(180), banner("new active", sameFirstLine),
                        shortLine("22:30:00", "g"), shortLine("22:30:01", "h"));
                expected.addAll(Files.readAllLines(files.get("active"), StandardCharsets.UTF_8));
                awaitCycles(2);
            }
            expected.add(append("active", shortLine("23:00:00", "appended")));
            awaitCycles(4);

            assertDeliveredOnce(expected);
            for (String role : rotatedRoles) {
                assertEquals(!deleteAfterUpload, Files.exists(files.get(role)),
                        role + " file deleted once fully uploaded only when delete-after-upload is on");
            }
            assertTrue(Files.exists(files.get("active")), "active file kept");
        }

        void renameRotation(boolean deleteAfterUpload) throws Exception {
            Instant base = Instant.now().minusSeconds(600);
            List<String> expected = new ArrayList<>(writeFile("active", "test.log", base, banner("active", true),
                    shortLine("20:00:00", "a"), shortLine("20:00:01", "b")));
            failing.set(false);
            stubCloudWatch(FailingStream.EARLIER, "", Failure.SERVICE_ERROR_CONTINUES_BATCH);
            configure(deleteAfterUpload);

            startService();
            awaitCycles(2);
            stopService();
            expected.add(append("active", shortLine("20:00:02", "written before rotation")));
            Path rotated = rotate(base.plusSeconds(30));
            expected.addAll(writeFile("active", "test.log", base.plusSeconds(60), banner("active", true),
                    shortLine("21:00:00", "c")));
            startService();
            awaitCycles(2);
            stopService();
            expected.add(append("active", shortLine("21:00:01", "written after restart")));
            startService();
            awaitCycles(2);

            assertDeliveredOnce(expected);
            assertEquals(!deleteAfterUpload, Files.exists(rotated),
                    "rotated file deleted once fully uploaded only when delete-after-upload is on");
        }

        void copyTruncate(boolean copyInLogDirectory, boolean deleteAfterUpload) throws Exception {
            Instant base = Instant.now().minusSeconds(600);
            List<String> expected = new ArrayList<>(writeFile("active", "test.log", base, banner("active", true),
                    shortLine("20:00:00", "a"), shortLine("20:00:01", "b")));
            failing.set(false);
            stubCloudWatch(FailingStream.EARLIER, "", Failure.SERVICE_ERROR_CONTINUES_BATCH);
            configure(deleteAfterUpload);

            startService();
            awaitCycles(2);
            stopService();
            Path copy = (copyInLogDirectory ? logDir
                    : Files.createDirectories(logDir.resolveSibling(logDir.getFileName() + "-archive")))
                    .resolve("test.log.1");
            Files.copy(files.get("active"), copy);
            Files.setLastModifiedTime(copy, FileTime.from(base.plusSeconds(60)));
            long truncatedLength = Files.size(files.get("active"));
            expected.addAll(writeFile("active", "test.log", base.plusSeconds(120), banner("active", true),
                    shortLine("21:00:00", "c regrown past the saved position"),
                    shortLine("21:00:01", "d regrown past the saved position")));
            assertTrue(Files.size(files.get("active")) > truncatedLength, "regrown past the saved position");
            startService();
            awaitCycles(3);

            assertDeliveredOnce(expected);
            if (copyInLogDirectory) {
                assertEquals(!deleteAfterUpload, Files.exists(copy),
                        "copy deleted once fully uploaded only when delete-after-upload is on");
            }
        }

        void legacyRecord() throws Exception {
            Instant base = Instant.now().minusSeconds(600);
            String banner = banner("active", true);
            writeFile("active", "test.log", base, banner, shortLine("20:00:00", "a"), shortLine("20:00:01", "b"));
            persistLegacyRecord(banner, Files.size(files.get("active")), base);
            failing.set(false);
            stubCloudWatch(FailingStream.EARLIER, "", Failure.SERVICE_ERROR_CONTINUES_BATCH);
            configure(false);

            startService();
            awaitCycles(2);
            List<String> expected = new ArrayList<>();
            expected.add(append("active", shortLine("20:00:02", "after upgrade")));
            awaitCycles(2);
            rotate(base.plusSeconds(30));
            expected.addAll(writeFile("active", "test.log", base.plusSeconds(60), banner,
                    shortLine("21:00:00", "c")));
            awaitCycles(3);

            assertDeliveredOnce(expected);
        }

        void ambiguousLegacyRecord() throws Exception {
            Instant base = Instant.now().minusSeconds(600);
            String banner = banner("active", true);
            List<String> expected = new ArrayList<>(writeFile("rotated", "test.log.1", base, banner,
                    shortLine("20:00:00", "a"), shortLine("20:00:01", "b")));
            expected.addAll(writeFile("active", "test.log", base.plusSeconds(60), banner,
                    shortLine("21:00:00", "c")));
            persistLegacyRecord(banner, Files.size(files.get("active")), base.plusSeconds(60));
            failing.set(false);
            stubCloudWatch(FailingStream.EARLIER, "", Failure.SERVICE_ERROR_CONTINUES_BATCH);
            configure(false);
            List<GreengrassLogMessage> warnings = Collections.synchronizedList(new ArrayList<>());
            Consumer<GreengrassLogMessage> listener = message -> {
                if ("WARN".equals(message.getLevel())) {
                    warnings.add(message);
                }
            };
            Slf4jLogAdapter.addGlobalListener(listener);
            try {
                startService();
                awaitCycles(3);
            } finally {
                Slf4jLogAdapter.removeGlobalListener(listener);
            }

            assertDeliveredOnce(expected);
            String bothFiles = Arrays.asList("test.log.1", "test.log").toString();
            assertTrue(warnings.stream().anyMatch(message -> bothFiles.equals(message.getContexts().get("files"))),
                    "warning names the files read from the start: " + warnings);
        }

        private void persistLegacyRecord(String firstLine, long position, Instant lastModified) throws Exception {
            String firstLineHash = Digest.calculate(firstLine + "\n");
            Map<String, Object> record = new HashMap<>();
            record.put(PERSISTED_CURRENT_PROCESSING_FILE_NAME, files.get("active").toString());
            record.put(PERSISTED_CURRENT_PROCESSING_FILE_HASH, firstLineHash);
            record.put(PERSISTED_CURRENT_PROCESSING_FILE_START_POSITION, position);
            record.put(PERSISTED_CURRENT_PROCESSING_FILE_LAST_MODIFIED_TIME, lastModified.toEpochMilli());
            record.put(PERSISTED_CURRENT_PROCESSING_FILE_LAST_ACCESSED_TIME, System.currentTimeMillis());
            config.lookupTopics(RUNTIME_STORE_NAMESPACE_TOPIC,
                    PERSISTED_COMPONENT_CURRENT_PROCESSING_FILE_INFORMATION_V2, COMPONENT, firstLineHash)
                    .updateFromMap(record,
                    new UpdateBehaviorTree(UpdateBehaviorTree.UpdateBehavior.REPLACE, System.currentTimeMillis()));
        }

        private Path rotate(Instant lastWritten) throws IOException {
            Path rotated = logDir.resolve("test.log.1");
            Files.move(files.get("active"), rotated);
            Files.setLastModifiedTime(rotated, FileTime.from(lastWritten));
            files.put("rotated", rotated);
            return rotated;
        }

        private String append(String role, String line) throws IOException {
            Files.write(files.get(role), (line + "\n").getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
            return line;
        }

        private void assertDeliveredOnce(List<String> expected) {
            List<String> extra = new ArrayList<>(delivered);
            List<String> missing = new ArrayList<>();
            for (String line : expected) {
                if (!extra.remove(line)) {
                    missing.add(line);
                }
            }
            assertEquals("missing=[] extra=[]", "missing=" + missing + " extra=" + extra,
                    "each line of every file delivered exactly once");
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

        private List<String> writeFile(String role, String name, Instant modified, String... lines)
                throws IOException {
            Path path = logDir.resolve(name);
            StringBuilder content = new StringBuilder();
            Set<String> streams = new HashSet<>();
            for (String line : lines) {
                content.append(line).append('\n');
                expectedLines.add(line);
                streams.add(streamFor(Character.isDigit(line.charAt(0)) ? line.substring(0, 10) : today()));
            }
            streamsReadingFile.put(role, streams);
            Files.write(path, content.toString().getBytes(StandardCharsets.UTF_8));
            Files.setLastModifiedTime(path, FileTime.from(modified));
            files.put(role, path);
            return Arrays.asList(lines);
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

    private static String shortLine(String time, String id) {
        return DAY_D + "T" + time + "Z [INFO] line-" + id;
    }

    private static String banner(String role, boolean sameFirstLine) {
        return sameFirstLine ? "=== service starting ===" : "=== service starting " + role + " ===";
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
}

