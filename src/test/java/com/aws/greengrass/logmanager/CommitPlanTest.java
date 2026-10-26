/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.logmanager;

import com.aws.greengrass.logmanager.model.CloudWatchAttempt;
import com.aws.greengrass.logmanager.model.CloudWatchAttemptLogFileInformation;
import com.aws.greengrass.logmanager.model.CloudWatchAttemptLogInformation;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(GGExtension.class)
class CommitPlanTest {
    private static final long OLDER = 100;
    private static final long OLD = 200;
    private static final long ACTIVE = 300;

    @Test
    void GIVEN_every_stream_uploaded_WHEN_planned_THEN_each_file_commits_its_furthest_position_in_either_order() {
        for (boolean furthestFirst : new boolean[]{true, false}) {
            HandBuiltAttempt attempt = new HandBuiltAttempt();
            CloudWatchAttemptLogFileInformation[] far = {record("a", 0, 0, 1000, OLD), record("b", 0, 0, 50, ACTIVE)};
            CloudWatchAttemptLogFileInformation[] near = {record("a", 0, 300, 900, OLD), record("b", 0, 0, 100,
                    ACTIVE)};
            attempt.stream("s1", true, furthestFirst ? far : near);
            attempt.stream("s2", true, furthestFirst ? near : far);

            assertEquals(Arrays.asList("a@1000", "b@100"), sorted(attempt.plan()), "furthestFirst=" + furthestFirst);
        }
    }

    @Test
    void GIVEN_new_stream_fails_in_active_file_WHEN_planned_THEN_rotated_files_commit_fully_and_active_stops_at_cut() {
        HandBuiltAttempt attempt = new HandBuiltAttempt("r1", "r2", "active");
        attempt.stream("old", true, record("r1", 0, 0, 500, OLDER), record("r2", 0, 0, 600, OLD),
                record("active", 0, 0, 250, ACTIVE));
        attempt.stream("new", false, record("active", 0, 250, 400, ACTIVE));

        assertEquals(Arrays.asList("r1@500", "r2@600", "active@250"), attempt.plan());
    }

    @Test
    void GIVEN_stream_fails_in_rotated_file_WHEN_planned_THEN_only_that_file_commits_up_to_the_cut() {
        HandBuiltAttempt attempt = new HandBuiltAttempt("r1", "r2", "active");
        attempt.stream("failed", false, record("r1", 0, 300, 500, OLDER));
        attempt.stream("uploaded", true, record("r1", 0, 0, 300, OLDER), record("r2", 0, 0, 600, OLD),
                record("active", 0, 0, 200, ACTIVE));

        assertEquals(Collections.singletonList("r1@300"), attempt.plan());
    }

    @Test
    void GIVEN_uploaded_stream_reaches_further_in_file_WHEN_planned_THEN_file_commits_failed_streams_first_entry() {
        HandBuiltAttempt attempt = new HandBuiltAttempt("active");
        attempt.stream("timestamped", true, record("active", 100, 100, 1000, ACTIVE));
        attempt.stream("untimestamped", false, record("active", 100, 400, 800, ACTIVE));

        assertEquals(Collections.singletonList("active@400"), attempt.plan());
    }

    @Test
    void GIVEN_two_failed_streams_WHEN_planned_THEN_cut_is_the_earliest_entry_in_read_order_in_either_order() {
        for (boolean earliestFirst : new boolean[]{true, false}) {
            HandBuiltAttempt attempt = new HandBuiltAttempt("r1", "active");
            CloudWatchAttemptLogFileInformation[] earliest = {record("r1", 0, 600, 700, OLD),
                    record("active", 0, 0, 50, ACTIVE)};
            CloudWatchAttemptLogFileInformation[] later = {record("r1", 0, 650, 800, OLD),
                    record("active", 0, 10, 20, ACTIVE)};
            attempt.stream("uploaded", true, record("r1", 0, 0, 600, OLD));
            attempt.stream("f1", false, earliestFirst ? earliest : later);
            attempt.stream("f2", false, earliestFirst ? later : earliest);

            assertEquals(Collections.singletonList("r1@600"), attempt.plan(), "earliestFirst=" + earliestFirst);
        }
    }

    @Test
    void GIVEN_batch_aborted_before_earliest_stream_was_attempted_WHEN_planned_THEN_that_stream_sets_the_cut() {
        HandBuiltAttempt attempt = new HandBuiltAttempt("r1", "active");
        attempt.stream("uploaded", true, record("r1", 0, 0, 900, OLD), record("active", 0, 0, 100, ACTIVE));
        attempt.stream("threw", false, record("active", 0, 100, 200, ACTIVE));
        attempt.stream("never attempted", false, record("r1", 0, 900, 1000, OLD));

        assertEquals(Collections.singletonList("r1@900"), attempt.plan());
    }

    @Test
    void GIVEN_file_before_cut_as_new_as_cut_file_WHEN_planned_THEN_that_file_gets_no_commit() {
        HandBuiltAttempt attempt = new HandBuiltAttempt("r2", "r1", "active");
        attempt.stream("old", true, record("r2", 0, 0, 500, OLDER), record("r1", 0, 0, 600, ACTIVE),
                record("active", 0, 0, 100, ACTIVE));
        attempt.stream("new", false, record("active", 0, 100, 200, ACTIVE));

        assertEquals(Arrays.asList("r2@500", "active@100"), attempt.plan());
    }

    @Test
    void GIVEN_failed_record_without_first_entry_start_WHEN_planned_THEN_cut_is_its_read_start() {
        HandBuiltAttempt attempt = new HandBuiltAttempt("active");
        attempt.stream("uploaded", true, record("active", 40, 40, 900, ACTIVE));
        attempt.stream("failed", false, CloudWatchAttemptLogFileInformation.builder().fileHash("active")
                .startPosition(40).bytesRead(500).lastModifiedTime(ACTIVE).build());

        assertEquals(Collections.singletonList("active@40"), attempt.plan());
    }

    @Test
    void GIVEN_failed_stream_read_nothing_WHEN_planned_THEN_each_file_commits_its_furthest_position() {
        HandBuiltAttempt attempt = new HandBuiltAttempt("r1", "active");
        attempt.stream("uploaded", true, record("r1", 0, 0, 900, OLD), record("active", 0, 0, 100, ACTIVE));
        attempt.stream("failed", false);

        assertEquals(Arrays.asList("active@100", "r1@900"), sorted(attempt.plan()));
    }

    @Test
    void GIVEN_failed_record_for_a_file_missing_from_read_order_WHEN_planned_THEN_nothing_commits() {
        HandBuiltAttempt attempt = new HandBuiltAttempt("r1");
        attempt.stream("uploaded", true, record("r1", 0, 0, 900, OLD));
        attempt.stream("failed", false, record("unknown", 0, 0, 100, ACTIVE));

        assertEquals(Collections.emptyList(), attempt.plan());
    }

    private static CloudWatchAttemptLogFileInformation record(String fileHash, long readStart, long firstEntryStart,
                                                              long end, long lastModified) {
        return CloudWatchAttemptLogFileInformation.builder().fileHash(fileHash).startPosition(readStart)
                .firstEntryStartPosition(firstEntryStart).bytesRead(end - readStart).lastModifiedTime(lastModified)
                .build();
    }

    private static List<String> sorted(List<String> commits) {
        List<String> sorted = new ArrayList<>(commits);
        Collections.sort(sorted);
        return sorted;
    }

    private static class HandBuiltAttempt {
        private final CloudWatchAttempt attempt = new CloudWatchAttempt();

        HandBuiltAttempt(String... fileHashesInReadOrder) {
            attempt.setLogStreamsToLogEventsMap(new LinkedHashMap<>());
            attempt.getFileHashesInReadOrder().addAll(Arrays.asList(fileHashesInReadOrder));
        }

        void stream(String name, boolean uploaded, CloudWatchAttemptLogFileInformation... records) {
            CloudWatchAttemptLogInformation stream = CloudWatchAttemptLogInformation.builder()
                    .componentName("TestComponent").build();
            for (CloudWatchAttemptLogFileInformation record : records) {
                stream.getAttemptLogFileInformationMap().put(record.getFileHash(), record);
            }
            attempt.getLogStreamsToLogEventsMap().put(name, stream);
            if (uploaded) {
                attempt.getLogStreamUploadedSet().add(name);
            }
        }

        List<String> plan() {
            return CommitPlan.of(attempt).stream().map(commit -> commit.getFileHash() + "@" + commit.getPosition())
                    .collect(Collectors.toList());
        }
    }
}
