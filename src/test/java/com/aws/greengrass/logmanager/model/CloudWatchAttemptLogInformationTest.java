/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.logmanager.model;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CloudWatchAttemptLogInformationTest {
    @Test
    void GIVEN_log_information_WHEN_events_added_THEN_getter_is_read_only_and_earliest_timestamp_tracks_adds() {
        CloudWatchAttemptLogInformation information = CloudWatchAttemptLogInformation.builder().build();
        information.addLogEvent(InputLogEvent.builder().timestamp(2000L).message("b").build());
        information.addLogEvent(InputLogEvent.builder().timestamp(1000L).message("a").build());

        assertThrows(UnsupportedOperationException.class, () -> information.getLogEvents()
                .add(InputLogEvent.builder().timestamp(1L).message("c").build()));
        assertThrows(UnsupportedOperationException.class, () -> information.getLogEvents().clear());
        assertEquals(2, information.getLogEvents().size());
        assertEquals(1000L, information.getEarliestTimestamp());
    }
}
