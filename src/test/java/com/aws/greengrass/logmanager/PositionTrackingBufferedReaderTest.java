/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.logmanager;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PositionTrackingBufferedReaderTest {

    @Test
    void GIVEN_japanese_characters_WHEN_readLine_THEN_position_counts_utf8_bytes() throws Exception {
        // Japanese characters are 3 bytes each in UTF-8
        String content = "在室判定\n";
        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);

        try (PositionTrackingBufferedReader reader = new PositionTrackingBufferedReader(
                new InputStreamReader(new ByteArrayInputStream(bytes), StandardCharsets.UTF_8))) {

            String line = reader.readLine();
            assertEquals("在室判定", line);
            // 4 Japanese chars × 3 bytes each = 12 bytes + 1 byte for '\n' = 13 bytes
            assertEquals(bytes.length, reader.position());
        }
    }

    @Test
    void GIVEN_multiple_lines_with_multibyte_WHEN_resume_from_position_THEN_no_overlap() throws Exception {
        // Simulates the actual LogManager use case:
        // 1. Read first part of file, note position
        // 2. Seek to that position in a new reader
        // 3. Verify no duplicate/overlapping content
        String fullContent = "2026-06-15 16:29:49 INFO: 在室判定用パラメータ: job=001\n"
                + "2026-06-15 16:29:51 INFO: 在室判定用パラメータ: job=002\n"
                + "2026-06-15 16:29:53 INFO: 在室判定用パラメータ: job=003\n";
        byte[] fullBytes = fullContent.getBytes(StandardCharsets.UTF_8);

        // Phase 1: Read first line, save position
        long savedPosition;
        try (PositionTrackingBufferedReader reader = new PositionTrackingBufferedReader(
                new InputStreamReader(new ByteArrayInputStream(fullBytes), StandardCharsets.UTF_8))) {

            reader.readLine(); // Read first line
            savedPosition = reader.position();
        }

        // Phase 2: Resume from saved position (simulates next upload cycle)
        byte[] remainingBytes = new byte[fullBytes.length - (int) savedPosition];
        System.arraycopy(fullBytes, (int) savedPosition, remainingBytes, 0, remainingBytes.length);

        try (PositionTrackingBufferedReader reader = new PositionTrackingBufferedReader(
                new InputStreamReader(new ByteArrayInputStream(remainingBytes), StandardCharsets.UTF_8))) {

            reader.setInitialPosition(savedPosition);

            String line2 = reader.readLine();
            // Should start cleanly with the next line — no fragment or overlap
            assertEquals("2026-06-15 16:29:51 INFO: 在室判定用パラメータ: job=002", line2);

            String line3 = reader.readLine();
            assertEquals("2026-06-15 16:29:53 INFO: 在室判定用パラメータ: job=003", line3);

            assertEquals(fullBytes.length, reader.position());
        }
    }
}
