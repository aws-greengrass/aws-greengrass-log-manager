/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.logmanager.services;

import com.aws.greengrass.logmanager.exceptions.InvalidLogGroupException;
import com.aws.greengrass.logmanager.model.ComponentLogConfiguration;
import com.aws.greengrass.logmanager.model.LogFile;
import com.aws.greengrass.logmanager.model.LogFileGroup;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.testcommons.testutilities.GGServiceTestUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static com.aws.greengrass.logmanager.util.TestUtils.createLogFileWithSize;
import static com.aws.greengrass.logmanager.util.TestUtils.rotateFilesByRenamingThem;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


@ExtendWith({MockitoExtension.class, GGExtension.class})
class DiskSpaceManagementServiceTest extends GGServiceTestUtil {
    @TempDir
    private Path directoryPath;

    @TempDir
    private Path workDirPath;

    public LogFileGroup arrangeLogGroup(Pattern pattern, long diskSpaceLimitBytes) throws InvalidLogGroupException {
        ComponentLogConfiguration config = ComponentLogConfiguration.builder()
                .directoryPath(directoryPath)
                .fileNameRegex(pattern).name("greengrass_test")
                .diskSpaceLimit(diskSpaceLimitBytes)
                .build();
        Instant instant = Instant.EPOCH;
        return LogFileGroup.create(config, instant, workDirPath);
    }

    public LogFile arrangeLogFile(String name, int byteSize) throws InterruptedException, IOException {
       LogFile file = createLogFileWithSize(directoryPath.resolve(name).toUri(), byteSize);
        // Wait to avoid file's lastModified to be the same if called to fast
        TimeUnit.MILLISECONDS.sleep(100);
       return file;
    }

    @Test
    void GIVEN_log_files_WHEN_max_disk_usage_exceeded_THEN_files_removed() throws IOException, InvalidLogGroupException,
            InterruptedException {
        // Given
        LogFile aLogFile = arrangeLogFile("test.log.1", 2048);
        LogFile activeFile = arrangeLogFile("test.log", 1024);
        LogFileGroup group = arrangeLogGroup(Pattern.compile("test.log\\w*"), 1024L);

        // When
        DiskSpaceManagementService service = new DiskSpaceManagementService();
        Instant lastProcessedFileInstant = Instant.ofEpochMilli(activeFile.lastModified());
        service.freeDiskSpace(group, lastProcessedFileInstant);

        // Then
        assertEquals(1, group.getLogFiles().size());
        assertTrue(Files.notExists(aLogFile.toPath()));
    }

    @Test
    void GIVEN_log_files_WHEN_max_disk_usage_exceeded_THEN_only_unprocessed_files_are_deletable()
            throws IOException, InvalidLogGroupException, InterruptedException {
        // Given
        LogFile cLogFile = arrangeLogFile("test.log.3", 2048);
        LogFile bLogFile = arrangeLogFile("test.log.2", 2048);
        LogFile aLogFile = arrangeLogFile("test.log.1", 2048);
        LogFile activeFile = arrangeLogFile("test.log", 1024);
        LogFileGroup group = arrangeLogGroup(Pattern.compile("test.log\\w*"), 1024L);

        // When
        DiskSpaceManagementService service = new DiskSpaceManagementService();
        Instant lastProcessedFileInstant = Instant.ofEpochMilli(aLogFile.lastModified());
        service.freeDiskSpace(group, lastProcessedFileInstant);

        // Then
        assertEquals(2, group.getLogFiles().size());
        assertTrue(Files.notExists(cLogFile.toPath()));
        assertTrue(Files.notExists(bLogFile.toPath()));
        assertTrue(Files.exists(aLogFile.toPath()));
        assertTrue(Files.exists(activeFile.toPath()));
    }

    @Test
    void GIVEN_log_files_WHEN_max_disk_usage_exceeded_THEN_active_file_is_not_removed()
            throws IOException, InvalidLogGroupException, InterruptedException {
        // Given
        LogFile activeFile = arrangeLogFile("test.log", 1024);
        LogFileGroup group = arrangeLogGroup(Pattern.compile("test.log\\w*"), 0L);

        // When
        DiskSpaceManagementService service = new DiskSpaceManagementService();
        service.freeDiskSpace(group, Instant.now());

        // Then
        assertEquals(1, group.getLogFiles().size());
        assertTrue(Files.exists(activeFile.toPath()));
    }

    @Test
    void GIVEN_file_was_delete_by_externally_WHEN_freeing_space_THEN_it_does_not_fail()
            throws IOException, InvalidLogGroupException, InterruptedException {
        // Given
        LogFile aLogFile = arrangeLogFile("test.log.1", 2048);
        arrangeLogFile("test.log", 1024);
        LogFileGroup group = arrangeLogGroup(Pattern.compile("test.log\\w*"), 0L);

        aLogFile.delete(); // Deleted externally
        assertTrue(Files.notExists(aLogFile.toPath()));

        // When
        DiskSpaceManagementService service = new DiskSpaceManagementService();
        service.freeDiskSpace(group, Instant.now());

        // Then
        assertEquals(1, group.getLogFiles().size());
    }

    @Test
    void GIVEN_file_rotates_WHEN_freeing_space_THEN_it_deletes_the_oldest_files_first()
            throws IOException, InvalidLogGroupException, InterruptedException {
        // Given
        LogFile aLogFile = arrangeLogFile("test.log.1", 2048);
        LogFile prevActive = arrangeLogFile("test.log", 1024);
        LogFileGroup group = arrangeLogGroup(Pattern.compile("test.log\\w*"), 0L);

        // When
        File newActive = rotateFilesByRenamingThem(prevActive, aLogFile); // Files rotate before freed
        DiskSpaceManagementService service = new DiskSpaceManagementService();
        service.freeDiskSpace(group, Instant.now());

        // Then
        assertEquals(directoryPath.resolve("test.log"), newActive.toPath());
        assertEquals(0, group.getLogFiles().size());
        assertTrue(Files.notExists(directoryPath.resolve("test.log.2")));
        assertTrue(Files.notExists(directoryPath.resolve("test.log.1")));
        assertTrue(Files.exists(directoryPath.resolve("test.log")));
    }
}