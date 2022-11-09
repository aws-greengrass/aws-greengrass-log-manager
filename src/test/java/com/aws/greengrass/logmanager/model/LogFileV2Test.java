package com.aws.greengrass.logmanager.model;

import com.aws.greengrass.util.Utils;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import static com.aws.greengrass.logmanager.util.TestUtils.createFileWithContent;
import static com.aws.greengrass.logmanager.util.TestUtils.rotateFilesByRenamingThem;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;


public class LogFileV2Test {
    @TempDir
    static Path directoryPath;
    private Path linksPath;

    @BeforeEach
    void setup() throws IOException {
        linksPath = directoryPath.resolve("work");
        Utils.createPaths(linksPath);
    }

    @AfterEach
    void tearDown() throws IOException {
        FileUtils.deleteDirectory(linksPath.toFile());
    }

    @Test
    void GIVEN_filesInDirectory_WHEN_rotated_THEN_weCanGetTheSourceFilePath() throws IOException {
        // Given

        File rotatedFile1 =  createFileWithContent(
                directoryPath.resolve("test.log.1").toUri(), "rotatedFile2");
        File activeFile = createFileWithContent(
                directoryPath.resolve("test.log").toUri(), "rotatedFile1");

        Path hardLinkPath = linksPath.resolve(activeFile.getName());
        LogFileV2 activeLogFile = LogFileV2.fromFile(activeFile, hardLinkPath);


        assertEquals(activeLogFile.toPath(), directoryPath.resolve("test.log"));

        // When

        rotateFilesByRenamingThem(new File[]{activeFile, rotatedFile1}, true);

        // Then
        assertEquals(activeLogFile.toPath(), directoryPath.resolve("test.log.1"));
    }


    @Test
    void GIVEN_filesInDirectory_WHEN_whenFileRotated_THEN_itTracksTheCorrectLastModified() throws IOException {
        // Given

        File rotatedFile1 =  createFileWithContent(
                directoryPath.resolve("test.log.1").toUri(), "rotatedFile2");
        long rotatedFile1LastModified = rotatedFile1.lastModified();
        File activeFile = createFileWithContent(
                directoryPath.resolve("test.log").toUri(), "rotatedFile1");
        long activeFileLastModified = activeFile.lastModified();

        Path hardLinkPath = linksPath.resolve(activeFile.getName());
        LogFileV2 activeLogFile = LogFileV2.fromFile(activeFile, hardLinkPath);

        assertEquals(activeLogFile.lastModified(), activeFileLastModified);

        // When

        rotateFilesByRenamingThem(new File[]{activeFile, rotatedFile1}, true);

        // Then
        assertEquals(activeLogFile.lastModified(), activeFileLastModified);
        assertNotEquals(activeLogFile.lastModified(), rotatedFile1LastModified);
    }


    @Test
    void GIVEN_filesInDirectory_WHEN_whenFileRotated_THEN_computedHashIsCorrect() throws IOException {
        // Given

        File rotatedFile =  createFileWithContent(
                directoryPath.resolve("test.log.1").toUri(), "rotatedFile2");
        long rotatedFileLastModified = rotatedFile.lastModified();
        File activeFile = createFileWithContent(
                directoryPath.resolve("test.log").toUri(), "rotatedFile1");
        long activeFileLastModified = activeFile.lastModified();

        // Create the log files that to track the Files
        LogFileV2 rotatedLogFile = LogFileV2.fromFile(rotatedFile, linksPath.resolve(rotatedFile.getName()));
        String rotatedFileHashBeforeRotation = rotatedLogFile.hashString();
        LogFileV2 activeLogFile = LogFileV2.fromFile(activeFile, linksPath.resolve(activeFile.getName()));
        String activeHashBeforeRotation = activeLogFile.hashString();

        assertEquals(rotatedFile.lastModified(), rotatedFileLastModified);
        assertEquals(activeLogFile.lastModified(), activeFileLastModified);
        assertEquals(activeHashBeforeRotation, activeLogFile.hashString());
        assertEquals(rotatedFileHashBeforeRotation, rotatedLogFile.hashString());

        // When

        rotateFilesByRenamingThem(new File[]{activeFile, rotatedFile}, true);

        // Then
        assertEquals(activeFileLastModified, activeLogFile.lastModified());
        assertEquals(activeHashBeforeRotation, activeLogFile.hashString());

        // Expect the previous file to throw an exception because it got deleted
        assertFalse(rotatedLogFile.isValid());
        assertEquals(rotatedFileLastModified, rotatedLogFile.lastModified());
        assertEquals("", rotatedLogFile.hashString());

    }

}
