package com.aws.greengrass.logmanager.model;

import com.aws.greengrass.logmanager.exceptions.InvalidLogGroupException;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import static com.aws.greengrass.logmanager.util.TestUtils.createFileWithContent;
import static com.aws.greengrass.logmanager.util.TestUtils.givenAStringOfSize;
import static com.aws.greengrass.logmanager.util.TestUtils.readFileContent;
import static com.aws.greengrass.logmanager.util.TestUtils.rotateFilesByRenamingThem;
import static com.aws.greengrass.logmanager.util.TestUtils.writeFile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith({MockitoExtension.class, GGExtension.class})
public class LogFileGroupTest {
    @TempDir
    static Path directoryPath;
    private Path linksPath;

    @BeforeEach
    void setup() {
        linksPath = directoryPath.resolve("hardlinks");
    }

    @AfterEach
    void tearDown() throws IOException {
        FileUtils.deleteDirectory(linksPath.toFile());
    }

    private boolean matchFilePath(Path expectedPath, LogFileV2 logFile) {
        try {
            return logFile.toPath().equals(expectedPath);
        } catch (IOException e) {
            return false;
        }
    }

    @Test
    void GIVEN_log_files_THEN_find_the_active_file() throws IOException, InterruptedException,
            InvalidLogGroupException {
        LogFile file = new LogFile(directoryPath.resolve("greengrass_test.log_1").toUri());
        byte[] bytesArray = givenAStringOfSize(1024).getBytes(StandardCharsets.UTF_8);
        writeFile(file, bytesArray);

        //Intentionally sleep lazily here to differ the creation time of two files.
        TimeUnit.SECONDS.sleep(1);

        LogFile file2 = new LogFile(directoryPath.resolve("greengrass_test.log_2").toUri());
        byte[] bytesArray2 = givenAStringOfSize(1024).getBytes(StandardCharsets.UTF_8);
        writeFile(file2, bytesArray2);

        Pattern pattern = Pattern.compile("^greengrass_test.log\\w*$");
        Instant instant = Instant.EPOCH;
        LogFileGroup logFileGroup = LogFileGroup.create(pattern, file.getParentFile().toURI(), instant, linksPath);

        assertEquals(2, logFileGroup.getLogFiles().size());
        assertFalse(logFileGroup.isActiveFile(file));
        assertTrue(logFileGroup.isActiveFile(file2));
    }

    @Test
    void GIVEN_logManagerGetsFiles_WHEN_fileRotateNotDeletingLastOneAfterRetrieved_THEN_itReturnsTheCorrectFiles()
            throws IOException, InvalidLogGroupException {
        // Given

        File rotatedFile1 = createFileWithContent(
                directoryPath.resolve("test.log.1").toUri(), "rotatedFile2");
        long rotatedFileLastModified = rotatedFile1.lastModified();
        File activeFile = createFileWithContent(
                directoryPath.resolve("test.log").toUri(), "rotatedFile1");
        long activeFileLastModified = activeFile.lastModified();

        Pattern filePattern = Pattern.compile("test.log\\w*", Pattern.CASE_INSENSITIVE);
        Instant lastUploadedFileInstant = Instant.EPOCH;
        LogFileGroup logGroup = LogFileGroup.create(filePattern, directoryPath.toUri(), lastUploadedFileInstant,
                linksPath);

        // When

        File[] directoryFiles = directoryPath.toFile().listFiles();
        File newActiveFile = rotateFilesByRenamingThem(new File[]{activeFile, rotatedFile1}, false);
        writeFile(newActiveFile, "activeFile".getBytes(StandardCharsets.UTF_8));
        List<LogFileV2> logFiles = logGroup.getLogFiles(directoryFiles);

        assertEquals(3, logFiles.size());

        Path[] expectedPaths = {
                directoryPath.resolve("test.log.2"),
                directoryPath.resolve("test.log.1"),
                directoryPath.resolve("test.log")
        };
        long[] expectedLastModified = {
                rotatedFileLastModified,
                activeFileLastModified,
                newActiveFile.lastModified()
        };
        String[] expectedContents = {
                "rotatedFile2",
                "rotatedFile1",
                "activeFile"
        };

        for (int i = 0; i < expectedPaths.length; i++) {
            AtomicInteger pointer = new AtomicInteger(i);
            Optional<LogFileV2> expectedFile = logFiles.stream()
                    .filter(file -> matchFilePath(expectedPaths[pointer.get()], file)).findFirst();

            assertTrue(expectedFile.isPresent());
            assertEquals(expectedLastModified[i], expectedFile.get().lastModified(),
                    "expected ts for file " + expectedFile.get().toPath() + " does not match");

            String content = readFileContent(expectedFile.get());
            assertEquals(expectedContents[i], content);
        }
    }

    @Test
    void GIVEN_logManagerGetsFiles_WHEN_fileRotateDeletingLastOneAfterRetrieved_THEN_itReturnsTheCorrectFiles()
            throws IOException, InvalidLogGroupException {
        // Given

        File rotatedFile1 = createFileWithContent(
                directoryPath.resolve("test.log.1").toUri(), "rotatedFile2");
        File activeFile = createFileWithContent(
                directoryPath.resolve("test.log").toUri(), "rotatedFile1");
        long oldActiveLastModified = activeFile.lastModified();

        Pattern filePattern = Pattern.compile("test.log\\w*", Pattern.CASE_INSENSITIVE);
        Instant lastUploadedFileInstant = Instant.EPOCH;
        LogFileGroup logGroup = LogFileGroup.create(filePattern, directoryPath.toUri(), lastUploadedFileInstant,
                linksPath);

        // When

        File[] directoryFiles = directoryPath.toFile().listFiles();
        // Rotate and delete the last file - only the contents of the file change not the names
        File newActiveFile = rotateFilesByRenamingThem(new File[]{activeFile, rotatedFile1}, true);
        writeFile(newActiveFile, "activeFile".getBytes(StandardCharsets.UTF_8));
        List<LogFileV2> logFiles = logGroup.getLogFiles(directoryFiles);

        // Assert

        assertEquals(2, logFiles.size());

        Path[] expectedPaths = {
                directoryPath.resolve("test.log.1"),
                directoryPath.resolve("test.log")
        };
        long[] expectedLastModified = {
                oldActiveLastModified,
                newActiveFile.lastModified()
        };
        String[] expectedContents = {
                "rotatedFile1",
                "activeFile"
        };

        for (int i = 0; i < expectedPaths.length; i++) {
            AtomicInteger pointer = new AtomicInteger(i);
            Optional<LogFileV2> expectedFile = logFiles.stream()
                    .filter(file -> matchFilePath(expectedPaths[pointer.get()], file)).findFirst();

            assertTrue(expectedFile.isPresent());
            assertEquals(expectedLastModified[i], expectedFile.get().lastModified(),
                    "expected ts for file " + expectedFile.get().toPath() + " does not match");

            String content = readFileContent(expectedFile.get());
            assertEquals(expectedContents[i], content);
        }
    }
}
