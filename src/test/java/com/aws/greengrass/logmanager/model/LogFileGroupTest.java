package com.aws.greengrass.logmanager.model;

import com.aws.greengrass.logmanager.exceptions.InvalidLogGroupException;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.aws.greengrass.logmanager.util.TestUtils.createLogFileWithSize;
import static com.aws.greengrass.logmanager.util.TestUtils.givenAStringOfSize;
import static com.aws.greengrass.logmanager.util.TestUtils.rotateFilesByRenamingThem;
import static com.aws.greengrass.logmanager.util.TestUtils.writeFile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith({MockitoExtension.class, GGExtension.class})
public class LogFileGroupTest {
    @TempDir
    static Path directoryPath;

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
        LogFileGroup logFileGroup = LogFileGroup.create(pattern, file.getParentFile().toURI(), instant);

        assertEquals(2, logFileGroup.getLogFiles().size());
        assertFalse(logFileGroup.isActiveFile(file));
        assertTrue(logFileGroup.isActiveFile(file2));
    }

    @Test
    void GIVEN_logManagerGetsFiles_WHEN_fileRotateAfterRetrieved_THEN_itReturnsTheCorrectFiles() throws IOException {
        // Given

        File rotatedFile1 = createLogFileWithSize(directoryPath.resolve("test.log.1").toUri(), 1024 * 5);
        File activeFile = createLogFileWithSize(directoryPath.resolve("test.log").toUri(), 1024);

        Pattern filePattern = Pattern.compile("test.log\\w*", Pattern.CASE_INSENSITIVE);
        Instant lastUploadedFileInstant = Instant.EPOCH;
        LogFileGroup logGroup = new LogFileGroup(
                new ArrayList<>(), filePattern, directoryPath.toUri(), lastUploadedFileInstant);

        // When

        File[] directoryFiles = directoryPath.toFile().listFiles();
        rotateFilesByRenamingThem(activeFile, rotatedFile1);
        List<LogFile> logFiles = logGroup.getLogFiles(directoryFiles);

        // Assert

        directoryFiles = directoryPath.toFile().listFiles();
        assertNotNull(directoryFiles);
        assertEquals(directoryFiles.length, logFiles.size());

        Path[] expectedPaths = {
            directoryPath.resolve("test.log.2"),
            directoryPath.resolve("test.log.1"),
            directoryPath.resolve("test.log")
        };

        List<Path> filePaths = logFiles.stream().map(File::toPath).collect(Collectors.toList());
        for (int i = 0; i < expectedPaths.length; i++) {
            AtomicInteger pointer = new AtomicInteger(i);
            Optional<Path> expectedPath = filePaths.stream()
                    .filter(path -> path.equals(expectedPaths[pointer.get()])).findFirst();
            assertTrue(expectedPath.isPresent());
        }
    }
}
