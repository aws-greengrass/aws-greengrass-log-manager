package com.aws.greengrass.logmanager.model;

import com.aws.greengrass.logmanager.util.TestUtils;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.util.Utils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.junit.jupiter.MockitoExtension;

import static com.aws.greengrass.logmanager.model.LogFile.HASH_VALUE_OF_EMPTY_STRING;
import static com.aws.greengrass.logmanager.util.TestUtils.givenAStringOfSize;
import static com.aws.greengrass.logmanager.util.TestUtils.rotateFilesByRenamingThem;
import static com.aws.greengrass.logmanager.util.TestUtils.writeFile;
import static com.aws.greengrass.util.Digest.calculate;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;


@ExtendWith({MockitoExtension.class, GGExtension.class})
public class LogFileTest {

    @TempDir
    static Path directoryPath;
    private final static int DEFAULT_BYTES_FOR_DIGEST_NUM = 1024;

    @Test
    void GIVEN_empty_file_WHEN_calculate_file_hash_THEN_we_get_null() throws IOException {
        LogFile file = new LogFile(directoryPath.resolve("greengrass_test.log").toUri());
        byte[] bytesArray = givenAStringOfSize(0).getBytes(StandardCharsets.UTF_8);
        writeFile(file, bytesArray);
        String fileHash = file.hashString();
        assertEquals(fileHash, HASH_VALUE_OF_EMPTY_STRING);
    }

    @Test
    void GIVEN_log_file_with_less_than_target_lines_in_one_line_WHEN_calculate_file_hash_THEN_we_get_null()
            throws IOException {
        LogFile file = new LogFile(directoryPath.resolve("greengrass_test.log").toUri());
        byte[] bytesArray = givenAStringOfSize(DEFAULT_BYTES_FOR_DIGEST_NUM - 100).getBytes(StandardCharsets.UTF_8);
        writeFile(file, bytesArray);
        String fileHash = file.hashString();
        assertEquals(fileHash, HASH_VALUE_OF_EMPTY_STRING);
    }


    @Test
    void GIVEN_log_file_with_equal_to_target_lines_in_one_line_WHEN_calculate_file_hash_THEN_we_get_null()
            throws IOException, NoSuchAlgorithmException {
        LogFile file = new LogFile(directoryPath.resolve("greengrass_test.log").toUri());
        byte[] bytesArray = givenAStringOfSize(DEFAULT_BYTES_FOR_DIGEST_NUM).getBytes(StandardCharsets.UTF_8);
        writeFile(file, bytesArray);
        String fileHash = file.hashString();
        String msg = new String(bytesArray);
        assertEquals(fileHash, calculate(msg));
    }

    @Test
    void GIVEN_log_file_with_more_than_target_lines_in_one_line_WHEN_calculate_file_hash_THEN_we_get_hash()
            throws IOException, NoSuchAlgorithmException {
        LogFile file = new LogFile(directoryPath.resolve("greengrass_test.log").toUri());
        byte[] bytesArray = givenAStringOfSize(DEFAULT_BYTES_FOR_DIGEST_NUM + 100).getBytes(StandardCharsets.UTF_8);
        writeFile(file, bytesArray);
        String fileHash = file.hashString();
        String msg = new String(bytesArray, 0, DEFAULT_BYTES_FOR_DIGEST_NUM);
        assertEquals(fileHash, calculate(msg));
    }

    @Test
    void GIVEN_log_file_with_less_than_target_lines_but_two_lines_WHEN_calculate_file_hash_THEN_we_get_hash()
            throws IOException, NoSuchAlgorithmException {
        LogFile file = new LogFile(directoryPath.resolve("greengrass_test.log").toUri());
        // create a string as an entire line
        StringBuilder builder = new StringBuilder();
        builder.append(givenAStringOfSize(DEFAULT_BYTES_FOR_DIGEST_NUM - 100)).append(System.lineSeparator());
        writeFile(file, builder.toString().getBytes(StandardCharsets.UTF_8));
        String fileHash = file.hashString();
        assertEquals(fileHash, calculate(builder.toString()));
    }

    @Test
    void GIVEN_log_file_with_more_than_target_lines_but_two_lines_WHEN_calculate_file_hash_THEN_we_get_hash()
            throws IOException, NoSuchAlgorithmException {
        LogFile file = new LogFile(directoryPath.resolve("greengrass_test.log").toUri());
        // create a string as an entire line
        StringBuilder builder = new StringBuilder();
        builder.append(givenAStringOfSize(DEFAULT_BYTES_FOR_DIGEST_NUM - 100)).append(System.lineSeparator());
        String expectedHash = calculate(builder.toString());
        builder.append(givenAStringOfSize(100));
        writeFile(file, builder.toString().getBytes(StandardCharsets.UTF_8));
        String fileHash = file.hashString();
        assertEquals(fileHash, expectedHash);
    }

    @Test
    void GIVEN_logFileTrackingHardlink_WHEN_trackedFileRotates_THEN_itDeletesTheOriginalFile() throws IOException {
        // Given

        Path testPath = directoryPath.resolve("itDeletesTheOriginalFile");
        Utils.createPaths(testPath.resolve("hardlinks"));

        File file = TestUtils.createFileWithContent(
                testPath.resolve("test.log"), "rotated");
        LogFile logFile = LogFile.of(file, testPath.resolve("hardlinks"));

        // When

        rotateFilesByRenamingThem(new File[]{file});
        logFile.delete();

        // Assert

        File[] directoryFiles = Arrays.stream(testPath.toFile().listFiles())
                        .filter(File::isFile)
                        .toArray(File[]::new);
        assertEquals(directoryFiles.length, 1);

        File remainingFile = directoryFiles[0];
        assertEquals(remainingFile.toPath(), file.toPath());
    }

    @Test
    void GIVEN_logFileTrackingRegularFile_WHEN_trackedFileRotates_THEN_itDeletesTheWrongFile() throws IOException {
        // Given

        Path testPath = directoryPath.resolve("itDeletesTheWrongFile");
        Utils.createPaths(testPath);

        File file = TestUtils.createFileWithContent(
                testPath.resolve("test.log"), "rotated");
        LogFile logFile = LogFile.of(file);

        // When

        rotateFilesByRenamingThem(new File[]{file});
        logFile.delete();

        // Assert

        File[] directoryFiles = Arrays.stream(testPath.toFile().listFiles())
                .filter(File::isFile)
                .toArray(File[]::new);
        assertEquals(directoryFiles.length, 1);

        File remainingFile = directoryFiles[0];
        assertEquals(remainingFile.toPath(), testPath.resolve("test.log.1"));
    }
}
