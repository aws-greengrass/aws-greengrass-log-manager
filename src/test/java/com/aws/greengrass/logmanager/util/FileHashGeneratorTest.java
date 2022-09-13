package com.aws.greengrass.logmanager.util;

import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith({MockitoExtension.class, GGExtension.class})
public class FileHashGeneratorTest {

    @TempDir
    static Path directoryPath;

    @Test
    void GIVEN_empty_file_WHEN_calculate_file_hash_THEN_we_get_null() throws IOException {
        File file = new File(directoryPath.resolve("greengrass_test.log").toUri());
        try {
            Optional fileHash = FileHashGenerator.calculateHashForLogFile(file,1);
            assertFalse(fileHash.isPresent());
        }
        finally {
            assertFalse(file.delete());
        }
    }

    @Test
    void GIVEN_log_file_with_less_than_target_lines_WHEN_calculate_file_hash_THEN_we_get_null() throws IOException {
        int DEFAULT_LINES_FOR_DIGEST_NUM = 2;
        File file = new File(directoryPath.resolve("greengrass_test.log").toUri());
        try (OutputStream fileOutputStream = Files.newOutputStream(file.toPath())) {
            for (int i = 0; i < DEFAULT_LINES_FOR_DIGEST_NUM - 1; i++) {
                fileOutputStream.write("line".getBytes(StandardCharsets.UTF_8));
                fileOutputStream.write(String.valueOf(i + 1).getBytes(StandardCharsets.UTF_8));
                fileOutputStream.write("\n".getBytes(StandardCharsets.UTF_8));
            }
        }
        try {
            Optional fileHash = FileHashGenerator.calculateHashForLogFile(file, DEFAULT_LINES_FOR_DIGEST_NUM);
            assertFalse(fileHash.isPresent());
        }
        finally {
            assertTrue(file.delete());
        }
    }

    @Test
    void GIVEN_log_file_with_equal_to_target_lines_WHEN_calculate_file_hash_THEN_we_get_null() throws IOException {
        int DEFAULT_LINES_FOR_DIGEST_NUM = 1;
        File file = new File(directoryPath.resolve("greengrass_test.log").toUri());
        try (OutputStream fileOutputStream = Files.newOutputStream(file.toPath())) {
            for (int i = 0; i < DEFAULT_LINES_FOR_DIGEST_NUM; i++) {
                fileOutputStream.write("line".getBytes(StandardCharsets.UTF_8));
                fileOutputStream.write(String.valueOf(i + 1).getBytes(StandardCharsets.UTF_8));
                fileOutputStream.write("\n".getBytes(StandardCharsets.UTF_8));
            }
        }
        try {
            Optional fileHash = FileHashGenerator.calculateHashForLogFile(file, DEFAULT_LINES_FOR_DIGEST_NUM);
            assertFalse(fileHash.isPresent());
        }
        finally {
            assertTrue(file.delete());
        }
    }

    @Test
    void GIVEN_log_file_with_more_than_target_lines_WHEN_calculate_file_hash_THEN_we_get_hash() throws IOException {
        int DEFAULT_LINES_FOR_DIGEST_NUM = 1;
        File file = new File(directoryPath.resolve("greengrass_test.log").toUri());
        try (OutputStream fileOutputStream = Files.newOutputStream(file.toPath())) {
            for (int i = 0; i < DEFAULT_LINES_FOR_DIGEST_NUM + 1; i++) {
                fileOutputStream.write("line".getBytes(StandardCharsets.UTF_8));
                fileOutputStream.write(String.valueOf(i + 1).getBytes(StandardCharsets.UTF_8));
                fileOutputStream.write("\n".getBytes(StandardCharsets.UTF_8));
            }
        }
        try {
            Optional fileHash = FileHashGenerator.calculateHashForLogFile(file, DEFAULT_LINES_FOR_DIGEST_NUM);
            assertNotNull(fileHash);
        }
        finally {
            assertTrue(file.delete());
        }
    }
}
