package com.aws.greengrass.logmanager.model;

import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.junit.jupiter.MockitoExtension;
import static com.aws.greengrass.util.Digest.calculate;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;

import static org.junit.jupiter.api.Assertions.assertEquals;


@ExtendWith({MockitoExtension.class, GGExtension.class})
public class LogFileTest {

    @TempDir
    static Path directoryPath;
    private final static int DEFAULT_LINES_FOR_DIGEST_NUM = 1;
    private static final String HASH_VALUE_OF_EMPTY_STRING = "";

    private void writeFiles(File file, int linesNeeded) throws IOException {
        try (OutputStream fileOutputStream = Files.newOutputStream(file.toPath())) {
            for (int i = 0; i < linesNeeded; i++) {
                fileOutputStream.write("line".getBytes(StandardCharsets.UTF_8));
                fileOutputStream.write(String.valueOf(i + 1).getBytes(StandardCharsets.UTF_8));
                fileOutputStream.write("\n".getBytes(StandardCharsets.UTF_8));
            }
        }
    }

    @Test
    void GIVEN_empty_file_WHEN_calculate_file_hash_THEN_we_get_null() throws IOException {
        File file = new File(directoryPath.resolve("greengrass_test.log").toUri());
        LogFile logFile = LogFile.of(file);
        String fileHash = logFile.hashString();
        assertEquals(fileHash, HASH_VALUE_OF_EMPTY_STRING);
    }

    @Test
    void GIVEN_log_file_with_equal_to_target_lines_WHEN_calculate_file_hash_THEN_we_get_null()
            throws IOException, NoSuchAlgorithmException {
        File file = new File(directoryPath.resolve("greengrass_test.log").toUri());
        writeFiles(file, DEFAULT_LINES_FOR_DIGEST_NUM);
        StringBuilder msg = new StringBuilder();
        for (int i = 0; i < DEFAULT_LINES_FOR_DIGEST_NUM; i++) {
            msg.append("line").append(i + 1);
        }
        LogFile logFile = LogFile.of(file);
        String fileHash = logFile.hashString();
        assertEquals(fileHash, calculate(msg.toString()));
        file.delete();
    }

    @Test
    void GIVEN_log_file_with_more_than_target_lines_WHEN_calculate_file_hash_THEN_we_get_hash()
            throws IOException, NoSuchAlgorithmException {
        File file = new File(directoryPath.resolve("greengrass_test.log").toUri());
        writeFiles(file, DEFAULT_LINES_FOR_DIGEST_NUM + 1);
        StringBuilder msg = new StringBuilder();
        for (int i = 0; i < DEFAULT_LINES_FOR_DIGEST_NUM; i++) {
            msg.append("line").append(i + 1);
        }
        LogFile logFile = LogFile.of(file);
        String fileHash = logFile.hashString();
        assertEquals(fileHash, calculate(msg.toString()));
        file.delete();
    }
}
