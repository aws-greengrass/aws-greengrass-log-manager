package com.aws.greengrass.logmanager.model;

import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.junit.jupiter.MockitoExtension;

import static com.aws.greengrass.logmanager.model.LogFile.HASH_VALUE_OF_EMPTY_STRING;
import static com.aws.greengrass.logmanager.model.LogFile.bytesNeeded;
import static com.aws.greengrass.util.Digest.calculate;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;


@ExtendWith({MockitoExtension.class, GGExtension.class})
public class LogFileTest {

    @TempDir
    static Path directoryPath;
    private final static int DEFAULT_BYTES_FOR_DIGEST_NUM = 1024;

    public static String getRandomTestStrings(int bytesNeeded) {
        StringBuilder testStrings = new StringBuilder();
        Random rnd = new Random();
        String testChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqestuvwxyz0123456789";
        while (testStrings.length() < bytesNeeded) {
            int charIdx = (int) (rnd.nextFloat() * testChars.length());
            testStrings.append(testChars.charAt(charIdx));
        }
        return testStrings.toString();
    }

    private void writeFiles(File file, byte[] byteArray) throws IOException {
        try (OutputStream fileOutputStream = Files.newOutputStream(file.toPath())) {
            fileOutputStream.write(byteArray);
        }
    }

    @Test
    void GIVEN_empty_file_WHEN_calculate_file_hash_THEN_we_get_null() throws IOException {
        File file = new File(directoryPath.resolve("greengrass_test.log").toUri());
        byte[] bytesArray = getRandomTestStrings(0).getBytes(StandardCharsets.UTF_8);
        writeFiles(file, bytesArray);
        LogFile logFile = LogFile.of(file);
        String fileHash = logFile.hashString();
        assertEquals(fileHash, HASH_VALUE_OF_EMPTY_STRING);
        file.delete();
    }

    @Test
    void GIVEN_log_file_with_less_than_target_lines_WHEN_calculate_file_hash_THEN_we_get_null()
            throws IOException {
        File file = new File(directoryPath.resolve("greengrass_test.log").toUri());
        byte[] bytesArray = getRandomTestStrings(DEFAULT_BYTES_FOR_DIGEST_NUM - 100).getBytes(StandardCharsets.UTF_8);
        writeFiles(file, bytesArray);
        LogFile logFile = LogFile.of(file);
        String fileHash = logFile.hashString();
        assertEquals(fileHash, HASH_VALUE_OF_EMPTY_STRING);
        file.delete();
    }


    @Test
    void GIVEN_log_file_with_equal_to_target_lines_WHEN_calculate_file_hash_THEN_we_get_null()
            throws IOException, NoSuchAlgorithmException {
        File file = new File(directoryPath.resolve("greengrass_test.log").toUri());
        byte[] bytesArray = getRandomTestStrings(DEFAULT_BYTES_FOR_DIGEST_NUM).getBytes(StandardCharsets.UTF_8);
        writeFiles(file, bytesArray);
        LogFile logFile = LogFile.of(file);
        String fileHash = logFile.hashString();
        String msg = new String(bytesArray);
        assertEquals(fileHash, calculate(msg));
        file.delete();
    }

    @Test
    void GIVEN_log_file_with_more_than_target_lines_WHEN_calculate_file_hash_THEN_we_get_hash()
            throws IOException, NoSuchAlgorithmException {
        File file = new File(directoryPath.resolve("greengrass_test.log").toUri());
        byte[] bytesArray = getRandomTestStrings(DEFAULT_BYTES_FOR_DIGEST_NUM + 100).getBytes(StandardCharsets.UTF_8);
        writeFiles(file, bytesArray);
        LogFile logFile = LogFile.of(file);
        String fileHash = logFile.hashString();
        String msg = new String(Arrays.copyOfRange(bytesArray, 0, DEFAULT_BYTES_FOR_DIGEST_NUM));
        assertEquals(fileHash, calculate(msg));
        file.delete();
    }

    @Test
    void GIVEN_log_file_with_less_than_target_lines_but_has_new_line_WHEN_calculate_file_hash_THEN_we_get_hash()
            throws IOException, NoSuchAlgorithmException {
        File file = new File(directoryPath.resolve("greengrass_test.log").toUri());
        StringBuilder testString = new StringBuilder(getRandomTestStrings(DEFAULT_BYTES_FOR_DIGEST_NUM - 100));
        testString.append(System.lineSeparator());
        byte[] bytesArray = testString.toString().getBytes(StandardCharsets.UTF_8);
        writeFiles(file, bytesArray);
        LogFile logFile = LogFile.of(file);
        String fileHash = logFile.hashString();
        assertEquals(fileHash, calculate(testString.toString()));
        file.delete();
    }
}
