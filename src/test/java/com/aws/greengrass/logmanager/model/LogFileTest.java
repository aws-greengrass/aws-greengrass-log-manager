package com.aws.greengrass.logmanager.model;

import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.junit.jupiter.MockitoExtension;

import static com.aws.greengrass.logmanager.model.LogFile.HASH_VALUE_OF_EMPTY_STRING;
import static com.aws.greengrass.logmanager.util.TestUtils.givenAStringOfSize;
import static com.aws.greengrass.logmanager.util.TestUtils.writeFile;
import static com.aws.greengrass.util.Digest.calculate;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.NoSuchAlgorithmException;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SuppressWarnings("PMD.CloseResource")
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
        SeekableByteChannel fileByteChannel =
                Files.newByteChannel(file.toPath(), StandardOpenOption.READ);
        String fileHash = file.hashStringWithChannel(fileByteChannel);
        assertEquals(fileHash, HASH_VALUE_OF_EMPTY_STRING);
        String fileHash1 = file.hashString();
        assertEquals(fileHash1, HASH_VALUE_OF_EMPTY_STRING);
        file.delete();
        fileByteChannel.close();
    }

    @Test
    void GIVEN_log_file_with_less_than_target_lines_in_one_line_WHEN_calculate_file_hash_THEN_we_get_null()
            throws IOException {
        LogFile file = new LogFile(directoryPath.resolve("greengrass_test.log").toUri());
        byte[] bytesArray = givenAStringOfSize(DEFAULT_BYTES_FOR_DIGEST_NUM - 100).getBytes(StandardCharsets.UTF_8);
        writeFile(file, bytesArray);
        SeekableByteChannel fileByteChannel =
                Files.newByteChannel(file.toPath(), StandardOpenOption.READ);
        String fileHash = file.hashStringWithChannel(fileByteChannel);
        assertEquals(fileHash, HASH_VALUE_OF_EMPTY_STRING);
        String fileHash1 = file.hashString();
        assertEquals(fileHash1, HASH_VALUE_OF_EMPTY_STRING);
        file.delete();
        fileByteChannel.close();
    }


    @Test
    void GIVEN_log_file_with_equal_to_target_lines_in_one_line_WHEN_calculate_file_hash_THEN_we_get_null()
            throws IOException, NoSuchAlgorithmException {
        LogFile file = new LogFile(directoryPath.resolve("greengrass_test.log").toUri());
        byte[] bytesArray = givenAStringOfSize(DEFAULT_BYTES_FOR_DIGEST_NUM).getBytes(StandardCharsets.UTF_8);
        writeFile(file, bytesArray);
        SeekableByteChannel fileByteChannel =
                Files.newByteChannel(file.toPath(), StandardOpenOption.READ);
        String fileHash = file.hashStringWithChannel(fileByteChannel);
        String msg = new String(bytesArray);
        assertEquals(fileHash, calculate(msg));
        String fileHash1 = file.hashString();
        assertEquals(fileHash1, calculate(msg));
        file.delete();
        fileByteChannel.close();
    }

    @Test
    void GIVEN_log_file_with_more_than_target_lines_in_one_line_WHEN_calculate_file_hash_THEN_we_get_hash()
            throws IOException, NoSuchAlgorithmException {
        LogFile file = new LogFile(directoryPath.resolve("greengrass_test.log").toUri());
        byte[] bytesArray = givenAStringOfSize(DEFAULT_BYTES_FOR_DIGEST_NUM + 100).getBytes(StandardCharsets.UTF_8);
        writeFile(file, bytesArray);
        SeekableByteChannel fileByteChannel =
                Files.newByteChannel(file.toPath(), StandardOpenOption.READ);
        String fileHash = file.hashStringWithChannel(fileByteChannel);
        String msg = new String(bytesArray, 0, DEFAULT_BYTES_FOR_DIGEST_NUM);
        assertEquals(fileHash, calculate(msg));
        String fileHash1 = file.hashString();
        assertEquals(fileHash1, calculate(msg));
        file.delete();
        fileByteChannel.close();
    }

    @Test
    void GIVEN_log_file_with_less_than_target_lines_but_two_lines_WHEN_calculate_file_hash_THEN_we_get_hash()
            throws IOException, NoSuchAlgorithmException {
        LogFile file = new LogFile(directoryPath.resolve("greengrass_test.log").toUri());
        // create a string as an entire line
        StringBuilder builder = new StringBuilder();
        builder.append(givenAStringOfSize(DEFAULT_BYTES_FOR_DIGEST_NUM - 100)).append(System.lineSeparator());
        writeFile(file, builder.toString().getBytes(StandardCharsets.UTF_8));
        SeekableByteChannel fileByteChannel =
                Files.newByteChannel(file.toPath(), StandardOpenOption.READ);
        String fileHash = file.hashStringWithChannel(fileByteChannel);
        assertEquals(fileHash, calculate(builder.toString()));
        String fileHash1 = file.hashString();
        assertEquals(fileHash1, calculate(builder.toString()));
        file.delete();
        fileByteChannel.close();
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
        SeekableByteChannel fileByteChannel =
                Files.newByteChannel(file.toPath(), StandardOpenOption.READ);
        String fileHash = file.hashStringWithChannel(fileByteChannel);
        String fileHash1 = file.hashString();
        assertEquals(fileHash, expectedHash);
        assertEquals(fileHash1, expectedHash);
        file.delete();
        fileByteChannel.close();
    }

}
