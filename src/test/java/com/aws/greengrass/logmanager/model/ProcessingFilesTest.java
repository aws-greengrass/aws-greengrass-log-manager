package com.aws.greengrass.logmanager.model;

import com.aws.greengrass.logmanager.LogManagerService;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.mockito.ScopedMock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;


@ExtendWith({MockitoExtension.class, GGExtension.class})
public class ProcessingFilesTest {
    private Optional<MockedStatic<Clock>> clockMock;

    @BeforeEach
    void setup() {
        clockMock = Optional.empty();
    }

    @AfterEach
    void cleanup() {
        this.clockMock.ifPresent(ScopedMock::close);
    }

    @SuppressWarnings("PMD.CloseResource")
    private void mockInstant(long expected) {
        this.clockMock.ifPresent(ScopedMock::close);
        Clock spyClock = spy(Clock.class);
        MockedStatic<Clock> clockMock;
        clockMock = mockStatic(Clock.class);
        clockMock.when(Clock::systemUTC).thenReturn(spyClock);
        when(spyClock.instant()).thenReturn(Instant.ofEpochMilli(expected));
        this.clockMock = Optional.of(clockMock);
    }

    @Test
    void GIVEN_filledProcessingFiles_WHEN_toMap_returnsMapRepresentation() {
        // Given
        ProcessingFiles processingFiles = new ProcessingFiles(5);
        LogManagerService.CurrentProcessingFileInformation fileInformationOne =
                LogManagerService.CurrentProcessingFileInformation.builder()
                .fileName("test.log")
                .fileHash("kj35435")
                .startPosition(1000)
                .lastModifiedTime(Instant.now().toEpochMilli())
                .build();
        processingFiles.put("kj35435", fileInformationOne);
        LogManagerService.CurrentProcessingFileInformation fileInformationTwo =
        LogManagerService.CurrentProcessingFileInformation.builder()
                .fileName("test_2023.log")
                .fileHash("54321")
                .startPosition(1000)
                .lastModifiedTime(Instant.now().toEpochMilli())
                .build();
        processingFiles.put("54321", fileInformationTwo);


        // Then
        Map<String, Object> expected = new HashMap<String, Object>(){{
            put("kj35435", fileInformationOne.convertToMapOfObjects());
            put("54321", fileInformationTwo.convertToMapOfObjects());
        }};
        assertEquals(expected, processingFiles.toMap());
    }

    @Test
    void GIVEN_filledProcessingFiles_WHEN_getMostRecentlyUsed_THEN_mostRecentValueReturned() {
        // Given
        ProcessingFiles processingFiles = new ProcessingFiles(60);

        processingFiles.put("12345", LogManagerService.CurrentProcessingFileInformation.builder()
                .fileName("test.log")
                .fileHash("12345")
                .startPosition(1000)
                .lastModifiedTime(Instant.now().toEpochMilli())
                .build());
        processingFiles.put("54321", LogManagerService.CurrentProcessingFileInformation.builder()
                .fileName("test_2023.log")
                .fileHash("54321")
                .startPosition(1000)
                .lastModifiedTime(Instant.now().toEpochMilli())
                .build());

        // Then
        assertEquals(processingFiles.getMostRecentlyUsed().getFileHash(), "54321");
    }

    @Test
    void GIVEN_processingFilesWithValue_WHEN_newValueAdded_THEN_stateEntriesGetCleared() {

        // Hold entries for a max of 1 day, unless accessed before that time
        ProcessingFiles processingFiles = new ProcessingFiles( 60 * 60 * 24);
        Instant now = Instant.now();

        // 2 days ago
        mockInstant(now.minusSeconds(60 * 60 * 24 * 2).toEpochMilli());

        processingFiles.put("12345", LogManagerService.CurrentProcessingFileInformation.builder()
                .fileName("test.log")
                .fileHash("12345")
                .startPosition(1000)
                .lastModifiedTime(Instant.now().toEpochMilli())
                .build());

        // Reset time back to now
        mockInstant(now.toEpochMilli());

        processingFiles.put("54321", LogManagerService.CurrentProcessingFileInformation.builder()
                .fileName("test_2023.log")
                .fileHash("54321")
                .startPosition(1000)
                .lastModifiedTime(now.minusSeconds(60 * 60 * 24 * 2).toEpochMilli())
                .build());

        assertEquals(processingFiles.size(), 1);
    }

    @Test
    void GIVEN_two_files_sharing_a_first_line_WHEN_claimed_THEN_each_gets_the_position_saved_for_its_bytes(
            @TempDir Path directory) throws IOException {
        String banner = "=== service starting ===\n";
        LogFile older = logFile(directory.resolve("test.log.1"), banner + "older one\nolder two\nolder three\n");
        LogFile newer = logFile(directory.resolve("test.log"), banner + "newer one, a longer line\n");
        long olderPosition = (banner + "older one\n").length();
        long newerPosition = newer.length();
        assertTrue(olderPosition < newerPosition && newerPosition <= older.length(),
                "the older file is long enough for either saved position");
        ProcessingFiles processingFiles = new ProcessingFiles(60);
        processingFiles.save(null, newer, newerPosition, newer.lastModified(), true);
        processingFiles.save(null, older, olderPosition, older.lastModified(), true);

        Map<String, String> claims = processingFiles.claim(Arrays.asList(older, newer));

        assertEquals(olderPosition, processingFiles.get(claims.get(older.getFileId())).getStartPosition());
        assertEquals(newerPosition, processingFiles.get(claims.get(newer.getFileId())).getStartPosition());
    }

    private static LogFile logFile(Path path, String content) throws IOException {
        Files.write(path, content.getBytes(StandardCharsets.UTF_8));
        return LogFile.of(path.toFile());
    }
}
