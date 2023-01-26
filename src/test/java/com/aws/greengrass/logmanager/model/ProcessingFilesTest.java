package com.aws.greengrass.logmanager.model;

import com.aws.greengrass.logmanager.LogManagerService;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import org.mockito.ScopedMock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Clock;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
        processingFiles.put(fileInformationOne);
        LogManagerService.CurrentProcessingFileInformation fileInformationTwo =
        LogManagerService.CurrentProcessingFileInformation.builder()
                .fileName("test_2023.log")
                .fileHash("54321")
                .startPosition(1000)
                .lastModifiedTime(Instant.now().toEpochMilli())
                .build();
        processingFiles.put(fileInformationTwo);


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

        processingFiles.put(LogManagerService.CurrentProcessingFileInformation.builder()
                .fileName("test.log")
                .fileHash("12345")
                .startPosition(1000)
                .lastModifiedTime(Instant.now().toEpochMilli())
                .build());
        processingFiles.put(LogManagerService.CurrentProcessingFileInformation.builder()
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

        processingFiles.put(LogManagerService.CurrentProcessingFileInformation.builder()
                .fileName("test.log")
                .fileHash("12345")
                .startPosition(1000)
                .lastModifiedTime(Instant.now().toEpochMilli())
                .build());

        // Reset time back to now
        mockInstant(now.toEpochMilli());

        processingFiles.put(LogManagerService.CurrentProcessingFileInformation.builder()
                .fileName("test_2023.log")
                .fileHash("54321")
                .startPosition(1000)
                .lastModifiedTime(now.minusSeconds(60 * 60 * 24 * 2).toEpochMilli())
                .build());

        assertEquals(processingFiles.size(), 1);
    }
}
