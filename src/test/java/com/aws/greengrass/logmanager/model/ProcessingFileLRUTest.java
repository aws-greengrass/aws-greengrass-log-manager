package com.aws.greengrass.logmanager.model;

import com.aws.greengrass.logmanager.LogManagerService;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;


@ExtendWith({MockitoExtension.class, GGExtension.class})
public class ProcessingFileLRUTest {

    @Test
    void GIVEN_filledLRU_WHEN_toMap_returnsMapRepresentation() {
        // Given
        ProcessingFileLRU lru = new ProcessingFileLRU(5);
        LogManagerService.CurrentProcessingFileInformation fileInformationOne =
                LogManagerService.CurrentProcessingFileInformation.builder()
                .fileName("test.log")
                .fileHash("kj35435")
                .startPosition(1000)
                .lastModifiedTime(Instant.now().toEpochMilli())
                .build();
        lru.put(fileInformationOne.getFileHash(), fileInformationOne);
        LogManagerService.CurrentProcessingFileInformation fileInformationTwo =
        LogManagerService.CurrentProcessingFileInformation.builder()
                .fileName("test_2023.log")
                .fileHash("54321")
                .startPosition(1000)
                .lastModifiedTime(Instant.now().toEpochMilli())
                .build();
        lru.put(fileInformationTwo.getFileHash(), fileInformationTwo);


        // Then
        Map<String, Object> expected = new HashMap<String, Object>(){{
            put("kj35435", fileInformationOne.convertToMapOfObjects());
            put("54321", fileInformationTwo.convertToMapOfObjects());
        }};
        assertEquals(expected, lru.toMap());
    }

    @Test
    void GIVEN_filledLRU_WHEN_head_THEN_mostRecentValueReturned() {
        // Given
        ProcessingFileLRU lru = new ProcessingFileLRU(2);

        lru.put("12345", LogManagerService.CurrentProcessingFileInformation.builder()
                .fileName("test.log")
                .fileHash("12345")
                .startPosition(1000)
                .lastModifiedTime(Instant.now().toEpochMilli())
                .build());
        lru.put("54321", LogManagerService.CurrentProcessingFileInformation.builder()
                .fileName("test_2023.log")
                .fileHash("54321")
                .startPosition(1000)
                .lastModifiedTime(Instant.now().toEpochMilli())
                .build());

        // Then
        assertEquals(lru.getMostRecentlyUsed().getFileHash(), "54321");
    }

    @Test
    void GIVEN_lruWithValue_WHEN_newValueAdded_THEN_stateEntriesGetCleared() {
        ProcessingFileLRU lru = new ProcessingFileLRU( 60 * 60 * 24);

        lru.put("12345", LogManagerService.CurrentProcessingFileInformation.builder()
                .fileName("test.log")
                .fileHash("12345")
                .startPosition(1000)
                .lastModifiedTime(Instant.now().toEpochMilli())
                .build());
        lru.put("54321", LogManagerService.CurrentProcessingFileInformation.builder()
                .fileName("test_2023.log")
                .fileHash("54321")
                .startPosition(1000)
                .lastModifiedTime(Instant.now().minusSeconds(60 * 60 * 24 * 2).toEpochMilli())
                .build());

        assertEquals(lru.size(), 1);
    }
}
