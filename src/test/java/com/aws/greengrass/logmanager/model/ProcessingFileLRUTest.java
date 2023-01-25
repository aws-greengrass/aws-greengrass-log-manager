package com.aws.greengrass.logmanager.model;

import com.aws.greengrass.logmanager.LogManagerService;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
        lru.put(fileInformationOne);
        LogManagerService.CurrentProcessingFileInformation fileInformationTwo =
        LogManagerService.CurrentProcessingFileInformation.builder()
                .fileName("test_2023.log")
                .fileHash("54321")
                .startPosition(1000)
                .lastModifiedTime(Instant.now().toEpochMilli())
                .build();
        lru.put(fileInformationTwo);


        // Then
        Map<String, Object> expected = new HashMap<String, Object>(){{
            put("kj35435", fileInformationOne.convertToMapOfObjects());
            put("54321", fileInformationTwo.convertToMapOfObjects());
        }};
        assertEquals(expected, lru.toMap());
    }


    @Test
    void GIVEN_filledLRU_WHEN_reachedCapacity_THEN_oldestRecordDeleted() {
        // Given
        ProcessingFileLRU lru = new ProcessingFileLRU(1);

        lru.put(LogManagerService.CurrentProcessingFileInformation.builder()
                .fileName("test.log")
                .fileHash("12345")
                .startPosition(1000)
                .lastModifiedTime(Instant.now().toEpochMilli())
                .build());
        lru.put(LogManagerService.CurrentProcessingFileInformation.builder()
                .fileName("test_2023.log")
                .fileHash("54321")
                .startPosition(1000)
                .lastModifiedTime(Instant.now().toEpochMilli())
                .build());

        // Then
        assertEquals(lru.getSize(), 1);
        assertFalse(lru.get("12345").isPresent());
        assertTrue(lru.get("54321").isPresent());
    }

}
