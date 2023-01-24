package com.aws.greengrass.logmanager.model;

import com.aws.greengrass.config.Topic;
import com.aws.greengrass.config.Topics;
import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.util.Coerce;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;

import java.io.File;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static com.aws.greengrass.logmanager.LogManagerService.PERSISTED_CURRENT_PROCESSING_FILE_HASH;
import static com.aws.greengrass.logmanager.LogManagerService.PERSISTED_CURRENT_PROCESSING_FILE_LAST_MODIFIED_TIME;
import static com.aws.greengrass.logmanager.LogManagerService.PERSISTED_CURRENT_PROCESSING_FILE_NAME;
import static com.aws.greengrass.logmanager.LogManagerService.PERSISTED_CURRENT_PROCESSING_FILE_START_POSITION;

/**
 * Stores information about a file that the Log Manager worked on or previously processed, to know what was the last
 * start position of the contents uploaded, the file hash, name and last modified.
 */
@Builder
@Getter
@Data
public class ProcessingFileInformation {
    //This is deprecated value in versions greater than 2.2, but keep it here to avoid
    // upgrade-downgrade issues.
    @JsonProperty(PERSISTED_CURRENT_PROCESSING_FILE_NAME)
    private String fileName;
    @JsonProperty(PERSISTED_CURRENT_PROCESSING_FILE_START_POSITION)
    private long startPosition;
    @JsonProperty(PERSISTED_CURRENT_PROCESSING_FILE_LAST_MODIFIED_TIME)
    private long lastModifiedTime;
    @JsonProperty(PERSISTED_CURRENT_PROCESSING_FILE_HASH)
    private String fileHash;
    private static final Logger logger = LogManager.getLogger(ProcessingFileInformation.class);

    public Map<String, Object> convertToMapOfObjects() {
        Map<String, Object> currentProcessingFileInformationMap = new HashMap<>();
        // @deprecated  This is deprecated value in versions greater than 2.2, but keep it here to avoid
        // upgrade-downgrade issues.
        currentProcessingFileInformationMap.put(PERSISTED_CURRENT_PROCESSING_FILE_NAME, fileName);
        currentProcessingFileInformationMap.put(PERSISTED_CURRENT_PROCESSING_FILE_START_POSITION, startPosition);
        currentProcessingFileInformationMap.put(PERSISTED_CURRENT_PROCESSING_FILE_LAST_MODIFIED_TIME,
                lastModifiedTime);
        currentProcessingFileInformationMap.put(PERSISTED_CURRENT_PROCESSING_FILE_HASH, fileHash);
        return currentProcessingFileInformationMap;
    }

    public void updateFromTopic(Topic topic) {
        switch (topic.getName()) {
            //  @deprecated  This is deprecated value in versions greater than 2.2, but keep it here to avoid
            // upgrade-downgrade issues.
            case PERSISTED_CURRENT_PROCESSING_FILE_NAME:
                fileName = Coerce.toString(topic);
                fileHash = getFileHashFromTopic(topic);
                break;
            case PERSISTED_CURRENT_PROCESSING_FILE_START_POSITION:
                startPosition = Coerce.toLong(topic);
                break;
            case PERSISTED_CURRENT_PROCESSING_FILE_LAST_MODIFIED_TIME:
                lastModifiedTime = Coerce.toLong(topic);
                break;
            case PERSISTED_CURRENT_PROCESSING_FILE_HASH:
                fileHash = getFileHashFromTopic(topic);
                break;
            default:
                break;
        }
    }

    public static ProcessingFileInformation convertFromMapOfObjects(
            Map<String, Object> currentProcessingFileInformationMap) {
        return ProcessingFileInformation.builder()
                .fileName(Coerce.toString(currentProcessingFileInformationMap
                        .get(PERSISTED_CURRENT_PROCESSING_FILE_NAME)))
                .lastModifiedTime(Coerce.toLong(currentProcessingFileInformationMap
                        .get(PERSISTED_CURRENT_PROCESSING_FILE_LAST_MODIFIED_TIME)))
                .startPosition(Coerce.toLong(currentProcessingFileInformationMap
                        .get(PERSISTED_CURRENT_PROCESSING_FILE_START_POSITION)))
                .fileHash(Coerce.toString(currentProcessingFileInformationMap
                        .get(PERSISTED_CURRENT_PROCESSING_FILE_HASH)))
                .build();
    }

    private String getFileHashFromTopic(Topic topic) {
        Topics topics = topic.parent;
        Topic hashTopic = topics.find(PERSISTED_CURRENT_PROCESSING_FILE_HASH);

        if (hashTopic != null) {
            return Coerce.toString(hashTopic);
        }

        Topic nameTopic = topics.find(PERSISTED_CURRENT_PROCESSING_FILE_NAME);

        if (nameTopic == null || Coerce.toString(nameTopic) == null) {
            return null;
        }

        try {
            Path filePath = Paths.get(Coerce.toString(nameTopic));
            File file = filePath.toFile();

            if (!file.exists() || !file.isFile()) {
                return null;
            }

            LogFile logFile = LogFile.of(file);
            return logFile.hashString();
        } catch (InvalidPathException e) {
            logger.atWarn().cause(e).log("File name is not a valid path");
        }

        return null;
    }
}
