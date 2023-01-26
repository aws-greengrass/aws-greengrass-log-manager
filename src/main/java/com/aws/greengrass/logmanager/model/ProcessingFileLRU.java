package com.aws.greengrass.logmanager.model;

import com.aws.greengrass.logmanager.LogManagerService;
import lombok.Getter;

import java.time.Instant;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;


/**
 * LRU cache that holds information about each file that is being processed. As long as a log file is in the file system
 * and can be found, we can't know for sure if that file won't get new contents written to it in the future. The LRU
 * will remove entries when adding a new element it will remove any entry that has not been modified in the
 * last maxInactiveTimeSeconds
 */
public class ProcessingFileLRU extends LinkedHashMap<String, LogManagerService.CurrentProcessingFileInformation> {
    static final long serialVersionUID = 1L;


    private final int maxInactiveTimeSeconds;


    /**
     * Creates an instance of the LRU.
     *
     * @param maxInactiveTimeSeconds - the maximum amount of seconds an entry can be in the cache since the last
     *                               modified
     */
    public ProcessingFileLRU(int maxInactiveTimeSeconds) {
        super(5, 0.75f, true);
        this.maxInactiveTimeSeconds = maxInactiveTimeSeconds;
    }

    @Override
    public LogManagerService.CurrentProcessingFileInformation put(
            String key,
            LogManagerService.CurrentProcessingFileInformation value) {
        LogManagerService.CurrentProcessingFileInformation result = super.put(key, value);
        evictStaleEntries();
        return result;
    }

    private void evictStaleEntries() {
        Iterator<Map.Entry<String, LogManagerService.CurrentProcessingFileInformation>> it = entrySet().iterator();
        Instant deadline = Instant.now().minusSeconds(maxInactiveTimeSeconds);

        while (it.hasNext()) {
            LogManagerService.CurrentProcessingFileInformation fileInfo = it.next().getValue();
            Instant lastModified = Instant.ofEpochMilli(fileInfo.getLastModifiedTime());

            if (lastModified.isBefore(deadline)) {
                remove(fileInfo.getFileHash());
            }
        }
    }


    /**
     * Converts the objects stored in the LRU into a map. Used serialize the LRU to be stored
     * on the runtime config.
     */
    public Map<String, Object> toMap() {
        HashMap<String, Object> map = new HashMap<>();

        this.forEach((key, value) -> {
            map.put(key, value.convertToMapOfObjects());
        });

        return map;
    }

    /**
     * Returns the most recently used item of the LRU.
     */
    public LogManagerService.CurrentProcessingFileInformation getMostRecentlyUsed() {
        Map.Entry<String, LogManagerService.CurrentProcessingFileInformation> entry = null;
        Iterator<Map.Entry<String, LogManagerService.CurrentProcessingFileInformation>> it = entrySet().iterator();

        while (it.hasNext()) {
            entry = it.next();
        }

        if (entry != null) {
            return  entry.getValue();
        }

        return null;
    }

    @Override
    @SuppressWarnings("PMD.CloneThrowsCloneNotSupportedException")
    public ProcessingFileLRU clone() {
        ProcessingFileLRU clone = (ProcessingFileLRU) super.clone();

        this.forEach((hash, processingFileInfo) -> {
            clone.put(hash, LogManagerService.CurrentProcessingFileInformation.convertFromMapOfObjects(
                    processingFileInfo.convertToMapOfObjects()));
        });

        return clone;
    }
}
