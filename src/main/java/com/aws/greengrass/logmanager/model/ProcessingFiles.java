package com.aws.greengrass.logmanager.model;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.logmanager.LogManagerService;

import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * Cache that holds information about files being processed. It will remove entries that have not been accessed in the
 * last maxInactiveTimeSeconds
 */
public class ProcessingFiles {
    private final Map<String, LogManagerService.CurrentProcessingFileInformation> cache;
    private final int maxInactiveTimeSeconds;
    private LogManagerService.CurrentProcessingFileInformation mostRecentlyUsed;
    private static final Logger logger = LogManager.getLogger(ProcessingFiles.class);


    /**
     * Creates an instance of the ProcessingFiles.
     *
     * @param maxInactiveTimeSeconds - the maximum amount of seconds an entry can be in the cache without being
     *                               accessed
     */
    public ProcessingFiles(int maxInactiveTimeSeconds) {
        this.cache = new HashMap<>();
        this.maxInactiveTimeSeconds = maxInactiveTimeSeconds;
    }

    /**
     * Stores the currently processing file information only if the entry does not exist.
     *
     * @param value - currently processing file information
     */
    public void putIfAbsent(LogManagerService.CurrentProcessingFileInformation value) {
        if (cache.get(value.getFileHash()) == null) {
            put(value);
        }
    }

    /**
     * Stores the currently processing file information, replacing and existing entry if it
     * is already present.
     *
     * @param value - currently processing file information
     */
    public void put(LogManagerService.CurrentProcessingFileInformation value) {
        value.setLastAccessedTime(Instant.now().toEpochMilli());
        this.mostRecentlyUsed = value;
        cache.put(value.getFileHash(), value);
        evictStaleEntries();
    }

    /**
     * Removes an entry from the cache for a provided file hash.
     *
     * @param fileHash - A file hash.
     */
    public void remove(String fileHash) {
        if (fileHash == null) {
            return;
        }

        if (cache.remove(fileHash) != null) {
            logger.atDebug().kv("hash", fileHash).log("Evicted file from cache");
        }
    }

    /**
     * Removes a list of entries from the cache.
     *
     * @param deletedHashes - A list of file hashes to remove
     */
    public void remove(List<String> deletedHashes) {
        if (deletedHashes != null) {
            deletedHashes.forEach(this::remove);
        }
    }

    /**
     * Returns a currently processing file information for a file hash.
     *
     * @param fileHash - A file hash
     */
    public LogManagerService.CurrentProcessingFileInformation get(String fileHash) {
        LogManagerService.CurrentProcessingFileInformation info = this.cache.get(fileHash);

        if (info != null) {
            info.setLastAccessedTime(Instant.now().toEpochMilli());
            mostRecentlyUsed = info;
            return info;
        }

        return null;
    }

    public int size() {
        return cache.size();
    }

    private void evictStaleEntries() {
        // TODO: This could be improved by additionally storing the nodes on a Heap
        Iterator<Map.Entry<String, LogManagerService.CurrentProcessingFileInformation>> it =
                cache.entrySet().iterator();
        Set<String> toRemove = new HashSet<>();
        Instant deadline = Instant.now().minusSeconds(maxInactiveTimeSeconds);

        while (it.hasNext()) {
            LogManagerService.CurrentProcessingFileInformation info = it.next().getValue();
            Instant lastAccessed = Instant.ofEpochMilli(info.getLastAccessedTime());

            if (lastAccessed.isBefore(deadline)) {
                toRemove.add(info.getFileHash());
            }
        }

        toRemove.forEach(cache::remove);
    }


    /**
     * Converts the objects stored in the cache into a map. Used serialize the processing files to be stored
     * on the runtime config.
     */
    public Map<String, Object> toMap() {
        HashMap<String, Object> map = new HashMap<>();

        cache.forEach((key, value) -> {
            map.put(key, value.convertToMapOfObjects());
        });

        return map;
    }

    /**
     * Returns the most recently used item.
     */
    public LogManagerService.CurrentProcessingFileInformation getMostRecentlyUsed() {
        if (mostRecentlyUsed != null) {
            return mostRecentlyUsed;
        }

        return null;
    }
}
