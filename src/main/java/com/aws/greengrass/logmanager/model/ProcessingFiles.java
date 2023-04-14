package com.aws.greengrass.logmanager.model;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.logmanager.LogManagerService;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

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
public class ProcessingFiles  {
    private final Map<String, Node> cache;
    private final int maxInactiveTimeSeconds;
    private Node mostRecentlyUsed;
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
     * Stored the currently processing file information.
     * @param value - currently processing file information
     */
    public void put(LogManagerService.CurrentProcessingFileInformation value) {
        mostRecentlyUsed =  Node.builder().lastAccessed(Instant.now().toEpochMilli()).info(value).build();
        cache.put(value.getFileHash(), mostRecentlyUsed);
        evictStaleEntries();
    }

    /**
     * Removes an entry from the cache for a provided file hash.
     * @param fileHash - A file hash.
     */
    public void remove(String fileHash) {
        if (fileHash == null) {
            return;
        }

        Node node = cache.remove(fileHash);

        if (node != null) {
            logger.atDebug().kv("hash", fileHash).log("Evicted file from cache");
        }
    }

    /**
     * Removes a list of entries from the cache.
     * @param deletedHashes - A list of file hashes to remove
     */
    public void remove(List<String> deletedHashes) {
        if (deletedHashes != null) {
            deletedHashes.forEach(this::remove);
        }
    }

    /**
     * Returns a currently processing file information for a file hash.
     * @param fileHash - A file hash
     */
    public LogManagerService.CurrentProcessingFileInformation get(String fileHash) {
        Node node = this.cache.get(fileHash);

        if (node != null) {
            node.setLastAccessed(Instant.now().toEpochMilli());
            mostRecentlyUsed = node;
            return node.getInfo();
        }

        return null;
    }

    public int size() {
        return cache.size();
    }

    private void evictStaleEntries() {
        // TODO: This could be improved by additionally storing the nodes on a Heap
        Iterator<Map.Entry<String, Node>> it = cache.entrySet().iterator();
        Set<String> toRemove = new HashSet<>();
        Instant deadline = Instant.now().minusSeconds(maxInactiveTimeSeconds);

        while (it.hasNext()) {
            Node node = it.next().getValue();
            Instant lastAccessed = Instant.ofEpochMilli(node.getLastAccessed());

            if (lastAccessed.isBefore(deadline)) {
                toRemove.add(node.info.getFileHash());
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
            map.put(key, value.getInfo().convertToMapOfObjects());
        });

        return map;
    }

    /**
     * Returns the most recently used item.
     */
    public LogManagerService.CurrentProcessingFileInformation getMostRecentlyUsed() {
        if (mostRecentlyUsed != null) {
            return mostRecentlyUsed.getInfo();
        }

        return null;
    }

    @Builder
    @Getter
    @Setter
    private static class Node {
        private long lastAccessed;
        private LogManagerService.CurrentProcessingFileInformation info;
    }
}
