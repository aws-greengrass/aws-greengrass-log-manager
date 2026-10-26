package com.aws.greengrass.logmanager.model;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.logmanager.LogManagerService;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;


/**
 * Cache that holds information about files being processed. It will remove entries that have not been accessed in the
 * last maxInactiveTimeSeconds
 *
 * <p>Entries are keyed by record key: the file's first-line hash for the first record of that hash, and
 * {@code <hash>.<n>} for later ones. A record names its file by the first-line hash plus a {@link PositionCheck}.
 */
public class ProcessingFiles {
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
     * Stores the currently processing file information only if the entry does not exist.
     *
     * @param key   - record key
     * @param value - currently processing file information
     */
    public void putIfAbsent(String key, LogManagerService.CurrentProcessingFileInformation value) {
        if (cache.get(key) == null) {
            put(key, value);
        }
    }

    /**
     * Stored the currently processing file information.
     *
     * @param key   - record key
     * @param value - currently processing file information
     */
    public void put(String key, LogManagerService.CurrentProcessingFileInformation value) {
        this.mostRecentlyUsed = Node.builder().lastAccessed(value.getLastAccessedTime()).info(value).build();
        cache.put(key, this.mostRecentlyUsed);
        evictStaleEntries();
    }

    /**
     * Saves the position reached in a listed file.
     *
     * @param key              - the file's record key, or null to allocate one
     * @param file             - the file
     * @param position         - the position to resume from
     * @param lastModifiedTime - the file's last modified time when it was read
     * @return the record key now holding the position; the given key when the file could not be read to check
     *     the position, which keeps the old record so the next cycle re-sends the span instead of skipping it
     */
    @SuppressWarnings("deprecation")
    public String save(String key, LogFile file, long position, long lastModifiedTime) {
        String positionCheck;
        try {
            positionCheck = PositionCheck.of(file, position);
        } catch (IOException e) {
            logger.atWarn().cause(e).kv("file", file.getSourcePath()).kv("position", position)
                    .log("Unable to read the file to check the saved position. Keeping the previous position");
            return key;
        }
        String recordKey = key == null ? newKey(file.hashString()) : key;
        put(recordKey, LogManagerService.CurrentProcessingFileInformation.builder()
                .fileName(file.getSourcePath())
                .fileHash(file.hashString())
                .startPosition(position)
                .positionCheck(positionCheck)
                .lastModifiedTime(lastModifiedTime)
                .build());
        return recordKey;
    }

    private String newKey(String firstLineHash) {
        String key = firstLineHash;
        for (int n = 1; cache.containsKey(key); n++) {
            key = firstLineHash + "." + n;
        }
        return key;
    }

    /**
     * Matches listed files to their saved records. Each record goes to at most one file, older files first. A
     * record matches a file with its first-line hash that is at least as long as the saved position and whose
     * bytes hash to the record's position check; a file matching several takes the furthest. A record saved
     * before position checks existed matches by first-line hash alone, and only when exactly one unmatched file
     * has that hash and is long enough. When several files have that hash, or the one file is shorter, the record
     * is dropped and those files are read from the start.
     *
     * @param files - listed files, oldest first
     * @return record key per file id, for the files that matched a record
     */
    public Map<String, String> claim(List<LogFile> files) {
        Map<String, String> claims = new HashMap<>();
        Set<String> unclaimed = new HashSet<>(cache.keySet());
        for (LogFile file : files) {
            String key = furthestCheckedRecord(file, unclaimed);
            if (key != null) {
                claims.put(file.getFileId(), key);
                unclaimed.remove(key);
            }
        }
        for (String key : unclaimed) {
            LogManagerService.CurrentProcessingFileInformation record = cache.get(key).getInfo();
            if (record.getPositionCheck() == null) {
                claimLegacyRecord(key, record, files, claims);
            }
        }
        files.stream().map(file -> claims.get(file.getFileId())).filter(Objects::nonNull).forEach(this::get);
        return claims;
    }

    private String furthestCheckedRecord(LogFile file, Set<String> keys) {
        String furthest = null;
        long furthestPosition = -1;
        for (String key : keys) {
            LogManagerService.CurrentProcessingFileInformation record = cache.get(key).getInfo();
            long position = record.getStartPosition();
            if (record.getPositionCheck() != null && position > furthestPosition
                    && position <= file.length() && file.hashString().equals(record.getFileHash())
                    && record.getPositionCheck().equals(checkOrNull(file, position))) {
                furthest = key;
                furthestPosition = position;
            }
        }
        return furthest;
    }

    private void claimLegacyRecord(String key, LogManagerService.CurrentProcessingFileInformation record,
                                   List<LogFile> files, Map<String, String> claims) {
        List<LogFile> sameFirstLine = files.stream()
                .filter(file -> !claims.containsKey(file.getFileId()))
                .filter(file -> file.hashString().equals(record.getFileHash()))
                .collect(Collectors.toList());
        if (sameFirstLine.isEmpty()) {
            return;
        }
        if (sameFirstLine.size() > 1) {
            cache.remove(key);
            logger.atWarn().kv("files", sameFirstLine.stream().map(LogFile::getFileId).collect(Collectors.toList()))
                    .kv("savedPosition", record.getStartPosition())
                    .log("A position saved by an older version matches several files that share a first line. "
                            + "Reading those files from the start, so some lines may be uploaded again");
            return;
        }
        LogFile file = sameFirstLine.get(0);
        if (record.getStartPosition() > file.length()) {
            cache.remove(key);
            return;
        }
        record.setPositionCheck(checkOrNull(file, record.getStartPosition()));
        claims.put(file.getFileId(), key);
    }

    private static String checkOrNull(LogFile file, long position) {
        try {
            return PositionCheck.of(file, position);
        } catch (IOException e) {
            logger.atDebug().cause(e).kv("file", file.getFileId()).log("Unable to check a saved position");
            return null;
        }
    }

    /**
     * Removes an entry from the cache for a provided record key.
     *
     * @param key - A record key.
     */
    public void remove(String key) {
        if (key == null) {
            return;
        }

        if (cache.remove(key) != null) {
            logger.atDebug().kv("key", key).log("Evicted file from cache");
        }
    }

    /**
     * Returns a currently processing file information for a record key.
     *
     * @param key - A record key
     */
    public LogManagerService.CurrentProcessingFileInformation get(String key) {
        Node node = this.cache.get(key);

        if (node != null) {
            node.setLastAccessed(Instant.now().toEpochMilli());
            node.getInfo().setLastAccessedTime(node.getLastAccessed());
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
            Map.Entry<String, Node> entry = it.next();
            Instant lastAccessed = Instant.ofEpochMilli(entry.getValue().getLastAccessed());

            if (lastAccessed.isBefore(deadline)) {
                toRemove.add(entry.getKey());
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
