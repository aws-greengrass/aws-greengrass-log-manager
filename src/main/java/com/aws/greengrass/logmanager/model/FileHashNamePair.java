package com.aws.greengrass.logmanager.model;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class FileHashNamePair {
    // filehash -> filename
    private Map<String, String> fileHashNamePairs = new ConcurrentHashMap<>();

    /**
     * Update the fileHash and fileName pair in case new file is created or file rotation happens.
     * @param fileHash The hash generated for a valid file (contains enough messages).
     * @param fileName The absolute file path of a file.
     */
    public void updatePair(String fileHash, String fileName) {
        fileHashNamePairs.putIfAbsent(fileHash, fileName);
        if (fileHashNamePairs.get(fileHash) != fileName) {
            fileHashNamePairs.replace(fileHash, fileName);
        }
    }

    public String get(String key) {
        return fileHashNamePairs.get(key);
    }

    public void remove(String fileHash) {
        fileHashNamePairs.remove(fileHash);
    }
}
