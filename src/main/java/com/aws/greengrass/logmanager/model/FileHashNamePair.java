package com.aws.greengrass.logmanager.model;


import java.io.File;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class FileHashNamePair {
    // filehash -> filename
    private Map<String, String> fileHashNamePairs = new ConcurrentHashMap<>();

    public void updatePair(Path path) {
        File folder = new File(path.toUri());
        LogFile[] files = LogFile.of(folder.listFiles());
        if (files.length != 0) {
            for (LogFile file : files) {
                String fileHash = file.hashString();
                String fileName = file.getAbsolutePath();
                fileHashNamePairs.putIfAbsent(fileHash, fileName);
                if (fileHashNamePairs.get(fileHash) != fileName) {
                    fileHashNamePairs.replace(fileHash, fileName);
                }
            }
        }
    }

    public String get(String key) {
        return fileHashNamePairs.get(key);
    }
}
