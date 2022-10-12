package com.aws.greengrass.logmanager.model;

import com.aws.greengrass.util.Utils;
import lombok.Getter;

import java.io.File;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;

public class LogFileGroup {
    @Getter
    List<LogFile> logFiles;

    private LogFileGroup(List<LogFile> files) {
        this.logFiles = files;
    }

    public static LogFileGroup create(Pattern filePattern, URI path, Instant lastUpdated) {
        File folder = new File(path);

        List<LogFile> allFiles = new ArrayList<>();

        if (!folder.isDirectory()) {
            throw new RuntimeException("Must be a folder.");
        }

        LogFile[] files = LogFile.of(folder.listFiles());
        if (files.length != 0) {
            for (LogFile file: files) {
                if (file.isFile()
                        && lastUpdated.isBefore(Instant.ofEpochMilli(file.lastModified()))
                        && filePattern.matcher(file.getName()).find()
                        && file.length() > 0) {
                    allFiles.add(file);
                }
            }
        }
        allFiles.sort(Comparator.comparingLong(LogFile::lastModified));
        //TODO: the following logic is only for ignoring the active file and keeping the PR small
        if (allFiles.size() - 1 <= 0) {
            return new LogFileGroup(new ArrayList<>());
        }
        allFiles = allFiles.subList(0, allFiles.size() - 1);
        return new LogFileGroup(allFiles);
    }

    public boolean isActiveFile(String fileHash) {
        if (Utils.isEmpty(fileHash)) {
            return false;
        }
        LogFile activeFile = logFiles.get(logFiles.size() - 1);
        return activeFile.hashString().equals(fileHash);
    }
}
