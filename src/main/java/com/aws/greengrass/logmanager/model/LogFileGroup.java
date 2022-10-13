package com.aws.greengrass.logmanager.model;

import com.aws.greengrass.logmanager.exceptions.InvalidLogGroupException;
import lombok.Getter;

import java.io.File;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import static com.aws.greengrass.logmanager.LogManagerService.ACTIVE_LOG_FILE_FEATURE_ENABLED_FLAG;

public final class LogFileGroup {
    @Getter
    private List<LogFile> logFiles;

    private LogFileGroup(List<LogFile> files) {
        this.logFiles = files;
    }

    /**
     * Create a list of Logfiles that are sorted based on lastModified time.
     * @param filePattern the fileNameRegex used for each component to recognize its log files.
     * @param path the directory path of the log files of component.
     * @param lastUpdated the saved updated time of the last uploaded log of a component.
     * @return list of logFile.
     * @throws InvalidLogGroupException the exception if this is not a valid directory.
     */
    public static LogFileGroup create(Pattern filePattern, URI path, Instant lastUpdated)
            throws InvalidLogGroupException {
        File folder = new File(path);

        if (!folder.isDirectory()) {
            throw new InvalidLogGroupException("Must be a folder.");
        }

        LogFile[] files = LogFile.of(folder.listFiles());
        List<LogFile> allFiles = new ArrayList<>();
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
        //TODO: setting this flag is only to develop incrementally without having to changed all tests yet, so that
        // we can avoid a massive PR. This will be removed in the end.
        if (!ACTIVE_LOG_FILE_FEATURE_ENABLED_FLAG.get()) {
            if (allFiles.size() - 1 <= 0) {
                return new LogFileGroup(new ArrayList<>());
            }
            allFiles = allFiles.subList(0, allFiles.size() - 1);
        }
        return new LogFileGroup(allFiles);
    }

    /**
     * Given a fileHash of a file, using the sorted log files to detect if the file is the active file.
     * @param fileHash the fileHash of the target file.
     * @return boolean if this is the active file.
     */
    public boolean isActiveFile(String fileHash) {
        if (fileHash.isEmpty()) {
            return false;
        }
        LogFile activeFile = logFiles.get(logFiles.size() - 1);
        return activeFile.hashString().equals(fileHash);
    }

    public void forEach(Consumer<LogFile> callback) {
        logFiles.forEach(callback::accept);
    }

    public boolean isEmpty() {
        return this.logFiles.isEmpty();
    }
}
