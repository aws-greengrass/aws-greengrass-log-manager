package com.aws.greengrass.logmanager.model;

import com.aws.greengrass.logmanager.exceptions.InvalidLogGroupException;
import com.aws.greengrass.util.Utils;
import lombok.Getter;

import java.io.File;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.regex.Pattern;


public final class LogFileGroup {
    @Getter
    private List<LogFile> logFiles;
    private static Map<String, LogFile> fileHashToLogFile;
    @Getter
    private final Pattern filePattern;
    private final URI directoryURI;
    private final Instant lastUpdated;

    private LogFileGroup(List<LogFile> files, Pattern filePattern, URI directoryURI, Instant lastUpdated) {
        this.logFiles = files;
        this.filePattern = filePattern;
        this.directoryURI = directoryURI;
        this.lastUpdated = lastUpdated;
    }

    /**
     * Create a list of Logfiles that are sorted based on lastModified time.
     * @param filePattern the fileNameRegex used for each component to recognize its log files.
     * @param directoryURI the directory path of the log files of component.
     * @param lastUpdated the saved updated time of the last uploaded log of a component.
     * @return list of logFile.
     * @throws InvalidLogGroupException the exception if this is not a valid directory.
     */
    public static LogFileGroup create(Pattern filePattern, URI directoryURI, Instant lastUpdated)
            throws InvalidLogGroupException {
        File folder = new File(directoryURI);
        fileHashToLogFile = new ConcurrentHashMap<>();

        if (!folder.isDirectory()) {
            throw new InvalidLogGroupException(String.format("%s must be a directory", directoryURI));
        }

        LogFile[] files = LogFile.of(folder.listFiles());
        List<LogFile> allFiles = new ArrayList<>();
        if (files.length != 0) {
            for (LogFile file: files) {
                String fileHash = file.hashString();
                boolean isModifiedAfterLastUpdatedFile =
                        lastUpdated.isBefore(Instant.ofEpochMilli(file.lastModified()));
                boolean isNameMatchPattern = filePattern.matcher(file.getName()).find();
                boolean isEmptyFileHash = Utils.isEmpty(fileHash);

                if (file.isFile()
                        && isModifiedAfterLastUpdatedFile
                        && isNameMatchPattern
                        && !isEmptyFileHash) {
                    allFiles.add(file);
                    fileHashToLogFile.put(fileHash, file);
                }
            }
        }
        allFiles.sort(Comparator.comparingLong(LogFile::lastModified));
        return new LogFileGroup(allFiles, filePattern, directoryURI, lastUpdated);
    }

    public void forEach(Consumer<LogFile> callback) {
        logFiles.forEach(callback::accept);
    }

    public boolean isEmpty() {
        return this.logFiles.isEmpty();
    }

    /**
     * Get the LogFile object from the fileHash.
     * @param fileHash the fileHash obtained from uploader.
     * @return the logFile.
     */
    public LogFile getFile(String fileHash) {
        return fileHashToLogFile.get(fileHash);
    }

    /**
     * Validate if the hash exists in the current logFileGroup.
     * @param fileHash the hash of the file.
     * @return boolean.
     */
    public boolean isHashExist(String fileHash) {
        return fileHashToLogFile.containsKey(fileHash);
    }

    /**
     * In case of file rotation happen between the processing files and handling upload results, the logFileGroup
     * uses the (Pattern filePattern, URI directoryURI, Instant lastUpdated) when created to take the latest snapshot
     * of the same directory. This can guarantee if the active file get rotated during the file processing, it will be
     * deleted because it is a rotated file now.
     * @return the LogFileGroup created by the current directory.
     * @throws InvalidLogGroupException when directory path is not pointing to a valid directory.
     */
    public LogFileGroup syncDirectory() throws InvalidLogGroupException {
        return create(this.filePattern, this.directoryURI, this.lastUpdated);
    }

    /**
     * Returns the size in bytes of all the contents being tracked on by the log group.
     */
    public long totalSizeInBytes() {
        long bytes = 0;
        for (LogFile log : logFiles) {
            bytes += log.length();
        }
        return bytes;
    }

    /**
     * Validate if the logFile is the active of one logFileGroup.
     * @param file the target file.
     * @return boolean.
     */
    public boolean isActiveFile(LogFile file) {
        if (logFiles.isEmpty()) {
            return false;
        }
        LogFile activeFile = logFiles.get(logFiles.size() - 1);
        return file.hashString().equals(activeFile.hashString());
    }
}
