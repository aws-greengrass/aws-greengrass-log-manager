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
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import static com.aws.greengrass.logmanager.LogManagerService.ACTIVE_LOG_FILE_FEATURE_ENABLED_FLAG;

public final class LogFileGroup {
    @Getter
    private List<LogFile> logFiles;
    private static Map<String, LogFile> fileHashToLogFile;
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
                System.out.println("before: " + file.getName() + " | empty hash: " + isEmptyFileHash + " | name "
                        + "match: "
                        + isNameMatchPattern + " | modify:" + isModifiedAfterLastUpdatedFile);
                if (file.isFile()
                        && isModifiedAfterLastUpdatedFile
                        && isNameMatchPattern
                        && !isEmptyFileHash) {
                    allFiles.add(file);
                    fileHashToLogFile.put(fileHash, file);
                    System.out.println(file.getName());
                }
            }
        }
        allFiles.sort(Comparator.comparingLong(LogFile::lastModified));
        //TODO: setting this flag is only to develop incrementally without having to changed all tests yet, so that
        // we can avoid a massive PR. This will be removed in the end.
        if (!ACTIVE_LOG_FILE_FEATURE_ENABLED_FLAG.get()) {
            if (allFiles.size() - 1 <= 0) {
                return new LogFileGroup(new ArrayList<>(), filePattern, directoryURI, lastUpdated);
            }
            allFiles = allFiles.subList(0, allFiles.size() - 1);
        }
        return new LogFileGroup(allFiles, filePattern, directoryURI, lastUpdated);
    }

    /**
     * Using the sorted log files to get the active file.
     * @return the active logFile.
     */
    public Optional<LogFile> getActiveFile() {
        if (logFiles.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(logFiles.get(logFiles.size() - 1));
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
    public Optional<LogFile> getFile(String fileHash) {
        if (!fileHashToLogFile.containsKey(fileHash)) {
            return Optional.empty();
        }
        return Optional.of(fileHashToLogFile.get(fileHash));
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

}
