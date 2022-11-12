package com.aws.greengrass.logmanager.model;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.logmanager.exceptions.InvalidLogGroupException;
import com.aws.greengrass.util.Utils;
import lombok.Getter;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.regex.Pattern;


public final class LogFileGroup {
    private static final Logger logger = LogManager.getLogger(LogFileGroup.class);
    @Getter
    private List<LogFile> logFiles;
    private final Map<String, LogFile> fileHashToLogFile;
    @Getter
    private final Pattern filePattern;

    private LogFileGroup(List<LogFile> files, Pattern filePattern, Map<String, LogFile> fileHashToLogFile) {
        this.logFiles = files;
        this.filePattern = filePattern;
        this.fileHashToLogFile = fileHashToLogFile;
    }

    /**
     * Create a list of Logfiles that are sorted based on lastModified time.
     *
     * @param componentLogConfiguration component log configuration
     * @param lastUpdated               the saved updated time of the last uploaded log of a component.
     * @param workDir                   component work directory
     * @return list of logFile.
     * @throws InvalidLogGroupException the exception if this is not a valid directory.
     */
    public static LogFileGroup create(ComponentLogConfiguration componentLogConfiguration,
                                      Instant lastUpdated, Path workDir)
            throws InvalidLogGroupException {
        URI directoryURI = componentLogConfiguration.getDirectoryPath().toUri();
        File folder = new File(directoryURI);

        if (!folder.isDirectory()) {
            throw new InvalidLogGroupException(String.format("%s must be a directory", directoryURI));
        }
        Pattern filePattern = componentLogConfiguration.getFileNameRegex();
        Path componentHardlinksDirectory = workDir.resolve(filePattern.toString());
        try {
            Utils.deleteFileRecursively(componentHardlinksDirectory.toFile());
            Utils.createPaths(componentHardlinksDirectory);
        } catch (IOException e) {
            throw new InvalidLogGroupException(
                    String.format("%s failed to create hard link directory", componentHardlinksDirectory), e);
        }

        File[] files = folder.listFiles();
        if (files == null) {
            logger.atWarn().log("logFiles is null");
            return new LogFileGroup(Collections.emptyList(), filePattern, new HashMap<>());
        }

        Map<String, LogFile> fileHashToLogFileMap = new ConcurrentHashMap<>();
        List<LogFile> allFiles = new ArrayList<>();
        // TODO: We have to add the rotation detection mechanism here otherwise there is a chance that while we are
        //  looping and creating the hardlinks the files gets rotated so the path that
        for (File file : files) {
            boolean isModifiedAfterLastUpdatedFile =
                    lastUpdated.isBefore(Instant.ofEpochMilli(file.lastModified()));
            boolean isNameMatchPattern = filePattern.matcher(file.getName()).find();

            if (file.isFile() && isModifiedAfterLastUpdatedFile && isNameMatchPattern) {
                try {
                    LogFile logfile = LogFile.of(file, componentHardlinksDirectory);
                    allFiles.add(logfile);
                    fileHashToLogFileMap.put(logfile.hashString(), logfile);
                } catch (IOException e) {
                    logger.atWarn().cause(e).log("Failed to create hardlink");
                }
            }
        }
        allFiles.sort(Comparator.comparingLong(LogFile::lastModified));
        return new LogFileGroup(allFiles, filePattern, fileHashToLogFileMap);
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
        // TODO: Check if rotation happened
        return file.hashString().equals(activeFile.hashString());
    }
}
