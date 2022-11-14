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
import java.util.Arrays;
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
    private final boolean isUsingHardlinks;
    @Getter
    private List<LogFile> logFiles;
    private final Map<String, LogFile> fileHashToLogFile;
    @Getter
    private final Pattern filePattern;

    private LogFileGroup(List<LogFile> files, Pattern filePattern, Map<String, LogFile> fileHashToLogFile,
                         boolean isUsingHardlinks) {
        this.logFiles = files;
        this.filePattern = filePattern;
        this.fileHashToLogFile = fileHashToLogFile;
        this.isUsingHardlinks = isUsingHardlinks;
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
        String componentName = componentLogConfiguration.getName();
        Path componentHardlinksDirectory = workDir.resolve(componentName);

        try {
            Utils.deleteFileRecursively(componentHardlinksDirectory.toFile());
            Utils.createPaths(componentHardlinksDirectory);
        } catch (IOException e) {
            throw new InvalidLogGroupException(
                    String.format("%s failed to create hard link directory", componentHardlinksDirectory), e);
        }

        File[] files = folder.listFiles();
        Pattern filePattern = componentLogConfiguration.getFileNameRegex();

        if (files == null || files.length == 0) {
            logger.atDebug().kv("component", componentName)
                    .kv("directory", directoryURI)
                    .log("No component logs are found in the directory");
            return new LogFileGroup(Collections.emptyList(), filePattern, new HashMap<>(), false);
        }

        Map<String, LogFile> fileHashToLogFileMap = new ConcurrentHashMap<>();
        boolean isUsingHardlinks = true;
        List<LogFile> allFiles;

        files = Arrays.stream(files)
                .filter(File::isFile)
                .filter(file -> lastUpdated.isBefore(Instant.ofEpochMilli(file.lastModified())))
                .filter(file -> filePattern.matcher(file.getName()).find()).toArray(File[]::new);

        // Convert files into log files
        try {
            allFiles = convertToLogFiles(files, componentHardlinksDirectory);
        } catch (IOException e) {
            logger.atDebug().cause(e).log("Failed to create hardlinks for files. Falling back to only uploading "
                    + "rotated files");
            isUsingHardlinks = false;
            allFiles = convertToLogFiles(files);
        }

        // Cache the logFiles by hash
        allFiles.forEach(logFile -> {
            fileHashToLogFileMap.put(logFile.hashString(), logFile);
        });

        return new LogFileGroup(allFiles, filePattern, fileHashToLogFileMap, isUsingHardlinks);
    }

    /**
     * Transform the files into log files that track the file through a hardlink that is created on the
     * hardLinkDirectory.
     * Files created this way can be tracked regardless of whether the underlying file rotates.
     *
     * @param files             - A array of files
     * @param hardLinkDirectory - A Path to the hardlink directory. Must be on the same volume the files are being
     *                          created
     * @throws IOException - If it fails to create the hard link
     */
    private static List<LogFile> convertToLogFiles(File[] files, Path hardLinkDirectory) throws IOException {
        List<LogFile> logFiles = new ArrayList<>(files.length);

        // TODO: We have to add the rotation detection mechanism here otherwise there is a chance that while we are
        //  looping and creating the hardlinks the files gets rotated so the path that

        // TODO: Retry on NoSuchFileException

        for (File file : files) {
            logFiles.add(LogFile.of(file, hardLinkDirectory));
        }

        logFiles.sort(Comparator.comparingLong(LogFile::lastModified));

        return logFiles;
    }

    /**
     * Transform the files into log files that point to the path of the file that is being passed in. It behaves the
     * same
     * as a java File. It is not resilient to file rotations. Meaning that if the underlying file rotates it will
     * get a reference to a different file than the one it was originally created with.
     *
     * @param files - An array of files
     */
    private static List<LogFile> convertToLogFiles(File... files) {
        List<LogFile> logFiles = new ArrayList<>(files.length);

        for (File file : files) {
            logFiles.add(LogFile.of(file));
        }

        logFiles.sort(Comparator.comparingLong(LogFile::lastModified));
        logFiles.remove(logFiles.size() - 1); // remove the active file

        return logFiles;
    }

    public void forEach(Consumer<LogFile> callback) {
        logFiles.forEach(callback::accept);
    }

    public boolean isEmpty() {
        return this.logFiles.isEmpty();
    }

    /**
     * Get the LogFile object from the fileHash.
     *
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
     *
     * @param file the target file.
     * @return boolean.
     */
    public boolean isActiveFile(LogFile file) {
        if (!isUsingHardlinks) {
            return false;
        }

        if (logFiles.isEmpty()) {
            return false;
        }

        LogFile activeFile = logFiles.get(logFiles.size() - 1);

        if (activeFile.hasRotated()) {
            return false;
        }

        return file.hashString().equals(activeFile.hashString());
    }
}
