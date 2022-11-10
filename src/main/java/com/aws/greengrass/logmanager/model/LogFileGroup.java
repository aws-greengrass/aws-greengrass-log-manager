package com.aws.greengrass.logmanager.model;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.logmanager.exceptions.InvalidLogGroupException;
import com.aws.greengrass.util.Utils;
import lombok.Getter;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.regex.Pattern;


public final class LogFileGroup {
    private static final Logger logger = LogManager.getLogger(LogFileGroup.class);
    private static final String DEFAULT_HARDLINKS_PATH = "aws.greengrass.LogManager/hardlinks";
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

        Path hardLinkPath = Paths.get(DEFAULT_HARDLINKS_PATH).resolve(folder.getName());
        try {
            Utils.createPaths(hardLinkPath);
        } catch (IOException e) {
            throw new InvalidLogGroupException(String.format("%s failed to create hard link directory", hardLinkPath));
        }

        File[] files = folder.listFiles();
        if (files == null) {
            logger.atWarn().log("logFiles is null");
            return new LogFileGroup(Collections.emptyList(), filePattern, directoryURI, lastUpdated);
        }

        List<LogFile> allFiles = new ArrayList<>();
        for (File file: files) {
            boolean isModifiedAfterLastUpdatedFile =
                    lastUpdated.isBefore(Instant.ofEpochMilli(file.lastModified()));
            boolean isNameMatchPattern = filePattern.matcher(file.getName()).find();

            if (file.isFile() && isModifiedAfterLastUpdatedFile && isNameMatchPattern) {
                try {
                    createHardLink(hardLinkPath, file);
                } catch (IOException e) {
                    logger.atWarn().cause(e).log("Failed to create hardlink");
                }
            }
        }

        LogFile[] hardLinks = LogFile.of(hardLinkPath.toFile().listFiles());
        for (LogFile hardLink : hardLinks) {
            allFiles.add(hardLink);
            fileHashToLogFile.put(hardLink.hashString(), hardLink);
        }

        allFiles.sort(Comparator.comparingLong(LogFile::lastModified));
        return new LogFileGroup(allFiles, filePattern, directoryURI, lastUpdated);
    }

    private static Path createHardLink(Path hardLinkDirPath, File file) throws IOException {
        Path fileHardLinkPath = hardLinkDirPath.resolve(file.getName());
        return Files.createLink(fileHardLinkPath, file.toPath());
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
