package com.aws.greengrass.logmanager.model;

import com.aws.greengrass.logmanager.exceptions.InvalidLogGroupException;
import lombok.Getter;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public final class LogFileGroup {
    @Getter
    private final Pattern filePattern;
    private final URI directoryURI;
    private final Instant lastUpdated;

    private LogFileGroup(Pattern filePattern, URI directoryURI, Instant lastUpdated) {
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

        if (!folder.isDirectory()) {
            throw new InvalidLogGroupException(String.format("%s must be a directory", directoryURI));
        }

        return new LogFileGroup(filePattern, directoryURI, lastUpdated);
    }

    /**
     * Returns a list of log files associated that match the patter and path of the log group.
     */
    public List<LogFile> getLogFiles() {
       return getFileMatchingPattern(directoryURI, filePattern)
               .filter(file -> lastUpdated.isBefore(Instant.ofEpochMilli(file.lastModified())))
               .map(LogFile::of)
               .sorted(Comparator.comparingLong(LogFile::lastModified))
               .collect(Collectors.toList());
    }

    private static Stream<File> getFileMatchingPattern(URI directoryURI, Pattern filePattern) {
        File folder = new File(directoryURI);

        File[] files = folder.listFiles();

        if (files == null) {
            return Stream.empty();
        }

        return Arrays.stream(files).filter(file -> filePattern.matcher(file.getName()).find());
    }

    public boolean isEmpty() {
        List<LogFile> logFiles = getLogFiles();
        return logFiles.isEmpty();
    }

    /**
     * Get the LogFile object from the fileHash.
     * @param fileHash the fileHash obtained from uploader.
     */
    public Optional<LogFile> getFile(String fileHash) {
        return getFileMatchingPattern(directoryURI, filePattern)
            .map(LogFile::of).filter(file -> {
                try (SeekableByteChannel channel = Files.newByteChannel(file.toPath(), StandardOpenOption.READ)) {
                   return Objects.equals(file.hashString(channel), fileHash);
                } catch (IOException e) {
                    return false;
                }
            }).findFirst();
    }

    /**
     * Returns the size in bytes of all the contents being tracked on by the log group.
     */
    public long totalSizeInBytes() {
        long bytes = 0;
        for (LogFile log : getLogFiles()) {
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
        List<LogFile> logFiles = getLogFiles();

        if (logFiles.isEmpty()) {
            return false;
        }
        LogFile activeFile = logFiles.get(logFiles.size() - 1);
        return file.hashString().equals(activeFile.hashString());
    }
}
