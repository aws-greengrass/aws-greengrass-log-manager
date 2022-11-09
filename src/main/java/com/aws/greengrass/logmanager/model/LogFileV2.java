package com.aws.greengrass.logmanager.model;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Optional;


/**
 * Represents a log file in the file system via a hardlink to said file.
 */
public final class LogFileV2 {
    public final Path hardLink;
    private Path sourcePath;
    private static final String HASH_VALUE_OF_EMPTY_STRING = "";
    private static final int bytesNeeded = 1024;

    private static final Logger logger = LogManager.getLogger(LogFileV2.class);
    private String hash;


    private LogFileV2(Path hardLink, Path sourcePath) {
       this.hardLink = hardLink;
       this.sourcePath = sourcePath;
       this.hash = HASH_VALUE_OF_EMPTY_STRING;
    }

    /**
     * Creates a LogFile from a regular java File. LogFile instances create a hardlink to a file and track it
     * regardless of the original file being rotated. One important consideration is that getting the original source
     * path if the original file rotates is only possible if the file remains on the same folder it originally was
     * created. Rotation mechanisms that rotate files to different folders will yield in an invalid source path
     * and a IOException will be thrown.
     *
     * @param sourceFile - the source java File.
     * @param hardLink - a File path to where the file hardlink should be stored
     * @throws IOException - if we fail to get the source file path
     */
    public static LogFileV2 fromFile(File sourceFile, Path hardLink) throws IOException {
        Path linkPath = Files.createLink(hardLink, sourceFile.toPath());
        LogFileV2 file = new LogFileV2(linkPath, sourceFile.toPath());
        file.computeHash();
        return file;
    }

    /**
     * Returns the computed hash of the log file.
     */
    public String hashString() {
        if (isValid()) {
            return hash;
        }

        // Correct file not being tracked any more try recomputing
        try {
            hash = computeHash();
        } catch (IOException e) {
            logger.debug("Failed to compute hash. This could happen because the source file got deleted.");
            hash = "";
        }

        return hash;
    }

    /**
     * Returns the last modified time since epoch in milliseconds.
     * @throws IOException - if fails to read hardlink path
     */
    public long lastModified() throws IOException {
        BasicFileAttributes targetAttributes = Files.readAttributes(hardLink, BasicFileAttributes.class);
        return targetAttributes.lastModifiedTime().toMillis();
    }

    /**
     * Get the path of the source file that generated the hardlink. Hard links just track inodes and not file paths. If
     * a file get rotated the best bet we have to get the source path is to scan the folder where the source file was
     * originally located.
     * <p>
     * IMPORTANT: If the components use a rotation scheme that moves files to different folders we won't be able to
     * get the path once a file is rotated.
     * </p>
     *
     * @throws IOException - If unable to find source file path
     */
    public Path toPath() throws IOException {
        if (isValid()) {
            return sourcePath;
        }

        Optional<Path> newSourcePath = scanSourceDirectoryForINode();

        if (newSourcePath.isPresent()) {
            sourcePath = newSourcePath.get();
            return sourcePath;
        }

        throw new IOException("Unable to find source file for hard link " + hardLink.toAbsolutePath());
    }

    /**
     * Returns a SeekableByteChannel so that the contents of the file can be read by specifying specific byte positions.
     *
     * @throws IOException - If the original file was deleted.
     */
    public SeekableByteChannel byteChannel() throws IOException {
        return Files.newByteChannel(hardLink, StandardOpenOption.READ);
    }

    /**
     * Returns a InputStream to read the contents of the file being tracked.
     *
     * @throws IOException - If the original file was deleted.
     */
    public InputStream inputStream() throws IOException {
        return Files.newInputStream(hardLink, StandardOpenOption.READ);
    }


    /**
     * There is no way of getting the source path from a hard link other than scanning the folder to check for the
     * same inode. This method checks is the path the hard link got created with is still tracking the same inode.
     *
     * <a href="https://stackoverflow.com/questions/35102169/find-out-the-path-to-the-original-file-of-a-hard-link">
     *     Find out the path to the original file of a hard link
     * </a>
     */
    public boolean isValid() {
        try {
            return Files.isSameFile(sourcePath, hardLink);
        } catch (IOException e) {
            return false;
        }
    }


    /**
     * Read target bytes from the file.
     * @return read byte array.
     */
    private String computeHash() throws IOException {
        byte[] bytesReadArray = new byte[bytesNeeded];
        int bytesRead;

        try (InputStream r = Files.newInputStream(this.toPath())) {
            bytesRead = r.read(bytesReadArray);
            String bytesReadString = new String(bytesReadArray, StandardCharsets.UTF_8);
            // if there is an entire line before 1KB, we hash the line; Otherwise, we hash 1KB to prevent super long
            // single line.
            if (bytesReadString.indexOf('\n') > -1) {
                return bytesReadString.substring(0, bytesReadString.indexOf('\n') + 1);
            }
            if (bytesRead >= bytesNeeded) {
                return bytesReadString;
            }
        }

        return "";
    }

    private Optional<Path> scanSourceDirectoryForINode() throws IOException {
        Path sourceDir = sourcePath.getParent();

        if (sourceDir == null) {
            return Optional.empty();
        }

        File[] files = sourceDir.toFile().listFiles();

        if (files == null) {
           return Optional.empty();
        }

        for (File file: files) {
            if (Files.isSameFile(file.toPath(), hardLink)) {
                return Optional.of(file.toPath());
            }
        }

        return Optional.empty();
    }

    /**
     * Removes the hard link. After calling this calling any other method in this class
     * will fail.
     */
    public boolean delete()  {
        try {
            Files.delete(hardLink);
            return true;
        } catch (IOException e) {
            return false;
        }
    }
}
