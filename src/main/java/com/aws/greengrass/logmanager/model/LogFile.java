package com.aws.greengrass.logmanager.model;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.logmanager.LogManagerService;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.aws.greengrass.util.Digest.calculate;

public class LogFile extends File {
    // custom serialVersionUID for class extends Serializable class
    private static final long serialVersionUID = 123;
    private static final Logger logger = LogManager.getLogger(LogManagerService.class);
    private static final int linesNeeded = 1;

    public LogFile(String pathname) {
        super(pathname);
    }

    /**
     * Convert the file to LogFile.
     * @param file The file to be converted.
     * @return
     */
    public static LogFile of(File file) {
        return new LogFile(file.getAbsolutePath());
    }

    /**
     * Convert list of files to list of LogFiles.
     * @param files The list of files to be converted.
     * @return
     */
    public static LogFile[] of(File... files) {
        if (files == null) {
            return new LogFile[] {};
        }
        return Arrays.stream(files).map(LogFile::of).toArray(LogFile[]::new);
    }

    public boolean isLogFile() {
        return super.isFile();
    }

    /**
     * Read target lines from the file.
     * The file must contain (minLine + 1) lines. One extra line is needed to prevent incomplete line when hashing.
     * @return an ArrayList of lines or empty ArrayList
     */
    private List<String> getLines() {
        List<String> linesRead = new ArrayList<>();
        String oneLine;
        try (BufferedReader r = Files.newBufferedReader(this.toPath(), StandardCharsets.UTF_8)) {
            // read target number of lines
            while (linesRead.size() < linesNeeded) {
                oneLine = r.readLine();
                if (oneLine == null) {
                    break;
                }
                linesRead.add(oneLine);
            }
        } catch (IOException e) {
            // File may not exist
            logger.atError().cause(e).log("Unable to read file {}", this.getAbsolutePath());
        }
        return linesRead;
    }

    /**
     * Get the hash of the logfile with target lines.
     * @return the calculated hash value of the logfile
     */
    public Optional<String> hashString() {
        if (!this.exists()) {
            return Optional.empty();
        }
        List<String> lines = getLines();
        if (lines.size() < linesNeeded) {
            return Optional.empty();
        }

        try {
            return Optional.of(calculate(String.join("", lines)));
        } catch (NoSuchAlgorithmException e) {
            logger.atError().cause(e).log("The Digest Algorithm is invalid.");
        }

        return Optional.empty();
    }
}
