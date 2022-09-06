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
import java.util.Optional;

import static com.aws.greengrass.util.Digest.calculate;

public class LogFile extends File {
    private static final Logger logger = LogManager.getLogger(LogManagerService.class);
    private static final String EMPTY_STRING = "";
    private static final int linesNeededNum = 1;

    public LogFile(String pathname) {
        super(pathname);
    }

    public static LogFile of(File file) {
        return new LogFile(file.getAbsolutePath());
    }

    public static LogFile[] of(File[] files) {
        if (files == null) return new LogFile[] {};
        return Arrays.stream(files).map(LogFile::of).toArray(LogFile[]::new);
    }

    @Override
    public boolean isFile() {
        return super.isFile();
    }

    /**
     * Read target lines from the file.
     * The file must contain (minLine + 1) lines. One extra line is needed to prevent incomplete line when hashing.
     * @return an ArrayList of lines or empty ArrayList
     */
    private String getLines() {
        ArrayList<String> linesReaded = new ArrayList<>();
        String oneLine;
        try (BufferedReader r = Files.newBufferedReader(this.toPath(), StandardCharsets.UTF_8)) {
            int linesReadForDigest = 0;
            // read target number of lines
            while (linesReadForDigest < linesNeededNum) {
                oneLine = r.readLine();
                if (oneLine == null) break;
                linesReadForDigest ++;
                linesReaded.add(oneLine);
            }
            // read one more line to prevent incomplete last line
            // if lines in file less than minLines + 1, then return empty
            if (r.readLine() == null || linesReadForDigest < linesNeededNum) {
                logger.atTrace().log("Not enough lines to digest file {} ", this.getAbsolutePath());
                return EMPTY_STRING;
            }
        } catch (IOException e) {
            // File may not exist
            logger.atError().cause(e).log("Unable to read file {}", this.getAbsolutePath());
        }
        return String.join("", linesReaded);
    }

    /**
     * Get the hash of the logfile with target lines.
     * @return the calculated hash value of the logfile
     */
    public Optional<String> hashString() {
        if (!this.exists()) return Optional.ofNullable(null);
        String lines = getLines();
        if (!lines.isEmpty()) {
            try {
                return Optional.ofNullable(calculate(lines));
            } catch (NoSuchAlgorithmException e) {
                logger.atError().cause(e).log("The Digest Algorithm is invalid.");
            }
        }
        return Optional.ofNullable(null);
    }
}
