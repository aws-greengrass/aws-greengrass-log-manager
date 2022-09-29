package com.aws.greengrass.logmanager.model;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.logmanager.LogManagerService;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import static com.aws.greengrass.util.Digest.calculate;

@SuppressFBWarnings("EQ_DOESNT_OVERRIDE_EQUALS")
public class LogFile extends File {
    // custom serialVersionUID for class extends Serializable class
    private static final long serialVersionUID = 123;
    private static final Logger logger = LogManager.getLogger(LogManagerService.class);
    public static final int bytesNeeded = 1024;
    public static final String HASH_VALUE_OF_EMPTY_STRING = "";
    private int didRead = -1;

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


    /**
     * Read target bytes from the file.
     * @return read byte array.
     */
    private byte[] readBytes() {
        byte[] bytesReadArray = new byte[bytesNeeded];
        try (InputStream r = Files.newInputStream(this.toPath())) {
            didRead = r.read(bytesReadArray);
        } catch (FileNotFoundException e) {
            // The file may be deleted as expected.
            logger.atDebug().cause(e).log("The file {} does not exist", this.getAbsolutePath());
        } catch (IOException e) {
            // File may not exist
            logger.atError().cause(e).log("Unable to read file {}", this.getAbsolutePath());
        }
        return bytesReadArray;
    }

    /**
     * Get the hash of the logfile with target lines.
     * @return the calculated hash value of the logfile, empty string if not enough lines for digest.
     */
    public String hashString() {
        String fileHash = HASH_VALUE_OF_EMPTY_STRING;
        try {
            if (!this.exists()) {
                return fileHash;
            }
            String bytesReadString = new String(readBytes(), StandardCharsets.UTF_8);
            // if the we read less then 1KB but it contains an entire line, it is still valid.
            if (didRead < bytesNeeded && bytesReadString.indexOf('\n') > -1) {
                fileHash = calculate(bytesReadString.substring(0, bytesReadString.indexOf('\n') + 1));
            }
            if (didRead == bytesNeeded) {
                fileHash = calculate(bytesReadString);
            }
        }  catch (NoSuchAlgorithmException e) {
            logger.atError().cause(e).log("The digest algorithm is invalid");
        }

        return fileHash;

    }
}
