package com.aws.greengrass.logmanager.model;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.logmanager.LogManagerService;
import com.aws.greengrass.util.Utils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;

import static com.aws.greengrass.util.Digest.calculate;

@SuppressFBWarnings("EQ_DOESNT_OVERRIDE_EQUALS")
public class LogFile extends File {
    // custom serialVersionUID for class extends Serializable class
    private static final long serialVersionUID = 123;
    private static final Logger logger = LogManager.getLogger(LogManagerService.class);
    public static final int bytesNeeded = 1024;
    public static final String HASH_VALUE_OF_EMPTY_STRING = "";
    private final String sourcePath;

    public LogFile(Path sourcePath, Path hardLinkPath) {
        super(hardLinkPath.toString());
        this.sourcePath = sourcePath.toString();
    }

    public LogFile(URI uri) {
        super(uri);
        this.sourcePath = uri.getPath();
    }

    /**
     * Convert the file to LogFile.
     * @param file The file to be converted.
     */
    public static LogFile of(File file) {
        return new LogFile(file.toURI());
    }

    /**
     * Convert the file to LogFile.
     *
     * @param sourceFile        The file to be converted.
     * @param hardLinkDirectory path where the hardlink should be stored
     * @throws IOException if can't find the source path
     */
    public static LogFile of(File sourceFile, Path hardLinkDirectory) throws IOException {
        // Why do we need this? - Hardlinks can't get the path of their source file. We need to keep track of which path
        // it was originally created with and in case the file on that path changes we need to scan the directory to
        // check for the inode that matches the hardlink to get the correct path.
        Path destinationPath = hardLinkDirectory.resolve(sourceFile.getName());
        Path sourcePath = sourceFile.toPath();
        Files.createLink(destinationPath, sourcePath);
        return new LogFile(sourcePath, destinationPath);
    }

    /**
     * Read target bytes from the file.
     * @return read byte array.
     */
    private String readBytesToString() {
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
        } catch (FileNotFoundException e) {
            // The file may be deleted as expected.
            logger.atDebug().cause(e).log("The file {} does not exist", this.getAbsolutePath());
        } catch (IOException e) {
            // File may not exist
            logger.atError().cause(e).log("Unable to read file {}", this.getAbsolutePath());
        }
        return "";
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
            String stringToHash = readBytesToString();
            if (!stringToHash.isEmpty()) {
                fileHash = calculate(stringToHash);
            }
        } catch (NoSuchAlgorithmException e) {
            logger.atError().cause(e).log("The digest algorithm is invalid");
        }
        return fileHash;
    }

    public boolean isEmpty() {
        return Utils.isEmpty(this.hashString());
    }

    /**
     * Determines if a file has rotated by comparing the current file on the sourcePath against the hardlink
     * path. This returns false if the file that the hardlink is tracking is not the same it was
     * originally created with.
     */
    public boolean hasRotated() {
        try {
            return !Files.isSameFile(Paths.get(sourcePath), toPath());
        } catch (IOException e) {
            return true;
        }
    }

    /**
     * Returns the source path of the file original file.
     */
    public String getSourcePath() {
        // TODO: This is wrong the file could have rotated by the time we call this and if we do that then we might
        //  end up deleting or returning a path for a file that is no the same the hardlink INODE is tracking
        return sourcePath;
    }
}
