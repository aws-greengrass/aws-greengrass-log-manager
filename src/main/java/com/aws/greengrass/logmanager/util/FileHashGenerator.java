package com.aws.greengrass.logmanager.util;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.logmanager.LogManagerService;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.NoSuchAlgorithmException;
import java.util.Optional;

import static com.aws.greengrass.util.Digest.calculate;

public final class FileHashGenerator {
    private static final Logger logger = LogManager.getLogger(LogManagerService.class);
    // private constructor to make checkstyle happy
    private FileHashGenerator() {
        
    }

    /**
     * calculate the hash for the log file, which takes and digests first some lines (default is 1) in the file.
     * If the content of file is insufficient, the hash is null.
     * @param logFile
     * @return the nullable calculated hash for active file
     */
    public static Optional<String> calculateHashForLogFile(File logFile, int DEFAULT_LINES_FOR_DIGEST_NUM) {
        Optional fileHash = Optional.ofNullable(null);
        // empty file, then do not read
        if (!logFile.isFile())
            return fileHash;
        try (BufferedReader r = Files.newBufferedReader(logFile.toPath(), StandardCharsets.UTF_8)) {
            StringBuilder data = new StringBuilder();
            int linesReadForDigest = 0;
            String oneLine;
            // read target number of lines; read one more line to prevent incomplete last line
            // Therefore, there must be DEFAULT_LINES_FOR_DIGEST_NUM + 1 lines in each file.
            while (linesReadForDigest <= DEFAULT_LINES_FOR_DIGEST_NUM) {
                oneLine = r.readLine();
                if (oneLine == null) break;
                linesReadForDigest ++;
                if (linesReadForDigest <= DEFAULT_LINES_FOR_DIGEST_NUM)
                    data.append(oneLine);
            }
            // there must be at least DEFAULT_LINES_FOR_DIGEST_NUM + 1 lines in each files,
            // otherwise the hash for the file is null
            if (linesReadForDigest == DEFAULT_LINES_FOR_DIGEST_NUM + 1)
                fileHash = Optional.ofNullable(calculate(data.toString()));
            else
                logger.atTrace().log("Not enough lines to digest file {} ", logFile.getAbsolutePath());
        } catch (IOException e) {
            // File probably does not exist
            logger.atError().cause(e).log("Unable to read file {}", logFile.getAbsolutePath());
        } catch (NoSuchAlgorithmException e) {
            logger.atError().cause(e).log("The Digest Algorithm is invalid.");
        }

        return fileHash;
    }
}
