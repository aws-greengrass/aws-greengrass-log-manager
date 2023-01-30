package com.aws.greengrass;

import ch.qos.logback.classic.Logger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.file.Path;
import java.util.Arrays;

import static com.aws.greengrass.artifacts.logGenerator.configureLogger;
import static com.aws.greengrass.artifacts.logGenerator.performLogging;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith({MockitoExtension.class})
public class LogGeneratorTest {
    @TempDir
    static Path tempPath;
    String logFileName = "localtest";
    String logFileExtention = "log";
    double logWriteFreqSeconds = 0.1;
    int totalLogNumbers = 50;
    int fileSizeBytes = 1024;

    @Test
    void GIVEN_request_THEN_log_file_Created()
            throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        // configure logger
        Logger logger = configureLogger(tempPath.toString(), logFileName, logFileExtention, fileSizeBytes);

        // start logging
        performLogging(logger, totalLogNumbers, logWriteFreqSeconds);

        // check if log file is created
        String activeFileName = logFileName + "." + logFileExtention;
        String[] pathnames = tempPath.toFile().list();
        assertTrue(pathnames.length >= 1);
        assertTrue(Arrays.asList(pathnames).contains(activeFileName));
        assertTrue(tempPath.resolve(activeFileName).toFile().length() > 0);
    }
}
