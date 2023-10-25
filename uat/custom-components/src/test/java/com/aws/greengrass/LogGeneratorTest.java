package com.aws.greengrass;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith({MockitoExtension.class})
public class LogGeneratorTest {
    @TempDir
    static Path tempPath;
    String logFileName = "localtest";
    String logWriteFreqMs = "100";
    String totalLogNumbers = "50";
    String fileSizeBytes = "1024";
    String fileSizeUnit = "KB";
    String componentName = "com.aws.greengrass.artifacts.LogGenerator";
    String activeFileName = logFileName + ".log";

    @Test
    void GIVEN_request_THEN_log_file_Created()
            throws ClassNotFoundException, IllegalAccessException, InstantiationException {

        String[] args = {logFileName, fileSizeBytes, fileSizeUnit, logWriteFreqMs,
                totalLogNumbers, tempPath.toString()};
        ((Consumer<String[]>) Class.forName(componentName).newInstance()).accept(args);

        // check if log file is created
        String[] pathnames = tempPath.toFile().list();
        assertTrue(pathnames.length >= 1);
        assertTrue(Arrays.asList(pathnames).contains(activeFileName));
        assertTrue(tempPath.resolve(activeFileName).toFile().length() > 0);
    }
}
