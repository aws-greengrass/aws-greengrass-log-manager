package com.aws.greengrass;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith({MockitoExtension.class})
public class LogGeneratorTest {
    @TempDir
    static Path tempPath;
    String logFileName = "localtest";
    String logFileExtention = "log";
    String logWriteFreqSeconds = "0.1";
    String totalLogNumbers = "50";
    String fileSizeBytes = "1024";
    String fileSizeUnit = "KB";
    String componentName = "com.aws.greengrass.artifacts.LogGenerator";
    String activeFileName = logFileName + "." + logFileExtention;

    @Test
    void GIVEN_request_THEN_log_file_Created()
            throws ClassNotFoundException, IllegalAccessException, InstantiationException {

        String[] args = {logFileName, logFileExtention, fileSizeBytes, fileSizeUnit, logWriteFreqSeconds,
                totalLogNumbers, tempPath.toString()};
        ((Consumer<String[]>) Class.forName(componentName).newInstance()).accept(args);

        // check if log file is created
        String[] pathnames = tempPath.toFile().list();
        assertTrue(pathnames.length >= 1);
        assertTrue(Arrays.asList(pathnames).contains(activeFileName));
        assertTrue(tempPath.resolve(activeFileName).toFile().length() > 0);
    }

    /*
    if tempPath is empty string, then the log files would be generated in your local machine. '
    This can be used for manually testing if logs are correctly written
     */
    @Test
    void GIVEN_empty_targetFilePath_in_Paras_THEN_default_path_is_local()
            throws ClassNotFoundException, InstantiationException, IllegalAccessException {

        String[] args = {logFileName, logFileExtention, fileSizeBytes, fileSizeUnit, logWriteFreqSeconds,
                totalLogNumbers, ""};
        ((Consumer<String[]>) Class.forName(componentName).newInstance()).accept(args);

        String localPath = System.getProperty("user.dir");
        File directory = new File(localPath);
        String[] pathnames = directory.list();
        assertTrue(pathnames.length >= 1);
        assertTrue(Arrays.asList(pathnames).contains(activeFileName));
        assertTrue(new File(localPath + "/" + activeFileName).length() > 0);

    }
}
