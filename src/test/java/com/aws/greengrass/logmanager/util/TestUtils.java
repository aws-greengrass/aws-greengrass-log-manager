package com.aws.greengrass.logmanager.util;

import com.aws.greengrass.logmanager.model.LogFile;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Random;

public final class TestUtils {
    private static Random rnd = new Random();

    private TestUtils() { }

    public static String givenAStringOfSize(int bytesNeeded) {
        StringBuilder testStrings = new StringBuilder();
        String testChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqestuvwxyz0123456789";
        while (testStrings.length() < bytesNeeded) {
            int charIdx = (int) (rnd.nextFloat() * testChars.length());
            testStrings.append(testChars.charAt(charIdx));
        }
        return testStrings.toString();
    }

    public static void writeFile(LogFile file, byte[] byteArray) throws IOException {
        try (OutputStream fileOutputStream = Files.newOutputStream(file.toPath())) {
            fileOutputStream.write(byteArray);
        }
    }

    public static LogFile createLogFileWithSize(URI uri, int bytesNeeded) throws IOException {
        LogFile file = new LogFile(uri);
        byte[] bytesArray = givenAStringOfSize(bytesNeeded).getBytes(StandardCharsets.UTF_8);
        writeFile(file, bytesArray);
        return file;
    }

    /**
     * Rotates the files by renaming them. For example, if the active log file gets full and there are other rotated files
     * for instance test.log test.log.1 test.log.2, where test.log is the active file. when rotated, there following
     * files will be present test.log test.log.1 test.log.2 test.log.3. test.log will be a new file and the previous
     * active file will become test.log.1 and test.log.1 will become test.log.2 and so on.
     */
    public static File rotateFilesByRenamingThem(File... files) throws IOException {
        // Create new active file
        String activeFilePath = files[0].getAbsolutePath();

        for (int i = files.length - 1; i >= 0; i--) {
            File current = files[i];
            // to avoid changing the file on the array. Simulates closer what would happen on a real scenario
            File toModify = new File(current.getAbsolutePath());
            toModify.renameTo(new File(activeFilePath + "." + (i + 1)));
        }

        File activeFile = new File(activeFilePath);
        activeFile.createNewFile();
        return activeFile;
    }
}
