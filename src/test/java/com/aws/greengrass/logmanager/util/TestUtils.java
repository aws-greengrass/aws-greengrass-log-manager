package com.aws.greengrass.logmanager.util;

import com.aws.greengrass.logmanager.model.LogFile;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
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

    public static void writeFile(File file, byte[] byteArray) throws IOException {
        try (OutputStream fileOutputStream = Files.newOutputStream(file.toPath())) {
            fileOutputStream.write(byteArray);
        }
    }

    public static File createFileWithContent(Path filePath, String content) throws IOException {
        File file = new File(filePath.toUri());
        byte[] bytesArray = content.getBytes(StandardCharsets.UTF_8);
        writeFile(file, bytesArray);
        return file;
    }

    public static LogFile createLogFileWithSize(URI uri, int bytesNeeded) throws IOException {
        LogFile file = new LogFile(uri);
        byte[] bytesArray = givenAStringOfSize(bytesNeeded).getBytes(StandardCharsets.UTF_8);
        writeFile(file, bytesArray);
        return file;
    }

    @SuppressWarnings("PMD.AssignmentInOperand")
    public static String readFileContent(File file) throws IOException {
        StringBuilder content = new StringBuilder();

        try (BufferedReader br = new BufferedReader(new InputStreamReader(Files.newInputStream(file.toPath())))) {
            String line;
            while ((line = br.readLine()) != null) {
                content.append(line);
            }
        }
        return content.toString();
    }

    public static File rotateFilesByRenamingThem(File... files) throws IOException {
        // Create new active file
        String activeFilePath = files[0].getAbsolutePath();

        for (int i = files.length - 1; i >= 0; i--) {
            File current = files[i];
            // to avoid changing the file on the array. Simulates closer what would happen on a real scenario
            File toModify = new File(activeFilePath + "." + (i + 1));
            current.renameTo(toModify);
        }

        File activeFile = new File(activeFilePath);
        activeFile.createNewFile();

        return activeFile;
    }
}
