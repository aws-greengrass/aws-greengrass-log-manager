package com.aws.greengrass.logmanager.util;

import com.aws.greengrass.logmanager.model.LogFile;

import java.io.IOException;
import java.io.OutputStream;
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
}
