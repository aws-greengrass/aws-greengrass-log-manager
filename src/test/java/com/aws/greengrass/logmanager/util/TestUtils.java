package com.aws.greengrass.logmanager.util;

import com.aws.greengrass.logmanager.model.LogFile;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public final class TestUtils {
    public static final long DEFAULT_GENERIC_POLLING_TIMEOUT_MILLIS =
            TimeUnit.SECONDS.toMillis(10);
    public static final long DEFAULT_POLLING_INTERVAL_MILLIS = 500;
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

    public static boolean eventuallyTrue(Supplier<Boolean> condition) throws InterruptedException {
        return eventuallyTrue(condition, DEFAULT_GENERIC_POLLING_TIMEOUT_MILLIS, DEFAULT_POLLING_INTERVAL_MILLIS);
    }

    public static boolean eventuallyTrue(Supplier<Boolean> condition, long pollingTimeoutMillis,
                                         long pollingIntervalMillis) throws InterruptedException {
        final Instant tryUntil = Instant.now().plus(Duration.ofMillis(pollingTimeoutMillis));

        while (Instant.now().isBefore(tryUntil)) {
            if (condition.get()) {
                return true;
            }

            Thread.sleep(pollingIntervalMillis);
        }

        throw new AssertionError("Condition was not eventually true");
    }
}
