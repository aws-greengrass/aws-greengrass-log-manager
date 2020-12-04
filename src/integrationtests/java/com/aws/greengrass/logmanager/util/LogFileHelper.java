/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.logmanager.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public final class LogFileHelper {

    private LogFileHelper() { }

    public static void addDataToFile(String data, Path filePath) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(filePath, StandardOpenOption.APPEND,
                StandardOpenOption.CREATE)) {
            writer.write(data + System.lineSeparator());
        }
    }

    public static List<String> generateRandomMessages() {
        List<String> msgs = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            int leftLimit = 48; // numeral '0'
            int rightLimit = 122; // letter 'z'
            int targetStringLength = 1024;
            Random random = new Random();

            String generatedString = random.ints(leftLimit, rightLimit + 1)
                    .filter(s -> (s <= 57 || s >= 65) && (s <= 90 || s >= 97))
                    .limit(targetStringLength)
                    .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                    .toString();
            msgs.add(generatedString);
        }
        return msgs;
    }

    public static void createTempFileAndWriteData(Path tempDirectoryPath, String fileNamePrefix, String fileNameSuffix)
            throws IOException {
        Path filePath = Files.createTempFile(tempDirectoryPath, fileNamePrefix, fileNameSuffix);
        File file = filePath.toFile();
        List<String> randomMessages = generateRandomMessages();
        for (String messageBytes : randomMessages) {
            addDataToFile(messageBytes, file.toPath());
        }
    }
}
