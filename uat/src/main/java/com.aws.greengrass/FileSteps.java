/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass;

import com.aws.greengrass.testing.model.ScenarioContext;
import com.aws.greengrass.testing.model.TestContext;
import com.aws.greengrass.testing.platform.Platform;
import com.google.inject.Inject;
import io.cucumber.guice.ScenarioScoped;
import io.cucumber.java.en.And;
import io.cucumber.java.en.Then;
import org.apache.commons.text.RandomStringGenerator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ScenarioScoped
public class FileSteps {

    private static final RandomStringGenerator RANDOM_STRING_GENERATOR =
            new RandomStringGenerator.Builder().withinRange('a', 'z').build();
    private static final String ACTIVEFILE = "ActiveFile";
    private static Logger LOGGER = LogManager.getLogger(FileSteps.class);
    private final Platform platform;
    private final TestContext testContext;
    private final ScenarioContext scenarioContext;


    /**
     * Arranges some log files with content on the /logs folder for a component
     * to simulate a devices where logs have already bee written.
     *
     * @param platform        number of log files to write.
     * @param testContext     name of the component.
     * @param scenarioContext name of the component.
     */
    @Inject
    public FileSteps(Platform platform, TestContext testContext, ScenarioContext scenarioContext) {
        this.platform = platform;
        this.testContext = testContext;
        this.scenarioContext = scenarioContext;
    }

    private static List<String> generateRandomMessages(int n, int length) {
        List<String> msgs = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            // TODO: Improves this as this is not how the logger writes the logs
            msgs.add(RANDOM_STRING_GENERATOR.generate(length));
        }
        return msgs;
    }

    private static List<File> getComponentLogFiles(String componentName, Path logsDirectory) {
        return Arrays.stream(logsDirectory.toFile().listFiles())
                .filter(File::isFile)
                .filter(file -> file.getName().startsWith(componentName))
                .sorted(Comparator.comparingLong(File::lastModified))
                .collect(Collectors.toList());
    }

    /**
     * Arranges some log files with content on the /logs folder for a component
     * to simulate a devices where logs have already bee written.
     *
     * @param numFiles      number of log files to write.
     * @param componentName name of the component.
     * @throws IOException thrown when file fails to be written.
     */
    @And("{int} temporary rotated log files for component {word} have been created")
    public void arrangeComponentLogFiles(int numFiles, String componentName) throws IOException {
        Path logsDirectory = testContext.installRoot().resolve("logs");
        LOGGER.info("Writing {} log files into {}", numFiles, logsDirectory.toString());
        if (!platform.files().exists(logsDirectory)) {
            throw new IllegalStateException("No logs directory");
        }
        scenarioContext.put(componentName + "LogDirectory", logsDirectory.toString());
        String filePrefix = "greengrass";
        if (!Objects.equals("aws.greengrass.Nucleus", componentName)) {
            filePrefix = componentName;
        }
        String fileName = "";
        for (int i = 0; i < numFiles; i++) {
            fileName = String.format("%s_%s.log", filePrefix, UUID.randomUUID());
            createFileAndWriteData(logsDirectory, fileName, false);
        }
        scenarioContext.put(componentName + ACTIVEFILE, logsDirectory.resolve(fileName).toAbsolutePath().toString());
    }

    private void createFileAndWriteData(Path tempDirectoryPath, String fileNamePrefix, boolean isTemp)
            throws IOException {
        Path filePath;
        if (isTemp) {
            filePath = Files.createTempFile(tempDirectoryPath, fileNamePrefix, "");
        } else {
            filePath = Files.createFile(tempDirectoryPath.resolve(fileNamePrefix));
        }
        File file = filePath.toFile();
        List<String> randomMessages = generateRandomMessages(10, 1024);
        for (String messageBytes : randomMessages) {
            addDataToFile(messageBytes, file.toPath());
        }
    }

    private void addDataToFile(String data, Path filePath) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(filePath, StandardOpenOption.APPEND)) {
            writer.write(data + "\r\n");
        }
    }

    /**
     * Arranges some log files with content on the /logs folder for a component
     * to simulate a devices where logs have already bee written.
     *
     * @param componentName name of the component.
     * @param nfiles        number of log files to write.
     */
    @Then("I verify that {int} log files for component {word} are still available")
    public void verifyRotatedFilesAvailable(int nfiles, String componentName) {
        Path logsDirectory = testContext.installRoot().resolve("logs");
        if (!platform.files().exists(logsDirectory)) {
            throw new IllegalStateException("No logs directory");
        }
        List<File> componentFiles = getComponentLogFiles(componentName, logsDirectory);
        assertEquals(nfiles, componentFiles.size());
    }

    /**
     * Arranges some log files with content on the /logs folder for a component
     * to have already bee written simulate a devices where logs.
     *
     * @param componentName name of the component.
     */
    @And("I verify the rotated files are deleted and that the active log file is present for component {word}")
    public void verifyActiveFile(String componentName) {
        Path logsDirectory = testContext.installRoot().resolve("logs");

        if (!platform.files().exists(logsDirectory)) {
            throw new IllegalStateException("No logs directory");
        }
        List<File> sortedFileList = getComponentLogFiles(componentName, logsDirectory);
        String expectedActiveFilePath = scenarioContext.get(componentName + this.ACTIVEFILE);
        File activeFile = sortedFileList.get(sortedFileList.size() - 1);
        assertEquals(1, sortedFileList.size());
        assertEquals(expectedActiveFilePath, activeFile.getAbsolutePath());
    }
}