package com.aws.greengrass;

import com.aws.greengrass.testing.model.ScenarioContext;
import com.aws.greengrass.testing.model.TestContext;
import com.aws.greengrass.testing.platform.Platform;
import com.google.inject.Inject;
import io.cucumber.guice.ScenarioScoped;
import io.cucumber.java.en.Given;
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
import java.util.List;

@ScenarioScoped
public class FileSteps {

    private static final RandomStringGenerator RANDOM_STRING_GENERATOR =
            new RandomStringGenerator.Builder().withinRange('a', 'z').build();
    private static Logger LOGGER = LogManager.getLogger(FileSteps.class);
    private final Platform platform;
    private final TestContext testContext;
    private final ScenarioContext scenarioContext;

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

    /**
     * Arranges some log files with content on the /logs folder for a component
     * to simulate a devices where logs have already bee written.
     *
     * @param numFiles      number of log files to write.
     * @param componentName name of the component.
     * @throws IOException thrown when file fails to be written.
     */
    @Given("{int} temporary rotated log files for component {word} have been created")
    public void arrangeComponentLogFiles(int numFiles, String componentName) throws IOException {
        Path logsDirectory = testContext.installRoot().resolve("logs");
        LOGGER.info("Writing {} log files into {}", numFiles, logsDirectory.toString());
        if (!platform.files().exists(logsDirectory)) {
            throw new IllegalStateException("No logs directory");
        }
        scenarioContext.put(componentName + "LogDirectory", logsDirectory.toString());
        String filePrefix = "greengrass";
        if (!componentName.equals("aws.greengrass.Nucleus")) {
            filePrefix = componentName;
        }

        for (int i = 0; i < numFiles; i++) {
            String fileName = String.format("%s_%d.log", filePrefix, i);
            createFileAndWriteData(logsDirectory, fileName, false);
        }
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

}