/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass;

import com.aws.greengrass.testing.api.ComponentPreparationService;
import com.aws.greengrass.testing.api.device.Device;
import com.aws.greengrass.testing.api.device.exception.CommandExecutionException;
import com.aws.greengrass.testing.api.device.model.CommandInput;
import com.aws.greengrass.testing.api.model.ComponentOverrideNameVersion;
import com.aws.greengrass.testing.api.model.ComponentOverrideVersion;
import com.aws.greengrass.testing.api.model.ComponentOverrides;
import com.aws.greengrass.testing.features.WaitSteps;
import com.aws.greengrass.testing.model.ScenarioContext;
import com.aws.greengrass.testing.model.TestContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import io.cucumber.datatable.DataTable;
import io.cucumber.guice.ScenarioScoped;
import io.cucumber.java.en.When;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.services.greengrassv2.model.ComponentDeploymentSpecification;
import software.amazon.awssdk.utils.ImmutableMap;
import software.amazon.awssdk.utils.StringUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;
import javax.inject.Inject;

import static com.aws.greengrass.testing.component.LocalComponentPreparationService.ARTIFACTS_DIR;
import static com.aws.greengrass.testing.component.LocalComponentPreparationService.LOCAL_STORE;
import static com.aws.greengrass.testing.component.LocalComponentPreparationService.RECIPE_DIR;
import static com.aws.greengrass.testing.features.GreengrassCliSteps.LOCAL_DEPLOYMENT_ID;

@ScenarioScoped
public class LocalDeploymentSteps {
    private static final Logger LOGGER = LogManager.getLogger(LocalDeploymentSteps.class);
    private static final String MERGE_CONFIG = "MERGE";
    private static final Path LOCAL_STORE_RECIPES = Paths.get("local:", "local-store", "recipes");
    private static final int MAX_DEPLOYMENT_RETRY_COUNT = 3;
    private final ComponentPreparationService componentPreparation;
    private final ComponentOverrides overrides;
    private final TestContext testContext;
    private final WaitSteps waits;
    private final ObjectMapper mapper;
    private final ScenarioContext scenarioContext;
    private final Path artifactPath;
    private final Path recipePath;
    private final Device device;

    @Inject
    @SuppressWarnings("MissingJavadocMethod")
    public LocalDeploymentSteps(
            final ComponentOverrides overrides,
            final TestContext testContext,
            final ComponentPreparationService componentPreparation,
            final ScenarioContext scenarioContext,
            final WaitSteps waits,
            final ObjectMapper mapper,
            final Device device) {
        this.overrides = overrides;
        this.testContext = testContext;
        this.componentPreparation = componentPreparation;
        this.scenarioContext = scenarioContext;
        this.waits = waits;
        this.mapper = mapper;
        this.artifactPath = testContext.installRoot().resolve(LOCAL_STORE).resolve(ARTIFACTS_DIR);
        this.recipePath = testContext.installRoot().resolve(LOCAL_STORE).resolve(RECIPE_DIR);
        this.device = device;
    }

    /**
     * implemented the step of installing a custom component with configuration.
     *
     * @param componentName         the name of the custom component
     * @param configurationTable    the table which describes the configurations
     * @throws InterruptedException InterruptedException could be throw out during the component deployment
     * @throws IOException          IOException could be throw out during preparation of the CLI command
     */
    @When("I install the component {word} from local store with configuration")
    public void installComponentWithConfiguration(final String componentName, final DataTable configurationTable)
            throws InterruptedException, IOException {
        List<Map<String, String>> configuration = configurationTable.asMaps(String.class, String.class);
        List<String> componentSpecs = Arrays.asList(
                componentName, LOCAL_STORE_RECIPES.resolve(String.format("%s.yaml", componentName)).toString()
        );
        LOGGER.info("I found the recipes in local {}",
                LOCAL_STORE_RECIPES.resolve(String.format("%s.yaml", componentName)).toString());
        installComponent(componentSpecs, configuration);
    }

    /**
     *  implemented the step of updating a custom component's configuration.
     *
     * @param componentName         the name of the custom component
     * @param configurationTable    the table which describes the configurations
     * @throws InterruptedException InterruptedException could be throw out during the component deployment
     * @throws IOException          IOException could be throw out during preparation of the CLI command
     */
    @When("I update the component {word} with configuration")
    public void updateComponentConfiguration(final String componentName, final DataTable configurationTable)
            throws InterruptedException, IOException {
        List<Map<String, String>> configuration = configurationTable.asMaps(String.class, String.class);
        CommandInput command = getCliDeploymentCommand(componentName, null, configuration);
        createLocalDeploymentWithRetry(command, 0);
    }

    private CommandInput getCliDeploymentCommand(String componentName, String componentVersion,
                                                 List<Map<String, String>> configuration) throws IOException {
        List<String> commandArgs = new ArrayList<>(Arrays.asList(
                "deployment",
                "create",
                "--artifactDir " + artifactPath.toString(),
                "--recipeDir " + recipePath.toString()));
        if (StringUtils.isNotBlank(componentVersion)) {
            commandArgs.add("--merge " + componentName + "=" + componentVersion);
        }
        String updateConfigArgs = getCliUpdateConfigArgs(componentName, configuration);
        if (!updateConfigArgs.isEmpty()) {
            commandArgs.add("--update-config '" + updateConfigArgs + "'");
        }
        LOGGER.info("The cli command: {}", commandArgs);
        return CommandInput.builder()
                .line(testContext.installRoot().resolve("bin").resolve("greengrass-cli").toString())
                .addAllArgs(commandArgs)
                .build();
    }

    private void createLocalDeploymentWithRetry(CommandInput commandInput, int retryCount) throws InterruptedException {
        try {
            String response = executeCommand(commandInput);
            LOGGER.info("The response from executing gg-cli command is {}", response);

            String[] responseArray = response.split(":");
            String deploymentId = responseArray[responseArray.length - 1];
            LOGGER.info("The local deployment response is " + deploymentId);
            scenarioContext.put(LOCAL_DEPLOYMENT_ID, deploymentId);
        } catch (CommandExecutionException e) {
            if (retryCount > MAX_DEPLOYMENT_RETRY_COUNT) {
                throw e;
            }
            waits.until(5, "SECONDS");
            LOGGER.warn("the deployment request threw an exception, retried {} times...",
                    retryCount, e);
            this.createLocalDeploymentWithRetry(commandInput, retryCount + 1);
        }
    }

    private String executeCommand(CommandInput input) throws CommandExecutionException {
        final StringJoiner joiner = new StringJoiner(" ").add(input.line());
        Optional.ofNullable(input.args()).ifPresent(args -> args.forEach(joiner::add));
        byte[] op = device.execute(CommandInput.builder()
                .workingDirectory(input.workingDirectory())
                .line("sh")
                .addArgs("-c", joiner.toString())
                .input(input.input())
                .timeout(input.timeout())
                .build());
        return new String(op, StandardCharsets.UTF_8);
    }

    private void installComponent(List<String> component, List<Map<String, String>> configuration)
            throws InterruptedException, IOException {
        //get the structure of local deployment with version
        final Map<String, ComponentDeploymentSpecification> localComponentSpec = prepareLocalComponent(component);
        // fulfill the local deployment with custom configuration
        for (Map.Entry<String, ComponentDeploymentSpecification> localComponent : localComponentSpec.entrySet()) {
            String componentName = localComponent.getKey();
            String componentVersion = localComponent.getValue().componentVersion();
            CommandInput command = getCliDeploymentCommand(componentName, componentVersion, configuration);
            createLocalDeploymentWithRetry(command, 0);
        }
    }

    private String getCliUpdateConfigArgs(String componentName, List<Map<String, String>> configuration)
            throws IOException {
        Map<String, Map<String, Object>> configurationUpdate = new HashMap<>();
        // config update for each component, in the format of <componentName, <MERGE/RESET, map>>
        updateConfigObject(componentName, configuration, configurationUpdate);
        if (configurationUpdate.isEmpty()) {
            return "";
        }
        return mapper.writeValueAsString(configurationUpdate);
    }

    // this only prepare the structure of local deployment and enter the version number
    @VisibleForTesting
    Map<String, ComponentDeploymentSpecification> prepareLocalComponent(
            List<String> component) {
        String name = component.get(0); // component name
        String value = component.get(1); // The entire yaml recipe
        ComponentOverrideNameVersion.Builder overrideNameVersion = ComponentOverrideNameVersion.builder()
                .name(name);
        String[] parts = value.split(":", 2); // The first : is version, and all contants after versions
        if (parts.length == 2) { // means it does have version number, and remainning are all contains
            overrideNameVersion.version(ComponentOverrideVersion.of(parts[0], parts[1]));
        } else { // this may not have a good version number
            overrideNameVersion.version(ComponentOverrideVersion.of("cloud", parts[0]));
        }
        overrides.component(name).ifPresent(overrideNameVersion::from);
        ComponentDeploymentSpecification.Builder builder = ComponentDeploymentSpecification.builder();
        componentPreparation.prepare(overrideNameVersion.build()).ifPresent(nameVersion -> {
            builder.componentVersion(nameVersion.version().value());
        });
        Map<String, ComponentDeploymentSpecification> components = new HashMap<>();
        components.put(name, builder.build());
        return components;
    }

    /**
     * Transform component config to CLI --update-config acceptable format. Put it in given configurationUpdate map.
     *
     * @param component           component name
     * @param configuration       list of configs given by cucumber step
     * @param configurationUpdate format: (componentName, (MERGE or RESET, (KV map)))
     */
    private void updateConfigObject(String component, List<Map<String, String>> configuration,
                                    Map<String, Map<String, Object>> configurationUpdate) throws IOException {
        if (configuration != null) {
            Map<String, Map<String, Object>> componentToConfig = new HashMap<>();
            for (Map<String, String> configKeyValue : configuration) {
                String value = configKeyValue.get("value");
                value = scenarioContext.applyInline(value);
                String[] parts = configKeyValue.get("key").split(":");
                String componentName;
                String path;
                if (parts.length == 1) {
                    componentName = component;
                    path = parts[0];
                } else {
                    componentName = parts[0];
                    path = parts[1];
                }

                Map<String, Object> config = componentToConfig.get(componentName);
                if (config == null) {
                    config = new HashMap<>();
                    componentToConfig.put(componentName, config);
                }
                Object objVal = value;
                try {
                    objVal = mapper.readValue(value, Map.class);
                } catch (IllegalArgumentException | IOException ignored) {
                }
                config.put(path, objVal);
            }
            for (Map.Entry<String, Map<String, Object>> entry : componentToConfig.entrySet()) {
                configurationUpdate.put(entry.getKey(), ImmutableMap.of(MERGE_CONFIG, entry.getValue()));
            }
        }
    }
}



