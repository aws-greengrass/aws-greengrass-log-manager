/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.testing.features;
import com.aws.greengrass.testing.api.ComponentPreparationService;
import com.aws.greengrass.testing.api.device.model.CommandInput;
import com.aws.greengrass.testing.model.ScenarioContext;
import com.aws.greengrass.testing.model.TestContext;
import com.aws.greengrass.testing.platform.Platform;
import io.cucumber.guice.ScenarioScoped;
import io.cucumber.java.en.And;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import static com.aws.greengrass.testing.component.LocalComponentPreparationService.ARTIFACTS_DIR;
import static com.aws.greengrass.testing.component.LocalComponentPreparationService.LOCAL_STORE;
import static com.aws.greengrass.testing.component.LocalComponentPreparationService.RECIPE_DIR;

@ScenarioScoped
public class GreengrassCliSteps {
    public static final String LOCAL_DEPLOYMENT_ID = "localDeploymentId";
    private Platform platform;
    private Path artifactPath;
    private Path recipePath;
    private TestContext testContext;
    private ScenarioContext scenarioContext;
    private ComponentPreparationService componentPreparation;
    private WaitSteps waitSteps;
    private static Logger LOGGER = LogManager.getLogger(GreengrassCliSteps.class);

    @Inject
    @SuppressWarnings("MissingJavadocMethod")
    public GreengrassCliSteps(Platform platform, TestContext testContext,
                              ComponentPreparationService componentPreparation,
                              ScenarioContext scenarioContext, WaitSteps waitSteps) {
        this.platform = platform;
        this.testContext = testContext;
        this.componentPreparation = componentPreparation;
        this.scenarioContext = scenarioContext;
        this.waitSteps = waitSteps;
        this.artifactPath = testContext.testDirectory().resolve(LOCAL_STORE).resolve(ARTIFACTS_DIR);
        ;
        this.recipePath = testContext.testDirectory().resolve(LOCAL_STORE).resolve(RECIPE_DIR);
    }
    /**
     * Verify a component status using the greengrass-cli.
     *
     * @param componentName name of the component
     * @param status        {RUNNING, BROKEN, FINISHED}
     * @param timeout       max seconds to wait before timing out
     * @throws InterruptedException {@link InterruptedException}
     */
    @And("I verify the {word} component is {word} using the greengrass-cli after {int} seconds")
    public void verifyComponentIsRunning(String componentName, String status, int timeout) throws InterruptedException {
        waitSteps.untilTrue(() -> this.getComponentStatus(componentName, status), timeout, TimeUnit.SECONDS);
    }
    private boolean getComponentStatus(String componentName, String componentStatus) {
        String response = platform.commands().executeToString(CommandInput.builder()
                .line(testContext.installRoot().resolve("bin").resolve("greengrass-cli").toString())
                .addAllArgs(Arrays.asList("component", "details", "--name", componentName))
                .build());
        LOGGER.debug(String.format("component status response received for component %s is %s",
                componentName, response));

        return response.contains(String.format("State: %s", componentStatus));
    }
}


