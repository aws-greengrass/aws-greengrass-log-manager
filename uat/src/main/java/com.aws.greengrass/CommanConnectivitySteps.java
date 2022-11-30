package com.aws.greengrass;

import com.aws.greengrass.testing.model.ScenarioContext;
import com.aws.greengrass.platforms.Platform;
import com.google.inject.Inject;
import io.cucumber.guice.ScenarioScoped;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.When;

import java.io.IOException;

@ScenarioScoped
public class CommanConnectivitySteps {

    private final ScenarioContext scenarioContext;


    @Inject
    public CommanConnectivitySteps(ScenarioContext scenarioContext) {
        this.scenarioContext = scenarioContext;
    }
    @Given("device network connectivity is {word}")
    @When("I set device network connectivity to {word}")
    public void setDeviceNetwork(final String connectivity) throws IOException, InterruptedException {
        if ("offline".equals(connectivity.toLowerCase())) {
            Platform.getInstance().getNetworkUtils().disconnectNetwork();
        } else {
            Platform.getInstance().getNetworkUtils().recoverNetwork();
        }
    }
}
