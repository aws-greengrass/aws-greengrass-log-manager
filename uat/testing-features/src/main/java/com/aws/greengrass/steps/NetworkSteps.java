/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.steps;

import com.aws.greengrass.NetworkUtils;
import io.cucumber.java.Before;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.When;

import java.io.IOException;

public class NetworkSteps {

    @Before
    public void connectToNetwork() throws IOException, InterruptedException {
        NetworkUtils.recoverNetwork();
    }

    @Given("device network connectivity is {word}")
    @When("I set device network connectivity to {word}")
    public void setDeviceNetwork(final String connectivity) throws IOException, InterruptedException {
        if ("offline".equals(connectivity.toLowerCase())) {
            NetworkUtils.disconnectNetwork();
        } else {
            NetworkUtils.recoverNetwork();
        }
    }
}
