/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass;

import io.cucumber.guice.ScenarioScoped;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;

@ScenarioScoped
class LogManagerSteps {
    @Given("I say hello LogManager")
    public void logManagerSayHello() {
        System.out.println("LogManager says hello");
    }

    @Then("It works")
    public void itWorked() {
        System.out.println("It works");
    }
}

