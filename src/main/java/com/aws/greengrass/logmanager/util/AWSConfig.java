/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.logmanager.util;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import lombok.Getter;

@Getter
public class AWSConfig {
    private static final Logger LOGGER = LogManager.getLogger(AWSConfig.class);

    // Env Variable set by nucleus for FIPS mode
    // This is configured in GGC config file and is not exposed in AWS Console.
    static final String FIPS_MODE = "AWS_GG_FIPS_MODE";
    static final boolean FIPS_MODE_DEFAULT_VALUE = false;

    private final boolean fipsEnabled;

    AWSConfig() {
        this.fipsEnabled = getBooleanValue(getValue(FIPS_MODE), FIPS_MODE, FIPS_MODE_DEFAULT_VALUE);
    }

    /**
     * Helper method to get configuration value from System Parameter or environment.
     *
     * @param optionName name of the parameter/environment variable
     * @return the value or null
     */
    private String getValue(String optionName) {
        //Get SystemProperty first, otherwise get from Environment
        String value = System.getProperty(optionName);
        if (value == null) {
            value = System.getenv(optionName);
        }
        return value;
    }

    /**
     * Helper method that handles boolean parameters.
     *
     * @param value        value passed in through the parameter/environment variable.
     * @param optionName   name of the parameter/environment variable.
     * @param defaultValue default value.
     * @return returns the parsed value of the parameter. If the value passed to the parameter is not a
     *     valid boolean string, the default value for the parameter will be returned.
     */
    private boolean getBooleanValue(String value, String optionName, boolean defaultValue) {
        if (value != null) {
            if ("true".equalsIgnoreCase(value)) {
                return true;
            } else if ("false".equalsIgnoreCase(value)) {
                return false;
            }
            LOGGER.warn("Unable to parse value {} for option {}. Using default value {}",
                    value, optionName, defaultValue);
        }
        return defaultValue;
    }
}
