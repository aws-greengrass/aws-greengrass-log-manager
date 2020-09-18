/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.logmanager.model.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ComponentsLogConfiguration extends CommonLogsConfiguration {
    @JsonProperty(required = true)
    private String componentName;
    @JsonProperty(required = true)
    private String logFileRegex;
    @JsonProperty(required = true)
    private String logFileDirectoryPath;
    @JsonProperty(defaultValue = "\"^[^\\\\s]+(\\\\s+[^\\\\s]+)*$\"")
    private String multiLineStartPattern;
}
