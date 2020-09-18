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
public class CommonLogsConfiguration {
    @JsonProperty(defaultValue = "INFO")
    private String minimumLogLevel;
    private String diskSpaceLimit;
    private String diskSpaceLimitUnit;
    @JsonProperty(defaultValue = "false")
    private String deleteLogFileAfterCloudUpload;
}
