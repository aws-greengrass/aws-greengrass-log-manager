/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.logmanager.model;

import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Builder
@Data
@Getter
@Setter
public class CloudWatchAttemptLogInformation {
    @Builder.Default
    private List<InputLogEvent> logEvents = new ArrayList<>();
    @Builder.Default
    private Map<String, CloudWatchAttemptLogFileInformation> attemptLogFileInformationMap = new HashMap<>();
    private String componentName;
}
