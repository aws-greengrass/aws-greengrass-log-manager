/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.logmanager.model;

import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import org.slf4j.event.Level;

import java.nio.file.Path;
import java.util.regex.Pattern;

@Builder
@Data
@Getter
public class ComponentLogConfiguration {
    private Pattern fileNameRegex;
    private Path directoryPath;
    private String name;
    private Pattern multiLineStartPattern;
    @Builder.Default
    private Level minimumLogLevel = Level.INFO;
    private Long diskSpaceLimit;
    private boolean deleteLogFileAfterCloudUpload;
    private boolean uploadToCloudWatch;
    private ComponentType componentType;
}
