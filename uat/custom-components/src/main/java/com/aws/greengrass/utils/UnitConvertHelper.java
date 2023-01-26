/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.utils;

import ch.qos.logback.core.util.FileSize;

public final class UnitConvertHelper {
    public UnitConvertHelper() {
    }

    public static int getFileSizeInBytes(double fileSize, String fileSizeUnit) {
        int fileSizeBytes = 0;
        switch (fileSizeUnit) {
            case "KB":
                fileSizeBytes = (int) (fileSize * FileSize.KB_COEFFICIENT);
                break;
            case "MB":
                fileSizeBytes = (int) (fileSize * FileSize.MB_COEFFICIENT);
                break;
            case "GB":
                fileSizeBytes = (int) (fileSize * FileSize.GB_COEFFICIENT);
                break;
            default:
                break;
        }
        return fileSizeBytes;
    }
}