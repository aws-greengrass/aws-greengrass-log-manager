/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.logmanager.util;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import com.aws.greengrass.util.Utils;

/**
 * Set of methods for LogLine digest
 */

public class Digest {
    public static final String SHA_256 = "SHA-256";

    public Digest() {
    }

    public String caculate(String logLines) throws NoSuchAlgorithmException {
        return calculate(SHA_256, logLines);

    }

    private String calculate(String algorithm, String logLines) throws NoSuchAlgorithmException {
        if (Utils.isEmpty(logLines)) {
            throw new IllegalArgumentException("Input logLine is empty");
        }
        MessageDigest digester = MessageDigest.getInstance(algorithm);
        return Base64.getEncoder().encodeToString(digester.digest(logLines.getBytes(StandardCharsets.UTF_8)));
    }
}
