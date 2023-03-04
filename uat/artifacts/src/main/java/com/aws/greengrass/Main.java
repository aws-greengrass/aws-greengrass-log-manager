/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass;

import java.util.function.Consumer;

public final class Main {
    private Main(){}
    public static void main(String[] args) throws ClassNotFoundException, IllegalAccessException,
            InstantiationException {
        ((Consumer<String[]>) Class.forName("com.aws.greengrass.artifacts." + System.getProperty("componentName"))
                .newInstance()).accept(args);
    }
}
