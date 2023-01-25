/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.artifacts;


import java.util.function.Consumer;

public class logGenerator implements Consumer<String[]> {
    @Override
    public void accept(String[] args) {
        System.out.println("This will be a custom component, now demonstrate we receive some parameters");
        for (String arg : args) {
            System.out.println(arg);
        }
    }
}

