/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.utils;

import ch.qos.logback.classic.Logger;

import java.util.concurrent.TimeUnit;

public final class LoggingHelper {
    private static double random;
    public LoggingHelper() {
    }

    public static void performLogging(Logger logger, int totalLogNumbers, long logWriteFreqSeconds, int logMsgSizeBytes) {
        for (int i=0; i < totalLogNumbers; i++) {
            random = Math.random();
            StringBuilder sb = generateLogMessage(i, logMsgSizeBytes);
            logger.info(sb.toString());
            try {
                TimeUnit.SECONDS.sleep(logWriteFreqSeconds);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private static StringBuilder generateLogMessage(int seq, int targetMsgSize) {
        String[] namesArray = {"Jimmy", "Bob", "Amy", "I", "He", "She", "They"};
        String[] verbsArray = {"play", "study", "develop", "know", "like"};
        String[] nounsArray = {"code", "math", "football", "computer", "chemistry"};

        int namesIndex = (int)(random * namesArray.length);
        int verbsIndex = (int)(random * verbsArray.length);
        int nounsIndex = (int)(random * nounsArray.length);
        StringBuilder sb = new StringBuilder().
                append(seq).
                append(" " + namesArray[namesIndex]).
                append(" " + verbsArray[verbsIndex]).
                append(" " + nounsArray[nounsIndex]);

        padLogMessage(sb, targetMsgSize);
        return sb;
    }

    private static void padLogMessage(StringBuilder sb, int targetSize) {
        int size = sb.length();
        while (size < targetSize) {
            sb.append((char)(random * 96 + 32));
            size ++;
        }
    }
}