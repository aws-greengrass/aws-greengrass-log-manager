/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.artifacts;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.rolling.RollingFileAppender;
import ch.qos.logback.core.rolling.SizeAndTimeBasedRollingPolicy;
import ch.qos.logback.core.util.FileSize;

import java.util.function.Consumer;

import static com.aws.greengrass.utils.LocalVerifyHelper.verifyLoggingSucceed;
import static com.aws.greengrass.utils.LoggingHelper.performLogging;
import static com.aws.greengrass.utils.UnitConvertHelper.getFileSizeInBytes;

public class logGeneratorDiffName implements Consumer<String[]> {
    private static final int MAX_HISTORY = 3;
    private static final String rotationNamePattern = "_%d{yyyy-MM-dd_HH-mm}_%i";
    private String logFileName;
    private String logFileExtension;
    private double fileSize;
    private String fileSizeUnit;
    private Long logWriteFreqSeconds;
    private int logMsgSizeBytes;
    private int fileSizeBytes;
    private int totalLogNumbers;

    @Override
    public void accept(String[] args) {
        logFileName = args[0];
        logFileExtension = args[1];
        fileSize = Double.parseDouble(args[2]);
        fileSizeUnit = args[3];
        logWriteFreqSeconds = Long.parseLong(args[4]);
        logMsgSizeBytes = Integer.parseInt(args[5]);
        totalLogNumbers = Integer.parseInt(args[6]);

        fileSizeBytes = getFileSizeInBytes(fileSize, fileSizeUnit);

        String currentPath = System.getProperty("user.dir");
        Logger logger = prepareLogger(currentPath, logFileName, logFileExtension, fileSizeBytes);

        logger.info("hello world !!!!!");

        // starting logging message
        performLogging(logger, totalLogNumbers, logWriteFreqSeconds, logMsgSizeBytes);

        // print list of log files and results would be presented in local machine, it is easier to verify locally
        // and manuaal. It is not used for the UAT verification.
        verifyLoggingSucceed(currentPath, logFileName, logFileExtension);
    }

    private Logger prepareLogger(String currentPath, String fileName, String extension, int fileSizeBytes) {
        LoggerContext loggerContext = new LoggerContext();
        Logger logger = loggerContext.getLogger("logGenerator");

        // appender: output destination
        RollingFileAppender<ILoggingEvent> appender = new RollingFileAppender<>();
        appender.setFile(currentPath + "/" + fileName + "." + extension);
        appender.setAppend(true);
        appender.setContext(loggerContext);

        // rolling policy
        SizeAndTimeBasedRollingPolicy rollingPolicy = new SizeAndTimeBasedRollingPolicy<>();
        rollingPolicy.setMaxFileSize(new FileSize(fileSizeBytes));
        rollingPolicy.setMaxHistory(MAX_HISTORY);
        rollingPolicy.setFileNamePattern(currentPath + "/" + fileName + rotationNamePattern + "." + extension);
        rollingPolicy.setParent(appender);
        rollingPolicy.setContext(loggerContext);
        appender.setRollingPolicy(rollingPolicy);
        rollingPolicy.start();

        // encoder
        PatternLayoutEncoder encoder = new PatternLayoutEncoder();
        encoder.setPattern("%d{yyyy-MM-dd HH:mm:ss} %-5level %logger{36} - %msg%n");
        encoder.setContext(loggerContext);
        encoder.start();

        // attach encoder to appender, then start
        appender.setEncoder(encoder);
        appender.start();

        // set logger
        logger.addAppender(appender);
        logger.setLevel(Level.INFO);

        return logger;
    }

}

