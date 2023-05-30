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
import software.amazon.awssdk.aws.greengrass.model.InvalidArgumentsError;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/*
This component rotates files with different name. Namely, the file is rotated and renamed by adding the timestamp and
 some seq number to differ from others. The rolling policy is LogBack.TimeBasedRollingPolicy,
 more information at https://logback.qos.ch/manual/appenders.html
 */
public class LogGenerator implements Consumer<String[]> {
    private static final String rotationNamePattern = "_%d{yyyy-MM-dd_HH-mm}_%i";
    private String logFileName;
    private int writeFreqMs;
    private int fileSizeBytes;
    private int numberOfLogs;
    private String logDirectory;


    @Override
    public void accept(String[] args) {
        init(args);

        try {
            generateLogs();
        } catch (Exception e) {
            System.exit(0);
        }
    }

    private void init(String[] args) {
        logFileName = args[0];
        fileSizeBytes = getFileSizeInBytes(Double.parseDouble(args[1]), args[2]);
        writeFreqMs = Integer.parseInt(args[3]);
        numberOfLogs = Integer.parseInt(args[4]);
        logDirectory = args[5];

        if (logDirectory.isEmpty()) {
            throw new InvalidArgumentsError("LogDirectory is required");
        }
    }

    private void generateLogs() throws InterruptedException {
        Logger logger = configureLogger();

        for (int i = 1; i <= numberOfLogs; i++) {
            String logLine = String.format("(seq: %d)", i);
            logger.info(logLine); // INFO LogGenerator (seq: 1)
            TimeUnit.MILLISECONDS.sleep(writeFreqMs);
        }
    }

    private Logger configureLogger() {
        LoggerContext loggerContext = new LoggerContext();
        Logger logger = loggerContext.getLogger("LogGenerator");

        // appender: output destination
        RollingFileAppender<ILoggingEvent> appender = new RollingFileAppender<>();
        appender.setFile(logDirectory + "/" + logFileName + ".log");
        appender.setAppend(true);
        appender.setContext(loggerContext);

        // rolling policy
        SizeAndTimeBasedRollingPolicy rollingPolicy = new SizeAndTimeBasedRollingPolicy<>();
        rollingPolicy.setMaxFileSize(new FileSize(fileSizeBytes));
        rollingPolicy.setFileNamePattern(logDirectory + "/" + logFileName + rotationNamePattern + ".log");
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

    private int getFileSizeInBytes(double fileSize, String fileSizeUnit) {
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
                throw new UnsupportedOperationException("Unsupported file size unit");
        }
        return fileSizeBytes;
    }
}

