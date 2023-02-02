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

import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/*
This component rotates files with different name. Namely, the file is rotated and renamed by adding the timestamp and
 some seq number to differ from others. The rolling policy is LogBack.TimeBasedRollingPolicy,
 more information at https://logback.qos.ch/manual/appenders.html
 */
public class logGenerator implements Consumer<String[]> {
    private static final int MAX_HISTORY = 3;
    private static final String rotationNamePattern = "_%d{yyyy-MM-dd_HH-mm}_%i";
    private static double random;
    private String logFileName;
    private String logFileExtension;
    private double fileSize;
    private String fileSizeUnit;
    private double logWriteFreqSeconds;
    private int logMsgSizeBytes;
    private int fileSizeBytes;
    private int totalLogNumbers;
    private String targetLogFilePath;

    @Override
    public void accept(String[] args) {
        logFileName = args[0];
        logFileExtension = args[1];
        fileSize = Double.parseDouble(args[2]);
        fileSizeUnit = args[3];
        logWriteFreqSeconds = Double.parseDouble(args[4]);
        totalLogNumbers = Integer.parseInt(args[5]);
        targetLogFilePath = args[6];

        // the default log FIle path is where this instance locates. If running OTF, it will be on DUT. If running
        // the uni testing, it would be on the local machine.
        String logFilePath = null;
        if (targetLogFilePath.isEmpty()) {
            targetLogFilePath = System.getProperty("user.dir");
            System.out.println("file path in logGenerator: " + System.getProperty("user.dir"));
            System.out.println("user home: " + System.getProperty("user.home"));
            File currentFile = new File(targetLogFilePath);
            File logFileParent = currentFile.getParentFile().getParentFile();
            logFilePath = logFileParent.getAbsolutePath() + "/logs";
            System.out.println("new file path: " + logFilePath);
        }

        try {
            fileSizeBytes = getFileSizeInBytes(fileSize, fileSizeUnit);
        } catch (UnsupportedOperationException e) {
            System.exit(0);
        }

        // configure the logger by setting the context, rolling policy, encoder
        Logger logger = configureLogger(logFilePath, logFileName, logFileExtension, fileSizeBytes);

        // starting logging message
        performLogging(logger, totalLogNumbers, logWriteFreqSeconds);

        //verify
        File logFileFolder = new File(logFilePath);
        System.out.println("files in the path: " + logFileFolder.listFiles().length);
        for (File file : logFileFolder.listFiles()) {
            System.out.println(file.getName());
        }
    }

    private static Logger configureLogger(String currentPath, String fileName, String extension, int fileSizeBytes) {
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

    private static void performLogging(Logger logger, int totalLogNumbers, double logWriteFreqSeconds) {
        for (int i=0; i < totalLogNumbers; i++) {
            random = Math.random();
            StringBuilder sb = generateLogMessage(i, logger.getName());
            logger.info(sb.toString());
            try {
                TimeUnit.MILLISECONDS.sleep(Double.valueOf(logWriteFreqSeconds * 1000).longValue());
            } catch (InterruptedException e) {
                System.exit(0);
            }
        }
    }

    private static StringBuilder generateLogMessage(int seq, String loggerName) {
        String[] namesArray = {"Jimmy", "Bob", "Amy", "I", "He", "She", "They"};
        String[] verbsArray = {"play", "study", "develop", "know", "like"};
        String[] nounsArray = {"code", "math", "football", "computer", "chemistry"};

        int namesIndex = (int)(random * namesArray.length);
        int verbsIndex = (int)(random * verbsArray.length);
        int nounsIndex = (int)(random * nounsArray.length);
        StringBuilder sb = new StringBuilder().
                append(seq).
                append("-" + loggerName).
                append(" " + namesArray[namesIndex]).
                append(" " + verbsArray[verbsIndex]).
                append(" " + nounsArray[nounsIndex]);

        return sb;
    }

    private static int getFileSizeInBytes(double fileSize, String fileSizeUnit) {
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

