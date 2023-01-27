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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Scanner;
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
        totalLogNumbers = Integer.parseInt(args[5]);

        fileSizeBytes = getFileSizeInBytes(fileSize, fileSizeUnit);

        String currentPath = System.getProperty("user.dir");

        // configure the logger by setting the context, rolling policy, encoder
        Logger logger = configureLogger(currentPath, logFileName, logFileExtension, fileSizeBytes);

        // starting logging message
        performLogging(logger, totalLogNumbers, logWriteFreqSeconds);

        // print list of log files and results would be logged by GG nucleus, thus it would be eventually presented in
        // local machine. This just provide an easier way to verify some logging info manually, and will not be used
        // for UAT.
        verifyLoggingSucceed(currentPath, logFileName, logFileExtension);
    }

    private Logger configureLogger(String currentPath, String fileName, String extension, int fileSizeBytes) {
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

    private static void performLogging(Logger logger, int totalLogNumbers, long logWriteFreqSeconds) {
        for (int i=0; i < totalLogNumbers; i++) {
            random = Math.random();
            StringBuilder sb = generateLogMessage(i, logger.getName());
            logger.info(sb.toString());
            try {
                TimeUnit.SECONDS.sleep(logWriteFreqSeconds);
            } catch (InterruptedException e) {
                e.printStackTrace();
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

    /*
    Intentionally provide number of log files created in Device during the logging. This is only used for developers to
    manually
    check results in the local machine
    */
    private static void verifyLoggingSucceed(String currentPath, String fileName, String extension) {
        File directory = new File(currentPath);
        listMyFiles(directory);

        File activeLogFile = new File(currentPath + "/" + fileName + "." + extension);
        try {
            if (activeLogFile.createNewFile()) {
                System.out.println("new file created at "+ activeLogFile.getAbsolutePath());
            } else {
                System.out.println("file existed");
            }
        } catch (IOException e) {
            System.exit(0);
        }

        // just try to verify if log messages are correctly written locally, instead of SSH to DUT devices. This
        // should not be used as verification in UAT.
        verifyFileIsWritten(activeLogFile);
    }

    private static void listMyFiles(File file) {
        String[] pathnames = file.list();
        System.out.println("listing files: " + pathnames.length);
        for (String pathname : pathnames) {
            System.out.println(pathname);
        }
    }

    /*
    This is only used to provide a way to read logs in file from local results, instead of SSH to the device.
     */
    private static void verifyFileIsWritten(File file) {
        // verify file is written
        Scanner myReader = null;
        try {
            myReader = new Scanner(file);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        while (myReader.hasNextLine()) {
            String data = myReader.nextLine();
            System.out.println(data);
        }
        myReader.close();
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
                break;
        }
        return fileSizeBytes;
    }
}

