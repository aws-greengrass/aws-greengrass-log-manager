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
import java.util.function.Consumer;

public class logGenerator implements Consumer<String[]> {
    private double random = Math.random();
    @Override
    public void accept(String[] args) {
        System.out.println("This will be a custom component, now demonstrate we receive some parameters");
        for (String arg : args) {
            System.out.println(arg);
        }
        String currentPath = System.getProperty("user.dir");
        Logger logger = prepareLogger(currentPath);

        logger.info("hello world !!!!!");

        for (int i=0; i < 10; i ++) {
            StringBuilder sb = generateLogMessage();
            logger.info(sb.toString());
        }

        verifyLoggingSucceed(currentPath);

    }

    private StringBuilder generateLogMessage() {
        String[] namesArray = {"Jimmy", "Bob", "Amy", "I", "He", "She", "They"};
        String[] verbsArray = {"play", "study", "develop", "know", "like"};
        String[] nounsArray = {"code", "math", "football", "computer", "chemistry"};

        int seq = 1;
        int namesIndex = (int)(random*namesArray.length);
        int verbsIndex = (int)(random*verbsArray.length);
        int nounsIndex = (int)(random*nounsArray.length);
        StringBuilder sb = new StringBuilder().
                        append(seq).
                        append(" " + namesArray[namesIndex]).
                        append(" " + verbsArray[verbsIndex]).
                        append(" " + nounsArray[nounsIndex]);

        padLogMessage(sb);
        return sb;
    }

    private void padLogMessage(StringBuilder sb) {
        int size = sb.length();
        int targetSize = 1024;
        while (size < targetSize) {
            sb.append((char)(random*96+32));
            size ++;
        }
    }

    private Logger prepareLogger(String currentPath) {
        LoggerContext loggerContext = new LoggerContext();
        Logger logger = loggerContext.getLogger("logGenerator");

        // appender: output destination
        RollingFileAppender<ILoggingEvent> appender = new RollingFileAppender<>();
        appender.setFile(currentPath + "/test.log");
        appender.setAppend(true);
        appender.setContext(loggerContext);

        // rolling policy
        SizeAndTimeBasedRollingPolicy rollingPolicy = new SizeAndTimeBasedRollingPolicy<>();
        rollingPolicy.setMaxFileSize(FileSize.valueOf("10MB"));
        rollingPolicy.setMaxHistory(Integer.parseInt("3"));
        rollingPolicy.setFileNamePattern(currentPath + "/test_%d{yyyy-MM-dd_HH}_%i.log");
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

    private void verifyLoggingSucceed(String currentPath) {
        File file = new File(currentPath);
        listMyFiles(file);

        File testFile = new File(currentPath + "/test.log");
        try {
            if (testFile.createNewFile()) {
                System.out.println("new file created at "+ testFile.getAbsolutePath());
            } else {
                System.out.println("file existed");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        verifyFileIsWritten(testFile);
    }

    private void verifyFileIsWritten(File testFile) {
        // verify file is written
        Scanner myReader = null;
        try {
            myReader = new Scanner(testFile);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        while (myReader.hasNextLine()) {
            String data = myReader.nextLine();
            System.out.println(data);
        }
        myReader.close();
    }

    private void listMyFiles(File file) {
        String[] pathnames = file.list();
        System.out.println("listing files: " + pathnames.length);
        for (String pathname : pathnames) {
            System.out.println(pathname);
        }
    }
}

