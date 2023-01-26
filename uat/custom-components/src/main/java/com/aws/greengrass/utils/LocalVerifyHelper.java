/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Scanner;

public final class LocalVerifyHelper {

    public LocalVerifyHelper() {
    }

    /*
    Provide number of log files created in Device during the logging. This is only used for developers to manually
    check results in the local machine
     */
    public static void verifyLoggingSucceed(String currentPath, String fileName, String extension) {
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
            e.printStackTrace();
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
}