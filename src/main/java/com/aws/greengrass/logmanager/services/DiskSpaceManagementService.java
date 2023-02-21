/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.logmanager.services;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.logmanager.model.ComponentLogConfiguration;
import com.aws.greengrass.util.Coerce;
import com.aws.greengrass.util.Utils;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.Watchable;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class DiskSpaceManagementService {
    private static final Logger logger = LogManager.getLogger(DiskSpaceManagementService.class);

    private final Object spaceManagementLock = new Object();
    private Future<?> spaceManagementThread;
    private final ExecutorService executorService;


    /**
     * Constructor.
     *
     * @param executorService {@link ExecutorService}
     */
    public DiskSpaceManagementService(ExecutorService executorService) {
        this.executorService = executorService;
    }


    public void start(Map<String, ComponentLogConfiguration> componentLogConfigurations) {
        this.scheduleSpaceManagementThread(componentLogConfigurations);
    }

    @SuppressWarnings("PMD.AvoidCatchingThrowable")
    private void scheduleSpaceManagementThread(Map<String, ComponentLogConfiguration> componentLogConfigurations) {
        synchronized (spaceManagementLock) {
            if (spaceManagementThread != null) {
                spaceManagementThread.cancel(true);
            }
            spaceManagementThread = this.executorService.submit(() -> {
                try {
                    logger.atInfo().log("Starting space management thread");
                    startWatchServiceOnLogFilePaths(componentLogConfigurations);
                } catch (IOException e) {
                    //TODO: fail the deployment?
                    logger.atError().cause(e).log("Unable to start space management thread.");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Throwable e) {
                    logger.atError().log("Failure in log manager space management", e);
                    scheduleSpaceManagementThread(componentLogConfigurations); // restart space management
                }
            });
        }
    }

    /**
     * Starts a Watch Service on all the log directories where customer has specified disk space limit.
     * If any file is modified and created in any of those directories, we get an event containing that information.
     * The log manager will then verify that the size of all log files in that directory for the component does not
     * exceed the disk space limit set by the customer. If it does, the log manager will delete the log files until
     * the limit is met.
     *
     * @throws IOException          if unable to initialise a new Watch Service.
     * @throws InterruptedException if thread is shutdown
     */
    private void startWatchServiceOnLogFilePaths(Map<String, ComponentLogConfiguration> componentLogConfigurations)
            throws IOException, InterruptedException {
        //TODO: Optimize this.
        // The optimization would be to have best of both worlds. The file watcher will mark the changed components
        // log directories. Another scheduled thread will look at that and clean up files if necessary.
        try (WatchService watchService = FileSystems.getDefault().newWatchService()) {
            try {
                componentLogConfigurations.forEach((componentName, componentLogConfiguration) -> {
                    // Only register the path of a component if the disk space limit is set.
                    if (componentLogConfiguration != null && componentLogConfiguration.getDiskSpaceLimit() != null
                            && componentLogConfiguration.getDiskSpaceLimit() > 0) {
                        Path path = componentLogConfiguration.getDirectoryPath();
                        try {
                            path.register(watchService, StandardWatchEventKinds.ENTRY_CREATE,
                                    StandardWatchEventKinds.ENTRY_MODIFY);
                            logger.atDebug().kv("filePath", path).log("Start watching file for log space management.");
                        } catch (IOException e) {
                            logger.atError().cause(e).log("Unable to watch {} for log space management.",
                                    path);
                        }
                    }
                });

                while (true) {
                    final WatchKey watchKey = watchService.take();
                    final Watchable watchable = watchKey.watchable();

                    //Since we are registering only paths in thw watch service, the watchables must be paths
                    if (watchable instanceof Path) {
                        final Path directory = (Path) watchable;
                        Map<String, ComponentLogConfiguration> updatedComponentsConfiguration = new HashMap<>();
                        // Based on the directory and file name created/modified, the log manager will store the
                        // component information which need to be checked.
                        List<ComponentLogConfiguration> allComponentsConfiguration =
                                componentLogConfigurations.values().stream()
                                        .filter(componentLogConfiguration ->
                                                componentLogConfiguration.getDirectoryPath().equals(directory))
                                        .collect(Collectors.toList());

                        // The events can have multiple log files in the same directory. We need to make sure we get the
                        // correct component based on the file name pattern.
                        for (WatchEvent<?> event : watchKey.pollEvents()) {
                            String fileName = Coerce.toString(event.context());
                            if (fileName == null) {
                                continue;
                            }
                            List<ComponentLogConfiguration> list = allComponentsConfiguration.stream()
                                    .filter(componentLogConfiguration -> componentLogConfiguration
                                            .getFileNameRegex().matcher(fileName).find())
                                    .collect(Collectors.toList());
                            list.forEach(componentLogConfiguration ->
                                    updatedComponentsConfiguration.putIfAbsent(componentLogConfiguration.getName(),
                                            componentLogConfiguration));
                        }

                        // Get the total log files size in the directories for all updates components. If the log files
                        // size exceed the limit set by the customer, the log manager will figure out the minimum bytes
                        // to be deleted inorder to meet the limit. It will then go ahead and delete the oldest files
                        // until the minimum bytes have been deleted.
                        for (ComponentLogConfiguration componentLogConfiguration :
                                updatedComponentsConfiguration.values()) {
                            try {
                                deleteFilesIfNecessary(componentLogConfiguration);
                            } catch (UncheckedIOException e) {
                                logger.atWarn().log("Unchecked error thrown when collecting files to be deleted",
                                        Utils.getUltimateCause(e));
                            }
                        }
                    }

                    if (!watchKey.reset()) {
                        logger.atError().log("Log Space Management encountered an issue. Returning.");
                        scheduleSpaceManagementThread(componentLogConfigurations);
                        break;
                    }
                }
            } catch (IOException e) {
                // If there is any other IOException, then we should restart the thread.
                scheduleSpaceManagementThread(componentLogConfigurations);
            }
        }
    }

    /**
     * Deletes the oldest log files which matches the file name pattern. The log files will be deleted until the
     * minimum number of bytes have been deleted.
     *
     * @param componentLogConfiguration Log configuration to investigate for deletion
     * @throws IOException for errors during directory walking
     */
    private void deleteFilesIfNecessary(ComponentLogConfiguration componentLogConfiguration) throws IOException {
        if (componentLogConfiguration.getDiskSpaceLimit() == null) {
            return;
        }
        try (Stream<Path> fileStream = Files.walk(componentLogConfiguration.getDirectoryPath())
                .filter(p -> {
                    File file = p.toFile();
                    return file.isFile() && componentLogConfiguration.getFileNameRegex()
                            .matcher(file.getName()).find();
                })) {
            List<Path> paths = fileStream.collect(Collectors.toList());
            long totalBytes = paths.stream().mapToLong(p -> p.toFile().length()).sum();
            long minimumBytesToBeDeleted = totalBytes - componentLogConfiguration.getDiskSpaceLimit();

            // If we don't need to remove any bytes, or if the file count is only 1 (or less), then there's nothing
            // to do.
            if (minimumBytesToBeDeleted <= 0 || paths.size() < 2) {
                return;
            }

            long bytesDeleted = 0;
            // Sort the files by the last modified time.
            paths.sort(Comparator.comparingLong((p) -> p.toFile().lastModified()));
            int fileIndex = 0;
            // stop before the end to skip the active file which should have the newest modified time
            while (bytesDeleted < minimumBytesToBeDeleted && fileIndex < paths.size() - 1) {
                Path fileToBeDeleted = paths.get(fileIndex++);
                long fileSize = fileToBeDeleted.toFile().length();
                try {
                    Files.deleteIfExists(fileToBeDeleted);
                } catch (IOException e) {
                    logger.atWarn().log("Unable to delete file: {}", fileToBeDeleted.toAbsolutePath(), e);
                    break;
                }
                logger.atInfo().log("Successfully deleted file: {}", fileToBeDeleted.toAbsolutePath());
                bytesDeleted += fileSize;
            }
        }
    }
}