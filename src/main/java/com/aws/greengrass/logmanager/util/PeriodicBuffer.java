/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.logmanager.util;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.Map;

/**
 * Generic buffer that accumulates updates and flushes them periodically.
 * This can be used for batching configuration updates or other operations
 * that benefit from being grouped together.
 *
 * @param <K> Key type for the buffered items
 * @param <V> Value type for the buffered items
 */
public class PeriodicBuffer<K, V> {
    private static final Logger logger = LogManager.getLogger(PeriodicBuffer.class);

    private final Map<K, V> buffer = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final Consumer<Map<K, V>> flushHandler;
    private final long intervalSeconds;
    private final String bufferName;
    private final Object bufferLock = new Object();

    private ScheduledFuture<?> flushTask;
    private volatile boolean isShutdown = false;

    /**
     * Creates a new periodic buffer.
     *
     * @param bufferName Name for logging purposes
     * @param intervalSeconds How often to flush the buffer
     * @param flushHandler Function to handle flushing buffered items
     */
    public PeriodicBuffer(String bufferName, long intervalSeconds, Consumer<Map<K, V>> flushHandler) {
        this.bufferName = bufferName;
        this.intervalSeconds = intervalSeconds;
        this.flushHandler = flushHandler;
    }

    /**
     * Starts the periodic flushing.
     */
    public void start() {
        if (isShutdown) {
            throw new IllegalStateException("Buffer has been shutdown");
        }

        flushTask = scheduler.scheduleAtFixedRate(
                this::flush,
                intervalSeconds,
                intervalSeconds,
                TimeUnit.SECONDS
        );

        logger.atInfo().log("Started periodic buffer '{}' with {}-second interval", bufferName, intervalSeconds);
    }

    /**
     * Adds an item to the buffer.
     *
     * @param key The key
     * @param value The value
     */
    public void put(K key, V value) {
        if (isShutdown) {
            logger.atWarn().log("Attempted to add to shutdown buffer '{}'", bufferName);
            return;
        }

        synchronized (bufferLock) {
            buffer.put(key, value);
        }
    }

    /**
     * Removes an item from the buffer.
     *
     * @param key The key to remove
     */
    public void remove(K key) {
        synchronized (bufferLock) {
            buffer.remove(key);
        }
    }

    /**
     * Manually triggers a flush of the buffer.
     */
    public void flush() {
        synchronized (bufferLock) {
            if (buffer.isEmpty()) {
                return;
            }

            logger.atInfo().log("Flushing buffer '{}' with {} items", bufferName, buffer.size());

            try {
                // Create a copy to pass to the handler
                Map<K, V> bufferCopy = new ConcurrentHashMap<>(buffer);
                flushHandler.accept(bufferCopy);

                // Clear the buffer after successful flush
                buffer.clear();

                logger.atDebug().log("Successfully flushed buffer '{}'", bufferName);
            } catch (Exception e) {
                logger.atError().cause(e).log("Failed to flush buffer '{}'", bufferName);
            }
        }
    }

    /**
     * Returns the current size of the buffer.
     */
    public int size() {
        synchronized (bufferLock) {
            return buffer.size();
        }
    }

    /**
     * Checks if the buffer is empty.
     */
    public boolean isEmpty() {
        synchronized (bufferLock) {
            return buffer.isEmpty();
        }
    }

    /**
     * Shuts down the buffer, flushing any remaining items.
     */
    public void shutdown() {
        if (isShutdown) {
            return;
        }

        isShutdown = true;

        // Cancel the scheduled task
        if (flushTask != null) {
            flushTask.cancel(false);
        }

        // Flush any remaining items
        flush();

        // Shutdown the scheduler
        scheduler.shutdown();

        logger.atInfo().log("Shutdown buffer '{}'", bufferName);
    }
}