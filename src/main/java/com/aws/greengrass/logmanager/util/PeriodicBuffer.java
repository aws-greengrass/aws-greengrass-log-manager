/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.logmanager.util;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

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

    private Map<K, V> buffer = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler;
    private final Consumer<Map<K, V>> flushHandler;
    private final long intervalSeconds;
    private final String bufferName;
    private final Object bufferLock = new Object();
    private final AtomicLong flushSuccessCount = new AtomicLong(0);
    private final AtomicLong flushFailureCount = new AtomicLong(0);
    private final AtomicInteger flushInProgress = new AtomicInteger(0);

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
        
        // Create a named thread factory for better debugging
        ThreadFactory namedThreadFactory = runnable -> {
            Thread thread = new Thread(runnable, "PeriodicBuffer-" + bufferName + "-Flusher");
            thread.setDaemon(true);
            return thread;
        };
        
        this.scheduler = Executors.newSingleThreadScheduledExecutor(namedThreadFactory);
    }

    /**
     * Starts the periodic buffer flushing operation.
     * Initializes a scheduled task that flushes the buffer at fixed intervals.
     *
     * @throws IllegalStateException if the buffer has already been shutdown
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
            logger.atTrace().kv("key", key).kv("bufferSize", buffer.size())
                    .log("Added item to buffer '{}'", bufferName);
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
            logger.atTrace().kv("key", key).kv("bufferSize", buffer.size())
                    .log("Removed item from buffer '{}'", bufferName);
        }
    }

    /**
     * Manually triggers a flush of the buffer.
     */
    public void flush() {
        flush(false);
    }

    /**
     * Triggers a flush of the buffer with option to wait for any in-progress flush.
     * 
     * @param waitForInProgress if true, waits for any in-progress flush to complete before proceeding
     */
    @SuppressWarnings("PMD.AvoidCatchingGenericException")
    private void flush(boolean waitForInProgress) {
        // Early check for empty buffer to avoid unnecessary locking
        synchronized (bufferLock) {
            if (buffer.isEmpty()) {
                logger.atDebug().log("Buffer '{}' is empty, nothing to flush", bufferName);
                return;
            }
        }

        // Check if another flush is already in progress
        if (!flushInProgress.compareAndSet(0, 1)) {
            if (waitForInProgress) {
                logger.atDebug().log("Flush already in progress for buffer '{}', waiting for completion", bufferName);
                // Wait for the current flush to complete
                while (flushInProgress.get() != 0) {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        logger.atWarn().log("Interrupted while waiting for flush to complete in buffer '{}'",
                                bufferName);
                        return;
                    }
                }
                // Try again after the previous flush completed
                flush(waitForInProgress);
            } else {
                logger.atDebug().log("Flush already in progress for buffer '{}', skipping", bufferName);
            }
            return;
        }

        try {
            Map<K, V> bufferToFlush;
            boolean shouldFlush;
            synchronized (bufferLock) {
                // Double-check if buffer is still non-empty
                if (buffer.isEmpty()) {
                    logger.atDebug().log("Buffer '{}' became empty, nothing to flush", bufferName);
                    // Don't return here - we need to go through finally block
                    bufferToFlush = new ConcurrentHashMap<>();
                    shouldFlush = false;
                } else {
                    logger.atInfo().log("Flushing buffer '{}' with {} items", bufferName, buffer.size());

                    // Swap the buffer instead of copying for better performance
                    bufferToFlush = buffer;
                    buffer = new ConcurrentHashMap<>();
                    shouldFlush = true;
                }
            }

            // Only proceed with flush if we have data
            if (shouldFlush) {
                long startTime = System.currentTimeMillis();
                try {
                    flushHandler.accept(bufferToFlush);
                    
                    long duration = System.currentTimeMillis() - startTime;
                    flushSuccessCount.incrementAndGet();
                    
                    logger.atDebug()
                            .kv("itemCount", bufferToFlush.size())
                            .kv("durationMs", duration)
                            .kv("successCount", flushSuccessCount.get())
                            .log("Successfully flushed buffer '{}'", bufferName);
                            
                } catch (RejectedExecutionException e) {
                    // Put the items back if flush was rejected
                    synchronized (bufferLock) {
                        bufferToFlush.forEach(buffer::putIfAbsent);
                    }
                    flushFailureCount.incrementAndGet();
                    logger.atError().cause(e)
                            .kv("failureCount", flushFailureCount.get())
                            .log("Flush rejected for buffer '{}', items restored", bufferName);
                } catch (RuntimeException e) {
                    // Catching RuntimeException because the flushHandler is a Consumer<Map<K, V>> that
                    // can throw any unchecked exception. We need to handle all possible runtime exceptions
                    // to ensure the buffer continues to function properly.
                    // CHECKSTYLE:OFF - We need to catch all runtime exceptions from user-provided handler
                    // PMD:OFF:AvoidCatchingGenericException - The flushHandler is a user-provided Consumer
                    // that can throw any unchecked exception, and we must ensure the buffer continues to
                    // function even if the handler fails. This is a valid use case for catching RuntimeException.
                    flushFailureCount.incrementAndGet();
                    logger.atError().cause(e)
                            .kv("failureCount", flushFailureCount.get())
                            .log("Failed to flush buffer '{}'. Items lost.", bufferName);
                    // CHECKSTYLE:ON
                    // PMD:ON:AvoidCatchingGenericException
                }
            }
        } finally {
            flushInProgress.set(0);
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
    @SuppressWarnings("PMD.AvoidCatchingGenericException")
    public void shutdown() {
        if (isShutdown) {
            return;
        }

        isShutdown = true;
        logger.atInfo().log("Shutting down buffer '{}'", bufferName);

        // Cancel the scheduled task
        if (flushTask != null) {
            flushTask.cancel(false);
        }

        // Flush any remaining items, waiting for any in-progress flush to complete
        try {
            flush(true);
        } catch (RuntimeException e) {
            // Catching RuntimeException because the flush method can throw any unchecked exception
            // from the flushHandler. We need to ensure shutdown completes even if the final flush fails.
            // CHECKSTYLE:OFF - We need to catch all runtime exceptions from user-provided handler
            // PMD:OFF:AvoidCatchingGenericException - The flushHandler called via flush(true) can throw
            // any unchecked exception. We must ensure shutdown completes successfully even if the final
            // flush operation fails. This is a valid use case for catching RuntimeException.
            logger.atError().cause(e).log("Error during final flush");
            // CHECKSTYLE:ON
            // PMD:ON:AvoidCatchingGenericException
        }

        // Shutdown the scheduler
        scheduler.shutdownNow();

        logger.atInfo()
                .kv("successfulFlushes", flushSuccessCount.get())
                .kv("failedFlushes", flushFailureCount.get())
                .log("Shutdown complete for buffer '{}'", bufferName);
    }

    /**
     * Gets the number of successful flushes.
     *
     * @return the number of successful flushes
     */
    public long getFlushSuccessCount() {
        return flushSuccessCount.get();
    }

    /**
     * Gets the number of failed flushes.
     *
     * @return the number of failed flushes
     */
    public long getFlushFailureCount() {
        return flushFailureCount.get();
    }
}
