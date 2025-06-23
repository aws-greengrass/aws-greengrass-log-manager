/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.logmanager.util;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
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
    private volatile CompletableFuture<Void> currentFlushFuture = CompletableFuture.completedFuture(null);

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
     * Triggers a flush of the buffer.
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
        // Don't attempt to flush after shutdown (unless waitForInProgress is true for shutdown flush)
        if (isShutdown && !waitForInProgress) {
            return;
        }

        Map<K, V> bufferToFlush = swapBuffer();

        // if buffer is empty, skip the flush
        if (bufferToFlush.isEmpty()) {
            try {
                logger.atDebug().log("Flush skipped for empty buffer '{}'", bufferName);
            } finally {
                flushInProgress.set(0);
                currentFlushFuture = CompletableFuture.completedFuture(null);
            }
            return;
        }

        if (waitForInProgress) {
            // During shutdown, ensure any previous flush is finished
            currentFlushFuture.join();
        } else {
            // If a flush is already in progress, skip this one
            if (!flushInProgress.compareAndSet(0, 1)) {
                return;
            }
        }


        if (waitForInProgress) {
            // run performFlush directly on the same thread for manual triggered flush
            try {
                performFlush(bufferToFlush);
            } finally {
                flushInProgress.set(0);
            }
            currentFlushFuture = CompletableFuture.completedFuture(null);
        } else {
            // otherwise, for lambda triggered flush, we run the performFlush on a background thread
            // assign the returned future to currentFlushFuture so that we can optionally wait for it in the next flush
            try {
                currentFlushFuture = CompletableFuture.runAsync(() -> {
                    try {
                        performFlush(bufferToFlush);
                    } finally {
                        flushInProgress.set(0);
                    }
                }, scheduler);
            } catch (RejectedExecutionException e) {
                // catching the scheduler exception, backup plan in case the async flush failed to schedule, force flush here
                try {
                    performFlush(bufferToFlush);
                } finally {
                    flushInProgress.set(0);
                }
                currentFlushFuture = CompletableFuture.completedFuture(null);
            }
        }
    }

    private Map<K, V> swapBuffer() {
        synchronized (bufferLock) {
            if (buffer.isEmpty()) {
                return new ConcurrentHashMap<>();
            }
            Map<K, V> bufferToFlush = buffer;
            buffer = new ConcurrentHashMap<>();
            return bufferToFlush;
        }
    }

    @SuppressWarnings("PMD.AvoidCatchingGenericException")
    private void performFlush(Map<K, V> bufferToFlush) {
        logger.atInfo().log("Flushing buffer '{}' with {} items", bufferName, bufferToFlush.size());
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
            synchronized (bufferLock) {
                // Restore items to buffer if flush failed
                logger.atError().cause(e).log("Flush rejected for buffer '{}', items restored", bufferName);
                bufferToFlush.forEach(buffer::putIfAbsent);
            }
            flushFailureCount.incrementAndGet();
            logger.atError().cause(e)
                    .kv("failureCount", flushFailureCount.get())
                    .log("Flush rejected for buffer '{}', items restored", bufferName);
        } catch (RuntimeException e) {
            flushFailureCount.incrementAndGet();
            logger.atError().cause(e)
                    .kv("failureCount", flushFailureCount.get())
                    .log("Flush failed for buffer '{}'", bufferName);
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
        
        // Then flush any remaining items
        try {
            flush(true);
            currentFlushFuture.join(); // Wait for final flush to complete
        } catch (RuntimeException e) {
            logger.atWarn().cause(e).log("Exception during shutdown flush for buffer '{}'", bufferName);
        }

        // Shutdown the scheduler
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            scheduler.shutdownNow();

            // the final flush operation fails. This is a valid use case for catching RuntimeException.
            logger.atError().cause(e).log("Error during final flush");
        }

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
