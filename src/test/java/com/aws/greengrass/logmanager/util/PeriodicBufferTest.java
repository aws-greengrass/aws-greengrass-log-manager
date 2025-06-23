/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.logmanager.util;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class PeriodicBufferTest {
    
    private PeriodicBuffer<String, String> buffer;
    private final List<Map<String, String>> flushedData = Collections.synchronizedList(new ArrayList<>());
    private final AtomicInteger flushCount = new AtomicInteger(0);
    private final AtomicBoolean flushHandlerThrowsException = new AtomicBoolean(false);
    private final AtomicBoolean flushHandlerThrowsRejectedExecutionException = new AtomicBoolean(false);
    private Consumer<Map<String, String>> defaultFlushHandler;

    @BeforeEach
    void setUp() {
        flushedData.clear();
        flushCount.set(0);
        flushHandlerThrowsException.set(false);
        flushHandlerThrowsRejectedExecutionException.set(false);
        
        defaultFlushHandler = data -> {
            if (flushHandlerThrowsRejectedExecutionException.get()) {
                throw new RejectedExecutionException("Test rejected execution");
            }
            if (flushHandlerThrowsException.get()) {
                throw new RuntimeException("Test exception");
            }
            flushedData.add(new ConcurrentHashMap<>(data));
            flushCount.incrementAndGet();
        };
    }

    @AfterEach
    void shutDown() {
        if (buffer != null) {
            buffer.shutdown();
        }
    }

    @Test
    void testPutAndSize() {
        buffer = new PeriodicBuffer<>("TestBuffer", 60, defaultFlushHandler);
        buffer.start();

        assertEquals(0, buffer.size());
        assertTrue(buffer.isEmpty());

        buffer.put("key1", "value1");
        assertEquals(1, buffer.size());
        assertFalse(buffer.isEmpty());

        buffer.put("key2", "value2");
        assertEquals(2, buffer.size());

        buffer.put("key1", "updatedValue1");
        assertEquals(2, buffer.size()); // Size should remain same after update
    }

    @Test
    void testRemove() {
        buffer = new PeriodicBuffer<>("TestBuffer", 60, defaultFlushHandler);
        buffer.start();

        buffer.put("key1", "value1");
        buffer.put("key2", "value2");
        assertEquals(2, buffer.size());

        buffer.remove("key1");
        assertEquals(1, buffer.size());

        buffer.remove("nonExistentKey");
        assertEquals(1, buffer.size());

        buffer.remove("key2");
        assertEquals(0, buffer.size());
        assertTrue(buffer.isEmpty());
    }

    @Test
    void testManualFlush() throws InterruptedException {
        buffer = new PeriodicBuffer<>("TestBuffer", 60, defaultFlushHandler);
        buffer.start();

        buffer.put("key1", "value1");
        buffer.put("key2", "value2");

        buffer.flush();
        
        // Wait for flush to complete
        Thread.sleep(100);

        assertEquals(1, flushCount.get());
        assertEquals(1, flushedData.size());
        assertEquals(2, flushedData.get(0).size());
        assertEquals("value1", flushedData.get(0).get("key1"));
        assertEquals("value2", flushedData.get(0).get("key2"));

        // Buffer should be empty after flush
        assertEquals(0, buffer.size());
    }

    @Test
    void testFlushEmptyBuffer() throws InterruptedException {
        buffer = new PeriodicBuffer<>("TestBuffer", 60, defaultFlushHandler);
        buffer.start();

        buffer.flush();
        
        Thread.sleep(100);

        // Flush handler should not be called for empty buffer
        assertEquals(0, flushCount.get());
        assertEquals(0, flushedData.size());
    }

    @Test
    @Timeout(5)
    void testPeriodicFlush() throws InterruptedException {
        CountDownLatch flushLatch = new CountDownLatch(1);
        Consumer<Map<String, String>> latchFlushHandler = data -> {
            defaultFlushHandler.accept(data);
            flushLatch.countDown();
        };

        buffer = new PeriodicBuffer<>("TestBuffer", 1, latchFlushHandler);
        buffer.start();

        buffer.put("key1", "value1");

        // Wait for periodic flush (1 second)
        assertTrue(flushLatch.await(2, TimeUnit.SECONDS));

        assertEquals(1, flushCount.get());
        assertEquals(1, flushedData.size());
        assertEquals("value1", flushedData.get(0).get("key1"));
    }

    @Test
    void testFlushHandlerRuntimeException() throws InterruptedException {
        buffer = new PeriodicBuffer<>("TestBuffer", 60, defaultFlushHandler);
        buffer.start();

        buffer.put("key1", "value1");
        flushHandlerThrowsException.set(true);

        buffer.flush();
        Thread.sleep(100);

        // Items should be lost after runtime exception
        assertEquals(0, buffer.size());
        assertEquals(1, buffer.getFlushFailureCount());
        assertEquals(0, buffer.getFlushSuccessCount());
    }

    @Test
    void testFlushHandlerRejectedExecutionException() throws InterruptedException {
        buffer = new PeriodicBuffer<>("TestBuffer", 60, defaultFlushHandler);
        buffer.start();

        buffer.put("key1", "value1");
        buffer.put("key2", "value2");
        flushHandlerThrowsRejectedExecutionException.set(true);

        buffer.flush();
        Thread.sleep(100);

        // Items should be restored after RejectedExecutionException
        assertEquals(2, buffer.size());
        assertEquals(1, buffer.getFlushFailureCount());
        assertEquals(0, buffer.getFlushSuccessCount());

        // Verify items can be flushed successfully later
        flushHandlerThrowsRejectedExecutionException.set(false);
        buffer.flush();
        Thread.sleep(100);

        assertEquals(0, buffer.size());
        assertEquals(1, buffer.getFlushSuccessCount());
    }

    @Test
    void testShutdownFlushesRemainingItems() throws InterruptedException {
        buffer = new PeriodicBuffer<>("TestBuffer", 60, defaultFlushHandler);
        buffer.start();

        buffer.put("key1", "value1");
        buffer.put("key2", "value2");

        buffer.shutdown();

        // Shutdown should flush remaining items
        assertEquals(1, flushCount.get());
        assertEquals(1, flushedData.size());
        assertEquals(2, flushedData.get(0).size());
    }

    @Test
    void testShutdownPreventsNewItems() {
        buffer = new PeriodicBuffer<>("TestBuffer", 60, defaultFlushHandler);
        buffer.start();

        buffer.shutdown();

        // Should not throw but should not add item either
        buffer.put("key1", "value1");
        
        // Manual flush after shutdown should not do anything
        buffer.flush();
        
        assertEquals(0, flushCount.get());
    }

    @Test
    void testMultipleShutdownCalls() {
        buffer = new PeriodicBuffer<>("TestBuffer", 60, defaultFlushHandler);
        buffer.start();

        buffer.put("key1", "value1");

        buffer.shutdown();
        int firstFlushCount = flushCount.get();

        // Second shutdown should be idempotent
        buffer.shutdown();
        assertEquals(firstFlushCount, flushCount.get());
    }

    @Test
    void testStartAfterShutdown() {
        buffer = new PeriodicBuffer<>("TestBuffer", 60, defaultFlushHandler);
        buffer.start();
        buffer.shutdown();

        assertThrows(IllegalStateException.class, () -> buffer.start());
    }

    @Test
    void testConcurrentPutOperations() throws InterruptedException {
        buffer = new PeriodicBuffer<>("TestBuffer", 60, defaultFlushHandler);
        buffer.start();

        int threadCount = 10;
        int itemsPerThread = 100;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completeLatch = new CountDownLatch(threadCount);

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < itemsPerThread; j++) {
                        buffer.put("thread" + threadId + "-key" + j, "value" + j);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    completeLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue(completeLatch.await(5, TimeUnit.SECONDS));

        buffer.flush();
        Thread.sleep(100);

        // All items should be flushed
        assertEquals(1, flushCount.get());
        assertEquals(threadCount * itemsPerThread, flushedData.get(0).size());

        executor.shutdown();
    }

    @Test
    void testConcurrentPutAndRemove() throws InterruptedException {
        buffer = new PeriodicBuffer<>("TestBuffer", 60, defaultFlushHandler);
        buffer.start();

        int operationCount = 1000;
        CountDownLatch completeLatch = new CountDownLatch(2);

        Thread putThread = new Thread(() -> {
            for (int i = 0; i < operationCount; i++) {
                buffer.put("key" + (i % 10), "value" + i);
            }
            completeLatch.countDown();
        });

        Thread removeThread = new Thread(() -> {
            for (int i = 0; i < operationCount; i++) {
                buffer.remove("key" + (i % 10));
            }
            completeLatch.countDown();
        });

        putThread.start();
        removeThread.start();

        assertTrue(completeLatch.await(5, TimeUnit.SECONDS));

        // Size should be small due to constant put/remove on same keys
        assertTrue(buffer.size() <= 10);
    }

    @Test
    void testFlushDuringConcurrentModification() throws InterruptedException {
        AtomicReference<Exception> flushException = new AtomicReference<>();
        CountDownLatch flushProcessingStarted = new CountDownLatch(1);
        CountDownLatch continueFlushProcessing = new CountDownLatch(1);
        AtomicReference<Map<String, String>> capturedFlushData = new AtomicReference<>();
        
        Consumer<Map<String, String>> safeFlushHandler = data -> {
            try {
                capturedFlushData.set(new ConcurrentHashMap<>(data));
                flushProcessingStarted.countDown();
                // Wait for modification thread to start
                assertTrue(continueFlushProcessing.await(2, TimeUnit.SECONDS));
                // Simulate processing time
                Thread.sleep(50);
                defaultFlushHandler.accept(data);
            } catch (Exception e) {
                flushException.set(e);
            }
        };

        buffer = new PeriodicBuffer<>("TestBuffer", 60, safeFlushHandler);
        buffer.start();

        // Pre-populate buffer
        for (int i = 0; i < 100; i++) {
            buffer.put("key" + i, "value" + i);
        }

        CountDownLatch modificationComplete = new CountDownLatch(1);

        // Start flush which will swap the buffer immediately
        Thread flushThread = new Thread(() -> buffer.flush());
        flushThread.start();

        // Wait for flush processing to start (buffer has been swapped)
        assertTrue(flushProcessingStarted.await(2, TimeUnit.SECONDS));

        // Now modify the buffer - these changes should go into the new buffer
        Thread modificationThread = new Thread(() -> {
            try {
                for (int i = 100; i < 200; i++) {
                    buffer.put("key" + i, "value" + i);
                    if (i % 10 == 0) {
                        buffer.remove("key" + (i - 50));
                    }
                }
                modificationComplete.countDown();
            } catch (Exception e) {
                Thread.currentThread().interrupt();
            }
        });
        modificationThread.start();

        // Allow flush processing to continue
        continueFlushProcessing.countDown();

        // Wait for both threads to complete
        flushThread.join(5000);
        assertTrue(modificationComplete.await(5, TimeUnit.SECONDS));

        // Give time for async flush to complete
        Thread.sleep(200);

        // Verify no exceptions occurred
        if (flushException.get() != null) {
            fail("Exception during flush: " + flushException.get());
        }

        // First flush should have exactly the original 100 items (buffer was swapped)
        assertEquals(1, flushedData.size());
        assertEquals(100, flushedData.get(0).size());
        
        // Verify the flushed data contains the original items
        Map<String, String> flushedMap = capturedFlushData.get();
        assertNotNull(flushedMap);
        for (int i = 0; i < 100; i++) {
            assertEquals("value" + i, flushedMap.get("key" + i));
        }

        // Buffer should contain new items added during flush
        // The new buffer should have items 100-199 minus some removed items
        assertTrue(buffer.size() > 0);
        assertTrue(buffer.size() < 100); // Should be less than 100 due to removals
    }

    @Test
    void testFlushStatistics() throws InterruptedException {
        buffer = new PeriodicBuffer<>("TestBuffer", 60, defaultFlushHandler);
        buffer.start();

        assertEquals(0, buffer.getFlushSuccessCount());
        assertEquals(0, buffer.getFlushFailureCount());

        // Successful flush
        buffer.put("key1", "value1");
        buffer.flush();
        Thread.sleep(100);
        assertEquals(1, buffer.getFlushSuccessCount());
        assertEquals(0, buffer.getFlushFailureCount());

        // Failed flush
        buffer.put("key2", "value2");
        flushHandlerThrowsException.set(true);
        buffer.flush();
        Thread.sleep(100);
        assertEquals(1, buffer.getFlushSuccessCount());
        assertEquals(1, buffer.getFlushFailureCount());

        // Another successful flush
        flushHandlerThrowsException.set(false);
        buffer.put("key3", "value3");
        buffer.flush();
        Thread.sleep(100);
        assertEquals(2, buffer.getFlushSuccessCount());
        assertEquals(1, buffer.getFlushFailureCount());
    }

    @Test
    void testConcurrentFlushSkipping() throws Exception {
        CountDownLatch firstFlushStarted = new CountDownLatch(1);
        CountDownLatch allowFirstFlushToProceed = new CountDownLatch(1);
        AtomicInteger flushCallCount = new AtomicInteger(0);

        PeriodicBuffer<String, String> buffer = new PeriodicBuffer<>("testBuffer", 10, items -> {
            flushCallCount.incrementAndGet();
            firstFlushStarted.countDown();          // Let the test know flush has started
            try {
                allowFirstFlushToProceed.await();   // Wait here to simulate long-running flush
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        buffer.put("k", "v");

        // Thread 1 will start the flush and block
        Thread flushThread1 = new Thread(buffer::flush);
        flushThread1.start();

        // Wait until flushThread1 gets into the flush method
        assertTrue(firstFlushStarted.await(3, TimeUnit.SECONDS), "First flush didn't start in time");

        // Thread 2 will attempt to flush but should skip since flushInProgress = 1
        Thread flushThread2 = new Thread(buffer::flush);
        flushThread2.start();

        // Allow flush to complete
        allowFirstFlushToProceed.countDown();

        flushThread1.join();
        flushThread2.join();

        assertEquals(1, flushCallCount.get(), "Only one flush should have been performed");
    }

    @Test
    void testBufferSwapping() throws InterruptedException {
        CountDownLatch flushStarted = new CountDownLatch(1);
        CountDownLatch continueFlush = new CountDownLatch(1);
        AtomicReference<Map<String, String>> capturedBuffer = new AtomicReference<>();

        Consumer<Map<String, String>> capturingFlushHandler = data -> {
            capturedBuffer.set(new ConcurrentHashMap<>(data)); // Make a copy to avoid reference issues
            flushStarted.countDown(); // Signal that flush has started and buffer is captured
            try {
                // Wait for test thread to add new items
                assertTrue(continueFlush.await(2, TimeUnit.SECONDS));
                Thread.sleep(50); // Brief processing time
                defaultFlushHandler.accept(data);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted during flush", e);
            }
        };

        buffer = new PeriodicBuffer<>("TestBuffer", 60, capturingFlushHandler);
        buffer.start();

        buffer.put("key1", "value1");

        Thread flushThread = new Thread(() -> buffer.flush());
        flushThread.start();

        // Wait for flush to start and capture the buffer
        assertTrue(flushStarted.await(2, TimeUnit.SECONDS));

        // Add new items while flush is processing old buffer
        buffer.put("key2", "value2");
        assertEquals(1, buffer.size()); // New buffer should have the new item

        // Allow flush to continue
        continueFlush.countDown();
        
        flushThread.join(3000);

        // Verify the flushed data only contains the old item
        assertNotNull(capturedBuffer.get());
        assertEquals(1, capturedBuffer.get().size());
        assertTrue(capturedBuffer.get().containsKey("key1"));
        assertFalse(capturedBuffer.get().containsKey("key2"));

        // Verify buffer still contains the new item
        assertEquals(1, buffer.size());
    }

    @Test
    @Timeout(5)
    void testShutdownWithEmptyBufferDoesNotDeadlock() throws InterruptedException {
        buffer = new PeriodicBuffer<>("TestBuffer", 60, defaultFlushHandler);
        buffer.start();

        // First, trigger a flush on an empty buffer to simulate the bug
        buffer.flush();
        Thread.sleep(100);

        // Now try to shutdown - this should not hang
        buffer.shutdown();

        // If we reach here, shutdown completed successfully
        // The test passing without timeout is the assertion
    }

    @Test
    @Timeout(5)
    void testConcurrentEmptyFlushAndShutdown() throws InterruptedException {
        buffer = new PeriodicBuffer<>("TestBuffer", 60, defaultFlushHandler);
        buffer.start();

        // Create multiple threads that will flush empty buffer
        int threadCount = 5;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch flushLatch = new CountDownLatch(threadCount);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount + 1);

        // Start threads that will flush empty buffer
        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    buffer.flush();
                    flushLatch.countDown();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        // Start shutdown thread
        executor.submit(() -> {
            try {
                startLatch.await();
                Thread.sleep(200); // Let some flushes happen first
                buffer.shutdown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        // Release all threads
        startLatch.countDown();

        // Wait for all flushes to complete
        assertTrue(flushLatch.await(3, TimeUnit.SECONDS));

        // Shutdown should also complete without hanging
        executor.shutdown();
        assertTrue(executor.awaitTermination(3, TimeUnit.SECONDS));
    }
}
