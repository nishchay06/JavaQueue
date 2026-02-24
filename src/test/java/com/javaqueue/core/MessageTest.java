package com.javaqueue.core;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import org.junit.jupiter.api.Test;

public class MessageTest {
    // ─── Test 1: Payload is stored correctly ─────────────────────────────────
    @Test
    void testPayloadStoredCorrectly() {
        Message message = new Message("hello");
        assertEquals("hello", message.getPayload());
    }

    // ─── Test 2: ID is not null or empty ─────────────────────────────────────
    @Test
    void testIdIsNotNull() {
        Message message = new Message("hello");
        assertNotNull(message.getId());
        assertFalse(message.getId().isEmpty());
    }

    // ─── Test 3: Two messages have different IDs ──────────────────────────────
    @Test
    void testTwoMessagesHaveDifferentIds() {
        Message m1 = new Message("first");
        Message m2 = new Message("second");
        assertNotEquals(m1.getId(), m2.getId());
    }

    // ─── Test 4: IDs are unique across concurrent creation ───────────────────
    // This is the important one — proves AtomicLong works correctly
    // when multiple threads create messages simultaneously.
    @Test
    void testIdsUniqueUnderConcurrentCreation() throws InterruptedException {
        int threadCount = 10;
        int messagesPerThread = 1000;
        int total = threadCount * messagesPerThread;

        // ConcurrentHashMap used as a set to collect all IDs thread-safely
        Set<String> ids = ConcurrentHashMap.newKeySet();

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                for (int j = 0; j < messagesPerThread; j++) {
                    ids.add(new Message("payload").getId());
                }
            });
        }

        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);

        // If any two messages got the same ID, the set size would be less than total
        assertEquals(total, ids.size(), "Duplicate IDs detected — AtomicLong is broken");
    }
}
