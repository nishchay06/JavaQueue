package com.javaqueue.core;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.javaqueue.exception.QueueNotFoundException;

public class QueueManagerTest {

    private QueueManager manager;

    @BeforeEach
    void setUp() {
        manager = new QueueManager();
    }

    // ─── Test 1: Create and retrieve a queue ──────────────────────────────────
    @Test
    void testCreateAndGetQueue() {
        manager.createQueue("orders");
        assertNotNull(manager.getQueue("orders"));
    }

    // ─── Test 2: Getting non-existent queue throws ────────────────────────────
    @Test
    void testGetNonExistentQueueThrows() {
        assertThrows(QueueNotFoundException.class,
                () -> manager.getQueue("does-not-exist"));
    }

    // ─── Test 3: Creating same queue twice returns same instance ──────────────
    // Proves computeIfAbsent is working — not creating duplicate queues
    @Test
    void testCreateSameQueueTwiceReturnsSameInstance() {
        MessageQueue q1 = manager.createQueue("orders");
        MessageQueue q2 = manager.createQueue("orders");
        assertSame(q1, q2);
    }

    // ─── Test 4: Delete queue ─────────────────────────────────────────────────
    @Test
    void testDeleteQueue() {
        manager.createQueue("orders");
        manager.deleteQueue("orders");
        assertThrows(QueueNotFoundException.class,
                () -> manager.getQueue("orders"));
    }

    // ─── Test 5: Delete non-existent queue is a no-op ─────────────────────────
    @Test
    void testDeleteNonExistentQueueIsNoOp() {
        assertDoesNotThrow(() -> manager.deleteQueue("does-not-exist"));
    }

    // ─── Test 6: List queues ──────────────────────────────────────────────────
    @Test
    void testListQueues() {
        manager.createQueue("orders");
        manager.createQueue("payments");
        manager.createQueue("notifications");

        assertTrue(manager.listQueues().contains("orders"));
        assertTrue(manager.listQueues().contains("payments"));
        assertTrue(manager.listQueues().contains("notifications"));
    }

    // ─── Test 7: Concurrent queue creation — no duplicates ────────────────────
    // Proves computeIfAbsent atomicity under concurrent load
    @Test
    void testConcurrentQueueCreationNoDuplicates() throws InterruptedException {
        int threadCount = 20;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        ConcurrentLinkedQueue<MessageQueue> results = new ConcurrentLinkedQueue<>();

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> results.add(manager.createQueue("orders")));
        }

        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);

        // All 20 threads should have gotten back the exact same instance
        MessageQueue first = results.peek();
        assertTrue(results.stream().allMatch(q -> q == first),
                "Multiple queue instances created — computeIfAbsent is broken");
    }

    // ── Test 8: createQueue with custom config ────────────────────────────────
    @Test
    void testCreateQueueWithConfig() throws InterruptedException {
        QueueConfig config = new QueueConfig(500, 2, null, null);
        MessageQueue queue = manager.createQueue("configured-queue", config);

        assertNotNull(queue);

        // Verify config is applied — publish, consume, NACK twice, message should drop
        queue.publish(new Message("hello"));

        Receipt r1 = queue.consume();
        queue.nack(r1.getReceiptHandle()); // retryCount = 1

        Receipt r2 = queue.consume();
        queue.nack(r2.getReceiptHandle()); // retryCount = 2 — hits limit

        // Queue should be empty now
        queue.publish(new Message("sentinel"));
        Receipt r3 = queue.consume();
        assertEquals("sentinel", r3.getMessage().getPayload());
    }

    // ── Test 9: deleteQueue stops scanner thread ──────────────────────────────
    @Test
    void testDeleteQueueStopsScanner() throws InterruptedException {
        manager.createQueue("temp-queue");
        MessageQueue queue = manager.getQueue("temp-queue");

        assertTrue(queue.getScannerThread().isAlive());

        manager.deleteQueue("temp-queue");

        assertFalse(queue.getScannerThread().isAlive());
    }

    // ── Test 10: DLQ is wired automatically ──────────────────────────────────
    @Test
    void testDlqWiredAutomatically() throws InterruptedException {
        QueueConfig config = new QueueConfig(100, 2, "my-dlq", null);
        MessageQueue queue = manager.createQueue("main-queue", config);

        // DLQ should have been auto-created
        assertNotNull(manager.getQueue("my-dlq"));

        // Exhaust retries — message should land in DLQ
        queue.publish(new Message("hello"));

        Receipt r1 = queue.consume();
        queue.nack(r1.getReceiptHandle());

        Receipt r2 = queue.consume();
        queue.nack(r2.getReceiptHandle());

        // Verify message arrived in DLQ
        MessageQueue dlq = manager.getQueue("my-dlq");
        Receipt dlqReceipt = dlq.consume();
        assertEquals("hello", dlqReceipt.getMessage().getPayload());
    }
}
