package com.javaqueue.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.javaqueue.exception.InvalidReceiptException;

public class DeliveryGuaranteesTest {

    // Short timeout and scan interval so tests run fast.
    // 100ms timeout + 50ms scan + 50ms buffer = sleep 200ms to guarantee scanner
    // has fired.
    private static final long VISIBILITY_TIMEOUT_MS = 100;
    private static final long SCAN_INTERVAL_MS = 50;

    private MessageQueue queue;

    @BeforeEach
    void setUp() {
        QueueConfig config = new QueueConfig(VISIBILITY_TIMEOUT_MS, 3, null);
        queue = new MessageQueue("test-queue", config, SCAN_INTERVAL_MS);
    }

    // ── Test 1: NACK requeues immediately ─────────────────────────────────────
    @Test
    void testNackRequeuesImmediately() throws InterruptedException {
        queue.publish(new Message("hello"));

        Receipt r1 = queue.consume();
        assertEquals("hello", r1.getMessage().getPayload());

        queue.nack(r1.getReceiptHandle());

        // No sleep — NACK requeues instantly, no need to wait for scanner
        Receipt r2 = queue.consume();
        assertEquals("hello", r2.getMessage().getPayload());
    }

    // ── Test 2: NACK does not wait for visibility timeout ─────────────────────
    @Test
    void testNackDoesNotWaitForTimeout() throws InterruptedException {
        queue.publish(new Message("hello"));

        Receipt r1 = queue.consume();
        queue.nack(r1.getReceiptHandle());

        // Consume immediately — well before the 100ms timeout
        // If NACK wasn't working, this would block forever
        Receipt r2 = queue.consume();
        assertNotNull(r2);
    }

    // ── Test 3: Visibility timeout requeues message ───────────────────────────
    @Test
    void testVisibilityTimeoutRequeuesMessage() throws InterruptedException {
        queue.publish(new Message("hello"));

        Receipt r1 = queue.consume();
        assertEquals("hello", r1.getMessage().getPayload());

        // Don't ACK — wait for timeout + scan interval + buffer
        Thread.sleep(200);

        // Message should have reappeared
        Receipt r2 = queue.consume();
        assertEquals("hello", r2.getMessage().getPayload());
    }

    // ── Test 4: Timeout does not fire if message is ACKed ─────────────────────
    @Test
    void testTimeoutDoesNotFireIfAcked() throws InterruptedException {
        queue.publish(new Message("hello"));

        Receipt r1 = queue.consume();
        queue.acknowledge(r1.getReceiptHandle());

        // Wait past the timeout — message should NOT reappear
        Thread.sleep(200);

        // Publish a sentinel so consume() doesn't block forever
        queue.publish(new Message("sentinel"));
        Receipt r2 = queue.consume();

        // If the first message reappeared it would come before sentinel
        assertEquals("sentinel", r2.getMessage().getPayload());
    }

    // ── Test 5: NACK increments retry count ───────────────────────────────────
    @Test
    void testNackIncrementsRetryCount() throws InterruptedException {
        queue.publish(new Message("hello"));

        // First delivery — retryCount should be 0
        Receipt r1 = queue.consume();
        InFlightEntry e1 = getInFlightEntry(r1.getReceiptHandle());
        assertEquals(0, e1.getRetryCount());

        queue.nack(r1.getReceiptHandle());

        // Second delivery — retryCount should be 1
        Receipt r2 = queue.consume();
        InFlightEntry e2 = getInFlightEntry(r2.getReceiptHandle());
        assertEquals(1, e2.getRetryCount());

        queue.acknowledge(r2.getReceiptHandle());
    }

    // ── Test 6: Retry limit drops message when no DLQ ─────────────────────────
    @Test
    void testRetryLimitDropsMessageWhenNoDlq() throws InterruptedException {
        // maxRetries = 2 — message dropped after 2 NACKs
        QueueConfig config = new QueueConfig(VISIBILITY_TIMEOUT_MS, 2, null);
        MessageQueue q = new MessageQueue("drop-queue", config, SCAN_INTERVAL_MS);

        q.publish(new Message("hello"));

        Receipt r1 = q.consume();
        q.nack(r1.getReceiptHandle()); // retryCount = 1

        Receipt r2 = q.consume();
        q.nack(r2.getReceiptHandle()); // retryCount = 2 — hits limit, dropped

        // Queue should now be empty — publish sentinel and verify it comes first
        q.publish(new Message("sentinel"));
        Receipt r3 = q.consume();
        assertEquals("sentinel", r3.getMessage().getPayload());

        q.close();
    }

    // ── Test 7: Retry limit sends to DLQ ──────────────────────────────────────
    @Test
    void testRetryLimitSendsToDlq() throws InterruptedException {
        MessageQueue dlq = new MessageQueue("dlq",
                QueueConfig.defaults(), SCAN_INTERVAL_MS);

        QueueConfig config = new QueueConfig(VISIBILITY_TIMEOUT_MS, 2, "dlq");
        MessageQueue q = new MessageQueue("main-queue", config, SCAN_INTERVAL_MS);
        q.setDeadLetterQueue(dlq);

        q.publish(new Message("hello"));

        Receipt r1 = q.consume();
        q.nack(r1.getReceiptHandle()); // retryCount = 1

        Receipt r2 = q.consume();
        q.nack(r2.getReceiptHandle()); // retryCount = 2 — hits limit, goes to DLQ

        // Original queue should be empty
        q.publish(new Message("sentinel"));
        Receipt r3 = q.consume();
        assertEquals("sentinel", r3.getMessage().getPayload());

        // DLQ should have the dead-lettered message
        Receipt dlqReceipt = dlq.consume();
        assertEquals("hello", dlqReceipt.getMessage().getPayload());

        q.close();
        dlq.close();
    }

    // ── Test 8: ACK after NACK throws ─────────────────────────────────────────
    @Test
    void testAcknowledgeAfterNackThrows() throws InterruptedException {
        queue.publish(new Message("hello"));

        Receipt r1 = queue.consume();
        queue.nack(r1.getReceiptHandle());

        // Receipt handle is now invalid — NACK removed it from in-flight map
        assertThrows(InvalidReceiptException.class,
                () -> queue.acknowledge(r1.getReceiptHandle()));
    }

    // ── Test 9: NACK with invalid handle throws ────────────────────────────────
    @Test
    void testNackInvalidHandleThrows() {
        assertThrows(InvalidReceiptException.class,
                () -> queue.nack("made-up-handle"));
    }

    // ── Test 10: close() stops scanner thread ─────────────────────────────────
    @Test
    void testCloseStopsScannerThread() throws InterruptedException {
        MessageQueue q = new MessageQueue("close-queue",
                QueueConfig.defaults(), SCAN_INTERVAL_MS);

        assertTrue(q.getScannerThread().isAlive());

        q.close();

        assertFalse(q.getScannerThread().isAlive());
    }

    // ── Helper — reads InFlightEntry directly for assertion ───────────────────
    // Package-private access — test is in same package as MessageQueue
    private InFlightEntry getInFlightEntry(String receiptHandle) {
        return queue.inFlightMessages.get(receiptHandle);
    }
}
