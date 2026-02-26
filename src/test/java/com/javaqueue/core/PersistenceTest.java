package com.javaqueue.core;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class PersistenceTest {

    private Path tempDir;
    private String logDir;

    @BeforeEach
    void setUp() throws IOException {
        tempDir = Files.createTempDirectory("javaqueue-persistence-test");
        logDir = tempDir.toString();
    }

    @AfterEach
    void tearDown() throws IOException {
        Files.walk(tempDir)
                .sorted(java.util.Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(java.io.File::delete);
    }

    // Simulates a restart by closing the queue and opening a new one
    // pointing at the same log file
    private MessageQueue restart(String queueName) {
        QueueConfig config = new QueueConfig(30_000, 3, null, logDir);
        return new MessageQueue(queueName, config, 1000);
    }

    // ── Test 1: Published message survives restart ────────────────────────────
    @Test
    void testPublishedMessageSurvivesRestart() throws InterruptedException {
        QueueConfig config = new QueueConfig(30_000, 3, null, logDir);
        MessageQueue queue = new MessageQueue("orders", config, 1000);

        queue.publish(new Message("Order1"));
        queue.close();

        // Simulate restart
        MessageQueue restarted = restart("orders");
        Receipt receipt = restarted.consume();

        assertEquals("Order1", receipt.getMessage().getPayload());
        restarted.close();
    }

    // ── Test 2: Acknowledged message not replayed on restart ──────────────────
    @Test
    void testAcknowledgedMessageNotReplayedOnRestart() throws InterruptedException {
        QueueConfig config = new QueueConfig(30_000, 3, null, logDir);
        MessageQueue queue = new MessageQueue("orders", config, 1000);

        queue.publish(new Message("Order1"));
        Receipt r = queue.consume();
        queue.acknowledge(r.getReceiptHandle());
        queue.close();

        // Simulate restart — queue should be empty
        MessageQueue restarted = restart("orders");

        // Publish a sentinel so consume() doesn't block forever
        restarted.publish(new Message("sentinel"));
        Receipt r2 = restarted.consume();

        assertEquals("sentinel", r2.getMessage().getPayload());
        restarted.close();
    }

    // ── Test 3: In-flight message requeued on restart ─────────────────────────
    @Test
    void testInFlightMessageRequeuedOnRestart() throws InterruptedException {
        QueueConfig config = new QueueConfig(30_000, 3, null, logDir);
        MessageQueue queue = new MessageQueue("orders", config, 1000);

        queue.publish(new Message("Order1"));
        queue.consume(); // consume but don't ACK — simulates crash
        queue.close();

        // Simulate restart — in-flight message should be requeued
        MessageQueue restarted = restart("orders");
        Receipt r = restarted.consume();

        assertEquals("Order1", r.getMessage().getPayload());
        restarted.close();
    }

    // ── Test 4: Multiple messages partially acked ─────────────────────────────
    @Test
    void testMultipleMessagesPartiallyAcked() throws InterruptedException {
        QueueConfig config = new QueueConfig(30_000, 3, null, logDir);
        MessageQueue queue = new MessageQueue("orders", config, 1000);

        // Publish 5 messages
        for (int i = 1; i <= 5; i++) {
            queue.publish(new Message("Order" + i));
        }

        // Consume 3, ACK 2, leave 1 in-flight
        Receipt r1 = queue.consume();
        queue.acknowledge(r1.getReceiptHandle());

        Receipt r2 = queue.consume();
        queue.acknowledge(r2.getReceiptHandle());

        queue.consume(); // in-flight — not ACKed

        queue.close();

        // After restart: 2 queued + 1 requeued from in-flight = 3 messages
        MessageQueue restarted = restart("orders");
        int count = 0;
        for (int i = 0; i < 3; i++) {
            Receipt r = restarted.consume();
            restarted.acknowledge(r.getReceiptHandle());
            count++;
        }

        assertEquals(3, count);
        restarted.close();
    }

    // ── Test 5: Retry count preserved across restart ──────────────────────────
    @Test
    void testRetryCountPreservedAcrossRestart() throws InterruptedException {
        QueueConfig config = new QueueConfig(30_000, 3, null, logDir);
        MessageQueue queue = new MessageQueue("orders", config, 1000);

        queue.publish(new Message("Order1"));
        Receipt r1 = queue.consume();
        queue.nack(r1.getReceiptHandle()); // retryCount = 1
        queue.close();

        // After restart, retry count should still be 1
        MessageQueue restarted = restart("orders");
        Receipt r2 = restarted.consume();
        InFlightEntry entry = restarted.inFlightMessages.get(r2.getReceiptHandle());

        assertEquals(1, entry.getRetryCount());
        restarted.close();
    }

    // ── Test 6: Log compacted after restart ───────────────────────────────────
    @Test
    void testLogCompactedAfterRestart() throws InterruptedException, IOException {
        QueueConfig config = new QueueConfig(30_000, 3, null, logDir);
        MessageQueue queue = new MessageQueue("orders", config, 1000);

        // Publish and ACK 5 messages
        for (int i = 1; i <= 5; i++) {
            queue.publish(new Message("Order" + i));
            Receipt r = queue.consume();
            queue.acknowledge(r.getReceiptHandle());
        }
        queue.close();

        // Restart — triggers compaction
        MessageQueue restarted = restart("orders");
        restarted.close();

        // Log file should contain 0 entries after compaction
        Path logFile = tempDir.resolve("orders.log");
        long lineCount = Files.lines(logFile)
                .filter(l -> !l.isBlank())
                .count();

        assertEquals(0, lineCount, "Log should be empty after all messages ACKed");
    }

    // ── Test 7: No persistence when logDirectory is null ─────────────────────
    @Test
    void testNoPersistenceWithNullLogDirectory() throws InterruptedException {
        // Default config — no logDirectory
        MessageQueue queue = new MessageQueue("orders", QueueConfig.defaults(), 1000);

        queue.publish(new Message("Order1"));
        queue.close();

        // No log file should have been created
        Path logFile = tempDir.resolve("orders.log");
        assertFalse(Files.exists(logFile), "No log file should be created without logDirectory");
    }
}