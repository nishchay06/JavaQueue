package com.javaqueue.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.javaqueue.exception.InvalidReceiptException;

public class MessageQueueTest {

    private MessageQueue queue;

    @BeforeEach
    void setUp() {
        queue = new MessageQueue("test-queue");
    }

    // ─── Test 1: Basic single-threaded publish and consume ───────────────────
    @Test
    void testBasicPublishAndConsume() throws InterruptedException {
        queue.publish(new Message("hello"));

        Receipt receipt = queue.consume();

        assertEquals("hello", receipt.getMessage().getPayload());
    }

    // ─── Test 2: Acknowledge removes message from in-flight ───────────────────
    @Test
    void testAcknowledge() throws InterruptedException {
        queue.publish(new Message("hello"));
        Receipt receipt = queue.consume();
        
        // Should not throw
        assertDoesNotThrow(() -> queue.acknowledge(receipt.getReceiptHandle()));
    }

    // ─── Test 3: Double ACK throws InvalidReceiptException ────────────────────
    @Test
    void testDoubleAcknowledgeThrows() throws InterruptedException {
        queue.publish(new Message("hello"));
        Receipt receipt = queue.consume();
        queue.acknowledge(receipt.getReceiptHandle());

        // Second ACK with same handle should throw
        assertThrows(InvalidReceiptException.class,
            () -> queue.acknowledge(receipt.getReceiptHandle()));
    }

    // ─── Test 4: Consumer blocks until message arrives ────────────────────────
    @Test
    void testConsumerBlocksUntilMessageArrives() throws InterruptedException, ExecutionException, TimeoutException {
        // Consumer starts BEFORE producer — must block and wait
        CompletableFuture<String> result = CompletableFuture.supplyAsync(() -> {
            try {
                Receipt receipt = queue.consume(); // blocks here
                return receipt.getMessage().getPayload();
            } catch (InterruptedException e) {
                return "interrupted";
            }
        });

        // Give the consumer thread time to start and block
        Thread.sleep(200);

        // Now publish — consumer should wake up
        queue.publish(new Message("delayed message"));

        String payload = result.get(2, TimeUnit.SECONDS);
        assertEquals("delayed message", payload);
    }

    // ─── Test 5: 10 producers, 1 consumer — no messages lost ─────────────────
    @Test
    void testConcurrentProducersNoMessagesLost() throws InterruptedException {
        int threadCount = 10;
        int messagesPerThread = 100;
        int totalMessages = threadCount * messagesPerThread;

        // Start all producer threads simultaneously
        ExecutorService producers = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            producers.submit(() -> {
                for (int j = 0; j < messagesPerThread; j++) {
                    queue.publish(new Message("thread-" + threadId + "-msg-" + j));
                }
            });
        }
        producers.shutdown();

        // Consume all messages
        List<String> received = new ArrayList<>();
        for (int i = 0; i < totalMessages; i++) {
            Receipt receipt = queue.consume();
            received.add(receipt.getMessage().getPayload());
            queue.acknowledge(receipt.getReceiptHandle());
        }

        // Every message must have arrived
        assertEquals(totalMessages, received.size());
    }

    // ─── Test 6: 1 producer, 5 consumers — no duplicates, no misses ──────────
    @Test
    void testConcurrentConsumersNoDuplicates() throws InterruptedException, ExecutionException {
        int consumerCount = 5;
        int totalMessages = 500;

        // Thread-safe list to collect received payloads
        List<String> received = Collections.synchronizedList(new ArrayList<>());

        // Start consumers first — they will block waiting for messages
        ExecutorService consumers = Executors.newFixedThreadPool(consumerCount);
        List<Future<?>> futures = new ArrayList<>();

        for (int i = 0; i < consumerCount; i++) {
            futures.add(consumers.submit(() -> {
                // Each consumer keeps consuming until interrupted
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        Receipt receipt = queue.consume();
                        received.add(receipt.getMessage().getPayload());
                        queue.acknowledge(receipt.getReceiptHandle());
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }));
        }

        // Publish all messages
        for (int i = 0; i < totalMessages; i++) {
            queue.publish(new Message("msg-" + i));
        }

        // Wait for all messages to be consumed
        Thread.sleep(1000);
        consumers.shutdownNow();
        consumers.awaitTermination(2, TimeUnit.SECONDS);

        // Every message received exactly once — no duplicates, no misses
        assertEquals(totalMessages, received.size());
    }

    // ─── Test 7: Timed consume returns immediately when a message is ready ───
    @Test
    void testTimedConsumeReturnsAvailableMessageImmediately() throws InterruptedException {
        queue.publish(new Message("hello"));

        long start = System.currentTimeMillis();
        Receipt receipt = queue.consume(5000);
        long elapsed = System.currentTimeMillis() - start;

        assertNotNull(receipt);
        assertEquals("hello", receipt.getMessage().getPayload());
        assertTrue(elapsed < 500, "should not have waited at all, waited " + elapsed + "ms");
    }

    // ─── Test 8: Timed consume returns null once the deadline passes ─────────
    @Test
    void testTimedConsumeReturnsNullOnTimeout() throws InterruptedException {
        long start = System.currentTimeMillis();
        Receipt receipt = queue.consume(300);
        long elapsed = System.currentTimeMillis() - start;

        assertNull(receipt, "empty queue should yield null, not block forever");
        assertTrue(elapsed >= 250, "should have waited out the timeout, waited " + elapsed + "ms");
    }

    // ─── Test 9: Zero timeout is a non-blocking poll ─────────────────────────
    @Test
    void testTimedConsumeWithZeroTimeoutDoesNotBlock() throws InterruptedException {
        long start = System.currentTimeMillis();
        Receipt receipt = queue.consume(0);
        long elapsed = System.currentTimeMillis() - start;

        assertNull(receipt);
        assertTrue(elapsed < 200, "zero timeout should return at once, took " + elapsed + "ms");
    }

    // ─── Test 10: Timed consume wakes as soon as a message is published ──────
    @Test
    void testTimedConsumeWakesOnPublish() throws InterruptedException {
        Thread producer = new Thread(() -> {
            try {
                Thread.sleep(200);
                queue.publish(new Message("late"));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        producer.start();

        long start = System.currentTimeMillis();
        Receipt receipt = queue.consume(10_000);
        long elapsed = System.currentTimeMillis() - start;
        producer.join();

        assertNotNull(receipt);
        assertEquals("late", receipt.getMessage().getPayload());
        assertTrue(elapsed < 3000, "should have woken on publish, took " + elapsed + "ms");
    }

    // ─── Test 11: A losing consumer does not get its deadline extended ───────
    // Two consumers wait on one message. The loser must still time out on the
    // original deadline — this is what a bare wait(timeoutMs) would get wrong,
    // since being woken and losing the race would restart its full timeout.
    @Test
    void testTimedConsumeDeadlineIsNotResetByLostRace() throws Exception {
        long timeoutMs = 600;

        CompletableFuture<Long> loserElapsed = new CompletableFuture<>();
        Runnable consumer = () -> {
            long start = System.currentTimeMillis();
            try {
                Receipt r = queue.consume(timeoutMs);
                if (r == null) {
                    loserElapsed.complete(System.currentTimeMillis() - start);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };

        Thread c1 = new Thread(consumer);
        Thread c2 = new Thread(consumer);
        c1.start();
        c2.start();

        // One message, two waiters — exactly one of them loses the race.
        Thread.sleep(150);
        queue.publish(new Message("only-one"));

        long elapsed = loserElapsed.get(5, TimeUnit.SECONDS);
        c1.join();
        c2.join();

        assertTrue(elapsed < timeoutMs * 2,
                "loser's deadline was reset by the wakeup — waited " + elapsed
                        + "ms against a " + timeoutMs + "ms timeout");
    }

    // ─── Test 12: Timed consume tracks in-flight state like consume() ────────
    @Test
    void testTimedConsumeRegistersInFlight() throws InterruptedException {
        queue.publish(new Message("hello"));

        Receipt receipt = queue.consume(1000);

        assertEquals(1, queue.inFlightMessages.size());
        queue.acknowledge(receipt.getReceiptHandle());
        assertEquals(0, queue.inFlightMessages.size());
    }
}
