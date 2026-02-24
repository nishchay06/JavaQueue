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
import static org.junit.jupiter.api.Assertions.assertThrows;
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
}
