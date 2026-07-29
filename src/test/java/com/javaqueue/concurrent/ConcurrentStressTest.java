package com.javaqueue.concurrent;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.javaqueue.core.Message;
import com.javaqueue.core.MessageQueue;
import com.javaqueue.core.Receipt;

public class ConcurrentStressTest {
    
    private MessageQueue queue;

    @BeforeEach
    void setUp() {
        queue = new MessageQueue("stress-test-queue");
    }

    // ─── Test 1: Many producers, many consumers — nothing lost, no duplicates ─
    // The ultimate test. 20 producers and 20 consumers running simultaneously.
    // Every message must be consumed exactly once.
    @Test
    void testManyProducersManyConsumers() throws InterruptedException {
        int producerCount = 20;
        int consumerCount = 20;
        int messagesPerProducer = 500;
        int totalMessages = producerCount * messagesPerProducer;

        // Thread-safe set to track every payload consumed
        Set<String> consumed = ConcurrentHashMap.newKeySet();
        AtomicInteger duplicates = new AtomicInteger(0);

        ExecutorService producers = Executors.newFixedThreadPool(producerCount);
        ExecutorService consumers = Executors.newFixedThreadPool(consumerCount);

        // Start consumers first — they block waiting for messages
        for (int i = 0; i < consumerCount; i++) {
            consumers.submit(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        Receipt receipt = queue.consume();
                        String payload = receipt.getMessage().getPayload();

                        // If add() returns false, this payload was already consumed — duplicate!
                        boolean added = consumed.add(payload);
                        if (!added) duplicates.incrementAndGet();

                        queue.acknowledge(receipt.getReceiptHandle());
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            });
        }

        // Start all producers simultaneously
        for (int i = 0; i < producerCount; i++) {
            final int producerId = i;
            producers.submit(() -> {
                for (int j = 0; j < messagesPerProducer; j++) {
                    // Unique payload per message so we can detect duplicates
                    queue.publish(new Message("producer-" + producerId + "-msg-" + j));
                }
            });
        }

        // Wait for producers to finish
        producers.shutdown();
        producers.awaitTermination(10, TimeUnit.SECONDS);

        // Give consumers time to drain the queue
        Thread.sleep(1000);
        consumers.shutdownNow();
        consumers.awaitTermination(5, TimeUnit.SECONDS);

        assertEquals(0, duplicates.get(), "Duplicate messages detected");
        assertEquals(totalMessages, consumed.size(), "Messages were lost");
    }

    // ─── Test 2: Sustained load — queue never deadlocks ──────────────────────
    // Runs producers and consumers for 5 seconds straight.
    // If the queue deadlocks, the test hangs and times out.
    @Test
    void testSustainedLoadNoDeadlock() throws InterruptedException {
        int threadCount = 10;
        // Generous: this bounds how long consumers get to clear the backlog,
        // not how long the drain is expected to take.
        final long DRAIN_TIMEOUT_MS = 60_000;
        AtomicInteger published = new AtomicInteger(0);
        AtomicInteger consumed = new AtomicInteger(0);

        ExecutorService producers = Executors.newFixedThreadPool(threadCount);
        ExecutorService consumers = Executors.newFixedThreadPool(threadCount);

        long endTime = System.currentTimeMillis() + 5000; // run for 5 seconds

        // Producers publish as fast as possible for 5 seconds
        for (int i = 0; i < threadCount; i++) {
            producers.submit(() -> {
                while (System.currentTimeMillis() < endTime) {
                    queue.publish(new Message("msg-" + published.incrementAndGet()));
                }
            });
        }

        // Consumers consume as fast as possible for 5 seconds
        for (int i = 0; i < threadCount; i++) {
            consumers.submit(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        Receipt receipt = queue.consume();
                        consumed.incrementAndGet();
                        queue.acknowledge(receipt.getReceiptHandle());
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            });
        }

        producers.shutdown();
        producers.awaitTermination(10, TimeUnit.SECONDS);

        // Drain until the queue is actually empty rather than sleeping a fixed
        // interval and hoping. On a machine with fewer cores than this test has
        // threads, producers outrun consumers and leave a backlog that a short
        // sleep will not clear -- which looks identical to message loss.
        int backlogAtProducerStop = queue.depth();
        long drainStart = System.currentTimeMillis();
        long drainDeadline = drainStart + DRAIN_TIMEOUT_MS;
        while (queue.depth() > 0 && System.currentTimeMillis() < drainDeadline) {
            Thread.sleep(10);
        }
        long drainMs = System.currentTimeMillis() - drainStart;

        consumers.shutdownNow();
        consumers.awaitTermination(5, TimeUnit.SECONDS);

        // Everything is quiescent now, so these three reads are consistent.
        int publishedCount = published.get();
        int consumedCount = consumed.get();
        int residualDepth = queue.depth();

        System.out.println("Published: " + publishedCount);
        System.out.println("Consumed:  " + consumedCount);
        System.out.println("Residual:  " + residualDepth);

        // Without these two, a passing run is ambiguous: a drain of ~0ms means
        // no backlog ever formed and the drain loop was never actually
        // exercised, which is a different result from a backlog that cleared.
        System.out.println("Backlog at producer stop: " + backlogAtProducerStop);
        System.out.println("Drain time: " + drainMs + "ms"
                + (backlogAtProducerStop == 0
                        ? " (no backlog formed -- consumers kept pace)"
                        : " to clear " + backlogAtProducerStop + " messages"));

        assertTrue(publishedCount > 0, "Nothing was published");
        assertTrue(consumedCount > 0, "Nothing was consumed");

        // The real invariant: every published message was either consumed or is
        // still sitting in the queue. A shortfall here is genuine loss and a
        // violation of at-least-once delivery. A backlog that merely failed to
        // drain in time is not.
        assertEquals(publishedCount, consumedCount + residualDepth,
                "Messages unaccounted for: published != consumed + still queued");

        assertEquals(0, residualDepth,
                "Queue did not drain within " + DRAIN_TIMEOUT_MS + "ms of producers stopping");
    }

    // ─── Test 3: Slow consumers — queue accumulates then drains ──────────────
    // Producers outpace consumers temporarily, messages pile up.
    // Then producers stop and consumers drain the backlog.
    // Tests that blocking and waking works correctly with a backlog.
    @Test
    void testSlowConsumersDrainBacklog() throws InterruptedException {
        int totalMessages = 1000;
        AtomicInteger consumed = new AtomicInteger(0);

        // Publish all messages upfront — queue builds up a backlog
        for (int i = 0; i < totalMessages; i++) {
            queue.publish(new Message("msg-" + i));
        }

        // Start slow consumers — they sleep between each consume
        ExecutorService consumers = Executors.newFixedThreadPool(5);
        for (int i = 0; i < 5; i++) {
            consumers.submit(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        Receipt receipt = queue.consume();
                        consumed.incrementAndGet();
                        queue.acknowledge(receipt.getReceiptHandle());
                        Thread.sleep(1); // simulate slow processing
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            });
        }

        // Wait long enough for all 1000 messages to be consumed at ~1ms each
        Thread.sleep(3000);
        consumers.shutdownNow();
        consumers.awaitTermination(5, TimeUnit.SECONDS);

        assertEquals(totalMessages, consumed.get(), "Backlog not fully drained");
    }
}
