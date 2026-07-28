package com.javaqueue.concurrent;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.javaqueue.core.Message;
import com.javaqueue.core.MessageQueue;
import com.javaqueue.core.QueueConfig;
import com.javaqueue.core.Receipt;

public class DeliveryGuaranteesStressTest {

    private static final long VISIBILITY_TIMEOUT_MS = 100;
    private static final long SCAN_INTERVAL_MS = 50;

    private MessageQueue queue;

    @BeforeEach
    void setUp() {
        QueueConfig config = new QueueConfig(VISIBILITY_TIMEOUT_MS, 5, null, null);
        queue = new MessageQueue("stress-queue", config, SCAN_INTERVAL_MS);
    }

    // ── Test 1: Concurrent NACKs — no deadlock, no exceptions ─────────────────
    @Test
    void testConcurrentNacksNoDeadlock() throws InterruptedException {
        int messageCount = 200;
        for (int i = 0; i < messageCount; i++) {
            queue.publish(new Message("msg-" + i));
        }

        ExecutorService consumers = Executors.newFixedThreadPool(10);
        AtomicInteger nackedCount = new AtomicInteger(0);

        for (int i = 0; i < 10; i++) {
            consumers.submit(() -> {
                for (int j = 0; j < 20; j++) {
                    try {
                        Receipt receipt = queue.consume();
                        queue.nack(receipt.getReceiptHandle());
                        nackedCount.incrementAndGet();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            });
        }

        consumers.shutdown();
        consumers.awaitTermination(10, TimeUnit.SECONDS);

        assertEquals(messageCount, nackedCount.get(),
                "Not all messages were NACKed — possible deadlock or exception");
    }

    // ── Test 2: Scanner and consumers running concurrently — no lost messages ──
    @Test
    void testScannerAndConsumersConcurrently() throws InterruptedException {
        int totalMessages = 100;
        Set<String> received = ConcurrentHashMap.newKeySet();

        // Publish all messages upfront
        for (int i = 0; i < totalMessages; i++) {
            queue.publish(new Message("msg-" + i));
        }

        ExecutorService consumers = Executors.newFixedThreadPool(5);

        for (int i = 0; i < 5; i++) {
            consumers.submit(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        Receipt receipt = queue.consume();
                        received.add(receipt.getMessage().getPayload());
                        queue.acknowledge(receipt.getReceiptHandle());
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            });
        }

        // Wait long enough for all messages to be consumed
        Thread.sleep(2000);
        consumers.shutdownNow();
        consumers.awaitTermination(5, TimeUnit.SECONDS);

        assertEquals(totalMessages, received.size(),
                "Messages lost while scanner was running concurrently");
    }

    // ── Test 3: All dead-lettered messages arrive in DLQ under load ───────────
    @Test
    void testDlqUnderLoad() throws InterruptedException {
        int messageCount = 20;
        int maxRetries = 2;

        // Give DLQ a high retry limit so messages are never dropped from it
        QueueConfig dlqConfig = new QueueConfig(10000, 100, null, null);
        MessageQueue dlq = new MessageQueue("dlq", dlqConfig, SCAN_INTERVAL_MS);

        QueueConfig config = new QueueConfig(VISIBILITY_TIMEOUT_MS, maxRetries, "dlq", null);
        MessageQueue q = new MessageQueue("main", config, SCAN_INTERVAL_MS);
        q.setDeadLetterQueue(dlq);

        for (int i = 0; i < messageCount; i++) {
            q.publish(new Message("msg-" + i));
        }

        // Each consumer NACKs exactly maxRetries times per message
        // then stops — so messages dead-letter cleanly
        ExecutorService consumers = Executors.newFixedThreadPool(5);
        AtomicInteger nackedCount = new AtomicInteger(0);

        for (int i = 0; i < 5; i++) {
            consumers.submit(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        Receipt receipt = q.consume();
                        q.nack(receipt.getReceiptHandle());
                        nackedCount.incrementAndGet();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            });
        }

        // Wait for all messages to be dead-lettered
        // Each message needs maxRetries NACKs — so wait for messageCount * maxRetries
        // NACKs
        long deadline = System.currentTimeMillis() + 5000;
        while (nackedCount.get() < messageCount * maxRetries
                && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }

        // Stop consumers before draining DLQ
        consumers.shutdownNow();
        consumers.awaitTermination(2, TimeUnit.SECONDS);

        // Give scanner one last cycle to process any remaining timeouts
        Thread.sleep(200);

        // Drain DLQ synchronously — consumers are stopped so no interference
        int dlqCount = 0;
        for (int i = 0; i < messageCount; i++) {
            Receipt r = dlq.consume();
            dlq.acknowledge(r.getReceiptHandle());
            dlqCount++;
        }

        assertEquals(messageCount, dlqCount,
                "Not all messages arrived in DLQ");

        q.close();
        dlq.close();
    }

    // ── Test 4: Sustained load with timeouts — no deadlock ────────────────────
    @Test
    void testSustainedLoadWithTimeouts() throws InterruptedException {
        int threadCount = 5;
        AtomicInteger published = new AtomicInteger(0);
        AtomicInteger consumed = new AtomicInteger(0);

        ExecutorService producers = Executors.newFixedThreadPool(threadCount);
        ExecutorService consumers = Executors.newFixedThreadPool(threadCount);

        long endTime = System.currentTimeMillis() + 3000; // 3 seconds

        for (int i = 0; i < threadCount; i++) {
            producers.submit(() -> {
                while (System.currentTimeMillis() < endTime) {
                    queue.publish(new Message("msg-" + published.incrementAndGet()));
                }
            });
        }

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
        producers.awaitTermination(5, TimeUnit.SECONDS);

        Thread.sleep(500);
        consumers.shutdownNow();
        consumers.awaitTermination(5, TimeUnit.SECONDS);

        System.out.println("Published: " + published.get());
        System.out.println("Consumed:  " + consumed.get());

        assertTrue(published.get() > 0, "Nothing was published");
        assertTrue(consumed.get() > 0, "Nothing was consumed");
        assertTrue(consumed.get() >= published.get(),
                "Messages lost under sustained load with timeouts enabled");
        assertTrue(consumed.get() <= published.get() * 2,
                "Too many duplicates — something is wrong");
    }

    // ── Test 5: Two queues that are each other's DLQ must not deadlock ────────
    //
    // Dead-lettering used to publish to the DLQ while still holding the source
    // queue's monitor. Holding lock A while acquiring lock B is only safe if
    // every thread acquires them in the same order — and a DLQ cycle means
    // they cannot. Thread 1 holds A and wants B; thread 2 holds B and wants A.
    //
    // Detected with ThreadMXBean rather than a bare timeout, so a failure names
    // the threads and the monitors instead of just hanging.
    @Test
    void testDeadLetterCycleDoesNotDeadlock() throws InterruptedException {
        // maxRetries = 1, so the first NACK sends the message to the other queue
        QueueConfig config = new QueueConfig(VISIBILITY_TIMEOUT_MS, 1, null, null);
        MessageQueue a = new MessageQueue("cycle-a", config, SCAN_INTERVAL_MS);
        MessageQueue b = new MessageQueue("cycle-b", config, SCAN_INTERVAL_MS);
        a.setDeadLetterQueue(b);
        b.setDeadLetterQueue(a);

        int rounds = 300;
        CountDownLatch finished = new CountDownLatch(2);

        Thread ta = churn(a, rounds, finished, "dlq-cycle-a");
        Thread tb = churn(b, rounds, finished, "dlq-cycle-b");
        ta.start();
        tb.start();

        ThreadMXBean threads = ManagementFactory.getThreadMXBean();
        boolean completed = false;

        // Poll for completion, checking for a deadlock on every tick
        for (int i = 0; i < 100 && !completed; i++) {
            completed = finished.await(100, TimeUnit.MILLISECONDS);

            long[] deadlocked = threads.findDeadlockedThreads();
            if (deadlocked != null) {
                StringBuilder detail = new StringBuilder("DLQ cycle deadlocked:\n");
                for (ThreadInfo info : threads.getThreadInfo(deadlocked)) {
                    detail.append("  ").append(info.getThreadName())
                            .append(" blocked on ").append(info.getLockName())
                            .append(" held by ").append(info.getLockOwnerName())
                            .append("\n");
                }
                fail(detail.toString());
            }
        }

        assertTrue(completed,
                "DLQ cycle did not deadlock outright but failed to make progress in 10s");

        a.close();
        b.close();
    }

    // Publishes, consumes, and NACKs in a loop — every NACK exceeds maxRetries
    // and therefore crosses into the other queue.
    private Thread churn(MessageQueue queue, int rounds, CountDownLatch finished, String name) {
        Thread thread = new Thread(() -> {
            try {
                for (int i = 0; i < rounds; i++) {
                    queue.publish(new Message("msg-" + i));
                    Receipt receipt = queue.consume();
                    queue.nack(receipt.getReceiptHandle());
                }
                finished.countDown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, name);

        // Daemon so a genuine deadlock cannot wedge the JVM after the test fails
        thread.setDaemon(true);
        return thread;
    }
}