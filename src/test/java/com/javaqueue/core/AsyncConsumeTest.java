package com.javaqueue.core;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Callback-based consume, which is what lets long polling stop parking a
 * server thread per waiting client.
 *
 * The blocking consume(timeout) holds a thread for the whole wait. Here the
 * caller registers interest and returns; a later publish hands the message
 * straight to the waiter.
 */
public class AsyncConsumeTest {

    private MessageQueue queue;

    @BeforeEach
    void setUp() {
        queue = new MessageQueue("async-test");
    }

    // ── Immediate delivery ────────────────────────────────────────────────────

    @Test
    void testHandlerRunsImmediatelyWhenMessageAvailable() {
        queue.publish(new Message("Order1"));

        AtomicReference<Receipt> delivered = new AtomicReference<>();
        queue.consumeAsync(delivered::set);

        assertNotNull(delivered.get(), "handler should run before consumeAsync returns");
        assertEquals("Order1", delivered.get().getMessage().getPayload());
    }

    @Test
    void testHandlerDoesNotRunWhenQueueEmpty() {
        AtomicReference<Receipt> delivered = new AtomicReference<>();
        queue.consumeAsync(delivered::set);

        assertNull(delivered.get());
    }

    // ── Deferred delivery ─────────────────────────────────────────────────────

    @Test
    void testPublishHandsMessageToWaiter() {
        AtomicReference<Receipt> delivered = new AtomicReference<>();
        queue.consumeAsync(delivered::set);

        queue.publish(new Message("Order1"));

        assertNotNull(delivered.get(), "publish should hand straight to the waiter");
        assertEquals("Order1", delivered.get().getMessage().getPayload());
    }

    // A message handed to a waiter must be in-flight exactly like one taken by
    // a blocking consume — same receipt, same acknowledge path.
    @Test
    void testHandedOffMessageIsInFlightAndAckable() {
        AtomicReference<Receipt> delivered = new AtomicReference<>();
        queue.consumeAsync(delivered::set);
        queue.publish(new Message("Order1"));

        assertEquals(1, queue.inFlightMessages.size());
        queue.acknowledge(delivered.get().getReceiptHandle());
        assertEquals(0, queue.inFlightMessages.size());
    }

    // A message that went to a waiter must not also be sitting in the queue.
    @Test
    void testHandedOffMessageIsNotAlsoQueued() throws InterruptedException {
        queue.consumeAsync(r -> {
        });
        queue.publish(new Message("Order1"));

        assertNull(queue.consume(200), "message was delivered twice");
    }

    // ── Cancellation ──────────────────────────────────────────────────────────

    @Test
    void testCancelPreventsDelivery() throws InterruptedException {
        AtomicReference<Receipt> delivered = new AtomicReference<>();
        MessageQueue.Waiter waiter = queue.consumeAsync(delivered::set);

        assertTrue(waiter.cancel(), "cancel should report that it cancelled a pending wait");
        queue.publish(new Message("Order1"));

        assertNull(delivered.get(), "a cancelled waiter must not receive anything");
        // and the message stays available for someone else
        assertEquals("Order1", queue.consume(1000).getMessage().getPayload());
    }

    @Test
    void testCancelAfterDeliveryReturnsFalse() {
        queue.publish(new Message("Order1"));
        MessageQueue.Waiter waiter = queue.consumeAsync(r -> {
        });

        assertFalse(waiter.cancel(), "cannot cancel a wait that already completed");
    }

    @Test
    void testDoubleCancelReturnsFalseSecondTime() {
        MessageQueue.Waiter waiter = queue.consumeAsync(r -> {
        });

        assertTrue(waiter.cancel());
        assertFalse(waiter.cancel());
    }

    // ── Fan-in ────────────────────────────────────────────────────────────────

    @Test
    void testWaitersAreServedInOrderOneMessageEach() {
        AtomicReference<Receipt> first = new AtomicReference<>();
        AtomicReference<Receipt> second = new AtomicReference<>();
        queue.consumeAsync(first::set);
        queue.consumeAsync(second::set);

        queue.publish(new Message("A"));
        assertEquals("A", first.get().getMessage().getPayload());
        assertNull(second.get(), "one message must go to exactly one waiter");

        queue.publish(new Message("B"));
        assertEquals("B", second.get().getMessage().getPayload());
    }

    @Test
    void testCancelledWaiterIsSkipped() {
        AtomicReference<Receipt> first = new AtomicReference<>();
        AtomicReference<Receipt> second = new AtomicReference<>();
        MessageQueue.Waiter firstWaiter = queue.consumeAsync(first::set);
        queue.consumeAsync(second::set);

        firstWaiter.cancel();
        queue.publish(new Message("A"));

        assertNull(first.get());
        assertEquals("A", second.get().getMessage().getPayload());
    }

    // ── Interaction with the rest of the queue ────────────────────────────────

    // A message requeued by the visibility scanner should reach a waiter too,
    // not sit in the queue while someone is waiting for it.
    @Test
    void testRequeuedMessageReachesWaiter() throws InterruptedException {
        QueueConfig config = new QueueConfig(100, 5, null, null);
        MessageQueue timed = new MessageQueue("requeue-test", config, 50);

        timed.publish(new Message("Order1"));
        timed.consume(1000); // consume without acking

        AtomicReference<Receipt> delivered = new AtomicReference<>();
        timed.consumeAsync(delivered::set);

        // Let the scanner time the message out and requeue it
        Thread.sleep(500);

        assertNotNull(delivered.get(), "requeued message should be handed to the waiter");
        assertEquals("Order1", delivered.get().getMessage().getPayload());
        timed.close();
    }

    @Test
    void testNackedMessageReachesWaiter() throws InterruptedException {
        queue.publish(new Message("Order1"));
        Receipt taken = queue.consume(1000);

        AtomicReference<Receipt> delivered = new AtomicReference<>();
        queue.consumeAsync(delivered::set);

        queue.nack(taken.getReceiptHandle());

        assertNotNull(delivered.get());
        assertEquals("Order1", delivered.get().getMessage().getPayload());
    }

    // ── Concurrency ───────────────────────────────────────────────────────────

    // Every message goes to exactly one waiter — no losses, no duplicates.
    @Test
    void testConcurrentWaitersEachGetExactlyOneMessage() throws InterruptedException {
        int count = 200;
        CountDownLatch delivered = new CountDownLatch(count);
        Set<String> received = ConcurrentHashMap.newKeySet();
        AtomicInteger duplicates = new AtomicInteger();

        for (int i = 0; i < count; i++) {
            queue.consumeAsync(receipt -> {
                if (!received.add(receipt.getMessage().getId())) {
                    duplicates.incrementAndGet();
                }
                delivered.countDown();
            });
        }

        Thread producer = new Thread(() -> {
            for (int i = 0; i < count; i++) {
                queue.publish(new Message("msg-" + i));
            }
        });
        producer.start();
        producer.join();

        assertTrue(delivered.await(5, TimeUnit.SECONDS),
                "not all waiters were served: " + delivered.getCount() + " outstanding");
        assertEquals(0, duplicates.get(), "a message reached more than one waiter");
        assertEquals(count, received.size());
    }

    // Cancels racing against publishes must never double-deliver or lose a
    // message: every message either reaches a waiter or stays in the queue.
    @Test
    void testCancelRacingWithPublishLosesNothing() throws InterruptedException {
        int rounds = 500;
        AtomicInteger deliveredCount = new AtomicInteger();

        for (int i = 0; i < rounds; i++) {
            MessageQueue.Waiter waiter = queue.consumeAsync(r -> deliveredCount.incrementAndGet());

            Thread canceller = new Thread(waiter::cancel);
            Thread publisher = new Thread(() -> queue.publish(new Message("m")));
            canceller.start();
            publisher.start();
            canceller.join();
            publisher.join();
        }

        // Whatever the interleaving, published == delivered + still queued
        int stillQueued = 0;
        while (queue.consume(50) != null) {
            stillQueued++;
        }
        assertEquals(rounds, deliveredCount.get() + stillQueued,
                "messages were lost or duplicated under cancel/publish races");
    }
}
