package com.javaqueue.core;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Fan-out delivery semantics.
 *
 * The model is SNS to SQS: publishing to a topic delivers the message into
 * every subscriber's queue, and each subscriber then consumes it completely
 * independently — its own visibility timeout, its own retries, its own DLQ.
 */
public class FanOutTest {

    private static final long VISIBILITY_TIMEOUT_MS = 100;

    // QueueManager.createQueue does not expose the scan interval, so queues it
    // builds use MessageQueue's 1000ms default.
    private static final long DEFAULT_SCAN_INTERVAL_MS = 1000;

    private TopicManager topics;
    private QueueManager queues;

    @BeforeEach
    void setUp() {
        queues = new QueueManager();
        topics = new TopicManager(queues);
    }

    // ── Core delivery ─────────────────────────────────────────────────────────

    @Test
    void testPublishDeliversToEverySubscriber() throws InterruptedException {
        Topic topic = topics.createTopic("orders-events");
        MessageQueue billing = queues.createQueue("billing");
        MessageQueue analytics = queues.createQueue("analytics");
        MessageQueue audit = queues.createQueue("audit");
        topic.subscribe(billing);
        topic.subscribe(analytics);
        topic.subscribe(audit);

        topic.publish(new Message("Order1"));

        assertEquals("Order1", billing.consume(1000).getMessage().getPayload());
        assertEquals("Order1", analytics.consume(1000).getMessage().getPayload());
        assertEquals("Order1", audit.consume(1000).getMessage().getPayload());
    }

    @Test
    void testPublishReturnsDeliveryCount() {
        Topic topic = topics.createTopic("orders-events");
        topic.subscribe(queues.createQueue("billing"));
        topic.subscribe(queues.createQueue("analytics"));

        assertEquals(2, topic.publish(new Message("Order1")));
    }

    // A publish that reaches nobody is a real condition worth surfacing, not
    // an error — returning 0 makes it observable without failing the producer.
    @Test
    void testPublishToTopicWithNoSubscribersReturnsZero() {
        Topic topic = topics.createTopic("orders-events");

        assertEquals(0, topic.publish(new Message("Order1")));
    }

    // Message is immutable, so all subscribers can share one instance rather
    // than each holding a copy. This is the payoff for Phase 1's immutability.
    @Test
    void testSubscribersShareTheSameMessageInstance() throws InterruptedException {
        Topic topic = topics.createTopic("orders-events");
        MessageQueue billing = queues.createQueue("billing");
        MessageQueue analytics = queues.createQueue("analytics");
        topic.subscribe(billing);
        topic.subscribe(analytics);

        Message published = new Message("Order1");
        topic.publish(published);

        Message fromBilling = billing.consume(1000).getMessage();
        Message fromAnalytics = analytics.consume(1000).getMessage();

        assertSame(published, fromBilling);
        assertSame(published, fromAnalytics);
        assertEquals(fromBilling.getId(), fromAnalytics.getId(),
                "one logical message should carry one id across all groups");
    }

    // Sharing the instance must not mean sharing the delivery. Each subscriber
    // gets its own receipt handle.
    @Test
    void testEachSubscriberGetsItsOwnReceipt() throws InterruptedException {
        Topic topic = topics.createTopic("orders-events");
        MessageQueue billing = queues.createQueue("billing");
        MessageQueue analytics = queues.createQueue("analytics");
        topic.subscribe(billing);
        topic.subscribe(analytics);

        topic.publish(new Message("Order1"));

        Receipt billingReceipt = billing.consume(1000);
        Receipt analyticsReceipt = analytics.consume(1000);

        assertNotEquals(billingReceipt.getReceiptHandle(), analyticsReceipt.getReceiptHandle());
    }

    @Test
    void testAckOnOneSubscriberDoesNotAffectAnother() throws InterruptedException {
        Topic topic = topics.createTopic("orders-events");
        MessageQueue billing = queues.createQueue("billing");
        MessageQueue analytics = queues.createQueue("analytics");
        topic.subscribe(billing);
        topic.subscribe(analytics);

        topic.publish(new Message("Order1"));

        Receipt billingReceipt = billing.consume(1000);
        billing.acknowledge(billingReceipt.getReceiptHandle());

        // analytics still has its own undelivered copy
        assertEquals("Order1", analytics.consume(1000).getMessage().getPayload());
    }

    @Test
    void testQueueSubscribedToTwoTopicsReceivesFromBoth() throws InterruptedException {
        Topic orders = topics.createTopic("orders-events");
        Topic payments = topics.createTopic("payment-events");
        MessageQueue audit = queues.createQueue("audit");
        orders.subscribe(audit);
        payments.subscribe(audit);

        orders.publish(new Message("OrderPlaced"));
        payments.publish(new Message("PaymentTaken"));

        assertNotNull(audit.consume(1000));
        assertNotNull(audit.consume(1000));
    }

    // ── Subscription timing ───────────────────────────────────────────────────

    // The defining limitation of fan-out: a topic keeps no history, so a late
    // subscriber has nothing to catch up on. This is what Phase 6 exists to fix.
    @Test
    void testLateSubscriberDoesNotReceiveEarlierMessages() throws InterruptedException {
        Topic topic = topics.createTopic("orders-events");
        MessageQueue billing = queues.createQueue("billing");
        topic.subscribe(billing);

        topic.publish(new Message("Before"));

        MessageQueue analytics = queues.createQueue("analytics");
        topic.subscribe(analytics);

        assertNull(analytics.consume(200), "a late subscriber must not be backfilled");
        assertEquals("Before", billing.consume(1000).getMessage().getPayload());
    }

    @Test
    void testUnsubscribeStopsFutureDelivery() throws InterruptedException {
        Topic topic = topics.createTopic("orders-events");
        MessageQueue billing = queues.createQueue("billing");
        topic.subscribe(billing);

        topic.unsubscribe("billing");
        topic.publish(new Message("AfterUnsubscribe"));

        assertNull(billing.consume(200));
    }

    // Unsubscribing detaches routing; it does not reach into the queue and
    // remove work already delivered there.
    @Test
    void testUnsubscribeLeavesAlreadyDeliveredMessages() throws InterruptedException {
        Topic topic = topics.createTopic("orders-events");
        MessageQueue billing = queues.createQueue("billing");
        topic.subscribe(billing);

        topic.publish(new Message("Delivered"));
        topic.unsubscribe("billing");

        assertEquals("Delivered", billing.consume(1000).getMessage().getPayload());
    }

    @Test
    void testDeleteTopicStopsDeliveryButLeavesQueueConsumable() throws InterruptedException {
        Topic topic = topics.createTopic("orders-events");
        MessageQueue billing = queues.createQueue("billing");
        topic.subscribe(billing);

        topic.publish(new Message("Delivered"));
        topics.deleteTopic("orders-events");

        assertEquals("Delivered", billing.consume(1000).getMessage().getPayload());
    }

    // ── Subscriber queue lifecycle ────────────────────────────────────────────

    // A subscription names a queue; it does not pin one particular instance.
    // Deleting and recreating a queue must not leave the topic publishing into
    // the dead object — those messages would be unreachable, and the recreated
    // queue would silently receive nothing.
    @Test
    void testPublishGoesToRecreatedQueueNotTheDeadInstance() throws InterruptedException {
        Topic topic = topics.createTopic("orders-events");
        topic.subscribe(queues.createQueue("billing"));

        queues.deleteQueue("billing");
        MessageQueue recreated = queues.createQueue("billing");

        topic.publish(new Message("Order1"));

        assertEquals("Order1", recreated.consume(1000).getMessage().getPayload(),
                "publish went to the deleted queue instance, not the live one");
    }

    // A deleted subscriber should not break delivery for its siblings.
    @Test
    void testPublishSkipsDeletedSubscriberAndStillDeliversToOthers() throws InterruptedException {
        Topic topic = topics.createTopic("orders-events");
        topic.subscribe(queues.createQueue("billing"));
        topic.subscribe(queues.createQueue("analytics"));

        queues.deleteQueue("billing");

        int delivered = topic.publish(new Message("Order1"));

        assertEquals(1, delivered, "delivery count should reflect only reachable subscribers");
        assertEquals("Order1",
                queues.getQueue("analytics").consume(1000).getMessage().getPayload());
    }

    // ── Interaction with delivery guarantees ──────────────────────────────────

    // Each subscriber has its own retry budget and DLQ, so one logical message
    // can succeed on one group and be dead-lettered on another.
    @Test
    void testMessageCanDeadLetterOnOneSubscriberAndAckOnAnother() throws InterruptedException {
        Topic topic = topics.createTopic("orders-events");

        // billing gives up after 1 delivery and routes to its own DLQ
        QueueConfig failing = new QueueConfig(VISIBILITY_TIMEOUT_MS, 1, "billing-dlq", null);
        MessageQueue billing = queues.createQueue("billing", failing);
        MessageQueue analytics = queues.createQueue("analytics");
        topic.subscribe(billing);
        topic.subscribe(analytics);

        topic.publish(new Message("Order1"));

        // billing rejects it — exhausts retries, lands in the DLQ
        billing.nack(billing.consume(1000).getReceiptHandle());
        assertEquals("Order1",
                queues.getQueue("billing-dlq").consume(1000).getMessage().getPayload());

        // analytics processes the same logical message successfully
        Receipt ok = analytics.consume(1000);
        assertEquals("Order1", ok.getMessage().getPayload());
        analytics.acknowledge(ok.getReceiptHandle());
    }

    // retryCounts is a per-queue map keyed by message id. Since subscribers
    // share one Message instance, a leak here would corrupt sibling groups.
    @Test
    void testRetryCountsDoNotLeakBetweenSubscribers() throws InterruptedException {
        Topic topic = topics.createTopic("orders-events");
        QueueConfig config = new QueueConfig(VISIBILITY_TIMEOUT_MS, 5, null, null);
        MessageQueue billing = queues.createQueue("billing", config);
        MessageQueue analytics = queues.createQueue("analytics", config);
        topic.subscribe(billing);
        topic.subscribe(analytics);

        topic.publish(new Message("Order1"));

        // Nack three times on billing only
        for (int i = 0; i < 3; i++) {
            billing.nack(billing.consume(1000).getReceiptHandle());
        }

        // analytics has never nacked this message — its retry count is still 0
        Receipt analyticsReceipt = analytics.consume(1000);
        assertEquals(0, analytics.inFlightMessages.get(analyticsReceipt.getReceiptHandle())
                .getRetryCount(), "retry state leaked across subscribers");
    }

    @Test
    void testEachSubscriberHasIndependentVisibilityTimeout() throws InterruptedException {
        Topic topic = topics.createTopic("orders-events");

        // Must go through the QueueManager — that is what delivery resolves
        // against. QueueManager uses the default 1000ms scan interval, so the
        // wait below is sized for that rather than the faster test constant.
        QueueConfig quick = new QueueConfig(VISIBILITY_TIMEOUT_MS, 5, null, null);
        MessageQueue billing = queues.createQueue("billing", quick);
        MessageQueue analytics = queues.createQueue("analytics");
        topic.subscribe(billing);
        topic.subscribe(analytics);

        topic.publish(new Message("Order1"));

        // Consume on billing and never ack — its scanner should redeliver
        billing.consume(1000);
        Thread.sleep(VISIBILITY_TIMEOUT_MS + DEFAULT_SCAN_INTERVAL_MS + 500);

        assertNotNull(billing.consume(1000), "billing should have redelivered after timeout");

        // analytics was untouched by billing's timeout
        assertEquals("Order1", analytics.consume(1000).getMessage().getPayload());
    }

    // ── Concurrency ───────────────────────────────────────────────────────────

    @Test
    void testConcurrentPublishLosesNothing() throws InterruptedException {
        Topic topic = topics.createTopic("orders-events");
        int subscriberCount = 3;
        for (int i = 0; i < subscriberCount; i++) {
            topic.subscribe(queues.createQueue("sub-" + i));
        }

        int producers = 5;
        int perProducer = 200;
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(producers);

        for (int p = 0; p < producers; p++) {
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < perProducer; i++) {
                        topic.publish(new Message("msg"));
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(15, TimeUnit.SECONDS));

        int expected = producers * perProducer;
        for (int i = 0; i < subscriberCount; i++) {
            int drained = drain(queues.getQueue("sub-" + i));
            assertEquals(expected, drained, "subscriber sub-" + i + " lost messages");
        }
    }

    // Subscription changes during an in-flight publish must not corrupt the
    // subscriber list or throw — publish iterates a snapshot.
    @Test
    void testConcurrentSubscribeDuringPublishIsSafe() throws InterruptedException {
        Topic topic = topics.createTopic("orders-events");
        topic.subscribe(queues.createQueue("stable"));

        AtomicInteger failures = new AtomicInteger();
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(4);

        pool.submit(() -> {
            try {
                start.await();
                for (int i = 0; i < 500; i++) {
                    topic.publish(new Message("msg-" + i));
                }
            } catch (Exception e) {
                failures.incrementAndGet();
            }
        });

        pool.submit(() -> {
            try {
                start.await();
                for (int i = 0; i < 100; i++) {
                    String name = "churn-" + i;
                    topic.subscribe(queues.createQueue(name));
                    topic.unsubscribe(name);
                }
            } catch (Exception e) {
                failures.incrementAndGet();
            }
        });

        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(15, TimeUnit.SECONDS));

        assertEquals(0, failures.get(), "concurrent subscribe during publish threw");
        assertTrue(topic.listSubscribers().contains("stable"));
    }

    // Drains a queue until it is empty, acknowledging as it goes.
    private int drain(MessageQueue queue) throws InterruptedException {
        int count = 0;
        Receipt receipt;
        while ((receipt = queue.consume(200)) != null) {
            queue.acknowledge(receipt.getReceiptHandle());
            count++;
        }
        return count;
    }
}
