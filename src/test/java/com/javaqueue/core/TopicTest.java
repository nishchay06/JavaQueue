package com.javaqueue.core;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.javaqueue.exception.QueueNotFoundException;
import com.javaqueue.exception.TopicNotFoundException;

/**
 * Topic registry and subscription management.
 *
 * A topic is a subscriber list, not a store — it routes messages into queues
 * that are owned independently by the QueueManager. These tests cover that
 * ownership boundary; FanOutTest covers delivery.
 */
public class TopicTest {

    private TopicManager topics;
    private QueueManager queues;

    @BeforeEach
    void setUp() {
        queues = new QueueManager();
        topics = new TopicManager(queues);
    }

    // ── Registry ──────────────────────────────────────────────────────────────

    @Test
    void testCreateTopicReturnsNamedTopic() {
        Topic topic = topics.createTopic("orders-events");

        assertEquals("orders-events", topic.getName());
    }

    @Test
    void testCreateTopicIsIdempotent() {
        Topic first = topics.createTopic("orders-events");
        Topic second = topics.createTopic("orders-events");

        assertSame(first, second, "creating the same topic twice must not replace it");
        assertEquals(1, topics.listTopics().size());
    }

    @Test
    void testGetUnknownTopicThrows() {
        assertThrows(TopicNotFoundException.class, () -> topics.getTopic("ghost"));
    }

    @Test
    void testListTopics() {
        topics.createTopic("orders-events");
        topics.createTopic("payment-events");

        assertEquals(2, topics.listTopics().size());
        assertTrue(topics.listTopics().contains("orders-events"));
        assertTrue(topics.listTopics().contains("payment-events"));
    }

    @Test
    void testDeleteTopic() {
        topics.createTopic("orders-events");
        topics.deleteTopic("orders-events");

        assertTrue(topics.listTopics().isEmpty());
        assertThrows(TopicNotFoundException.class, () -> topics.getTopic("orders-events"));
    }

    @Test
    void testDeleteUnknownTopicIsSilent() {
        assertDoesNotThrow(() -> topics.deleteTopic("never-existed"));
    }

    // Queues are owned by the QueueManager. Deleting a topic removes the
    // routing, never the queues it was routing to.
    @Test
    void testDeleteTopicDoesNotDeleteSubscriberQueues() {
        Topic topic = topics.createTopic("orders-events");
        topic.subscribe(queues.createQueue("billing"));

        topics.deleteTopic("orders-events");

        assertTrue(queues.listQueues().contains("billing"));
        assertEquals("billing", queues.getQueue("billing").getName());
    }

    // ── Subscription management ───────────────────────────────────────────────

    @Test
    void testSubscribeAddsSubscriber() {
        Topic topic = topics.createTopic("orders-events");
        topic.subscribe(queues.createQueue("billing"));

        assertEquals(1, topic.listSubscribers().size());
        assertTrue(topic.listSubscribers().contains("billing"));
    }

    @Test
    void testSubscribeIsIdempotent() {
        Topic topic = topics.createTopic("orders-events");
        MessageQueue billing = queues.createQueue("billing");

        topic.subscribe(billing);
        topic.subscribe(billing);

        assertEquals(1, topic.listSubscribers().size(),
                "double subscribe must not register the queue twice");
    }

    @Test
    void testUnsubscribeRemovesSubscriber() {
        Topic topic = topics.createTopic("orders-events");
        topic.subscribe(queues.createQueue("billing"));

        topic.unsubscribe("billing");

        assertTrue(topic.listSubscribers().isEmpty());
    }

    @Test
    void testUnsubscribeUnknownQueueIsSilent() {
        Topic topic = topics.createTopic("orders-events");

        assertDoesNotThrow(() -> topic.unsubscribe("never-subscribed"));
    }

    // Subscriptions resolve through the QueueManager at delivery time, so a
    // queue it does not know about would silently never receive anything.
    // Fail at subscribe, where the mistake is, rather than at publish.
    @Test
    void testSubscribingUnregisteredQueueThrows() {
        Topic topic = topics.createTopic("orders-events");
        MessageQueue detached = new MessageQueue("not-in-the-manager");

        assertThrows(QueueNotFoundException.class, () -> topic.subscribe(detached));
        assertTrue(topic.listSubscribers().isEmpty());
        detached.close();
    }

    // Subscribing by name is the replay path: on restart, topic subscriptions
    // may be restored before the queues themselves are recreated, so an
    // unresolvable name is recorded rather than rejected.
    @Test
    void testSubscribingByNameDoesNotRequireQueueToExistYet() {
        Topic topic = topics.createTopic("orders-events");

        assertDoesNotThrow(() -> topic.subscribe("not-yet-created"));
        assertTrue(topic.listSubscribers().contains("not-yet-created"));
    }

    @Test
    void testNewTopicHasNoSubscribers() {
        assertTrue(topics.createTopic("orders-events").listSubscribers().isEmpty());
    }

    @Test
    void testSubscriberListIsNotLiveView() {
        Topic topic = topics.createTopic("orders-events");
        topic.subscribe(queues.createQueue("billing"));

        var snapshot = topic.listSubscribers();
        topic.subscribe(queues.createQueue("analytics"));

        assertEquals(1, snapshot.size(),
                "listSubscribers() must return a snapshot, not a live view of internal state");
    }

    // ── Concurrency ───────────────────────────────────────────────────────────

    // Same guarantee QueueManager gives: concurrent creation of one name
    // yields exactly one topic, not a lost-update race.
    @Test
    void testConcurrentCreateTopicYieldsOneTopic() throws InterruptedException {
        int threads = 20;
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(threads);

        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    start.await();
                    topics.createTopic("contended");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));

        assertEquals(1, topics.listTopics().size());
    }

    @Test
    void testConcurrentSubscribeIsSafe() throws InterruptedException {
        Topic topic = topics.createTopic("orders-events");
        int subscriberCount = 50;
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(10);

        for (int i = 0; i < subscriberCount; i++) {
            String queueName = "sub-" + i;
            pool.submit(() -> {
                try {
                    start.await();
                    topic.subscribe(queues.createQueue(queueName));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));

        assertEquals(subscriberCount, topic.listSubscribers().size());
    }
}
