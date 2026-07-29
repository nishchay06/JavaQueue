package com.javaqueue.core;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

/**
 * The accessors an operator view needs.
 *
 * Everything here was previously observable only from inside the package or
 * by consuming, which is a destructive way to answer "how deep is this queue".
 */
public class ObservabilityTest {

    @Test
    void testQueueDepthCountsWaitingMessages() throws InterruptedException {
        MessageQueue queue = new MessageQueue("orders");

        assertEquals(0, queue.depth());
        queue.publish(new Message("a"));
        queue.publish(new Message("b"));
        assertEquals(2, queue.depth());

        queue.consume(1000);
        assertEquals(1, queue.depth(), "a consumed message is no longer waiting");
        queue.close();
    }

    @Test
    void testInFlightCountTracksUnacknowledgedMessages() throws InterruptedException {
        MessageQueue queue = new MessageQueue("orders");
        queue.publish(new Message("a"));

        assertEquals(0, queue.inFlightCount());
        Receipt receipt = queue.consume(1000);
        assertEquals(1, queue.inFlightCount());

        queue.acknowledge(receipt.getReceiptHandle());
        assertEquals(0, queue.inFlightCount());
        queue.close();
    }

    // Reading depth must not consume anything — the whole point of an
    // observability accessor.
    @Test
    void testDepthIsNonDestructive() throws InterruptedException {
        MessageQueue queue = new MessageQueue("orders");
        queue.publish(new Message("a"));

        queue.depth();
        queue.depth();

        assertEquals("a", queue.consume(1000).getMessage().getPayload());
        queue.close();
    }

    @Test
    void testDeadLetterQueueNameIsVisible() {
        QueueManager manager = new QueueManager();
        MessageQueue queue = manager.createQueue("orders",
                new QueueConfig(30_000, 3, "orders-dlq", null));

        assertEquals("orders-dlq", queue.getDeadLetterQueueName());
        assertEquals(null, manager.getQueue("orders-dlq").getDeadLetterQueueName());
    }

    // A log knows which groups have read from it — needed to show lag per
    // group without the caller having to guess names.
    @Test
    void testLogListsItsKnownGroups() {
        MessageLog log = new MessageLog("events");
        log.append(new Message("a"));

        assertTrue(log.groups().isEmpty(), "a log starts with no groups");

        log.poll("billing", 10);
        log.commit("analytics", 0);

        assertEquals(List.of("analytics", "billing"), log.groups().stream().sorted().toList());
        log.close();
    }

    @Test
    void testLogRecordCount() {
        MessageLog log = new MessageLog("events",
                new LogConfig(0, 2, OffsetResetPolicy.EARLIEST, null));

        log.append(new Message("a"));
        log.append(new Message("b"));
        log.append(new Message("c")); // trims the oldest

        assertEquals(2, log.recordCount(), "counts retained records, not offsets assigned");
        assertEquals(1, log.beginOffset());
        assertEquals(3, log.endOffset());
        log.close();
    }
}
