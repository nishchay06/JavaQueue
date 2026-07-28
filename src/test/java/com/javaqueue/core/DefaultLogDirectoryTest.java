package com.javaqueue.core;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A server-wide default log directory.
 *
 * Until now durability was per-queue: you had to pass logDirectory on every
 * create, so a queue made over HTTP without one silently lost its messages on
 * restart. A default on the registry makes persistence a property of the
 * deployment rather than of each individual call.
 */
public class DefaultLogDirectoryTest {

    private Path tempDir;
    private String logDir;

    @BeforeEach
    void setUp() throws IOException {
        tempDir = Files.createTempDirectory("javaqueue-default-logdir-test");
        logDir = tempDir.toString();
    }

    @AfterEach
    void tearDown() throws IOException {
        Files.walk(tempDir)
                .sorted(java.util.Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(java.io.File::delete);
    }

    @Test
    void testQueuesGetPersistenceFromTheManagerDefault() {
        QueueManager manager = new QueueManager(logDir);

        MessageQueue queue = manager.createQueue("orders");
        queue.publish(new Message("Order1"));
        queue.close();

        assertTrue(Files.exists(tempDir.resolve("orders.log")),
                "a queue created without explicit config should inherit the default log directory");
    }

    @Test
    void testMessagesSurviveRestartWithOnlyTheManagerDefault() throws InterruptedException {
        QueueManager manager = new QueueManager(logDir);
        MessageQueue queue = manager.createQueue("orders");
        queue.publish(new Message("Order1"));
        queue.close();

        MessageQueue restarted = new QueueManager(logDir).createQueue("orders");

        assertEquals("Order1", restarted.consume(1000).getMessage().getPayload());
        restarted.close();
    }

    // An explicit logDirectory is a deliberate override — it must win.
    @Test
    void testExplicitLogDirectoryOverridesTheDefault() throws IOException {
        Path override = Files.createDirectory(tempDir.resolve("elsewhere"));
        QueueManager manager = new QueueManager(logDir);

        MessageQueue queue = manager.createQueue("orders",
                new QueueConfig(30_000, 3, null, override.toString()));
        queue.publish(new Message("Order1"));
        queue.close();

        assertTrue(Files.exists(override.resolve("orders.log")));
        assertFalse(Files.exists(tempDir.resolve("orders.log")));
    }

    // A dead letter queue that loses its contents on restart defeats the point
    // of having one — it exists precisely to hold messages for later inspection.
    @Test
    void testAutoCreatedDeadLetterQueueInheritsTheDefault() {
        QueueManager manager = new QueueManager(logDir);

        manager.createQueue("orders", new QueueConfig(30_000, 3, "orders-dlq", null));
        manager.getQueue("orders-dlq").publish(new Message("Dead1"));
        manager.deleteQueue("orders-dlq");

        assertTrue(Files.exists(tempDir.resolve("orders-dlq.log")),
                "an auto-created DLQ should be as durable as the queue that feeds it");
    }

    // Existing behaviour must be untouched: no default means no persistence.
    @Test
    void testManagerWithoutDefaultCreatesNoLogs() {
        QueueManager manager = new QueueManager();

        MessageQueue queue = manager.createQueue("orders");
        queue.publish(new Message("Order1"));
        queue.close();

        assertFalse(Files.exists(tempDir.resolve("orders.log")));
    }

    @Test
    void testTopicManagerTakesTheSameDefault() {
        QueueManager queues = new QueueManager(logDir);
        TopicManager topics = new TopicManager(queues, logDir);

        topics.createTopic("orders-events");
        topics.getTopic("orders-events").subscribe(queues.createQueue("billing"));
        topics.close();

        TopicManager restarted = new TopicManager(new QueueManager(logDir), logDir);

        assertTrue(restarted.getTopic("orders-events").listSubscribers().contains("billing"));
    }
}
