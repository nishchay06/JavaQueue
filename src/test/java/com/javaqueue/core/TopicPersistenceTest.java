package com.javaqueue.core;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.javaqueue.exception.TopicNotFoundException;

/**
 * Topic wiring must survive a restart.
 *
 * Without this, a crash leaves a topic that still accepts publishes and
 * silently delivers to nobody — the worst failure mode available, because
 * everything looks healthy.
 */
public class TopicPersistenceTest {

    private Path tempDir;
    private String logDir;
    private QueueManager queues;
    private TopicManager topics;

    @BeforeEach
    void setUp() throws IOException {
        tempDir = Files.createTempDirectory("javaqueue-topic-test");
        logDir = tempDir.toString();
        queues = new QueueManager();
        topics = new TopicManager(queues, logDir);
    }

    @AfterEach
    void tearDown() throws IOException {
        Files.walk(tempDir)
                .sorted(java.util.Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(java.io.File::delete);
    }

    // Simulates a restart: a fresh registry pair over the same log directory.
    // Queues are recreated by the application on startup, as they are today.
    private TopicManager restart(String... queueNames) {
        queues = new QueueManager();
        for (String name : queueNames) {
            queues.createQueue(name);
        }
        return new TopicManager(queues, logDir);
    }

    // ── Survival ──────────────────────────────────────────────────────────────

    @Test
    void testSubscriptionsSurviveRestart() {
        Topic topic = topics.createTopic("orders-events");
        topic.subscribe(queues.createQueue("billing"));
        topic.subscribe(queues.createQueue("analytics"));

        TopicManager restarted = restart("billing", "analytics");

        assertEquals(2, restarted.getTopic("orders-events").listSubscribers().size());
        assertTrue(restarted.getTopic("orders-events").listSubscribers().contains("billing"));
        assertTrue(restarted.getTopic("orders-events").listSubscribers().contains("analytics"));
    }

    // A topic that exists but has no subscribers is a real, distinct state —
    // it must not be indistinguishable from a topic that was never created.
    @Test
    void testEmptyTopicSurvivesRestart() {
        topics.createTopic("orders-events");

        TopicManager restarted = restart();

        assertTrue(restarted.listTopics().contains("orders-events"));
        assertTrue(restarted.getTopic("orders-events").listSubscribers().isEmpty());
        assertThrows(TopicNotFoundException.class, () -> restarted.getTopic("never-created"));
    }

    @Test
    void testUnsubscribeSurvivesRestart() {
        Topic topic = topics.createTopic("orders-events");
        topic.subscribe(queues.createQueue("billing"));
        topic.subscribe(queues.createQueue("analytics"));
        topic.unsubscribe("billing");

        TopicManager restarted = restart("billing", "analytics");

        assertEquals(1, restarted.getTopic("orders-events").listSubscribers().size());
        assertTrue(restarted.getTopic("orders-events").listSubscribers().contains("analytics"));
    }

    @Test
    void testDeletedTopicDoesNotComeBack() {
        topics.createTopic("orders-events");
        topics.createTopic("payment-events");
        topics.deleteTopic("orders-events");

        TopicManager restarted = restart();

        assertFalse(restarted.listTopics().contains("orders-events"));
        assertTrue(restarted.listTopics().contains("payment-events"));
    }

    @Test
    void testResubscribeAfterUnsubscribeSurvivesRestart() {
        Topic topic = topics.createTopic("orders-events");
        topic.subscribe(queues.createQueue("billing"));
        topic.unsubscribe("billing");
        topic.subscribe("billing");

        TopicManager restarted = restart("billing");

        assertTrue(restarted.getTopic("orders-events").listSubscribers().contains("billing"));
    }

    // ── End to end ────────────────────────────────────────────────────────────

    @Test
    void testFanOutStillWorksAfterRestart() throws InterruptedException {
        Topic topic = topics.createTopic("orders-events");
        topic.subscribe(queues.createQueue("billing"));
        topic.subscribe(queues.createQueue("analytics"));

        TopicManager restarted = restart("billing", "analytics");
        int delivered = restarted.getTopic("orders-events").publish(new Message("AfterRestart"));

        assertEquals(2, delivered);
        assertEquals("AfterRestart", queues.getQueue("billing").consume(1000).getMessage().getPayload());
        assertEquals("AfterRestart", queues.getQueue("analytics").consume(1000).getMessage().getPayload());
    }

    // Subscriptions are restored before the application has necessarily
    // recreated every queue. The record must be kept, and start delivering
    // once a queue answers to that name.
    @Test
    void testSubscriptionToNotYetRecreatedQueueIsRetained() throws InterruptedException {
        Topic topic = topics.createTopic("orders-events");
        topic.subscribe(queues.createQueue("billing"));

        // Restart without recreating billing
        TopicManager restarted = restart();
        Topic restoredTopic = restarted.getTopic("orders-events");

        assertTrue(restoredTopic.listSubscribers().contains("billing"),
                "subscription must be retained even though the queue is missing");
        assertEquals(0, restoredTopic.publish(new Message("Nobody")),
                "delivery should be skipped while the queue is absent");

        // Application creates the queue late — delivery resumes
        MessageQueue billing = queues.createQueue("billing");
        assertEquals(1, restoredTopic.publish(new Message("NowItWorks")));
        assertEquals("NowItWorks", billing.consume(1000).getMessage().getPayload());
    }

    // ── Log file behaviour ────────────────────────────────────────────────────

    @Test
    void testLogFileCreatedWhenLogDirectoryConfigured() {
        topics.createTopic("orders-events");

        assertTrue(Files.exists(tempDir.resolve("_topics.log")));
    }

    @Test
    void testNoPersistenceWithNullLogDirectory() throws IOException {
        // A directory of its own — the shared fixture already opened a log in
        // tempDir, so asserting against that would prove nothing.
        Path untouched = Files.createDirectory(tempDir.resolve("no-persistence"));

        TopicManager transientTopics = new TopicManager(new QueueManager(), null);
        transientTopics.createTopic("orders-events");
        transientTopics.getTopic("orders-events").subscribe("billing");

        assertTrue(Files.list(untouched).findAny().isEmpty(),
                "no log file should be written without a logDirectory");
        assertTrue(transientTopics.getTopic("orders-events").listSubscribers().contains("billing"),
                "the topic should still work in memory");
    }

    // A crash mid-write leaves a partial line. Replay must recover everything
    // before it rather than losing the whole topic configuration.
    @Test
    void testCorruptedLineIsSkipped() throws IOException {
        Topic topic = topics.createTopic("orders-events");
        topic.subscribe(queues.createQueue("billing"));

        Files.writeString(tempDir.resolve("_topics.log"), "{\"op\":\"SUBSCRIBE\",\"top",
                StandardOpenOption.APPEND);

        TopicManager restarted = restart("billing");

        assertTrue(restarted.getTopic("orders-events").listSubscribers().contains("billing"));
    }

    // Replay rewrites the log as current state, so churn does not accumulate
    // across restarts — the same compaction the message WAL does.
    @Test
    void testLogIsCompactedAfterReplay() throws IOException {
        Topic topic = topics.createTopic("orders-events");
        for (int i = 0; i < 20; i++) {
            topic.subscribe("churn-" + i);
            topic.unsubscribe("churn-" + i);
        }
        topic.subscribe(queues.createQueue("billing"));

        long linesBefore = Files.lines(tempDir.resolve("_topics.log")).filter(l -> !l.isBlank()).count();
        restart("billing");
        long linesAfter = Files.lines(tempDir.resolve("_topics.log")).filter(l -> !l.isBlank()).count();

        assertTrue(linesBefore > 40, "expected churn to accumulate, saw " + linesBefore);
        assertEquals(2, linesAfter,
                "after compaction expected one CREATE plus one SUBSCRIBE, saw " + linesAfter);
    }

    @Test
    void testEmptyLogDirectoryStartsClean() {
        TopicManager fresh = new TopicManager(new QueueManager(), logDir);

        assertTrue(fresh.listTopics().isEmpty());
        assertNull(fresh.listTopics().stream().findAny().orElse(null));
    }
}
