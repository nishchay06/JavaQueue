package com.javaqueue.core;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Append and read semantics for the retained log.
 *
 * The defining property, and the one every other behaviour follows from:
 * reading does not remove. Two groups both see every record.
 */
public class MessageLogTest {

    private MessageLog log;

    @BeforeEach
    void setUp() {
        log = new MessageLog("orders");
    }

    @AfterEach
    void tearDown() {
        log.close();
    }

    private void appendAll(String... payloads) {
        for (String payload : payloads) {
            log.append(new Message(payload));
        }
    }

    private static List<String> payloadsOf(LogRecords records) {
        return records.messages().stream().map(Message::getPayload).toList();
    }

    // ── Append ────────────────────────────────────────────────────────────────

    @Test
    void testOffsetsStartAtZeroAndIncrementByOne() {
        assertEquals(0, log.append(new Message("a")));
        assertEquals(1, log.append(new Message("b")));
        assertEquals(2, log.append(new Message("c")));
    }

    @Test
    void testEmptyLogHasMatchingBeginAndEndOffsets() {
        assertEquals(0, log.beginOffset());
        assertEquals(0, log.endOffset());
    }

    @Test
    void testEndOffsetIsTheNextOffsetToBeAssigned() {
        appendAll("a", "b", "c");

        assertEquals(0, log.beginOffset());
        assertEquals(3, log.endOffset(), "endOffset is exclusive — the next offset, not the last");
    }

    // ── Reading does not remove ───────────────────────────────────────────────

    @Test
    void testPollReturnsRecordsInOrder() {
        appendAll("a", "b", "c");

        LogRecords records = log.poll("g1", 10);

        assertEquals(List.of("a", "b", "c"), payloadsOf(records));
        assertEquals(0, records.startOffset());
        assertEquals(3, records.nextOffset());
    }

    // The whole reason a log exists rather than a queue.
    @Test
    void testTwoGroupsBothReadEveryRecord() {
        appendAll("a", "b", "c");

        assertEquals(List.of("a", "b", "c"), payloadsOf(log.poll("billing", 10)));
        assertEquals(List.of("a", "b", "c"), payloadsOf(log.poll("analytics", 10)));
    }

    @Test
    void testPollingDoesNotShrinkTheLog() {
        appendAll("a", "b", "c");

        log.poll("g1", 10);

        assertEquals(0, log.beginOffset());
        assertEquals(3, log.endOffset(), "reading must not remove anything");
    }

    @Test
    void testPollRespectsMaxRecords() {
        appendAll("a", "b", "c", "d", "e");

        LogRecords first = log.poll("g1", 2);
        assertEquals(List.of("a", "b"), payloadsOf(first));
        assertEquals(2, first.nextOffset());

        assertEquals(List.of("c", "d"), payloadsOf(log.poll("g1", 2)));
    }

    @Test
    void testPollAtEndReturnsEmptyNotError() {
        appendAll("a");
        log.poll("g1", 10);

        LogRecords empty = log.poll("g1", 10);

        assertTrue(empty.isEmpty());
        assertEquals(1, empty.startOffset());
        assertEquals(1, empty.nextOffset());
    }

    @Test
    void testPollOnEmptyLogReturnsEmpty() {
        assertTrue(log.poll("g1", 10).isEmpty());
    }

    @Test
    void testPollSeesRecordsAppendedAfterAnEarlierPoll() {
        appendAll("a");
        log.poll("g1", 10);

        appendAll("b");

        assertEquals(List.of("b"), payloadsOf(log.poll("g1", 10)));
    }

    // ── Position advances, groups are independent ─────────────────────────────

    @Test
    void testPollAdvancesPosition() {
        appendAll("a", "b", "c");

        assertEquals(0, log.position("g1"));
        log.poll("g1", 2);
        assertEquals(2, log.position("g1"));
    }

    @Test
    void testGroupsProgressIndependently() {
        appendAll("a", "b", "c", "d");

        log.poll("fast", 4);
        log.poll("slow", 1);

        assertEquals(4, log.position("fast"));
        assertEquals(1, log.position("slow"));
        assertEquals(List.of("b", "c", "d"), payloadsOf(log.poll("slow", 10)));
    }

    @Test
    void testNewGroupStartsAtTheBeginningByDefault() {
        appendAll("a", "b");

        // A group first seen after the records were written still gets them —
        // unlike fan-out, where a late subscriber is never backfilled.
        assertEquals(List.of("a", "b"), payloadsOf(log.poll("late", 10)));
    }

    @Test
    void testNewGroupStartsAtTheEndUnderLatestPolicy() {
        MessageLog latest = new MessageLog("latest",
                new LogConfig(0, 0, OffsetResetPolicy.LATEST, null));
        latest.append(new Message("before"));

        assertTrue(latest.poll("g1", 10).isEmpty(),
                "LATEST means a new group skips what it missed");

        latest.append(new Message("after"));
        assertEquals(List.of("after"), payloadsOf(latest.poll("g1", 10)));
        latest.close();
    }

    // ── Seek ──────────────────────────────────────────────────────────────────

    @Test
    void testSeekBackwardsReReads() {
        appendAll("a", "b", "c");
        log.poll("g1", 10);

        log.seek("g1", 1);

        assertEquals(List.of("b", "c"), payloadsOf(log.poll("g1", 10)));
    }

    @Test
    void testSeekForwardsSkips() {
        appendAll("a", "b", "c");

        log.seek("g1", 2);

        assertEquals(List.of("c"), payloadsOf(log.poll("g1", 10)));
    }

    @Test
    void testSeekToBeginningAndToEnd() {
        appendAll("a", "b", "c");
        log.poll("g1", 10);

        log.seekToBeginning("g1");
        assertEquals(0, log.position("g1"));

        log.seekToEnd("g1");
        assertEquals(3, log.position("g1"));
        assertTrue(log.poll("g1", 10).isEmpty());
    }

    // ── Lag ───────────────────────────────────────────────────────────────────

    // Lag is measured against the committed offset, not the read position:
    // records read but not committed are still outstanding work.
    @Test
    void testLagIsMeasuredFromCommittedNotPosition() {
        appendAll("a", "b", "c");

        assertEquals(3, log.lag("g1"));

        log.poll("g1", 3);
        assertEquals(3, log.lag("g1"), "polling alone does not reduce lag");

        log.commit("g1", 3);
        assertEquals(0, log.lag("g1"));
    }

    @Test
    void testLagOfEmptyLogIsZero() {
        assertEquals(0, log.lag("g1"));
    }

    // ── Concurrency ───────────────────────────────────────────────────────────

    @Test
    void testConcurrentAppendsAssignUniqueGaplessOffsets() throws InterruptedException {
        int producers = 8;
        int perProducer = 500;
        Set<Long> offsets = ConcurrentHashMap.newKeySet();
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(producers);

        for (int p = 0; p < producers; p++) {
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < perProducer; i++) {
                        offsets.add(log.append(new Message("m")));
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(15, TimeUnit.SECONDS));

        int total = producers * perProducer;
        assertEquals(total, offsets.size(), "offsets were duplicated");
        assertEquals(total, log.endOffset());
        assertFalse(offsets.contains((long) total), "offsets should be gapless from 0 to n-1");
        assertTrue(offsets.contains(0L) && offsets.contains((long) total - 1));
    }

    @Test
    void testConcurrentPollsFromDifferentGroupsDoNotInterfere() throws InterruptedException {
        int records = 300;
        for (int i = 0; i < records; i++) {
            log.append(new Message("m-" + i));
        }

        int groups = 6;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(groups);
        ExecutorService pool = Executors.newFixedThreadPool(groups);
        ConcurrentHashMap<String, Integer> readCounts = new ConcurrentHashMap<>();

        for (int g = 0; g < groups; g++) {
            String group = "group-" + g;
            pool.submit(() -> {
                try {
                    start.await();
                    int seen = 0;
                    LogRecords batch;
                    while (!(batch = log.poll(group, 7)).isEmpty()) {
                        seen += batch.size();
                    }
                    readCounts.put(group, seen);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }

        start.countDown();
        assertTrue(done.await(15, TimeUnit.SECONDS));
        pool.shutdown();

        assertEquals(groups, readCounts.size());
        readCounts.forEach((group, seen) -> assertEquals(records, seen,
                group + " did not read every record"));
    }
}
