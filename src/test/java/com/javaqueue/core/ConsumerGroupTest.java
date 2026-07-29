package com.javaqueue.core;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.javaqueue.exception.OffsetOutOfRangeException;

/**
 * Position versus committed offset — the core of the log model.
 *
 * A group has two cursors. The position is in-memory and advanced by poll; it
 * is lost when the consumer restarts. The committed offset is durable and only
 * moves when commit is called.
 *
 * The distance between them is the entirety of delivery semantics, and the two
 * crash tests below are the point of the whole phase.
 */
public class ConsumerGroupTest {

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

    // ── The two cursors ───────────────────────────────────────────────────────

    @Test
    void testPollAdvancesPositionButNotCommitted() {
        appendAll("a", "b", "c");

        log.poll("g1", 3);

        assertEquals(3, log.position("g1"));
        assertEquals(0, log.committed("g1"), "poll must never move the committed offset");
    }

    @Test
    void testCommitAdvancesCommittedButNotPosition() {
        appendAll("a", "b", "c");
        log.poll("g1", 3);

        log.commit("g1", 3);

        assertEquals(3, log.committed("g1"));
        assertEquals(3, log.position("g1"));
    }

    @Test
    void testCommitTheNextOffsetNotTheLastRecordRead() {
        appendAll("a", "b", "c");

        LogRecords records = log.poll("g1", 10);
        log.commit("g1", records.nextOffset());
        log.resetPositionToCommitted("g1");

        assertTrue(log.poll("g1", 10).isEmpty(),
                "committing nextOffset should leave nothing to redeliver");
    }

    // Committing the last record's offset instead of nextOffset re-delivers it
    // forever — a classic off-by-one in Kafka consumer code.
    @Test
    void testCommittingTheLastReadOffsetRedeliversThatRecord() {
        appendAll("a", "b", "c");

        LogRecords records = log.poll("g1", 10);
        log.commit("g1", records.nextOffset() - 1);
        log.resetPositionToCommitted("g1");

        assertEquals(List.of("c"), payloadsOf(log.poll("g1", 10)));
    }

    // ── Restart behaviour ─────────────────────────────────────────────────────

    @Test
    void testRestartResetsPositionToCommitted() {
        appendAll("a", "b", "c", "d");
        log.poll("g1", 4);
        log.commit("g1", 2);

        log.resetPositionToCommitted("g1"); // as a consumer restart would

        assertEquals(2, log.position("g1"));
        assertEquals(List.of("c", "d"), payloadsOf(log.poll("g1", 10)));
    }

    @Test
    void testUncommittedRecordsAreReReadAfterRestart() {
        appendAll("a", "b", "c");
        log.poll("g1", 3); // read everything, commit nothing

        log.resetPositionToCommitted("g1");

        assertEquals(List.of("a", "b", "c"), payloadsOf(log.poll("g1", 10)));
    }

    // ── Delivery guarantees: the payoff ───────────────────────────────────────

    // Commit AFTER processing. A crash between processing and commit means the
    // work is done twice — but nothing is ever missed.
    @Test
    void testCommitAfterProcessingGivesAtLeastOnce() {
        appendAll("a", "b", "c", "d");
        List<String> processed = new ArrayList<>();

        // First run: process a batch, crash before committing it
        LogRecords batch = log.poll("g1", 2);
        processed.addAll(payloadsOf(batch));
        // crash here — no commit
        log.resetPositionToCommitted("g1");

        // Second run: same batch comes back, then the rest
        LogRecords retry = log.poll("g1", 2);
        processed.addAll(payloadsOf(retry));
        log.commit("g1", retry.nextOffset());

        LogRecords rest = log.poll("g1", 10);
        processed.addAll(payloadsOf(rest));
        log.commit("g1", rest.nextOffset());

        assertEquals(List.of("a", "b", "a", "b", "c", "d"), processed);
        assertTrue(processed.containsAll(List.of("a", "b", "c", "d")),
                "at-least-once must never lose a record");
        assertEquals(6, processed.size(), "duplicates are the price of at-least-once");
    }

    // Commit BEFORE processing. A crash after committing but before the work is
    // done means those records are never seen again. This test asserts the data
    // loss, because demonstrating the failure is the point.
    @Test
    void testCommitBeforeProcessingLosesRecordsOnCrash() {
        appendAll("a", "b", "c", "d");
        List<String> processed = new ArrayList<>();

        LogRecords batch = log.poll("g1", 2);
        log.commit("g1", batch.nextOffset()); // commit first
        // crash here — "a" and "b" were never processed
        log.resetPositionToCommitted("g1");

        LogRecords rest = log.poll("g1", 10);
        processed.addAll(payloadsOf(rest));
        log.commit("g1", rest.nextOffset());

        assertEquals(List.of("c", "d"), processed);
        assertTrue(!processed.contains("a") && !processed.contains("b"),
                "at-most-once loses whatever was in flight at the crash");
    }

    // ── Independence ──────────────────────────────────────────────────────────

    @Test
    void testGroupsCommitIndependently() {
        appendAll("a", "b", "c", "d");

        log.poll("fast", 4);
        log.commit("fast", 4);
        log.poll("slow", 1);
        log.commit("slow", 1);

        assertEquals(4, log.committed("fast"));
        assertEquals(1, log.committed("slow"));
        assertEquals(0, log.lag("fast"));
        assertEquals(3, log.lag("slow"));
    }

    @Test
    void testNewGroupHasCommittedAtItsStartPosition() {
        appendAll("a", "b");

        assertEquals(0, log.committed("fresh"));
        assertEquals(2, log.lag("fresh"));
    }

    // ── Validation ────────────────────────────────────────────────────────────

    @Test
    void testCommitBeyondEndOffsetIsRejected() {
        appendAll("a", "b");

        assertThrows(OffsetOutOfRangeException.class, () -> log.commit("g1", 99));
    }

    @Test
    void testCommitAtEndOffsetIsAllowed() {
        appendAll("a", "b");

        log.commit("g1", 2); // "I have consumed everything"

        assertEquals(2, log.committed("g1"));
        assertEquals(0, log.lag("g1"));
    }

    @Test
    void testCommitNegativeOffsetIsRejected() {
        appendAll("a");

        assertThrows(OffsetOutOfRangeException.class, () -> log.commit("g1", -1));
    }

    // Committing backwards is legal — it is how you deliberately reprocess.
    @Test
    void testCommittingBackwardsIsAllowed() {
        appendAll("a", "b", "c");
        log.poll("g1", 3);
        log.commit("g1", 3);

        log.commit("g1", 1);
        log.resetPositionToCommitted("g1");

        assertEquals(List.of("b", "c"), payloadsOf(log.poll("g1", 10)));
    }
}
