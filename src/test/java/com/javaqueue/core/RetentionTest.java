package com.javaqueue.core;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.javaqueue.exception.OffsetOutOfRangeException;

/**
 * Retention — the only thing that removes records — and what happens to a
 * group whose offset gets trimmed out from under it.
 *
 * A slow consumer group silently losing data is one of the genuine operational
 * hazards of running Kafka. All three reset policies are reachable here, and
 * each one is tested, because the point is that the choice has consequences.
 */
public class RetentionTest {

    private static final long SCAN_INTERVAL_MS = 30;

    private final List<MessageLog> opened = new ArrayList<>();

    @AfterEach
    void tearDown() {
        opened.forEach(MessageLog::close);
    }

    private MessageLog logWith(LogConfig config) {
        MessageLog log = new MessageLog("orders", config, SCAN_INTERVAL_MS);
        opened.add(log);
        return log;
    }

    private static List<String> payloadsOf(LogRecords records) {
        return records.messages().stream().map(Message::getPayload).toList();
    }

    // ── Trimming by count ─────────────────────────────────────────────────────

    @Test
    void testMaxRecordsTrimsOldest() {
        MessageLog log = logWith(new LogConfig(0, 3, OffsetResetPolicy.EARLIEST, null));

        for (int i = 0; i < 5; i++) {
            log.append(new Message("m-" + i));
        }

        assertEquals(3, log.endOffset() - log.beginOffset(), "log should hold only 3 records");
        assertEquals(List.of("m-2", "m-3", "m-4"), payloadsOf(log.poll("g1", 10)));
    }

    // Trimming advances baseOffset rather than renumbering, so an offset always
    // means the same record for the life of the log.
    @Test
    void testTrimmingAdvancesBaseOffsetAndNeverReusesOffsets() {
        MessageLog log = logWith(new LogConfig(0, 2, OffsetResetPolicy.EARLIEST, null));

        log.append(new Message("a"));
        log.append(new Message("b"));
        assertEquals(0, log.beginOffset());

        long thirdOffset = log.append(new Message("c"));

        assertEquals(2, thirdOffset, "offsets keep counting up regardless of trimming");
        assertEquals(1, log.beginOffset(), "oldest retained offset moved forward");
        assertEquals(3, log.endOffset());
    }

    @Test
    void testRetainedRecordKeepsItsOriginalOffset() {
        MessageLog log = logWith(new LogConfig(0, 2, OffsetResetPolicy.EARLIEST, null));
        log.append(new Message("a"));
        long bOffset = log.append(new Message("b"));
        log.append(new Message("c"));

        log.seek("g1", bOffset);

        assertEquals(List.of("b", "c"), payloadsOf(log.poll("g1", 10)));
    }

    @Test
    void testNoPolicyRetainsEverything() {
        MessageLog log = logWith(LogConfig.defaults());

        for (int i = 0; i < 500; i++) {
            log.append(new Message("m-" + i));
        }

        assertEquals(0, log.beginOffset());
        assertEquals(500, log.endOffset());
    }

    // ── Trimming by age ───────────────────────────────────────────────────────

    @Test
    void testRetentionMsTrimsOldRecords() throws InterruptedException {
        MessageLog log = logWith(new LogConfig(150, 0, OffsetResetPolicy.EARLIEST, null));

        log.append(new Message("old"));
        Thread.sleep(400); // let it age out and the scanner run

        assertEquals(1, log.beginOffset(), "aged-out record should have been trimmed");
        assertTrue(log.poll("g1", 10).isEmpty());
    }

    @Test
    void testRecordInsideTheWindowSurvives() throws InterruptedException {
        MessageLog log = logWith(new LogConfig(2000, 0, OffsetResetPolicy.EARLIEST, null));

        log.append(new Message("fresh"));
        Thread.sleep(200);

        assertEquals(0, log.beginOffset());
        assertEquals(List.of("fresh"), payloadsOf(log.poll("g1", 10)));
    }

    // ── Offset out of range ───────────────────────────────────────────────────

    // A group that has kept up is untouched by a trim.
    @Test
    void testGroupStillInRangeIsUnaffectedByTrim() {
        MessageLog log = logWith(new LogConfig(0, 3, OffsetResetPolicy.EARLIEST, null));
        for (int i = 0; i < 3; i++) {
            log.append(new Message("m-" + i));
        }
        log.poll("g1", 3);
        log.commit("g1", 3);

        log.append(new Message("m-3")); // trims m-0

        assertEquals(3, log.committed("g1"));
        assertEquals(List.of("m-3"), payloadsOf(log.poll("g1", 10)));
    }

    @Test
    void testEarliestPolicyJumpsToBeginOffset() {
        MessageLog log = logWith(new LogConfig(0, 2, OffsetResetPolicy.EARLIEST, null));
        log.append(new Message("a"));
        log.seek("g1", 0); // group sits at the very start

        log.append(new Message("b"));
        log.append(new Message("c")); // trims "a", baseOffset -> 1

        // Group's offset 0 no longer exists — EARLIEST rewinds to what is left
        assertEquals(1, log.position("g1"));
        assertEquals(List.of("b", "c"), payloadsOf(log.poll("g1", 10)));
    }

    @Test
    void testLatestPolicySkipsTheGap() {
        MessageLog log = logWith(new LogConfig(0, 2, OffsetResetPolicy.LATEST, null));
        log.append(new Message("a"));
        log.seek("g1", 0);

        log.append(new Message("b"));
        log.append(new Message("c")); // trims "a"

        // LATEST accepts the loss and jumps to the end — "b" and "c" are
        // skipped even though they are still retained.
        assertEquals(3, log.position("g1"));
        assertTrue(log.poll("g1", 10).isEmpty());
    }

    @Test
    void testErrorPolicyRefusesToGuess() {
        MessageLog log = logWith(new LogConfig(0, 2, OffsetResetPolicy.ERROR, null));
        log.append(new Message("a"));
        log.seek("g1", 0);

        log.append(new Message("b"));
        log.append(new Message("c")); // trims "a"

        assertThrows(OffsetOutOfRangeException.class, () -> log.poll("g1", 10));
    }

    // Reading ahead of what exists is always an error, whatever the policy —
    // silently rewinding would hide the caller's mistake.
    @Test
    void testSeekBeyondEndIsAlwaysAnError() {
        MessageLog log = logWith(LogConfig.defaults());
        log.append(new Message("a"));

        assertThrows(OffsetOutOfRangeException.class, () -> log.seek("g1", 99));
    }

    // ── Interaction with lag ──────────────────────────────────────────────────

    @Test
    void testLagStaysMeaningfulAfterTrimming() {
        MessageLog log = logWith(new LogConfig(0, 3, OffsetResetPolicy.EARLIEST, null));
        for (int i = 0; i < 5; i++) {
            log.append(new Message("m-" + i));
        }

        // A brand-new group starts at the oldest retained record
        assertEquals(3, log.lag("fresh"), "lag counts only what is still retained");
    }

    // ── Concurrency ───────────────────────────────────────────────────────────

    // Trimming must never hand a consumer a record that has been removed, nor
    // throw while a poll is in flight.
    @Test
    void testConcurrentPollAndTrimAreSafe() throws InterruptedException {
        MessageLog log = logWith(new LogConfig(0, 50, OffsetResetPolicy.EARLIEST, null));

        Thread producer = new Thread(() -> {
            for (int i = 0; i < 5000; i++) {
                log.append(new Message("m-" + i));
            }
        });

        List<Throwable> failures = new ArrayList<>();
        Thread consumer = new Thread(() -> {
            try {
                for (int i = 0; i < 5000; i++) {
                    LogRecords batch = log.poll("g1", 10);
                    batch.messages().forEach(Message::getPayload);
                }
            } catch (Throwable t) {
                synchronized (failures) {
                    failures.add(t);
                }
            }
        });

        producer.start();
        consumer.start();
        producer.join();
        consumer.join();

        assertTrue(failures.isEmpty(), "poll threw while retention was trimming: " + failures);
    }
}
