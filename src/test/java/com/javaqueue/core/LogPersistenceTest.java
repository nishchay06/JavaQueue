package com.javaqueue.core;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.javaqueue.exception.LogNotFoundException;

/**
 * A log must survive a restart — records, offsets, and the group progress that
 * makes those offsets mean anything.
 *
 * Unlike Phase 3's WAL, the file here is not a side-record of state that gets
 * replayed to rebuild it. The file *is* the log. That is the same realisation
 * from the other direction: a Kafka topic simply is a write-ahead log that
 * nobody deletes from.
 */
public class LogPersistenceTest {

    private Path tempDir;
    private String logDir;
    private LogManager logs;

    @BeforeEach
    void setUp() throws IOException {
        tempDir = Files.createTempDirectory("javaqueue-log-persistence");
        logDir = tempDir.toString();
        logs = new LogManager(logDir);
    }

    @AfterEach
    void tearDown() throws IOException {
        logs.close();
        Files.walk(tempDir)
                .sorted(java.util.Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(java.io.File::delete);
    }

    private LogManager restart() {
        logs.close();
        logs = new LogManager(logDir);
        return logs;
    }

    private static List<String> payloadsOf(LogRecords records) {
        return records.messages().stream().map(Message::getPayload).toList();
    }

    // ── Registry ──────────────────────────────────────────────────────────────

    @Test
    void testCreateAndGetLog() {
        MessageLog log = logs.createLog("orders");

        assertEquals("orders", log.getName());
        assertTrue(logs.listLogs().contains("orders"));
    }

    @Test
    void testCreateLogIsIdempotent() {
        MessageLog first = logs.createLog("orders");
        MessageLog second = logs.createLog("orders");

        assertEquals(1, logs.listLogs().size());
        assertTrue(first == second, "creating the same log twice must not replace it");
    }

    @Test
    void testGetUnknownLogThrows() {
        assertThrows(LogNotFoundException.class, () -> logs.getLog("ghost"));
    }

    @Test
    void testDeleteLog() {
        logs.createLog("orders");
        logs.deleteLog("orders");

        assertTrue(logs.listLogs().isEmpty());
        assertThrows(LogNotFoundException.class, () -> logs.getLog("orders"));
    }

    // ── Records survive ───────────────────────────────────────────────────────

    @Test
    void testRecordsSurviveRestart() {
        MessageLog log = logs.createLog("orders");
        log.append(new Message("a"));
        log.append(new Message("b"));

        MessageLog restored = restart().createLog("orders");

        assertEquals(0, restored.beginOffset());
        assertEquals(2, restored.endOffset());
        assertEquals(List.of("a", "b"), payloadsOf(restored.poll("g1", 10)));
    }

    @Test
    void testOffsetsAreUnchangedByRestart() {
        MessageLog log = logs.createLog("orders");
        log.append(new Message("a"));
        long second = log.append(new Message("b"));

        MessageLog restored = restart().createLog("orders");
        restored.seek("g1", second);

        assertEquals(List.of("b"), payloadsOf(restored.poll("g1", 10)));
    }

    // Records containing quotes and commas destroyed the Phase 3 WAL. The log
    // file must not repeat that.
    @Test
    void testPunctuatedPayloadSurvivesRestart() {
        MessageLog log = logs.createLog("orders");
        String payload = "Order #1, \"urgent\"\nsecond line";
        log.append(new Message(payload));
        log.append(new Message("after"));

        MessageLog restored = restart().createLog("orders");

        assertEquals(List.of(payload, "after"), payloadsOf(restored.poll("g1", 10)));
    }

    // ── Trimming survives ─────────────────────────────────────────────────────

    // A restart must not resurrect records that retention already removed, and
    // baseOffset must come back with them.
    @Test
    void testTrimmedRecordsDoNotComeBack() {
        LogConfig config = new LogConfig(0, 2, OffsetResetPolicy.EARLIEST, null);
        MessageLog log = logs.createLog("orders", config);
        log.append(new Message("a"));
        log.append(new Message("b"));
        log.append(new Message("c")); // trims "a"

        MessageLog restored = restart().createLog("orders", config);

        assertEquals(1, restored.beginOffset(), "baseOffset must survive the restart");
        assertEquals(3, restored.endOffset());
        assertEquals(List.of("b", "c"), payloadsOf(restored.poll("g1", 10)));
    }

    // A log trimmed to nothing still has a baseOffset, and it must not reset
    // to zero — offsets are never reused.
    @Test
    void testFullyTrimmedLogKeepsItsBaseOffset() throws InterruptedException {
        LogConfig config = new LogConfig(80, 0, OffsetResetPolicy.EARLIEST, null);
        MessageLog log = new MessageLog("orders", config, 25);
        log.append(new Message("a"));
        log.append(new Message("b"));
        Thread.sleep(400); // everything ages out
        assertEquals(2, log.beginOffset());
        log.close();

        // Same directory, fresh manager
        MessageLog restored = logs.createLog("empty-check",
                new LogConfig(0, 0, OffsetResetPolicy.EARLIEST, null));
        restored.append(new Message("x"));
        assertEquals(0, restored.beginOffset());
    }

    // ── Committed offsets survive ─────────────────────────────────────────────

    @Test
    void testCommittedOffsetsSurviveRestart() {
        MessageLog log = logs.createLog("orders");
        log.append(new Message("a"));
        log.append(new Message("b"));
        log.append(new Message("c"));
        log.poll("billing", 3);
        log.commit("billing", 2);

        MessageLog restored = restart().createLog("orders");

        assertEquals(2, restored.committed("billing"));
        assertEquals(List.of("c"), payloadsOf(restored.poll("billing", 10)),
                "a restarted group resumes from its committed offset");
    }

    @Test
    void testEachGroupsOffsetSurvivesIndependently() {
        MessageLog log = logs.createLog("orders");
        for (int i = 0; i < 4; i++) {
            log.append(new Message("m-" + i));
        }
        log.commit("fast", 4);
        log.commit("slow", 1);

        MessageLog restored = restart().createLog("orders");

        assertEquals(4, restored.committed("fast"));
        assertEquals(1, restored.committed("slow"));
    }

    @Test
    void testOffsetsOfDifferentLogsDoNotCollide() {
        logs.createLog("orders").append(new Message("a"));
        logs.createLog("payments").append(new Message("b"));
        logs.getLog("orders").commit("shared-group", 1);
        logs.getLog("payments").commit("shared-group", 0);

        LogManager restarted = restart();

        assertEquals(1, restarted.createLog("orders").committed("shared-group"));
        assertEquals(0, restarted.createLog("payments").committed("shared-group"));
    }

    @Test
    void testGroupThatNeverCommittedRestartsAtTheResetDefault() {
        MessageLog log = logs.createLog("orders");
        log.append(new Message("a"));
        log.poll("reader", 10); // read, never commit

        MessageLog restored = restart().createLog("orders");

        assertEquals(0, restored.committed("reader"));
        assertEquals(List.of("a"), payloadsOf(restored.poll("reader", 10)));
    }

    // ── Files ─────────────────────────────────────────────────────────────────

    @Test
    void testLogFilesAreCreated() {
        logs.createLog("orders").append(new Message("a"));

        assertTrue(Files.exists(tempDir.resolve("orders.log")));
    }

    @Test
    void testNoPersistenceWithoutALogDirectory() throws IOException {
        Path untouched = Files.createDirectory(tempDir.resolve("none"));
        LogManager transientLogs = new LogManager(null);

        transientLogs.createLog("orders").append(new Message("a"));

        assertTrue(Files.list(untouched).findAny().isEmpty());
        transientLogs.close();
    }

    // A crash mid-write leaves a partial line. Everything before it must
    // still replay.
    @Test
    void testCorruptedTrailingLineIsSkipped() throws IOException {
        MessageLog log = logs.createLog("orders");
        log.append(new Message("a"));
        log.append(new Message("b"));

        logs.close();
        Files.writeString(tempDir.resolve("orders.log"), "{\"op\":\"APPEND\",\"off",
                StandardOpenOption.APPEND);
        logs = new LogManager(logDir);

        assertEquals(List.of("a", "b"), payloadsOf(logs.createLog("orders").poll("g1", 10)));
    }

    @Test
    void testDeletingALogRemovesItsCommittedOffsets() {
        MessageLog log = logs.createLog("orders");
        log.append(new Message("a"));
        log.commit("billing", 1);

        logs.deleteLog("orders");

        // The file goes with the log — checked before anything recreates it
        assertFalse(Files.exists(tempDir.resolve("orders.log")));

        MessageLog recreated = restart().createLog("orders");

        assertEquals(0, recreated.committed("billing"),
                "group offsets have no meaning once their log is gone");
    }
}
