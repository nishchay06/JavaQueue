package com.javaqueue.core;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class WalTest {

    private Path tempDir;

    @BeforeEach
    void setUp() throws IOException {
        // Create a fresh temp directory for each test
        tempDir = Files.createTempDirectory("javaqueue-wal-test");
    }

    @AfterEach
    void tearDown() throws IOException {
        // Clean up log files after each test
        Files.walk(tempDir)
                .sorted(java.util.Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(java.io.File::delete);
    }

    // ── Test 1: Write and read a single entry ─────────────────────────────────
    @Test
    void testWriteAndReadSingleEntry() throws IOException {
        Path logFile = tempDir.resolve("test.log");
        WalWriter writer = new WalWriter(logFile);

        Message message = new Message("hello");
        writer.append(LogEntry.publish(message));
        writer.close();

        List<LogEntry> entries = WalReader.read(logFile);

        assertEquals(1, entries.size());
        assertEquals(LogOperation.PUBLISH, entries.get(0).getOp());
        assertEquals(message.getId(), entries.get(0).getMsgId());
        assertEquals("hello", entries.get(0).getPayload());
    }

    // ── Test 2: Write and read multiple entries in order ──────────────────────
    @Test
    void testWriteAndReadMultipleEntries() throws IOException {
        Path logFile = tempDir.resolve("test.log");
        WalWriter writer = new WalWriter(logFile);

        Message m1 = new Message("msg1");
        Message m2 = new Message("msg2");
        writer.append(LogEntry.publish(m1));
        writer.append(LogEntry.publish(m2));
        writer.append(LogEntry.consume(m1, "handle-1", 0));
        writer.append(LogEntry.ack("handle-1"));
        writer.close();

        List<LogEntry> entries = WalReader.read(logFile);

        assertEquals(4, entries.size());
        assertEquals(LogOperation.PUBLISH, entries.get(0).getOp());
        assertEquals(LogOperation.PUBLISH, entries.get(1).getOp());
        assertEquals(LogOperation.CONSUME, entries.get(2).getOp());
        assertEquals(LogOperation.ACK, entries.get(3).getOp());
    }

    // ── Test 3: Read non-existent file returns empty list ─────────────────────
    @Test
    void testReadNonExistentFileReturnsEmpty() {
        Path logFile = tempDir.resolve("nonexistent.log");
        List<LogEntry> entries = WalReader.read(logFile);
        assertTrue(entries.isEmpty());
    }

    // ── Test 4: Compact rewrites file with only survivors ─────────────────────
    @Test
    void testCompactRewritesFile() throws IOException {
        Path logFile = tempDir.resolve("test.log");
        WalWriter writer = new WalWriter(logFile);

        Message m1 = new Message("msg1");
        Message m2 = new Message("msg2");
        Message m3 = new Message("msg3");
        writer.append(LogEntry.publish(m1));
        writer.append(LogEntry.publish(m2));
        writer.append(LogEntry.publish(m3));
        writer.append(LogEntry.consume(m1, "handle-1", 0));
        writer.append(LogEntry.ack("handle-1"));

        // Compact with only m2 and m3 as survivors
        List<LogEntry> survivors = List.of(
                LogEntry.publish(m2),
                LogEntry.publish(m3));
        writer.compact(survivors);
        writer.close();

        List<LogEntry> entries = WalReader.read(logFile);

        assertEquals(2, entries.size());
        assertEquals(m2.getId(), entries.get(0).getMsgId());
        assertEquals(m3.getId(), entries.get(1).getMsgId());
    }

    // ── Test 5: All entry types round-trip correctly ───────────────────────────
    @Test
    void testAllLogEntryTypesRoundTrip() throws IOException {
        Path logFile = tempDir.resolve("test.log");
        WalWriter writer = new WalWriter(logFile);

        Message message = new Message("payload123");
        writer.append(LogEntry.publish(message));
        writer.append(LogEntry.consume(message, "handle-abc", 2));
        writer.append(LogEntry.ack("handle-abc"));
        writer.append(LogEntry.nack("handle-xyz"));
        writer.close();

        List<LogEntry> entries = WalReader.read(logFile);

        assertEquals(4, entries.size());

        // PUBLISH
        assertEquals(LogOperation.PUBLISH, entries.get(0).getOp());
        assertEquals(message.getId(), entries.get(0).getMsgId());
        assertEquals("payload123", entries.get(0).getPayload());

        // CONSUME
        assertEquals(LogOperation.CONSUME, entries.get(1).getOp());
        assertEquals(message.getId(), entries.get(1).getMsgId());
        assertEquals("handle-abc", entries.get(1).getHandle());
        assertEquals(2, entries.get(1).getRetryCount());

        // ACK
        assertEquals(LogOperation.ACK, entries.get(2).getOp());
        assertEquals("handle-abc", entries.get(2).getHandle());

        // NACK
        assertEquals(LogOperation.NACK, entries.get(3).getOp());
        assertEquals("handle-xyz", entries.get(3).getHandle());
    }
}