package com.javaqueue.core;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.javaqueue.json.JsonUtils;

/**
 * The on-disk form of a log's records.
 *
 * Note what this is not: Phase 3's WAL was a side-record of queue state,
 * replayed to reconstruct it. Here the file *is* the log — replay is simply
 * reading it back. That is the phase's central insight from the other
 * direction: a Kafka topic is a write-ahead log nobody deletes from.
 *
 * Two record types, one JSON object per line:
 *
 *   {"op":"APPEND","offset":"5","msgId":"5","payload":"..."}
 *   {"op":"TRIM","offset":"3"}
 *
 * TRIM records what retention removed, so a restart does not resurrect records
 * that were already dropped. Compaction on startup keeps the file from growing
 * without bound.
 */
class RecordStore {

    /** One line of the file. A TRIM carries only the new base offset. */
    record Entry(boolean append, long offset, String messageId, String payload) {

        static Entry appended(long offset, Message message) {
            return new Entry(true, offset, message.getId(), message.getPayload());
        }

        static Entry trimmed(long newBaseOffset) {
            return new Entry(false, newBaseOffset, null, null);
        }

        String toJson() {
            Map<String, String> fields = new LinkedHashMap<>();
            fields.put("op", append ? "APPEND" : "TRIM");
            fields.put("offset", String.valueOf(offset));
            if (append) {
                fields.put("msgId", messageId);
                fields.put("payload", payload);
            }
            return JsonUtils.toJson(fields);
        }

        static Entry fromJson(String line) {
            Map<String, String> fields = JsonUtils.fromJson(line);
            String op = fields.get("op");
            String offset = fields.get("offset");
            if (op == null || offset == null) {
                throw new IllegalArgumentException("Log record missing op or offset: " + line);
            }
            if (op.equals("TRIM")) {
                return trimmed(Long.parseLong(offset));
            }
            return new Entry(true, Long.parseLong(offset),
                    fields.get("msgId"), fields.get("payload"));
        }
    }

    private final Path file;
    private BufferedWriter writer;

    RecordStore(String logDirectory, String logName) throws IOException {
        this.file = Path.of(logDirectory, logName + ".log");
        Files.createDirectories(file.getParent());
        this.writer = new BufferedWriter(new FileWriter(file.toFile(), true));
    }

    static List<Entry> read(String logDirectory, String logName) {
        List<Entry> entries = new ArrayList<>();
        Path file = Path.of(logDirectory, logName + ".log");
        if (!Files.exists(file)) {
            return entries;
        }

        try {
            for (String line : Files.readAllLines(file)) {
                if (line.isBlank()) {
                    continue;
                }
                try {
                    entries.add(Entry.fromJson(line));
                } catch (Exception e) {
                    // A partial line at the tail is what a crash looks like.
                    // Losing the whole log over it would be a poor trade.
                    System.err.println("WARNING: Skipping corrupted log record: " + line);
                }
            }
        } catch (IOException e) {
            System.err.println("WARNING: Could not read log file " + file + " — " + e.getMessage());
        }
        return entries;
    }

    static void delete(String logDirectory, String logName) {
        try {
            Files.deleteIfExists(Path.of(logDirectory, logName + ".log"));
        } catch (IOException e) {
            System.err.println("WARNING: Could not delete log file for '" + logName
                    + "' — " + e.getMessage());
        }
    }

    synchronized void append(Entry entry) {
        try {
            writer.write(entry.toJson());
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            System.err.println("WARNING: Could not write log record: " + e.getMessage());
        }
    }

    /**
     * Rewrites the file as current state: a leading TRIM carrying baseOffset,
     * then one APPEND per surviving record.
     *
     * The leading TRIM is not redundant — a log trimmed empty has no APPEND
     * records to imply its base offset, and resetting to zero would reuse
     * offsets that have already been handed out.
     */
    synchronized void compact(long baseOffset, List<Entry> survivors) {
        try {
            writer.close();
            writer = new BufferedWriter(new FileWriter(file.toFile(), false));
            writer.write(Entry.trimmed(baseOffset).toJson());
            writer.newLine();
            for (Entry entry : survivors) {
                writer.write(entry.toJson());
                writer.newLine();
            }
            writer.flush();
        } catch (IOException e) {
            System.err.println("WARNING: Could not compact log file: " + e.getMessage());
        }
    }

    synchronized void close() throws IOException {
        writer.close();
    }
}
