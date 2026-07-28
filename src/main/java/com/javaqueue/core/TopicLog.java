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
 * Append-only log of topic configuration — which topics exist, and which
 * queues subscribe to them.
 *
 * Same shape as the message WAL, for the same reason: without it, a restart
 * leaves topics that accept publishes and deliver to nobody. Unlike the
 * message WAL there is one shared file rather than one per topic, because
 * this is small, low-churn metadata.
 *
 * Records are flat JSON, one per line, written through the shared JsonUtils
 * so topic and queue names containing quotes or commas survive.
 */
class TopicLog {

    static final String LOG_FILE_NAME = "_topics.log";

    /** One line of the log. */
    record Record(TopicOperation op, String topic, String queue) {

        String toJson() {
            Map<String, String> fields = new LinkedHashMap<>();
            fields.put("op", op.name());
            fields.put("topic", topic);
            fields.put("queue", queue);
            return JsonUtils.toJson(fields);
        }

        static Record fromJson(String line) {
            Map<String, String> fields = JsonUtils.fromJson(line);
            String op = fields.get("op");
            if (op == null) {
                throw new IllegalArgumentException("Topic log record has no 'op': " + line);
            }
            return new Record(TopicOperation.valueOf(op), fields.get("topic"), fields.get("queue"));
        }
    }

    private final Path logFile;
    private BufferedWriter writer;

    TopicLog(String logDirectory) throws IOException {
        this.logFile = Path.of(logDirectory, LOG_FILE_NAME);
        Files.createDirectories(logFile.getParent());
        this.writer = new BufferedWriter(new FileWriter(logFile.toFile(), true));
    }

    /**
     * Reads the log without opening it for writing.
     *
     * Corrupted lines are skipped with a warning rather than failing the
     * replay — a partial line at the tail is the expected shape of a crash,
     * and losing every topic because of it would be a poor trade.
     */
    static List<Record> read(String logDirectory) {
        List<Record> records = new ArrayList<>();
        Path logFile = Path.of(logDirectory, LOG_FILE_NAME);

        if (!Files.exists(logFile)) {
            return records;
        }

        try {
            for (String line : Files.readAllLines(logFile)) {
                if (line.isBlank()) {
                    continue;
                }
                try {
                    records.add(Record.fromJson(line));
                } catch (Exception e) {
                    System.err.println("WARNING: Skipping corrupted topic log record: " + line);
                }
            }
        } catch (IOException e) {
            System.err.println("WARNING: Could not read topic log " + logFile + " — " + e.getMessage());
        }
        return records;
    }

    // Synchronized: subscriptions can change from any thread.
    synchronized void append(Record record) {
        try {
            writer.write(record.toJson());
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            System.err.println("WARNING: Could not write to topic log: " + e.getMessage());
        }
    }

    /** Rewrites the log as the current state, discarding superseded records. */
    synchronized void compact(List<Record> survivors) {
        try {
            writer.close();
            writer = new BufferedWriter(new FileWriter(logFile.toFile(), false));
            for (Record record : survivors) {
                writer.write(record.toJson());
                writer.newLine();
            }
            writer.flush();
        } catch (IOException e) {
            System.err.println("WARNING: Could not compact topic log: " + e.getMessage());
        }
    }

    synchronized void close() throws IOException {
        writer.close();
    }
}
