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
 * Durable committed offsets for every (log, group) pair.
 *
 * One shared file rather than one per log, mirroring Kafka — where consumer
 * offsets live in a single __consumer_offsets topic, which is itself just
 * another log. Later records win, and compaction on startup collapses the
 * history down to current state.
 */
class OffsetStore {

    static final String FILE_NAME = "_offsets.log";

    /** Identifies one group's progress on one log. */
    record Key(String log, String group) {
    }

    private final Path file;
    private BufferedWriter writer;

    OffsetStore(String logDirectory) throws IOException {
        this.file = Path.of(logDirectory, FILE_NAME);
        Files.createDirectories(file.getParent());
        this.writer = new BufferedWriter(new FileWriter(file.toFile(), true));
    }

    /** Reads current state — later records overwrite earlier ones. */
    static Map<Key, Long> read(String logDirectory) {
        Map<Key, Long> offsets = new LinkedHashMap<>();
        Path file = Path.of(logDirectory, FILE_NAME);
        if (!Files.exists(file)) {
            return offsets;
        }

        try {
            for (String line : Files.readAllLines(file)) {
                if (line.isBlank()) {
                    continue;
                }
                try {
                    Map<String, String> fields = JsonUtils.fromJson(line);
                    String log = fields.get("log");
                    String group = fields.get("group");
                    String offset = fields.get("offset");
                    if (log == null || group == null || offset == null) {
                        throw new IllegalArgumentException("incomplete offset record");
                    }
                    offsets.put(new Key(log, group), Long.parseLong(offset));
                } catch (Exception e) {
                    System.err.println("WARNING: Skipping corrupted offset record: " + line);
                }
            }
        } catch (IOException e) {
            System.err.println("WARNING: Could not read offset store — " + e.getMessage());
        }
        return offsets;
    }

    synchronized void record(String log, String group, long offset) {
        Map<String, String> fields = new LinkedHashMap<>();
        fields.put("log", log);
        fields.put("group", group);
        fields.put("offset", String.valueOf(offset));
        try {
            writer.write(JsonUtils.toJson(fields));
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            System.err.println("WARNING: Could not write offset record: " + e.getMessage());
        }
    }

    synchronized void compact(Map<Key, Long> current) {
        try {
            writer.close();
            writer = new BufferedWriter(new FileWriter(file.toFile(), false));
            for (Map.Entry<Key, Long> entry : current.entrySet()) {
                Map<String, String> fields = new LinkedHashMap<>();
                fields.put("log", entry.getKey().log());
                fields.put("group", entry.getKey().group());
                fields.put("offset", String.valueOf(entry.getValue()));
                writer.write(JsonUtils.toJson(fields));
                writer.newLine();
            }
            writer.flush();
        } catch (IOException e) {
            System.err.println("WARNING: Could not compact offset store: " + e.getMessage());
        }
    }

    /** Drops every group's progress on a log — used when the log is deleted. */
    synchronized List<Key> keysFor(Map<Key, Long> all, String log) {
        List<Key> matching = new ArrayList<>();
        for (Key key : all.keySet()) {
            if (key.log().equals(log)) {
                matching.add(key);
            }
        }
        return matching;
    }

    synchronized void close() throws IOException {
        writer.close();
    }
}
