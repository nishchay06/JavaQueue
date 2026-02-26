package com.javaqueue.core;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class WalWriter {

    private final Path logFile;
    private BufferedWriter writer;

    public WalWriter(Path logFile) throws IOException {
        this.logFile = logFile;

        // Auto-create parent directories if they don't exist
        Files.createDirectories(logFile.getParent());

        // Open in append mode — existing entries are preserved
        this.writer = new BufferedWriter(new FileWriter(logFile.toFile(), true));
    }

    // Synchronized — publish() and nack() can be called from multiple threads.
    // Without this, two threads could interleave writes and corrupt the log.
    synchronized void append(LogEntry entry) throws IOException {
        writer.write(entry.toJson());
        writer.newLine();

        // Flush after every write — guarantees durability at the cost of throughput.
        // This is the trade-off: zero message loss on crash, but slower than batching.
        writer.flush();
    }

    // Rewrites the log file from scratch with only the survivor entries.
    // Called after replay to remove acknowledged and in-flight entries.
    synchronized void compact(List<LogEntry> survivors) throws IOException {
        writer.close();

        // Overwrite — not append mode this time
        writer = new BufferedWriter(new FileWriter(logFile.toFile(), false));

        for (LogEntry entry : survivors) {
            writer.write(entry.toJson());
            writer.newLine();
        }

        writer.flush();
    }

    void close() throws IOException {
        writer.close();
    }
}
