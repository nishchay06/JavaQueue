package com.javaqueue.core;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

class WalReader {
    // Stateless utility class — no fields, no constructor needed.
    private WalReader() {
    }

    // Reads the log file line by line and parses each entry.
    // Returns empty list if file does not exist — first startup with no prior
    // state.
    static List<LogEntry> read(Path logFile) {
        List<LogEntry> entries = new ArrayList<>();

        if (!Files.exists(logFile)) {
            return entries;
        }

        try {
            List<String> lines = Files.readAllLines(logFile);
            for (String line : lines) {
                if (line.isBlank())
                    continue;
                try {
                    entries.add(LogEntry.fromJson(line));
                } catch (Exception e) {
                    // Skip corrupted lines — partial writes on crash are expected
                    // at the end of the file. Log a warning and continue replay.
                    System.err.println("WARNING: Skipping corrupted WAL entry: " + line);
                }
            }
        } catch (IOException e) {
            System.err.println("WARNING: Could not read WAL file: " + logFile + " — " + e.getMessage());
        }

        return entries;
    }
}
