package com.javaqueue.core;

/**
 * Configuration for a retained log.
 *
 * Note what is absent compared with QueueConfig: no visibility timeout, no max
 * retries, no dead letter queue. A log has one progress mechanism — the
 * committed offset — so none of that machinery applies.
 */
public class LogConfig {

    // Trim records older than this. 0 means no age limit.
    private final long retentionMs;

    // Trim oldest once the log exceeds this many records. 0 means no size limit.
    private final int maxRecords;

    // Where a group starts, and what happens when its offset falls out of range.
    private final OffsetResetPolicy resetPolicy;

    // Null means no persistence, same convention as QueueConfig.
    private final String logDirectory;

    public LogConfig(long retentionMs, int maxRecords,
            OffsetResetPolicy resetPolicy, String logDirectory) {
        this.retentionMs = retentionMs;
        this.maxRecords = maxRecords;
        this.resetPolicy = resetPolicy;
        this.logDirectory = logDirectory;
    }

    /** Retain everything, start new groups at the beginning, no persistence. */
    public static LogConfig defaults() {
        return new LogConfig(0, 0, OffsetResetPolicy.EARLIEST, null);
    }

    public long getRetentionMs() {
        return retentionMs;
    }

    public int getMaxRecords() {
        return maxRecords;
    }

    public OffsetResetPolicy getResetPolicy() {
        return resetPolicy;
    }

    public String getLogDirectory() {
        return logDirectory;
    }
}
