package com.javaqueue.core;

/**
 * Daemon thread that applies a log's retention policy on a timer.
 *
 * Deliberately the same shape as VisibilityScanner: the loop lives here, the
 * logic lives in MessageLog where the monitor protects it.
 */
public class RetentionScanner implements Runnable {

    private final MessageLog log;
    private final long scanIntervalMs;

    public RetentionScanner(MessageLog log, long scanIntervalMs) {
        this.log = log;
        this.scanIntervalMs = scanIntervalMs;
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                Thread.sleep(scanIntervalMs);
                log.applyRetention();
            } catch (InterruptedException e) {
                // sleep() clears the flag when it throws — restore it so the
                // loop condition can see it and exit.
                Thread.currentThread().interrupt();
            }
        }
    }
}
