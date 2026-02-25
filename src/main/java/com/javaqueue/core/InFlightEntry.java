package com.javaqueue.core;

public class InFlightEntry {

    private final Message message;

    // Wall-clock time when this entry was created — used by the scanner
    // to determine if the visibility timeout has elapsed.
    private final long consumedAtMs;

    // How many times this message has been delivered.
    // Starts at 0 on first consume, incremented on each NACK or timeout.
    private final int retryCount;

    // Called on first consume — retryCount starts at 0.
    public InFlightEntry(Message message) {
        this.message = message;
        this.consumedAtMs = System.currentTimeMillis();
        this.retryCount = 0;
    }

    // Called when requeuing — carries over the retry count from
    // the previous delivery attempt.
    public InFlightEntry(Message message, int retryCount) {
        this.message = message;
        this.consumedAtMs = System.currentTimeMillis();
        this.retryCount = retryCount;
    }

    // Returns true if enough time has passed that this message
    // should be returned to the queue.
    public boolean isTimedOut(long timeoutMs) {
        return System.currentTimeMillis() - consumedAtMs > timeoutMs;
    }

    // Returns a NEW entry with incremented retry count and
    // a fresh consumedAtMs — immutable, never mutates in place.
    public InFlightEntry incrementRetry() {
        return new InFlightEntry(message, retryCount + 1);
    }

    public Message getMessage() {
        return message;
    }

    public long getConsumedAtMs() {
        return consumedAtMs;
    }

    public int getRetryCount() {
        return retryCount;
    }
}
