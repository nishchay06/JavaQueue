package com.javaqueue.core;

public class QueueConfig {

    // How long a message stays invisible after consume() before
    // being requeued by the scanner. Default: 30 seconds.
    private final long visibilityTimeoutMs;

    // Max number of times a message can be delivered before
    // being sent to the DLQ or dropped. Default: 3.
    private final int maxRetries;

    // Name of the dead letter queue. Null means drop on retry exhaustion.
    private final String deadLetterQueueName;

    public QueueConfig(long visibilityTimeoutMs, int maxRetries, String deadLetterQueueName) {
        this.visibilityTimeoutMs = visibilityTimeoutMs;
        this.maxRetries = maxRetries;
        this.deadLetterQueueName = deadLetterQueueName;
    }

    // Convenience factory — used when creating queues without explicit config.
    public static QueueConfig defaults() {
        return new QueueConfig(30_000, 3, null);
    }

    public long getVisibilityTimeoutMs() {
        return visibilityTimeoutMs;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public String getDeadLetterQueueName() {
        return deadLetterQueueName;
    }
}
