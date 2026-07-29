package com.javaqueue.core;

/**
 * What to do when a group's offset no longer exists — because retention
 * trimmed past it, or because a seek landed outside the retained range.
 *
 * This is Kafka's auto.offset.reset, and the choice is a real one: EARLIEST
 * reprocesses, LATEST silently skips whatever was lost, ERROR refuses to guess.
 */
public enum OffsetResetPolicy {
    /** Jump to the oldest retained offset — reprocess everything still there. */
    EARLIEST,
    /** Jump to the end — skip the gap, accepting the loss. */
    LATEST,
    /** Refuse, and make the operator decide. */
    ERROR
}
