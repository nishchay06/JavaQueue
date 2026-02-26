package com.javaqueue.core;

import java.util.concurrent.atomic.AtomicLong;

public final class Message {

    // One shared counter across ALL Message instances.
    // AtomicLong.getAndIncrement() is thread-safe without a lock —
    // it uses CPU-level compare-and-swap (CAS) under the hood.
    private static final AtomicLong ID_COUNTER = new AtomicLong(0);

    private final String id;
    private final String payload;

    public Message(String payload) {
        this.id = String.valueOf(ID_COUNTER.getAndIncrement());
        this.payload = payload;
    }

    // Used during WAL replay — restores a message with its original ID.
    // Not for general use — always use Message(String payload) in production code.
    Message(String id, String payload) {
        this.id = id;
        this.payload = payload;
    }

    public String getId() {
        return id;
    }

    public String getPayload() {
        return payload;
    }

    @Override
    public String toString() {
        return "Message{id='" + id + "', payload='" + payload + "'}";
    }
}
