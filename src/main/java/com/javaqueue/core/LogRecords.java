package com.javaqueue.core;

import java.util.List;

/**
 * The result of a poll: the records read, and the offsets they span.
 *
 * nextOffset is what the group's position advanced to — and therefore what a
 * consumer commits once it has finished processing these records. Committing
 * the offset of the last record instead would re-deliver it forever, which is
 * a classic off-by-one in Kafka client code.
 */
public record LogRecords(List<Message> messages, long startOffset, long nextOffset) {

    public static LogRecords empty(long offset) {
        return new LogRecords(List.of(), offset, offset);
    }

    public boolean isEmpty() {
        return messages.isEmpty();
    }

    public int size() {
        return messages.size();
    }
}
