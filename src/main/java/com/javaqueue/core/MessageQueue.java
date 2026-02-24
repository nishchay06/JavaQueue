package com.javaqueue.core;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import com.javaqueue.exception.InvalidReceiptException;

public class MessageQueue {

    public final String name;

    // The actual queue of messages waiting to be consumed.
    // LinkedList implements Queue — gives us O(1) add to tail, remove from head.
    private final Queue<Message> messages = new LinkedList<>();

    // Tracks messages that have been consumed but not yet acknowledged.
    // Key = receipt handle, Value = the message.
    // Shares the same intrinsic lock as messages — no extra synchronization needed.
    private final Map<String, Message> inFlightMessages = new HashMap<>();

    public MessageQueue(String name) {
        this.name = name;
    }

    public void publish(Message message) {
        synchronized (this) {
            messages.add(message);

            // Wake up all threads waiting in consume().
            // We use notifyAll() not notify() because with multiple consumers,
            // notify() might wake the wrong thread — one that isn't actually
            // waiting for a message. notifyAll() is safer, the while loop
            // in consume() handles the case where a woken thread loses the race.
            notifyAll();
        }
    }

    public Receipt consume() throws InterruptedException {
        synchronized (this) {

            // while — not if. Protects against spurious wakeups and the case
            // where multiple consumers are woken but only one gets the message.
            while (messages.isEmpty()) {
                wait(); // releases the lock and sleeps until notifyAll() is called
            }

            Message message = messages.poll();
            Receipt receipt = new Receipt(message);

            inFlightMessages.put(receipt.getReceiptHandle(), message);
            return receipt;
        }
    }

    public void acknowledge(String receiptHandle) {
        synchronized (this) {
            Message message = inFlightMessages.remove(receiptHandle);

            if (message == null) {
                throw new InvalidReceiptException(receiptHandle);
            }
        }
    }

    public String getName() {
        return name;
    }
}
