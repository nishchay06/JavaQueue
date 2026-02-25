package com.javaqueue.core;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import com.javaqueue.exception.InvalidReceiptException;

public class MessageQueue {

    public final String name;

    private final QueueConfig config;
    private MessageQueue deadLetterQueue;
    private final Thread scannerThread;

    // Tracks retry count per message ID across requeues
    private final Map<String, Integer> retryCounts = new HashMap<>();

    // The actual queue of messages waiting to be consumed.
    // LinkedList implements Queue — gives us O(1) add to tail, remove from head.
    private final Queue<Message> messages = new LinkedList<>();

    // Tracks messages that have been consumed but not yet acknowledged.
    // Key = receipt handle, Value = the message.
    // Shares the same intrinsic lock as messages — no extra synchronization needed.
    final Map<String, InFlightEntry> inFlightMessages = new HashMap<>();

    public MessageQueue(String name) {
        this(name, QueueConfig.defaults());
    }

    public MessageQueue(String name, QueueConfig config) {
        this(name, config, 1000);
    }

    public MessageQueue(String name, QueueConfig config, long scanIntervalMs) {
        this.name = name;
        this.config = config;

        VisibilityScanner scanner = new VisibilityScanner(this, scanIntervalMs);
        this.scannerThread = new Thread(scanner, "scanner-" + name);
        this.scannerThread.setDaemon(true);
        this.scannerThread.start();
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

            int retryCount = retryCounts.getOrDefault(message.getId(), 0);
            inFlightMessages.put(receipt.getReceiptHandle(), new InFlightEntry(message, retryCount));
            return receipt;
        }
    }

    public void acknowledge(String receiptHandle) {
        synchronized (this) {
            InFlightEntry inFlightEntry = inFlightMessages.remove(receiptHandle);

            if (inFlightEntry == null) {
                throw new InvalidReceiptException(receiptHandle);
            }
            retryCounts.remove(inFlightEntry.getMessage().getId());
        }
    }

    public void close() {
        scannerThread.interrupt();
        try {
            scannerThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void setDeadLetterQueue(MessageQueue dlq) {
        this.deadLetterQueue = dlq;
    }

    private void requeueOrDeadLetter(Message message, int newRetryCount) {
        if (newRetryCount >= config.getMaxRetries()) {
            retryCounts.remove(message.getId());
            if (deadLetterQueue != null) {
                deadLetterQueue.publish(message);
            } else {
                System.err.println("WARNING: Message " + message.getId()
                        + " exceeded max retries and has been dropped from queue '"
                        + name + "'");
            }
        } else {
            retryCounts.put(message.getId(), newRetryCount);
            messages.add(message);
            notifyAll();
        }
    }

    public void scanAndRequeue() {
        synchronized (this) {
            inFlightMessages.entrySet().stream()
                    .filter(e -> e.getValue().isTimedOut(config.getVisibilityTimeoutMs()))
                    .map(Map.Entry::getKey)
                    .toList() // collects to a new list before we start removing — avoids
                              // ConcurrentModificationException
                    .forEach(handle -> {
                        InFlightEntry entry = inFlightMessages.remove(handle);
                        requeueOrDeadLetter(entry.getMessage(), entry.getRetryCount() + 1);
                    });
        }
    }

    public void nack(String receiptHandle) {
        synchronized (this) {
            InFlightEntry entry = inFlightMessages.remove(receiptHandle);
            if (entry == null) {
                throw new InvalidReceiptException(receiptHandle);
            }
            requeueOrDeadLetter(entry.getMessage(), entry.getRetryCount() + 1);
        }
    }

    public String getName() {
        return name;
    }

    // Package-private for testing
    Thread getScannerThread() {
        return scannerThread;
    }
}
