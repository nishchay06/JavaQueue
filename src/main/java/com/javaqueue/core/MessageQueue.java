package com.javaqueue.core;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
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

    private final WalWriter walWriter;

    public MessageQueue(String name) {
        this(name, QueueConfig.defaults());
    }

    public MessageQueue(String name, QueueConfig config) {
        this(name, config, 1000);
    }

    public MessageQueue(String name, QueueConfig config, long scanIntervalMs) {
        this.name = name;
        this.config = config;
        // Initialize WAL if persistence is configured
        WalWriter writer = null;
        if (config.getLogDirectory() != null) {
            try {
                Path logFile = Path.of(config.getLogDirectory(), name + ".log");
                List<LogEntry> entries = WalReader.read(logFile);
                if (!entries.isEmpty()) {
                    synchronized (this) {
                        replay(entries);
                    }
                }
                writer = new WalWriter(logFile);
                if (!entries.isEmpty()) {
                    compactLog(writer);
                }
            } catch (IOException e) {
                System.err.println("WARNING: Could not initialize WAL for queue '"
                        + name + "': " + e.getMessage());
            }
        }
        this.walWriter = writer;

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

            if (walWriter != null) {
                try {
                    walWriter.append(LogEntry.publish(message));
                } catch (IOException e) {
                    System.err.println("WARNING: Could not write to WAL: " + e.getMessage());
                }
            }
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
            inFlightMessages.put(receipt.getReceiptHandle(),
                    new InFlightEntry(message, retryCount));

            if (walWriter != null) {
                try {
                    walWriter.append(LogEntry.consume(message, receipt.getReceiptHandle(), retryCount));
                } catch (IOException e) {
                    System.err.println("WARNING: Could not write to WAL: " + e.getMessage());
                }
            }
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

            if (walWriter != null) {
                try {
                    walWriter.append(LogEntry.ack(receiptHandle));
                } catch (IOException e) {
                    System.err.println("WARNING: Could not write to WAL: " + e.getMessage());
                }
            }
        }
    }

    public void close() {
        scannerThread.interrupt();
        try {
            scannerThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (walWriter != null) {
            try {
                walWriter.close();
            } catch (IOException e) {
                System.err.println("WARNING: Could not close WAL for queue '"
                        + name + "': " + e.getMessage());
            }
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

            if (walWriter != null) {
                try {
                    walWriter.append(LogEntry.nack(receiptHandle));
                } catch (IOException e) {
                    System.err.println("WARNING: Could not write to WAL: " + e.getMessage());
                }
            }

            requeueOrDeadLetter(entry.getMessage(), entry.getRetryCount() + 1);
        }
    }

    public String getName() {
        return name;
    }

    private void replay(List<LogEntry> entries) {
        for (LogEntry entry : entries) {
            switch (entry.getOp()) {
                case PUBLISH -> {
                    // Restore message with its original ID from the log
                    Message message = new Message(entry.getMsgId(), entry.getPayload());
                    messages.add(message);
                    if (entry.getRetryCount() > 0) {
                        retryCounts.put(message.getId(), entry.getRetryCount());
                    }
                }
                case CONSUME -> {
                    // Find the message in the queue and move it to in-flight
                    Message message = messages.stream()
                            .filter(m -> m.getId().equals(entry.getMsgId()))
                            .findFirst()
                            .orElse(null);
                    if (message != null) {
                        messages.remove(message);
                        inFlightMessages.put(entry.getHandle(),
                                new InFlightEntry(message, entry.getRetryCount()));
                    }
                }
                case ACK -> {
                    // Message was acknowledged — remove from in-flight
                    InFlightEntry removed = inFlightMessages.remove(entry.getHandle());
                    if (removed != null) {
                        retryCounts.remove(removed.getMessage().getId());
                    }
                }
                case NACK -> {
                    // Message was NACKed — remove from in-flight and requeue
                    InFlightEntry removed = inFlightMessages.remove(entry.getHandle());
                    if (removed != null) {
                        requeueOrDeadLetter(removed.getMessage(),
                                removed.getRetryCount() + 1);
                    }
                }
            }
        }

        // Any messages still in-flight at end of log were in-flight when JVM crashed.
        // Treat as implicit NACK — requeue them all.
        new ArrayList<>(inFlightMessages.entrySet())
                .forEach(e -> {
                    inFlightMessages.remove(e.getKey());
                    requeueOrDeadLetter(e.getValue().getMessage(),
                            e.getValue().getRetryCount() + 1);
                });
    }

    private void compactLog(WalWriter writer) {
        // Build survivor list — only currently queued messages as PUBLISH entries.
        // In-flight, acknowledged, and dropped messages are not included.
        List<LogEntry> survivors = messages.stream()
                .map(LogEntry::publish)
                .collect(java.util.stream.Collectors.toList());

        try {
            writer.compact(survivors);
        } catch (IOException e) {
            System.err.println("WARNING: Could not compact WAL for queue '"
                    + name + "': " + e.getMessage());
        }
    }

    // Package-private for testing
    Thread getScannerThread() {
        return scannerThread;
    }
}
