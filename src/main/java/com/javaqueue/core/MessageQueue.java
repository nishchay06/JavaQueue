package com.javaqueue.core;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.function.Consumer;

import com.javaqueue.exception.InvalidReceiptException;

public class MessageQueue {

    public final String name;

    private final QueueConfig config;

    // volatile: written by whoever wires the queue up (QueueManager, on some
    // other thread) and read by consumers and the scanner.
    private volatile MessageQueue deadLetterQueue;

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

    // Consumers that registered interest and went away rather than parking a
    // thread. Guarded by this queue's monitor, like everything else here.
    private final Deque<AsyncWaiter> waiters = new ArrayDeque<>();

    private final WalWriter walWriter;

    /** Handle on a registered async consume, so the caller can give up. */
    public interface Waiter {
        /**
         * @return true if this cancelled a wait that was still pending; false
         *         if a message had already been delivered to it
         */
        boolean cancel();
    }

    private final class AsyncWaiter implements Waiter {
        private final Consumer<Receipt> handler;
        private boolean settled; // guarded by MessageQueue.this

        AsyncWaiter(Consumer<Receipt> handler) {
            this.handler = handler;
        }

        @Override
        public boolean cancel() {
            synchronized (MessageQueue.this) {
                if (settled) {
                    return false;
                }
                settled = true;
                waiters.remove(this);
                return true;
            }
        }
    }

    /**
     * A message claimed by a waiter, to be handed over once the monitor is
     * released — the caller's handler must never run while we hold the lock.
     */
    private record Delivery(AsyncWaiter waiter, Receipt receipt) {

        void dispatch() {
            waiter.handler.accept(receipt);
        }
    }

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
        List<Message> departing = List.of();
        if (config.getLogDirectory() != null) {
            try {
                Path logFile = Path.of(config.getLogDirectory(), name + ".log");
                List<LogEntry> entries = WalReader.read(logFile);
                if (!entries.isEmpty()) {
                    synchronized (this) {
                        departing = replay(entries);
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
        flushToDeadLetterQueue(deadLetterQueue, departing);

        VisibilityScanner scanner = new VisibilityScanner(this, scanIntervalMs);
        this.scannerThread = new Thread(scanner, "scanner-" + name);
        this.scannerThread.setDaemon(true);
        this.scannerThread.start();
    }

    public void publish(Message message) {
        Delivery delivery;

        synchronized (this) {
            if (walWriter != null) {
                try {
                    walWriter.append(LogEntry.publish(message));
                } catch (IOException e) {
                    System.err.println("WARNING: Could not write to WAL: " + e.getMessage());
                }
            }
            delivery = enqueueOrHandOff(message);
        }

        // Outside the monitor: the handler is caller code and may do anything,
        // including publishing to another queue. Same rule as the dead letter
        // path — never run someone else's code while holding this lock.
        if (delivery != null) {
            delivery.dispatch();
        }
    }

    /**
     * Registers interest in the next message without holding a thread.
     *
     * If a message is available the handler runs on the calling thread before
     * this returns. Otherwise the waiter is queued and a later publish, nack,
     * or visibility-timeout requeue hands the message straight to it.
     *
     * The handler runs at most once. Callers that want to give up — a long
     * poll reaching its deadline, say — must cancel the returned Waiter.
     */
    public Waiter consumeAsync(Consumer<Receipt> handler) {
        AsyncWaiter waiter = new AsyncWaiter(handler);
        Receipt immediate = null;

        synchronized (this) {
            if (messages.isEmpty()) {
                waiters.add(waiter);
            } else {
                waiter.settled = true;
                immediate = deliverMessage(messages.poll());
            }
        }

        if (immediate != null) {
            handler.accept(immediate);
        }
        return waiter;
    }

    /**
     * Hands the message to a waiting async consumer, or queues it if there is
     * none. Caller MUST hold the monitor.
     *
     * An async waiter is preferred over a thread blocked in consume(): it is
     * already registered, and waking a blocked thread only for it to lose the
     * race is wasted work. Either way exactly one consumer gets the message.
     *
     * @return the delivery to dispatch after releasing the monitor, or null
     */
    private Delivery enqueueOrHandOff(Message message) {
        AsyncWaiter waiter = waiters.poll();
        if (waiter == null) {
            messages.add(message);

            // Wake up all threads waiting in consume().
            // We use notifyAll() not notify() because with multiple consumers,
            // notify() might wake the wrong thread — one that isn't actually
            // waiting for a message. notifyAll() is safer, the while loop
            // in consume() handles the case where a woken thread loses the race.
            notifyAll();
            return null;
        }

        waiter.settled = true;
        return new Delivery(waiter, deliverMessage(message));
    }

    public Receipt consume() throws InterruptedException {
        synchronized (this) {

            // while — not if. Protects against spurious wakeups and the case
            // where multiple consumers are woken but only one gets the message.
            while (messages.isEmpty()) {
                wait(); // releases the lock and sleeps until notifyAll() is called
            }
            return deliverNext();
        }
    }

    // Long-polling consume — waits up to timeoutMs for a message.
    // Returns null if the timeout expires with the queue still empty.
    // This is what HTTP long polling sits on: hold the connection instead of
    // making the client hammer the server with empty GETs.
    public Receipt consume(long timeoutMs) throws InterruptedException {
        synchronized (this) {

            // Deadline, not a plain wait(timeoutMs). A spurious wakeup or a
            // lost race with another consumer would otherwise restart the full
            // timeout each time round the loop.
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (messages.isEmpty()) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    return null;
                }
                wait(remaining);
            }
            return deliverNext();
        }
    }

    // Moves the head message to in-flight and returns its receipt.
    // Caller MUST hold this object's monitor, and messages MUST be non-empty.
    private Receipt deliverNext() {
        return deliverMessage(messages.poll());
    }

    // Marks a message as delivered: in-flight, receipted, recorded in the WAL.
    // Caller MUST hold this object's monitor. The message may come from the
    // queue (deliverNext) or straight from a publish that found a waiter.
    private Receipt deliverMessage(Message message) {
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

    /**
     * Either requeues the message here, or reports that it must leave for the
     * dead letter queue.
     *
     * Caller MUST hold this queue's monitor. The DLQ publish is deliberately
     * NOT done here: publishing takes the DLQ's monitor, and doing that while
     * still holding this one means a thread holds lock A while acquiring lock
     * B. That is only safe if every thread acquires them in the same order,
     * and two queues configured as each other's DLQ make that impossible —
     * one thread goes A then B, the other B then A, and both hang forever.
     *
     * So the message is handed back instead, and the caller publishes it once
     * it has released this monitor. See flushToDeadLetterQueue.
     *
     * @param deliveries collects any hand-off to a waiting async consumer, to
     *                   be dispatched by the caller after the monitor is released
     * @return the message to publish to the DLQ, or null if nothing leaves
     */
    private Message requeueOrDeadLetter(Message message, int newRetryCount,
            List<Delivery> deliveries) {
        if (newRetryCount < config.getMaxRetries()) {
            retryCounts.put(message.getId(), newRetryCount);
            Delivery delivery = enqueueOrHandOff(message);
            if (delivery != null) {
                deliveries.add(delivery);
            }
            return null;
        }

        retryCounts.remove(message.getId());

        if (deadLetterQueue == null) {
            System.err.println("WARNING: Message " + message.getId()
                    + " exceeded max retries and has been dropped from queue '"
                    + name + "'");
            return null;
        }
        return message;
    }

    /**
     * Publishes messages that have left this queue for its DLQ.
     *
     * MUST be called with this queue's monitor released. The window between
     * removing the message here and it landing in the DLQ is not atomic, which
     * is fine: the queue is at-least-once, and the DLQ writes its own WAL entry
     * on arrival. Holding the lock across the publish would not have made it
     * crash-atomic anyway — it would only have added the deadlock.
     */
    private void flushToDeadLetterQueue(MessageQueue dlq, List<Message> departing) {
        if (dlq == null) {
            return;
        }
        for (Message message : departing) {
            dlq.publish(message);
        }
    }

    public void scanAndRequeue() {
        List<Message> departing = new ArrayList<>();
        List<Delivery> deliveries = new ArrayList<>();
        MessageQueue dlq;

        synchronized (this) {
            dlq = deadLetterQueue;
            inFlightMessages.entrySet().stream()
                    .filter(e -> e.getValue().isTimedOut(config.getVisibilityTimeoutMs()))
                    .map(Map.Entry::getKey)
                    .toList() // collects to a new list before we start removing — avoids
                              // ConcurrentModificationException
                    .forEach(handle -> {
                        InFlightEntry entry = inFlightMessages.remove(handle);
                        Message leaving = requeueOrDeadLetter(entry.getMessage(),
                                entry.getRetryCount() + 1, deliveries);
                        if (leaving != null) {
                            departing.add(leaving);
                        }
                    });
        }

        dispatch(deliveries);
        flushToDeadLetterQueue(dlq, departing);
    }

    // Hands claimed messages to their waiters. MUST be called with the monitor
    // released — the handlers are caller code.
    private void dispatch(List<Delivery> deliveries) {
        for (Delivery delivery : deliveries) {
            delivery.dispatch();
        }
    }

    public void nack(String receiptHandle) {
        List<Message> departing = new ArrayList<>();
        List<Delivery> deliveries = new ArrayList<>();
        MessageQueue dlq;

        synchronized (this) {
            dlq = deadLetterQueue;
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

            Message leaving = requeueOrDeadLetter(entry.getMessage(),
                    entry.getRetryCount() + 1, deliveries);
            if (leaving != null) {
                departing.add(leaving);
            }
        }

        dispatch(deliveries);
        flushToDeadLetterQueue(dlq, departing);
    }

    public String getName() {
        return name;
    }

    // ── Observability ─────────────────────────────────────────────────────────
    // Non-destructive accessors, so an operator view can answer "how deep is
    // this queue" without consuming from it.

    /** Messages waiting to be consumed. */
    public int depth() {
        synchronized (this) {
            return messages.size();
        }
    }

    /** Messages consumed but not yet acknowledged. */
    public int inFlightCount() {
        synchronized (this) {
            return inFlightMessages.size();
        }
    }

    /** Consumers currently parked in an async wait. */
    public int waiterCount() {
        synchronized (this) {
            return waiters.size();
        }
    }

    /** The configured dead letter queue name, or null if there is none. */
    public String getDeadLetterQueueName() {
        return config.getDeadLetterQueueName();
    }

    public QueueConfig getConfig() {
        return config;
    }

    // Returns messages bound for the DLQ, to be published by the caller once
    // this queue's monitor is released — same contract as requeueOrDeadLetter.
    // In practice this is always empty during construction, since the DLQ is
    // wired up by QueueManager after the constructor returns.
    private List<Message> replay(List<LogEntry> entries) {
        List<Message> departing = new ArrayList<>();
        // Nobody can be waiting during construction, so this stays empty —
        // it exists to satisfy requeueOrDeadLetter's contract.
        List<Delivery> deliveries = new ArrayList<>();

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
                        Message leaving = requeueOrDeadLetter(removed.getMessage(),
                                removed.getRetryCount() + 1, deliveries);
                        if (leaving != null) {
                            departing.add(leaving);
                        }
                    }
                }
            }
        }

        // Any messages still in-flight at end of log were in-flight when JVM crashed.
        // Treat as implicit NACK — requeue them all.
        new ArrayList<>(inFlightMessages.entrySet())
                .forEach(e -> {
                    inFlightMessages.remove(e.getKey());
                    Message leaving = requeueOrDeadLetter(e.getValue().getMessage(),
                            e.getValue().getRetryCount() + 1, deliveries);
                    if (leaving != null) {
                        departing.add(leaving);
                    }
                });

        return departing;
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
