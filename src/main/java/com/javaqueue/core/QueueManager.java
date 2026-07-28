package com.javaqueue.core;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.javaqueue.exception.QueueNotFoundException;

public class QueueManager {

    // ConcurrentHashMap handles its own thread safety internally.
    // We don't need synchronized here because the map itself guarantees
    // atomic put/get/remove operations across threads.
    private final ConcurrentHashMap<String, MessageQueue> queues = new ConcurrentHashMap<>();

    /**
     * Applied to queues created without an explicit logDirectory. Null means
     * no persistence, which is the historical behaviour.
     *
     * Durability used to be strictly per-queue, so a queue created without one
     * — over HTTP, or as an auto-created DLQ — silently lost its messages on
     * restart. A default here makes persistence a property of the deployment
     * instead of something every caller has to remember.
     */
    private final String defaultLogDirectory;

    public QueueManager() {
        this(null);
    }

    public QueueManager(String defaultLogDirectory) {
        this.defaultLogDirectory = defaultLogDirectory;
    }

    public MessageQueue createQueue(String name) {
        return createQueue(name, QueueConfig.defaults());
    }

    public MessageQueue createQueue(String name, QueueConfig config) {
        QueueConfig effective = applyDefaultLogDirectory(config);

        // computeIfAbsent is atomic — if two threads call createQueue("orders")
        // simultaneously, only one MessageQueue is created. Not two.
        MessageQueue queue = queues.computeIfAbsent(name,
                n -> new MessageQueue(n, effective));

        String dlqName = config.getDeadLetterQueueName();
        if (dlqName != null) {
            // The DLQ gets the default too. A dead letter queue that loses its
            // contents on restart defeats the point of having one.
            MessageQueue dlq = queues.computeIfAbsent(dlqName,
                    n -> new MessageQueue(n, applyDefaultLogDirectory(QueueConfig.defaults())));
            queue.setDeadLetterQueue(dlq);
        }

        return queue;
    }

    // An explicit logDirectory is a deliberate override and always wins.
    private QueueConfig applyDefaultLogDirectory(QueueConfig config) {
        if (defaultLogDirectory == null || config.getLogDirectory() != null) {
            return config;
        }
        return new QueueConfig(
                config.getVisibilityTimeoutMs(),
                config.getMaxRetries(),
                config.getDeadLetterQueueName(),
                defaultLogDirectory);
    }

    public MessageQueue getQueue(String name) {
        MessageQueue queue = queues.get(name);
        if (queue == null) {
            throw new QueueNotFoundException(name);
        }
        return queue;
    }

    /**
     * The non-throwing counterpart to getQueue — returns null if absent.
     *
     * For callers where a missing queue is an expected condition rather than
     * an error, such as fan-out delivery to a subscriber that has since been
     * deleted. Using getQueue there would mean driving normal control flow
     * with exceptions; a separate check would be a TOCTOU race.
     */
    public MessageQueue findQueue(String name) {
        return queues.get(name);
    }

    public void deleteQueue(String name) {
        MessageQueue queue = queues.remove(name);
        if (queue != null) {
            queue.close();
        }
    }

    public Set<String> listQueues() {
        return queues.keySet();
    }
}

/*
 * Why not this?
 * if (!queues.containsKey(name)) {
 * queues.put(name, new MessageQueue(name));
 * }
 * ```
 * 
 * Because that's two separate operations — `containsKey` and `put`. Another
 * thread could slip in between them:
 * ```
 * Thread 1: containsKey("orders") → false
 * Thread 2: containsKey("orders") → false ← both see it missing
 * Thread 1: put("orders", new MessageQueue())
 * Thread 2: put("orders", new MessageQueue()) ← overwrites Thread 1's queue!
 * 
 */