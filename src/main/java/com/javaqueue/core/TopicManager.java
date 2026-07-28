package com.javaqueue.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.javaqueue.exception.TopicNotFoundException;

/**
 * Registry of named topics — the same shape as QueueManager, for the same
 * reasons: ConcurrentHashMap handles its own thread safety, and
 * computeIfAbsent makes creation atomic so two concurrent
 * createTopic("orders") calls produce exactly one topic.
 *
 * Unlike QueueManager, this registry is persisted. Queue *contents* survive a
 * restart through the message WAL, but which queues exist is rebuilt by the
 * application on startup. Topic wiring cannot work that way: a topic whose
 * subscriber list was lost still accepts publishes and silently delivers to
 * nobody, so the configuration itself is logged.
 */
public class TopicManager {

    private final ConcurrentHashMap<String, Topic> topics = new ConcurrentHashMap<>();

    // Topics route into queues, so the registry that owns those queues is
    // needed to resolve a subscription by name at delivery time.
    private final QueueManager queueManager;

    // Null disables persistence, matching QueueConfig.logDirectory.
    private final TopicLog topicLog;

    public TopicManager(QueueManager queueManager) {
        this(queueManager, null);
    }

    public TopicManager(QueueManager queueManager, String logDirectory) {
        this.queueManager = queueManager;

        TopicLog log = null;
        if (logDirectory != null) {
            List<TopicLog.Record> records = TopicLog.read(logDirectory);
            try {
                log = new TopicLog(logDirectory);
            } catch (IOException e) {
                System.err.println("WARNING: Could not initialize topic log: " + e.getMessage());
            }
            this.topicLog = log;

            if (!records.isEmpty()) {
                replay(records);
                compact();
            }
        } else {
            this.topicLog = null;
        }
    }

    public Topic createTopic(String name) {
        Topic existing = topics.get(name);
        if (existing != null) {
            return existing;
        }

        // computeIfAbsent so concurrent creation of one name yields one topic.
        // The mapping function must stay side-effect free, so the log write
        // happens after, guarded by whether this call actually created it.
        boolean[] created = { false };
        Topic topic = topics.computeIfAbsent(name, n -> {
            created[0] = true;
            return new Topic(n, queueManager, topicLog);
        });

        if (created[0] && topicLog != null) {
            topicLog.append(new TopicLog.Record(TopicOperation.CREATE_TOPIC, name, null));
        }
        return topic;
    }

    public Topic getTopic(String name) {
        Topic topic = topics.get(name);
        if (topic == null) {
            throw new TopicNotFoundException(name);
        }
        return topic;
    }

    /**
     * Removes the topic and, with it, its routing.
     *
     * Subscriber queues are owned by the QueueManager and are deliberately
     * left alone — they may still hold undelivered messages, and may be
     * subscribed to other topics.
     */
    public void deleteTopic(String name) {
        if (topics.remove(name) != null && topicLog != null) {
            topicLog.append(new TopicLog.Record(TopicOperation.DELETE_TOPIC, name, null));
        }
    }

    /** A snapshot of topic names — not a live view of internal state. */
    public Set<String> listTopics() {
        return new LinkedHashSet<>(topics.keySet());
    }

    /** Closes the topic log. Topics themselves hold no other resources. */
    public void close() {
        if (topicLog != null) {
            try {
                topicLog.close();
            } catch (IOException e) {
                System.err.println("WARNING: Could not close topic log: " + e.getMessage());
            }
        }
    }

    // Rebuilds topics and subscriptions from the log. Subscriptions are
    // restored by name without requiring the queue to exist yet — the
    // application may not have recreated it at this point in startup.
    private void replay(List<TopicLog.Record> records) {
        for (TopicLog.Record record : records) {
            switch (record.op()) {
                case CREATE_TOPIC ->
                    topics.computeIfAbsent(record.topic(),
                            n -> new Topic(n, queueManager, topicLog));

                case DELETE_TOPIC -> topics.remove(record.topic());

                case SUBSCRIBE -> {
                    // A SUBSCRIBE can precede its CREATE_TOPIC only in a
                    // corrupted log; creating on demand keeps replay total.
                    Topic topic = topics.computeIfAbsent(record.topic(),
                            n -> new Topic(n, queueManager, topicLog));
                    topic.restoreSubscriber(record.queue());
                }

                case UNSUBSCRIBE -> {
                    Topic topic = topics.get(record.topic());
                    if (topic != null) {
                        topic.unsubscribeQuietly(record.queue());
                    }
                }
            }
        }
    }

    // Rewrites the log as current state: one CREATE_TOPIC per topic, then one
    // SUBSCRIBE per subscription. Deleted topics and churned subscriptions
    // disappear rather than accumulating across restarts.
    private void compact() {
        if (topicLog == null) {
            return;
        }

        List<TopicLog.Record> survivors = new ArrayList<>();
        for (Topic topic : topics.values()) {
            survivors.add(new TopicLog.Record(TopicOperation.CREATE_TOPIC, topic.getName(), null));
            for (String subscriber : topic.listSubscribers()) {
                survivors.add(new TopicLog.Record(TopicOperation.SUBSCRIBE,
                        topic.getName(), subscriber));
            }
        }
        topicLog.compact(survivors);
    }
}
