package com.javaqueue.core;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.javaqueue.exception.QueueNotFoundException;

/**
 * A named fan-out point: publishing here delivers the message into every
 * subscriber's queue.
 *
 * A topic is a subscriber list, not a store. It keeps no history, so a queue
 * that subscribes later sees only messages published after it joined — the
 * limitation that motivates the partitioned log in Phase 6.
 *
 * Subscriber queues are owned by the QueueManager, not by the topic. That
 * keeps the relationship many-to-many: a queue may subscribe to several
 * topics, and unsubscribing detaches routing without touching the queue.
 */
public class Topic {

    private final String name;
    private final QueueManager queueManager;

    // Null when the TopicManager has no log directory — no persistence, same
    // backward-compatible convention as QueueConfig.logDirectory.
    private final TopicLog topicLog;

    // A subscription records a queue NAME, not a MessageQueue instance.
    //
    // Holding the instance would pin one particular object: delete and
    // recreate a queue under the same name and the topic would keep publishing
    // into the dead one, where the messages are unreachable and the recreated
    // queue silently receives nothing. Resolving by name at delivery time
    // always routes to whatever queue currently answers to that name.
    //
    // The set is keyed by name, which makes subscribe idempotent for free.
    private final Set<String> subscriberNames = ConcurrentHashMap.newKeySet();

    Topic(String name, QueueManager queueManager, TopicLog topicLog) {
        this.name = name;
        this.queueManager = queueManager;
        this.topicLog = topicLog;
    }

    public String getName() {
        return name;
    }

    /**
     * Subscribes a live queue.
     *
     * The queue must be registered with the QueueManager, since that is what
     * delivery resolves against. Subscribing something it does not know about
     * would record a subscription that silently never delivers, so it fails
     * here — where the mistake is — instead of at publish time.
     */
    public void subscribe(MessageQueue queue) {
        if (queueManager.findQueue(queue.getName()) == null) {
            throw new QueueNotFoundException(queue.getName());
        }
        subscribe(queue.getName());
    }

    /**
     * Subscribes by name, without requiring the queue to exist yet.
     *
     * This is the replay path: on restart, topic subscriptions are restored
     * before the queues themselves are necessarily recreated, so a name that
     * does not resolve is recorded rather than rejected. It will start
     * delivering as soon as a queue answers to that name.
     */
    public void subscribe(String queueName) {
        if (subscriberNames.add(queueName)) {
            log(TopicOperation.SUBSCRIBE, queueName);
        }
    }

    public void unsubscribe(String queueName) {
        if (subscriberNames.remove(queueName)) {
            log(TopicOperation.UNSUBSCRIBE, queueName);
        }
    }

    // Replay applies the log; it must not append to it while doing so.
    void restoreSubscriber(String queueName) {
        subscriberNames.add(queueName);
    }

    void unsubscribeQuietly(String queueName) {
        subscriberNames.remove(queueName);
    }

    private void log(TopicOperation op, String queueName) {
        if (topicLog != null) {
            topicLog.append(new TopicLog.Record(op, name, queueName));
        }
    }

    /**
     * A snapshot of subscriber names — not a live view of internal state.
     *
     * A name here means a subscription exists, not that the queue does. A
     * subscriber whose queue has been deleted stays listed until explicitly
     * unsubscribed, the same way SNS keeps a subscription whose endpoint has
     * gone away.
     */
    public Set<String> listSubscribers() {
        return new LinkedHashSet<>(subscriberNames);
    }

    /**
     * Delivers the message to every current subscriber whose queue still exists.
     *
     * The same immutable Message instance goes to all of them rather than a
     * copy per subscriber: one payload in memory, and one logical id that can
     * be correlated across groups. Per-queue state (retry counts, in-flight
     * entries, receipt handles) is keyed outside the message, so subscribers
     * cannot interfere with each other through it.
     *
     * Delivery is independent and best-effort per subscriber, matching SNS.
     * There is no cross-subscriber atomicity: the same logical message may be
     * acknowledged on one subscriber and dead-lettered on another.
     *
     * The subscriber list is resolved into a snapshot before any publish, so
     * no topic-level lock is held while a queue's monitor is acquired — the
     * rule Phase 4.2 established for dead letter queues. Never hold one
     * queue's lock while publishing to another.
     *
     * @return how many subscribers actually received the message
     */
    public int publish(Message message) {
        List<MessageQueue> targets = resolveSubscribers();

        for (MessageQueue queue : targets) {
            queue.publish(message);
        }
        return targets.size();
    }

    // Resolves subscriber names to live queues, skipping any that have been
    // deleted. A deleted subscriber must not break delivery for its siblings.
    private List<MessageQueue> resolveSubscribers() {
        List<MessageQueue> resolved = new ArrayList<>(subscriberNames.size());

        for (String subscriberName : subscriberNames) {
            MessageQueue queue = queueManager.findQueue(subscriberName);
            if (queue == null) {
                System.err.println("WARNING: Topic '" + name + "' has a subscription to queue '"
                        + subscriberName + "', which no longer exists — skipping delivery");
                continue;
            }
            resolved.add(queue);
        }
        return resolved;
    }
}
