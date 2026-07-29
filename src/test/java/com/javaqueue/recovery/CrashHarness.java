package com.javaqueue.recovery;

import com.javaqueue.core.Message;
import com.javaqueue.core.MessageQueue;
import com.javaqueue.core.QueueConfig;

/**
 * Child process for {@link CrashRecoveryTest}. Publishes forever and reports
 * each message only once {@code publish} has returned, so the parent knows
 * exactly which messages the queue accepted before it was killed.
 *
 * Never shuts down cleanly -- the parent SIGKILLs it. That is the point: no
 * close(), no final flush, no shutdown hook.
 */
public final class CrashHarness {

    private CrashHarness() {
    }

    public static void main(String[] args) {
        String logDirectory = args[0];
        int payloadSize = args.length > 1 ? Integer.parseInt(args[1]) : 128;
        String filler = "x".repeat(payloadSize);

        MessageQueue queue = new MessageQueue(
                "crash", new QueueConfig(600_000, 3, null, logDirectory));

        long sequence = 0;
        while (true) {
            queue.publish(new Message(sequence + ":" + filler));
            // Printed only after publish returns. Everything the parent reads
            // here is a message the queue claimed to have durably accepted.
            System.out.println(sequence);
            System.out.flush();
            sequence++;
        }
    }
}
