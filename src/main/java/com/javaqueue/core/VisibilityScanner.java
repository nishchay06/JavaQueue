package com.javaqueue.core;

public class VisibilityScanner implements Runnable {

    private final MessageQueue queue;
    private final long scanIntervalMs;

    // scanIntervalMs is a parameter — not hardcoded to 1000ms.
    // Tests pass 50ms so they don't have to wait a full second.
    // Production passes 1000ms.
    public VisibilityScanner(MessageQueue queue, long scanIntervalMs) {
        this.queue = queue;
        this.scanIntervalMs = scanIntervalMs;
    }

    @Override
    public void run() {
        // Keep scanning until the thread is interrupted.
        // Interrupted means close() was called — time to stop.
        while (!Thread.currentThread().isInterrupted()) {
            try {
                Thread.sleep(scanIntervalMs);

                // All the real logic lives in MessageQueue where
                // it is protected by the queue's lock.
                queue.scanAndRequeue();

            } catch (InterruptedException e) {
                // sleep() clears the interrupted flag when it throws.
                // Restore it so the while condition can see it and exit.
                Thread.currentThread().interrupt();
            }
        }
    }
}
