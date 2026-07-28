package com.javaqueue;

import com.javaqueue.core.QueueManager;
import com.javaqueue.core.TopicManager;
import com.javaqueue.server.QueueServer;

public class Main {

    private static final int PORT = 8080;

    public static void main(String[] args) throws Exception {
        QueueManager queues = new QueueManager();
        TopicManager topics = new TopicManager(queues);
        QueueServer server = new QueueServer(queues, topics, PORT);

        // Stop the server on Ctrl-C so queue close() runs and the WAL is flushed.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                server.stop();
            } catch (Exception e) {
                System.err.println("WARNING: Error during shutdown: " + e.getMessage());
            }
        }));

        server.start();
        System.out.println("JavaQueue listening on http://localhost:" + server.getPort());
        System.out.println();
        System.out.println("  curl -X POST localhost:" + PORT + "/queues/orders");
        System.out.println("  curl -X POST localhost:" + PORT + "/queues/orders/messages "
                + "-d '{\"payload\":\"Order #1\"}'");
        System.out.println("  curl localhost:" + PORT + "/queues/orders/messages");
        System.out.println("  curl -X DELETE localhost:" + PORT + "/queues/orders/messages/{receiptHandle}");
        System.out.println();
        System.out.println("  # fan-out: one publish, every subscriber queue gets a copy");
        System.out.println("  curl -X POST localhost:" + PORT + "/topics/orders-events");
        System.out.println("  curl -X POST localhost:" + PORT + "/topics/orders-events/subscriptions/orders");
        System.out.println("  curl -X POST localhost:" + PORT + "/topics/orders-events/messages "
                + "-d '{\"payload\":\"Order #1\"}'");

        server.join();
    }
}
