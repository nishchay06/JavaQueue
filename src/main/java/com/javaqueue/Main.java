package com.javaqueue;

import com.javaqueue.core.QueueManager;
import com.javaqueue.server.QueueServer;

public class Main {

    private static final int PORT = 8080;

    public static void main(String[] args) throws Exception {
        QueueManager manager = new QueueManager();
        QueueServer server = new QueueServer(manager, PORT);

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

        server.join();
    }
}
