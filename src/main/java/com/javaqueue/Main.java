package com.javaqueue;

import com.javaqueue.core.QueueManager;
import com.javaqueue.core.TopicManager;
import com.javaqueue.server.QueueServer;

public class Main {

    private static final int DEFAULT_PORT = 8080;

    public static void main(String[] args) throws Exception {
        int port = intArg(args, "--port", DEFAULT_PORT);
        String logDir = stringArg(args, "--log-dir", System.getenv("JAVAQUEUE_LOG_DIR"));

        // One log directory for the whole server. Without it, every queue
        // created over HTTP would need logDirectory in its body to be durable,
        // and an auto-created DLQ could never be durable at all.
        QueueManager queues = new QueueManager(logDir);
        TopicManager topics = new TopicManager(queues, logDir);
        QueueServer server = new QueueServer(queues, topics, port);

        // Stop the server on Ctrl-C so queue close() runs and the WAL is flushed.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                server.stop();
                topics.close();
            } catch (Exception e) {
                System.err.println("WARNING: Error during shutdown: " + e.getMessage());
            }
        }));

        server.start();
        System.out.println("JavaQueue listening on http://localhost:" + server.getPort());
        System.out.println(logDir == null
                ? "Persistence: OFF — pass --log-dir=<path> to survive restarts"
                : "Persistence: " + logDir);
        System.out.println();
        System.out.println("  curl -X POST localhost:" + port + "/queues/orders");
        System.out.println("  curl -X POST localhost:" + port + "/queues/orders/messages "
                + "-d '{\"payload\":\"Order #1\"}'");
        System.out.println("  curl localhost:" + port + "/queues/orders/messages");
        System.out.println("  curl -X DELETE localhost:" + port + "/queues/orders/messages/{receiptHandle}");
        System.out.println();
        System.out.println("  # fan-out: one publish, every subscriber queue gets a copy");
        System.out.println("  curl -X POST localhost:" + port + "/topics/orders-events");
        System.out.println("  curl -X POST localhost:" + port + "/topics/orders-events/subscriptions/orders");
        System.out.println("  curl -X POST localhost:" + port + "/topics/orders-events/messages "
                + "-d '{\"payload\":\"Order #1\"}'");

        server.join();
    }

    // Reads --name=value from the command line, falling back to a default.
    private static String stringArg(String[] args, String name, String fallback) {
        String prefix = name + "=";
        for (String arg : args) {
            if (arg.startsWith(prefix)) {
                String value = arg.substring(prefix.length());
                return value.isBlank() ? fallback : value;
            }
        }
        return fallback;
    }

    private static int intArg(String[] args, String name, int fallback) {
        String value = stringArg(args, name, null);
        if (value == null) {
            return fallback;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            System.err.println("WARNING: Ignoring invalid " + name + "=" + value
                    + ", using " + fallback);
            return fallback;
        }
    }
}
