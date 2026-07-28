package com.javaqueue.server;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

import com.javaqueue.core.QueueManager;
import com.javaqueue.core.TopicManager;

/**
 * Wraps a Jetty server around a QueueManager and a TopicManager.
 *
 * The managers are passed in rather than owned, so tests can inspect state
 * directly instead of going through HTTP for everything. Lifecycle is explicit:
 * constructing the server does not bind the port — start() does.
 */
public class QueueServer {

    private final Server server;
    private final ServerConnector connector;

    private final QueueHandler handler;

    public QueueServer(QueueManager queueManager, TopicManager topicManager, int port) {
        this(queueManager, topicManager, port, 0);
    }

    /**
     * @param maxThreads size of Jetty's thread pool, or 0 for its default.
     *                   Deliberately constrainable so tests can prove long
     *                   polling holds no thread per waiting client.
     */
    public QueueServer(QueueManager queueManager, TopicManager topicManager,
            int port, int maxThreads) {
        this.server = maxThreads > 0
                ? new Server(new QueuedThreadPool(maxThreads))
                : new Server();

        // One acceptor and one selector, so a small pool leaves as much as
        // possible for actually handling requests.
        this.connector = new ServerConnector(server, 1, 1);
        this.connector.setPort(port);
        this.server.addConnector(connector);

        this.handler = new QueueHandler(queueManager, topicManager);
        this.server.setHandler(handler);
    }

    public void start() throws Exception {
        server.start();
    }

    public void stop() throws Exception {
        server.stop();
        handler.close();
    }

    /**
     * The port actually bound. Constructing with port 0 asks the OS for any
     * free port — tests use that to avoid conflicts, then read it back here.
     * Only meaningful after start().
     */
    public int getPort() {
        return connector.getLocalPort();
    }

    public boolean isRunning() {
        return server.isRunning();
    }

    /** Blocks the calling thread until the server stops. */
    public void join() throws InterruptedException {
        server.join();
    }
}
