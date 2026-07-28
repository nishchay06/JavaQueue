package com.javaqueue.server;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;

import com.javaqueue.core.QueueManager;

/**
 * Wraps a Jetty server around a QueueManager.
 *
 * The manager is passed in rather than owned, so tests can inspect queue state
 * directly instead of going through HTTP for everything. Lifecycle is explicit:
 * constructing the server does not bind the port — start() does.
 */
public class QueueServer {

    private final Server server;
    private final ServerConnector connector;

    public QueueServer(QueueManager manager, int port) {
        this.server = new Server();
        this.connector = new ServerConnector(server);
        this.connector.setPort(port);
        this.server.addConnector(connector);
        this.server.setHandler(new QueueHandler(manager));
    }

    public void start() throws Exception {
        server.start();
    }

    public void stop() throws Exception {
        server.stop();
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
