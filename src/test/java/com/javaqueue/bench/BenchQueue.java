package com.javaqueue.bench;

import com.javaqueue.core.MessageQueue;
import com.javaqueue.core.QueueConfig;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

/**
 * A queue plus the temp directory backing it, so benchmarks can open and
 * discard configurations without leaking files between iterations.
 */
final class BenchQueue implements AutoCloseable {

    /**
     * Well beyond any benchmark iteration. The visibility scanner must never
     * requeue a message mid-run, or throughput would include redelivery work
     * that a healthy consumer would never trigger.
     */
    private static final long VISIBILITY_TIMEOUT_MS = 600_000;

    private static final int MAX_RETRIES = 3;

    private final MessageQueue queue;
    private final Path logDirectory;

    private BenchQueue(MessageQueue queue, Path logDirectory) {
        this.queue = queue;
        this.logDirectory = logDirectory;
    }

    /**
     * @param durability {@code memory} for heap-only, {@code wal} to write
     *                   through the write-ahead log
     */
    static BenchQueue open(String name, String durability) throws IOException {
        Path directory = switch (durability) {
            case "memory" -> null;
            case "wal" -> Files.createTempDirectory("javaqueue-bench-");
            default -> throw new IllegalArgumentException("unknown durability: " + durability);
        };

        QueueConfig config = new QueueConfig(
                VISIBILITY_TIMEOUT_MS,
                MAX_RETRIES,
                null,
                directory == null ? null : directory.toString());

        return new BenchQueue(new MessageQueue(name, config), directory);
    }

    MessageQueue queue() {
        return queue;
    }

    @Override
    public void close() throws IOException {
        queue.close();
        if (logDirectory != null) {
            deleteRecursively(logDirectory);
        }
    }

    private static void deleteRecursively(Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        try (var paths = Files.walk(root)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(path);
            }
        }
    }
}
