package com.javaqueue.recovery;

import com.javaqueue.core.MessageQueue;
import com.javaqueue.core.QueueConfig;
import com.javaqueue.core.Receipt;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Kills a publishing JVM with SIGKILL and checks what survives.
 *
 * The existing WAL tests close the queue cleanly before reopening it, which
 * exercises replay but not crash. This starts a real child process, lets it
 * confirm a few thousand publishes, destroys it without warning, and then
 * reopens the log to see whether every confirmed message came back.
 *
 * Scope: this proves durability against *process* death. It does not prove
 * durability against power loss -- WalWriter flushes to the OS page cache and
 * never calls FileChannel.force(), so the data tested here survives precisely
 * because the OS outlives the process. See BENCHMARKS.md.
 */
class CrashRecoveryTest {

    private static final int CONFIRMATIONS_BEFORE_KILL = 5_000;
    private static final long STARTUP_TIMEOUT_MS = 60_000;

    @Test
    void everyConfirmedMessageSurvivesSigkill(@TempDir Path logDirectory) throws Exception {
        Process child = startPublisher(logDirectory);
        long lastConfirmed;

        try {
            lastConfirmed = readConfirmationsThenKill(child);
        } finally {
            child.destroyForcibly();
            child.waitFor(30, TimeUnit.SECONDS);
        }

        assertTrue(lastConfirmed >= CONFIRMATIONS_BEFORE_KILL - 1,
                "Child did not confirm enough publishes before being killed: " + lastConfirmed);

        // The child was SIGKILLed, so it never closed the queue or flushed on
        // exit. Anything readable now got there during normal operation.
        assertNotEquals(0, child.exitValue(),
                "Child exited cleanly -- the kill did not do what this test needs");

        Set<Long> recovered = drainSequenceNumbers(logDirectory);

        System.out.println("Child exit code:      " + child.exitValue() + " (SIGKILL)");
        System.out.println("Confirmed publishes:  " + (lastConfirmed + 1));
        System.out.println("Recovered from WAL:   " + recovered.size());

        // Every sequence number the child printed had already returned from
        // publish(). Losing any of them means the queue acknowledged a write it
        // could not recover.
        StringBuilder missing = new StringBuilder();
        int missingCount = 0;
        for (long seq = 0; seq <= lastConfirmed; seq++) {
            if (!recovered.contains(seq)) {
                missingCount++;
                if (missing.length() < 200) {
                    missing.append(seq).append(' ');
                }
            }
        }

        assertEquals(0, missingCount,
                "Lost " + missingCount + " of " + (lastConfirmed + 1)
                        + " confirmed messages after SIGKILL. Missing: " + missing);
    }

    private Process startPublisher(Path logDirectory) throws Exception {
        String java = ProcessHandle.current().info().command()
                .orElse(System.getProperty("java.home") + "/bin/java");

        ProcessBuilder builder = new ProcessBuilder(
                java,
                "-cp", System.getProperty("java.class.path"),
                CrashHarness.class.getName(),
                logDirectory.toString());
        builder.redirectErrorStream(false);
        return builder.start();
    }

    /** Returns the highest sequence number the child confirmed. */
    private long readConfirmationsThenKill(Process child) throws Exception {
        long deadline = System.currentTimeMillis() + STARTUP_TIMEOUT_MS;
        long lastConfirmed = -1;

        try (BufferedReader out = new BufferedReader(
                new InputStreamReader(child.getInputStream()))) {
            String line;
            while (lastConfirmed < CONFIRMATIONS_BEFORE_KILL - 1
                    && System.currentTimeMillis() < deadline
                    && (line = out.readLine()) != null) {
                lastConfirmed = Long.parseLong(line.trim());
            }
        }

        // Kill while it is still mid-publish -- no quiescing, no close().
        child.destroyForcibly();
        child.waitFor(30, TimeUnit.SECONDS);
        return lastConfirmed;
    }

    private Set<Long> drainSequenceNumbers(Path logDirectory) throws Exception {
        MessageQueue reopened = new MessageQueue(
                "crash", new QueueConfig(600_000, 3, null, logDirectory.toString()));

        Set<Long> sequences = new HashSet<>();
        try {
            Receipt receipt;
            while ((receipt = reopened.consume(2_000)) != null) {
                String payload = receipt.getMessage().getPayload();
                sequences.add(Long.parseLong(payload.substring(0, payload.indexOf(':'))));
                reopened.acknowledge(receipt.getReceiptHandle());
            }
        } finally {
            reopened.close();
        }
        return sequences;
    }
}
