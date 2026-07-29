package com.javaqueue.bench;

import com.javaqueue.core.Message;
import com.javaqueue.core.MessageQueue;
import com.javaqueue.core.QueueConfig;
import com.javaqueue.core.Receipt;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * End-to-end publish -> consume -> acknowledge, measured as throughput and as a
 * latency distribution.
 *
 * The round trip is deliberately the unit of work rather than a bare publish:
 * publishing without consuming grows the queue without bound, so a long run
 * would measure allocation pressure rather than queue behaviour. Acknowledging
 * every message keeps depth flat and the numbers steady-state.
 *
 * The {@code durability} axis is the interesting one. {@code memory} keeps
 * everything in the heap. {@code wal} writes each operation through the
 * write-ahead log, which currently means a BufferedWriter flush into the OS
 * page cache -- not an fsync -- so it costs a syscall per publish but not a
 * disk seek.
 */
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgs = {"-Xms1g", "-Xmx1g"})
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class RoundTripBenchmark {

    /** Long enough that a healthy queue never hits it; short enough to fail fast if it stalls. */
    private static final long CONSUME_TIMEOUT_MS = 10_000;

    @Param({"64", "1024", "8192"})
    public int payloadSize;

    @Param({"memory", "wal"})
    public String durability;

    private BenchQueue fixture;
    private String payload;

    @Setup(Level.Trial)
    public void createPayload() {
        payload = "x".repeat(payloadSize);
    }

    /**
     * Rebuilt every iteration so a WAL run starts from an empty log. Without
     * this the log file grows across the whole trial and later iterations pay
     * for earlier ones.
     */
    @Setup(Level.Iteration)
    public void openQueue() throws IOException {
        fixture = BenchQueue.open("roundtrip", durability);
    }

    @TearDown(Level.Iteration)
    public void closeQueue() throws IOException {
        fixture.close();
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void throughput(Blackhole bh) throws InterruptedException {
        roundTrip(bh);
    }

    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void latency(Blackhole bh) throws InterruptedException {
        roundTrip(bh);
    }

    private void roundTrip(Blackhole bh) throws InterruptedException {
        MessageQueue queue = fixture.queue();
        queue.publish(new Message(payload));

        Receipt receipt = queue.consume(CONSUME_TIMEOUT_MS);
        if (receipt == null) {
            throw new IllegalStateException(
                    "consume timed out after " + CONSUME_TIMEOUT_MS + "ms -- "
                            + "the queue should always hold this thread's own message");
        }

        queue.acknowledge(receipt.getReceiptHandle());
        bh.consume(receipt);
    }
}
