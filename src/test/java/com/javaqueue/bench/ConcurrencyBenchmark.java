package com.javaqueue.bench;

import com.javaqueue.core.Message;
import com.javaqueue.core.MessageQueue;
import com.javaqueue.core.Receipt;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * How aggregate throughput scales as producers and consumers contend for one
 * queue. Every thread runs the same publish -> consume -> acknowledge round
 * trip against a single shared {@link MessageQueue}.
 *
 * Thread count comes from the JMH command line rather than an annotation, so a
 * scaling curve is one invocation:
 *
 * <pre>
 *   -t 1 -t 2 -t 4 -t 8
 * </pre>
 *
 * A thread may consume a message published by a different thread. That is the
 * point -- it is the contended multi-producer, multi-consumer path. It also
 * stays deadlock-free: a thread only consumes after its own publish, so at
 * least one message is always outstanding when any thread calls consume.
 */
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgs = {"-Xms1g", "-Xmx1g"})
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class ConcurrencyBenchmark {

    private static final long CONSUME_TIMEOUT_MS = 30_000;
    private static final int PAYLOAD_SIZE = 1024;

    @Param({"memory", "wal"})
    public String durability;

    private BenchQueue fixture;
    private String payload;

    @Setup(Level.Trial)
    public void createPayload() {
        payload = "x".repeat(PAYLOAD_SIZE);
    }

    @Setup(Level.Iteration)
    public void openQueue() throws IOException {
        fixture = BenchQueue.open("concurrency", durability);
    }

    @TearDown(Level.Iteration)
    public void closeQueue() throws IOException {
        fixture.close();
    }

    @Benchmark
    public void contendedRoundTrip(Blackhole bh) throws InterruptedException {
        MessageQueue queue = fixture.queue();
        queue.publish(new Message(payload));

        Receipt receipt = queue.consume(CONSUME_TIMEOUT_MS);
        if (receipt == null) {
            throw new IllegalStateException(
                    "consume timed out after " + CONSUME_TIMEOUT_MS + "ms under contention");
        }

        queue.acknowledge(receipt.getReceiptHandle());
        bh.consume(receipt);
    }
}
