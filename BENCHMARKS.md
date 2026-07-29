# Benchmarks

Measured with [JMH](https://openjdk.org/projects/code-tools/jmh/). Reproduce with:

```bash
./benchmarks.sh                                   # full suite, ~6 min
./benchmarks.sh RoundTripBenchmark                # throughput + latency only
./benchmarks.sh ConcurrencyBenchmark -t 4         # scaling at 4 threads
```

Read the [limitations](#limitations) before quoting any of these numbers.

## What is measured

The unit of work is a full **publish → consume → acknowledge** round trip, not a bare
publish. Publishing without consuming grows the queue without bound, so a sustained run
would end up measuring allocation pressure rather than queue behaviour. Acknowledging
every message keeps depth flat and the numbers steady-state.

Two durability modes:

| mode | behaviour |
|---|---|
| `memory` | heap only, nothing written to disk |
| `wal` | every operation appended to the write-ahead log |

## Environment

| | |
|---|---|
| CPU | Apple M1 — 8 cores (4 performance + 4 efficiency) |
| Memory | 8 GB |
| OS | macOS 15.6 |
| JDK | 21.0.10 LTS |
| Heap | `-Xms1g -Xmx1g` |
| JMH | 1 fork, 3 × 1 s warmup, 5 × 1 s measurement |

## Throughput

Single-threaded, ops/sec.

| payload | `memory` | `wal` | WAL penalty |
|--------:|---------:|------:|------------:|
| 64 B | 2,000,816 | 88,412 | 22.6× |
| 1 KB | 1,882,963 | 55,052 | 34.2× |
| 8 KB | 1,978,951 | 17,652 | **112×** |

**In-memory throughput is flat across payload size** — 2.00M, 1.88M, 1.98M ops/s from
64 B to 8 KB, a 128× range in message size for no consistent change in throughput. The
1 KB dip is within run-to-run noise. Cost is dominated by lock acquisition and queue
bookkeeping, not by moving bytes.

**The WAL path degrades 5× over the same range** — 88k → 17.6k ops/s. Here payload size
is the dominant term.

So the two paths are bound by different things: the in-memory queue is **contention-bound**,
the WAL is **bandwidth-bound**. They need different optimisations.

## Latency

Microseconds per round trip.

| config | mean | p50 | p90 | p99 | p99.9 | p99.99 | max |
|---|---:|---:|---:|---:|---:|---:|---:|
| `memory` 64 B | 0.55 | 0.42 | 0.50 | 1.42 | 5.95 | 216 | 1,927 |
| `memory` 1 KB | 0.70 | 0.46 | 0.50 | 1.92 | 30.2 | 250 | 3,625 |
| `memory` 8 KB | 1.93 | 0.46 | 0.67 | 6.74 | 228 | 2,311 | 13,517 |
| `wal` 64 B | 12.5 | 10.0 | 14.6 | 35.8 | 218 | 2,022 | 5,284 |
| `wal` 1 KB | 19.3 | 16.2 | 22.3 | 53.7 | 403 | 3,260 | 10,912 |
| `wal` 8 KB | 61.5 | 45.6 | 68.1 | 160 | 2,214 | 13,248 | 67,764 |

Median in-memory round trip is **~0.46 µs** and barely moves with payload size, which is
the same contention-bound story the throughput numbers tell.

**The tails are bad.** In-memory 1 KB runs a 0.46 µs median against a 3.6 ms maximum —
roughly four orders of magnitude. The `wal` 8 KB case reaches 67.8 ms. The p99.9-to-max
jump points at GC pauses and lock convoying rather than anything in the write path.
This is the most actionable result here: median latency looks excellent and the tail
does not.

## Concurrency scaling

`ConcurrencyBenchmark`, 1 KB payloads, all threads contending on one queue.

| threads | `memory` ops/s | vs. 1 thread | `wal` ops/s | vs. 1 thread |
|--------:|---------------:|-------------:|------------:|-------------:|
| 1 | 1,654,526 ± 1,695,360 | 1.00× | 56,106 ± 14,774 | 1.00× |
| 2 | 1,108,610 ± 196,850 | 0.67× | 53,016 ± 15,778 | 0.94× |
| 4 | 1,251,032 ± 77,358 | 0.76× | 48,847 ± 29,637 | 0.87× |
| 8 | 1,255,389 ± 108,752 | 0.76× | 52,231 ± 19,085 | 0.93× |
| 16 | 1,259,673 ± 172,132 | 0.76× | 55,906 ± 28,421 | 1.00× |

**Adding threads does not add throughput.** The in-memory path drops to ~0.7× of
single-threaded and then sits on a hard plateau — 1.25M, 1.26M, 1.26M ops/s at 4, 8 and
16 threads, flat to within 0.5% while thread count quadruples. The WAL path is flat
within error across the whole range. `MessageQueue` guards a `LinkedList` with a single monitor, so every producer and
consumer serialises through one lock — extra threads contend rather than parallelise,
and pay context-switch cost for the privilege.

Two honest caveats on this table specifically:

- **The 1-thread `memory` figure has a ±102% error bar** and should not be quoted. The
  scaling ratios against it are indicative only. A publication-quality number needs
  `-f 3` or more forks.
- **The M1's cores are heterogeneous** (4 performance + 4 efficiency). Past 4 threads the
  OS starts scheduling onto efficiency cores, so the 8-thread row conflates lock
  contention with slower cores. A scaling curve on homogeneous cores would be cleaner.

The direction of the result is solid — more threads never helps — but the exact ratios
are soft.

## Durability caveat

The `wal` numbers do **not** measure crash-safe durability.

`WalWriter` calls `BufferedWriter.flush()`, which pushes bytes into the OS page cache. It
never calls `FileChannel.force()`. So the WAL survives **process** crash — `kill -9`, an
uncaught exception — but **not** machine crash or power loss, where anything still in the
page cache is gone.

A real fsync per append would cost substantially more than the 22–112× measured here.
Treat these as the floor for durable operation, not the price of it.

## Limitations

- Single machine, single run, 1 fork. Fine for relative comparisons; not a claim about
  absolute performance on other hardware.
- No fsync, as above.
- No comparison against Kafka, RabbitMQ, or SQS. Without a baseline these numbers say
  what the queue does, not whether that is good.
- Payload sizes stop at 8 KB.
- Benchmarks run on a laptop with other processes active, which is a plausible source of
  the wide error bars in the concurrency table.

## Open questions

1. **Can the lock be split?** Separate producer and consumer locks, or a lock-free ring
   buffer, would test whether the flat scaling curve is really the single monitor.
2. **Where does the tail come from?** Running under `-prof gc` would separate GC pauses
   from lock convoying.
3. **What does fsync actually cost?** Making the sync policy configurable would turn the
   durability caveat above into a measured three-way tradeoff.
4. **How does it compare?** A Kafka baseline in Docker would give all of this a reference
   point.
