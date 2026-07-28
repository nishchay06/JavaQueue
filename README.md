# JavaQueue

A message queue built from scratch in Java, designed as a deep-learning project to understand the internals of distributed messaging systems like Amazon SQS, RabbitMQ, and Apache Kafka.

Rather than wrapping existing libraries, this project works directly with raw Java concurrency primitives — `synchronized`, `wait()`/`notifyAll()`, `AtomicLong`, and `ConcurrentHashMap` — to understand what production queue systems are actually doing under the hood.

---

## Why Build This?

Using SQS at work gives you the interface of a message queue, not the internals. This project answers the questions that production use leaves open:

- How does a queue safely handle multiple threads producing and consuming simultaneously?
- What does "blocking" actually mean at the thread level, and how is it implemented?
- How do delivery guarantees like at-least-once work mechanically?
- What happens when a consumer crashes mid-processing — how does the queue recover?
- How do production systems ensure messages survive a server restart?
- What trade-offs exist between throughput, durability, and complexity?

---

## Project Structure

```
javaqueue/
├── pom.xml
└── src/
    ├── main/
    │   └── java/
    │       └── com/javaqueue/
    │           ├── Main.java
    │           ├── core/
    │           │   ├── Message.java           # Immutable value object with atomic ID generation
    │           │   ├── Receipt.java           # Unique per delivery, used to ACK
    │           │   ├── MessageQueue.java      # Core queue — all concurrency lives here
    │           │   ├── QueueManager.java      # Thread-safe registry of named queues
    │           │   ├── QueueConfig.java       # Visibility timeout, max retries, DLQ, log dir
    │           │   ├── InFlightEntry.java     # Tracks message + timestamp + retry count
    │           │   ├── VisibilityScanner.java # Daemon thread — requeues timed-out messages
    │           │   ├── LogEntry.java          # Immutable WAL record with JSON serializer
    │           │   ├── LogOperation.java      # Enum: PUBLISH, CONSUME, ACK, NACK
    │           │   ├── WalWriter.java         # Append-only log file writer
    │           │   └── WalReader.java         # Replays log on startup
    │           ├── server/
    │           │   ├── QueueServer.java       # Jetty lifecycle — start, stop, bound port
    │           │   └── QueueHandler.java      # Routes HTTP requests onto the QueueManager
    │           ├── json/
    │           │   └── JsonUtils.java         # Hand-written flat JSON — shared by the WAL and the HTTP layer
    │           └── exception/
    │               ├── QueueNotFoundException.java
    │               └── InvalidReceiptException.java
    └── test/
        └── java/
            └── com/javaqueue/
                ├── core/
                │   ├── MessageTest.java
                │   ├── MessageQueueTest.java
                │   ├── QueueManagerTest.java
                │   ├── DeliveryGuaranteesTest.java
                │   ├── LogEntryTest.java
                │   ├── WalTest.java
                │   └── PersistenceTest.java
                ├── json/
                │   └── JsonUtilsTest.java
                ├── server/
                │   └── QueueServerTest.java
                └── concurrent/
                    ├── ConcurrentStressTest.java
                    └── DeliveryGuaranteesStressTest.java
```

---

## Phase Roadmap

| Phase | Name | Status | Key Addition |
|-------|------|--------|-------------|
| 1 | In-Memory Core | ✅ Complete | Named queues, publish, blocking consume, ACK |
| 2 | Delivery Guarantees | ✅ Complete | Visibility timeout, NACK, retry limit, dead letter queue |
| 3 | Persistence | ✅ Complete | Write-ahead log — messages survive JVM restart |
| 4 | Networking | ✅ Complete | HTTP API with long polling so external processes can connect |
| 5 | Consumer Groups | 📋 Designed | SNS-style fan-out — a topic delivers to N subscriber queues |
| 6 | Partitioned Log | ⏳ Planned | Kafka-style retained log with per-group offsets, retention, partitions |

---

## Phase 1 — In-Memory Core

### How It Works

**Publish** — a producer calls `publish(message)`. The message is added to a `LinkedList` inside a `synchronized` block, then `notifyAll()` wakes any waiting consumers.

**Consume** — a consumer calls `consume()`. If the queue is empty, the thread calls `wait()`, releasing the lock and sleeping until a message arrives. When woken, it re-checks the condition in a `while` loop (not `if` — guards against spurious wakeups and lost races), takes the message, creates a `Receipt`, stores it in the in-flight map, and returns it.

**Acknowledge** — the consumer calls `acknowledge(receiptHandle)`. The queue looks up the handle in the in-flight map, removes it, and the message's lifecycle is complete.

### Key Design Decisions

| Problem | Primitive | Why |
|---------|-----------|-----|
| Mutual exclusion on the queue | `synchronized` (intrinsic lock) | Simplest correct option; teaches the Java Memory Model directly |
| Consumer blocking on empty queue | `wait()` / `notifyAll()` | Fundamental OS-level mechanism; forces understanding of monitor conditions |
| Unique message IDs | `AtomicLong` | Lock-free counter using compare-and-swap |
| Thread-safe queue registry | `ConcurrentHashMap` | Teaches when to delegate thread safety vs build it |
| In-flight message tracking | `HashMap` inside `synchronized` block | Shares the queue's existing lock — no extra synchronization needed |

### Why Not `LinkedBlockingQueue`?

The JDK ships `LinkedBlockingQueue` which is correct, fast, and battle-tested. It is deliberately not used here because the goal is to understand what it does internally. Once you've implemented the primitives yourself, comparing your implementation to the JDK source becomes a rich learning exercise.

---

## Phase 2 — Delivery Guarantees

### The Problem Phase 1 Left Open

If a consumer crashes after calling `consume()` but before calling `acknowledge()`, the message is lost forever — stuck in the in-flight map with no way to recover. Phase 2 fixes this.

### How It Works

**Visibility Timeout** — when a message is consumed, a timestamp is recorded. A background daemon thread (`VisibilityScanner`) wakes every second, scans all in-flight messages, and requeues any that haven't been ACKed within the configured timeout. This is exactly how SQS works internally.

**NACK** — a consumer can explicitly reject a message with `nack(receiptHandle)`. The message is requeued immediately without waiting for the timeout. The retry count is incremented.

**Retry Limit** — every message tracks how many times it has been delivered. When the retry count hits the configured limit, the message is no longer requeued.

**Dead Letter Queue** — instead of dropping exhausted messages, they are published to a separate DLQ for inspection. The DLQ is a normal `MessageQueue` — it can be consumed from like any other queue.

### Message Lifecycle

```
publish()
    └──► Queued
              └──► In-Flight  (consume())
                        ├──► Acknowledged      (acknowledge())  — lifecycle ends
                        ├──► Queued again      (nack() or timeout, retryCount < maxRetries)
                        ├──► Dead-Lettered     (retryCount >= maxRetries, DLQ configured)
                        └──► Dropped           (retryCount >= maxRetries, no DLQ)
```

### Key Design Decisions

| Problem | Approach | Why |
|---------|----------|-----|
| Timeout detection | Background daemon thread scanning in-flight map | Single scanner per queue; same pattern SQS uses internally |
| Scanner thread safety | Shares queue's `synchronized(this)` lock | No additional synchronization — scanner participates in existing mutual exclusion |
| Retry count persistence across requeues | Separate `retryCounts` map keyed by message ID | InFlightEntry is created fresh on each consume(); retry count must survive outside it |
| Scanner shutdown | `interrupt()` + `join()` in `close()` | Blocks until scanner fully stops — predictable, no fire-and-forget |
| DLQ wiring | Auto-created by `QueueManager` if it doesn't exist | Simpler for callers; DLQ is kept alive after original queue is deleted |

---

## Phase 3 — Persistence

### The Problem Phase 2 Left Open

Every queue is in-memory. If the JVM crashes, all messages in all queues are gone — including messages that were published but not yet consumed, and messages that were consumed but not yet acknowledged.

### How It Works

**Write-Ahead Log (WAL)** — before any state change is applied in memory, it is written to a log file on disk first. Every `publish()`, `consume()`, `acknowledge()`, and `nack()` appends one JSON entry to the queue's log file and flushes to disk immediately.

**Replay on startup** — when a queue is created with a log directory, it reads the existing log file and replays every entry to reconstruct in-memory state. Messages that were in-flight at crash time are requeued (treated as implicit NACKs).

**Log compaction** — after replay, the log is rewritten with only the surviving queued messages as PUBLISH entries. This prevents the log from growing unboundedly across restarts.

### Log File Format

One JSON entry per line, append-only:

```
{"op":"PUBLISH","msgId":"1","payload":"Order1","handle":"","retryCount":0,"ts":1700000001000}
{"op":"CONSUME","msgId":"1","payload":"","handle":"abc-123","retryCount":0,"ts":1700000002000}
{"op":"ACK","msgId":"","payload":"","handle":"abc-123","retryCount":0,"ts":1700000003000}
```

### Key Design Decisions

| Problem | Approach | Why |
|---------|----------|-----|
| Flush strategy | Flush after every write (fsync) | Zero message loss on crash — teaches the durability vs throughput trade-off viscerally |
| Log structure | One file per queue | Clean separation, easier replay, mirrors Kafka partition logs |
| In-flight on restart | Requeue (implicit NACK) | At-least-once — never lose a message, accept rare duplicates |
| JSON format | Hand-written serializer, no libraries | Forces understanding of the format; no external dependencies |
| Corrupted lines | Skip and warn | Partial writes at end of file are expected on crash — don't fail the whole replay |
| Backward compatibility | `logDirectory: null` disables persistence | All Phase 1 and 2 behaviour unchanged when no log directory configured |

---

## Phase 4 — Networking

### The Problem Phase 3 Left Open

Everything so far runs inside one JVM. A real queue is a *server* — producers and consumers are separate processes on separate machines. Phase 4 puts an HTTP API in front of the `QueueManager`.

### API

| Method | Path | Response |
|--------|------|----------|
| `GET` | `/queues` | `200` `{"queues":["orders"]}` |
| `POST` | `/queues/{name}` | `201` `{"name":"orders"}` — optional JSON config body |
| `DELETE` | `/queues/{name}` | `204`, or `404` if it never existed |
| `POST` | `/queues/{name}/messages` | `201` `{"messageId":"42"}` — body `{"payload":"..."}` |
| `GET` | `/queues/{name}/messages?waitSeconds=n` | `200` message + receipt handle, or `204` on timeout |
| `DELETE` | `/queues/{name}/messages/{handle}` | `204` — acknowledge |
| `POST` | `/queues/{name}/messages/{handle}/nack` | `204` — reject and requeue |

Errors come back as `{"error":"..."}` with `400` (malformed body), `404` (unknown queue, bad receipt handle, unknown path), `405` (wrong method), or `500`.

### Long Polling

`GET /queues/{name}/messages` doesn't return empty immediately. It holds the connection open for up to `waitSeconds` (default 5) and returns as soon as a message arrives — the same mechanism SQS long polling uses to avoid clients hammering the server with empty receives.

This is backed by a new `MessageQueue.consume(long timeoutMs)`, which uses `wait(remaining)` against a fixed deadline rather than a bare `wait(timeoutMs)`. The distinction matters: a spurious wakeup, or losing the race for a message to another consumer, would otherwise restart the full timeout on every pass through the loop.

### Key Design Decisions

| Problem | Approach | Why |
|---------|----------|-----|
| Server owns the QueueManager? | Passed in from outside | Tests can inspect queue state directly instead of asserting everything through HTTP |
| Lifecycle | Explicit `start()`, not auto-start in constructor | Constructing an object should not bind a socket |
| Test ports | Bind port `0`, read back `getPort()` | OS picks a free port — no conflicts between parallel runs |
| Blocking a Jetty thread while long polling | Allowed, but capped at 20s | Simplest correct approach; the cap stops slow clients from starving the thread pool |
| Reading the request body | `Content.Source.asString()` | A single `request.read()` returns only the chunk that happens to be available — not the whole body |
| JSON | Hand-written parser, now with full escaping | Phase 3's regex-splitting parser broke on commas and quotes in payloads; Phase 4 replaces it with a real character scanner |

### Phase 4.1 — One Parser, Not Two

Phase 4 fixed JSON escaping at the HTTP layer and left the WAL's own serializer untouched. That turned out to be the worst of both worlds: a payload like `Order #1, "urgent"` was accepted over HTTP, held correctly in memory, and then quietly destroyed on the way to disk.

Three failure modes, each pinned by a test before anything was changed:

| Payload | Replayed as | Cause |
|---------|-------------|-------|
| `Order #1, "urgent"` | `Order #1,` | Unescaped quote ended the field early |
| `line one\nline two` | split across two records | The log is line-delimited; a raw newline desynchronises everything after it |
| `","handle":"spoofed","retryCount":99,"x":"` | `handle=spoofed`, `retryCount=99` | A payload could forge its own log metadata |

Two further bugs surfaced during the fix: `fromJson()` never restored the `ts` field, so every replayed entry was re-stamped with the restart time; and `payload.isEmpty() ? null : payload` collapsed an empty payload into null, making it indistinguishable from "this field does not apply to this operation".

The fix was to stop having two parsers. `JsonUtils` moved into its own `com.javaqueue.json` package that both `core` and `server` depend on. `LogEntry` still hand-builds its object literal so `retryCount` and `ts` stay unquoted numbers — which keeps the on-disk format byte-compatible with logs written by Phase 3.

**The lesson:** the duplicate implementation *was* the bug. Two parsers for one format means one of them is always the stale one, and the format's invariants only hold where somebody remembered to enforce them.

### Phase 4.2 — The Dead Letter Deadlock

Found while designing Phase 5, in code that had been shipped since Phase 2 and passed every test.

`requeueOrDeadLetter()` published to the dead letter queue from *inside* `synchronized(this)` — holding lock A while acquiring lock B. That is safe only while every thread acquires the two locks in the same order, and two queues configured as each other's DLQ make that impossible:

```
Thread 1: nack on A → holds A, wants B
Thread 2: nack on B → holds B, wants A
```

Both hang forever. No timeout, no recovery. The test output showed it reached further than the two nacking threads:

```
scanner-cycle-a blocked on A held by dlq-cycle-a
dlq-cycle-a     blocked on B held by dlq-cycle-b
dlq-cycle-b     blocked on A held by dlq-cycle-a
```

The visibility scanner joins the cycle, so timeout recovery for that queue stops too — every in-flight message on it silently stops being redelivered.

The fix is to stop holding two locks at once rather than to order them. `requeueOrDeadLetter()` now *returns* the message that must leave, and the caller publishes it after releasing the monitor. Because the nested acquisition is gone entirely, cycle length is irrelevant — 2-queue, 3-queue, and self-cycle configurations are all verified.

The regression test uses `ThreadMXBean.findDeadlockedThreads()` rather than a bare timeout, so a future regression names the threads and monitors instead of hanging the build with no explanation.

**The lesson:** a lock protects *state*, not *operations*. Holding one across a call into another object's lock buys no atomicity that survives a crash — here it bought nothing at all, and cost a permanent hang.

### Try It

```bash
mvn compile exec:java

curl -X POST localhost:8080/queues/orders
curl -X POST localhost:8080/queues/orders/messages -d '{"payload":"Order #1, \"urgent\""}'
curl localhost:8080/queues/orders/messages
curl -X DELETE localhost:8080/queues/orders/messages/{receiptHandle}
```

---

## Getting Started

**Prerequisites**
- JDK 21+
- Maven 3.6+

**Build**
```bash
mvn compile
```

**Run** — starts the HTTP server on port 8080
```bash
mvn compile exec:java
```

**Test**
```bash
mvn test -Dsurefire.useFile=false
```

**Run with persistence enabled**
```java
QueueConfig config = new QueueConfig(30_000, 3, null, "/tmp/javaqueue-logs");
MessageQueue queue = manager.createQueue("orders", config);
```

---

## Test Results

```
Tests run: 129, Failures: 0, Errors: 0, Skipped: 0

├── MessageTest                    4 tests  — value object correctness, concurrent ID uniqueness
├── MessageQueueTest              12 tests  — blocking consume, timed consume, ACK, concurrency
├── QueueManagerTest              10 tests  — create, delete, config, DLQ wiring, scanner shutdown
├── DeliveryGuaranteesTest        10 tests  — timeout requeue, NACK, retry limit, DLQ, close()
├── LogEntryTest                  19 tests  — WAL record escaping, field spoofing, legacy format
├── WalTest                       10 tests  — read/write/compact, torn writes, punctuated payloads
├── PersistenceTest               11 tests  — survive restart, compaction, retry count preserved
├── JsonUtilsTest                 25 tests  — parser round-trips, escapes, malformed input rejection
├── QueueServerTest               20 tests  — full HTTP round-trip, long polling, status codes, routing
├── ConcurrentStressTest           3 tests  — 5.1M messages, sustained load, backlog draining
└── DeliveryGuaranteesStressTest   5 tests  — concurrent NACKs, scanner + consumers, DLQ cycles
```

Stress test results (5 producers, 5 consumers, 3 seconds):
```
Published: 5,093,389
Consumed:  5,093,389
```

---

## Concepts Covered

**Phase 1**
- `AtomicLong` and compare-and-swap (CAS)
- `synchronized`, intrinsic locks, and the Java Memory Model
- `wait()` / `notifyAll()` and why `while` not `if`
- Spurious wakeups and thread contention
- `ConcurrentHashMap.computeIfAbsent()` atomicity
- Competing consumers model vs pub/sub
- Why receipt handles are per-delivery, not per-message

**Phase 2**
- Daemon threads and clean shutdown with `interrupt()` + `join()`
- Background scanner pattern — separating the timer from the logic
- Why you never modify a `Map` while iterating it (`ConcurrentModificationException`)
- Retry state tracking across multiple requeues
- Lock independence — why publishing to a DLQ inside a `synchronized` block is safe
- Immutability as a correctness guarantee, even inside synchronized blocks

**Phase 3**
- Write-ahead log — the foundation of every durable storage system
- Why flush-every-write destroys throughput (and why Kafka batches)
- Log compaction — why it exists and what problem it solves
- Crash recovery — replaying a log to reconstruct state
- Why `notifyAll()` requires a monitor (`IllegalMonitorStateException`)
- Hand-written serialization — understanding the format you depend on

**Phase 4**
- Timed waiting — `wait(timeout)` against a deadline, and why a bare timeout is wrong
- Long polling as a latency/load trade-off, and what it costs in server threads
- Thread pool starvation — why an uncapped server-side wait is a denial-of-service on yourself
- Mapping queue semantics onto HTTP verbs and status codes (`204` for "nothing yet", not `200` with an empty body)
- Streaming request bodies — why one `read()` is not the whole body
- Writing a real character-scanning parser instead of splitting on delimiters
- Escaping as a correctness boundary — unescaped input in a structured format is an injection bug, not a formatting one
- Why a duplicated implementation of one format is itself the defect
- Evolving a persisted format without breaking files already on disk
- Lock ordering, and why an AB–BA cycle is a permanent hang rather than a slowdown
- Detecting deadlock programmatically with `ThreadMXBean.findDeadlockedThreads()`
- That a lock protects state, not operations — holding one across a call into another object's lock buys no crash-atomicity