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
    │           │   ├── QueueConfig.java       # Visibility timeout, max retries, DLQ name
    │           │   ├── InFlightEntry.java     # Tracks message + timestamp + retry count
    │           │   └── VisibilityScanner.java # Daemon thread — requeues timed-out messages
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
                │   └── DeliveryGuaranteesTest.java
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
| 3 | Persistence | ⏳ Planned | Write-ahead log — messages survive JVM restart |
| 4 | Networking | ⏳ Planned | HTTP API so external processes can connect |
| 5 | Consumer Groups | ⏳ Planned | Kafka-style groups with per-group offsets |

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

## Getting Started

**Prerequisites**
- JDK 21+
- Maven 3.6+

**Build**
```bash
mvn compile
```

**Run**
```bash
mvn compile exec:java -Dexec.mainClass="com.javaqueue.Main"
```

**Test**
```bash
mvn test -Dsurefire.useFile=false
```

---

## Test Results

```
Tests run: 37, Failures: 0, Errors: 0, Skipped: 0

├── MessageTest                    4 tests  — value object correctness, concurrent ID uniqueness
├── MessageQueueTest               6 tests  — blocking consume, ACK, concurrent producers/consumers
├── QueueManagerTest              10 tests  — create, delete, config, DLQ wiring, scanner shutdown
├── DeliveryGuaranteesTest        10 tests  — timeout requeue, NACK, retry limit, DLQ, close()
├── ConcurrentStressTest           3 tests  — 4M+ messages, sustained load, backlog draining
└── DeliveryGuaranteesStressTest   4 tests  — concurrent NACKs, scanner + consumers, DLQ under load
```

Stress test results (5 producers, 5 consumers, 3 seconds, visibility timeout enabled):
```
Published: 2,438,483
Consumed:  2,438,483
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