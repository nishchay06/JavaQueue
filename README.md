# JavaQueue

A message queue built from scratch in Java, designed as a deep-learning project to understand the internals of distributed messaging systems like Amazon SQS, RabbitMQ, and Apache Kafka.

Rather than wrapping existing libraries, this project works directly with raw Java concurrency primitives — `synchronized`, `wait()`/`notifyAll()`, `AtomicLong`, and `ConcurrentHashMap` — to understand what production queue systems are actually doing under the hood.

---

## Why Build This?

Using SQS at work gives you the interface of a message queue, not the internals. This project answers the questions that production use leaves open:

- How does a queue safely handle multiple threads producing and consuming simultaneously?
- What does "blocking" actually mean at the thread level, and how is it implemented?
- How do delivery guarantees like at-least-once work mechanically?
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
    │           │   ├── Message.java          # Immutable value object with atomic ID generation
    │           │   ├── Receipt.java          # Unique per delivery, used to ACK
    │           │   ├── MessageQueue.java     # Core queue — all concurrency lives here
    │           │   └── QueueManager.java     # Thread-safe registry of named queues
    │           └── exception/
    │               ├── QueueNotFoundException.java
    │               └── InvalidReceiptException.java
    └── test/
        └── java/
            └── com/javaqueue/
                ├── core/
                │   ├── MessageTest.java
                │   ├── MessageQueueTest.java
                │   └── QueueManagerTest.java
                └── concurrent/
                    └── ConcurrentStressTest.java
```

---

## Phase Roadmap

This project is built incrementally. Each phase adds one meaningful layer of complexity.

| Phase | Name | Status | Key Addition |
|-------|------|--------|-------------|
| 1 | In-Memory Core | ✅ Complete | Named queues, publish, blocking consume, ACK |
| 2 | Delivery Guarantees | 🔜 Next | Visibility timeout, NACK, retry limit, dead letter queue |
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
Tests run: 20, Failures: 0, Errors: 0, Skipped: 0

├── MessageTest          4 tests  — value object correctness, concurrent ID uniqueness
├── MessageQueueTest     6 tests  — blocking consume, ACK, concurrent producers/consumers
├── QueueManagerTest     7 tests  — create, delete, list, concurrent creation atomicity
└── ConcurrentStressTest 3 tests  — 4.7M messages under sustained load, backlog draining
```

Stress test result on a MacBook (20 producers, 20 consumers, 5 seconds):
```
Published: 4,660,349
Consumed:  4,660,349
```

---

## Concepts Covered

- `AtomicLong` and compare-and-swap (CAS)
- `synchronized`, intrinsic locks, and the Java Memory Model
- `wait()` / `notifyAll()` and why `while` not `if`
- Spurious wakeups and lost wakeups
- Thread contention vs race conditions
- `ConcurrentHashMap.computeIfAbsent()` atomicity
- Competing consumers model vs pub/sub
- Why receipt handles are per-delivery, not per-message
