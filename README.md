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
    │           │   ├── WalReader.java         # Replays log on startup
    │           │   ├── Topic.java             # Named subscriber list; fan-out publish
    │           │   ├── TopicManager.java      # Registry of topics; replays topic config
    │           │   ├── TopicLog.java          # Persists SUBSCRIBE / UNSUBSCRIBE records
    │           │   ├── TopicOperation.java    # Enum: CREATE_TOPIC, DELETE_TOPIC, SUBSCRIBE, UNSUBSCRIBE
    │           │   ├── MessageLog.java        # Retained offset-indexed log — the Kafka model
    │           │   ├── LogManager.java        # Registry of logs; owns the shared offset store
    │           │   ├── LogConfig.java         # retentionMs, maxRecords, resetPolicy, log dir
    │           │   ├── LogRecords.java        # A poll result: records + the offsets they span
    │           │   ├── OffsetResetPolicy.java # Enum: EARLIEST, LATEST, ERROR
    │           │   ├── RecordStore.java       # The log file — APPEND and TRIM records
    │           │   ├── OffsetStore.java       # Shared _offsets.log, mirroring __consumer_offsets
    │           │   └── RetentionScanner.java  # Daemon thread — trims by age
    │           ├── server/
    │           │   ├── QueueServer.java       # Jetty lifecycle — start, stop, bound port
    │           │   └── QueueHandler.java      # Routes HTTP requests onto the QueueManager
    │           ├── json/
    │           │   └── JsonUtils.java         # Hand-written flat JSON — shared by the WAL and the HTTP layer
    │           └── exception/
    │               ├── QueueNotFoundException.java
    │               ├── TopicNotFoundException.java
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
                │   ├── PersistenceTest.java
                │   ├── TopicTest.java
                │   ├── FanOutTest.java
                │   ├── TopicPersistenceTest.java
                │   ├── AsyncConsumeTest.java
                │   ├── DefaultLogDirectoryTest.java
                │   ├── MessageLogTest.java
                │   ├── ConsumerGroupTest.java
                │   ├── RetentionTest.java
                │   └── LogPersistenceTest.java
                ├── json/
                │   └── JsonUtilsTest.java
                ├── server/
                │   ├── QueueServerTest.java
                │   ├── TopicServerTest.java
                │   └── LogServerTest.java
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
| 5 | Consumer Groups | ✅ Complete | SNS-style fan-out — a topic delivers to N subscriber queues |
| 5.1 | Async Long Polling | ✅ Complete | Waiting costs a scheduled task, not a thread; durable-by-default log directory |
| 6 | Retained Log | ✅ Complete | Kafka-style log — one copy, N cursors, retention, offset commit |
| 7 | Partitions | ⏳ Planned | Key-based routing, partition assignment, rebalancing |

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
| Blocking a Jetty thread while long polling | Originally allowed, capped at 20s. **Replaced in Phase 5.1** — see below | Simplest correct approach to start with; the cap existed only to stop slow clients starving the pool |
| Reading the request body | `Content.Source.asString()` | A single `request.read()` returns only the chunk that happens to be available — not the whole body |
| JSON | Hand-written parser, now with full escaping | Phase 3's regex-splitting parser broke on commas and quotes in payloads; Phase 4 replaces it with a real character scanner |

### Try It

```bash
mvn compile exec:java

curl -X POST localhost:8080/queues/orders
curl -X POST localhost:8080/queues/orders/messages -d '{"payload":"Order #1, \"urgent\""}'
curl localhost:8080/queues/orders/messages
curl -X DELETE localhost:8080/queues/orders/messages/{receiptHandle}
```

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

---

## Phase 5 — Consumer Groups via Fan-Out

### The Problem Phase 4 Left Open

A message goes to exactly one consumer. If billing *and* analytics both need to see every order, there is no way to express that — a second consumer on the same queue competes for messages rather than receiving its own copy.

### How It Works

A **topic** is a named subscriber list. Publishing to it delivers the message into every subscriber's queue, and each subscriber consumes independently from there. This is SNS → SQS.

**A consumer group is a subscriber queue.** Several consumers pulling from one subscriber queue compete for its messages — that is the group, and it already worked from Phase 1. Two subscriber queues on the same topic are two groups, each receiving everything.

```
                          ┌──► billing queue    ──► billing consumers   (group 1)
publish ──► orders-events ─┤
                          └──► analytics queue  ──► analytics consumers (group 2)
```

Nothing in `MessageQueue` changed. Every Phase 2 guarantee and Phase 3 persistence behaviour carries over untouched, because a subscriber queue *is* an ordinary queue.

### Key Design Decisions

| Problem | Approach | Why |
|---------|----------|-----|
| Copy the message per subscriber? | Share the one immutable instance | One payload in memory, and one logical id to correlate across groups. This is where Phase 1's immutability decision finally pays |
| What does a subscription record? | The queue **name**, not the instance | Holding the instance pins one object — delete and recreate a queue and the topic would publish into the dead one, where messages are unreachable |
| Queue ownership | Topics never own queues | Keeps the relationship many-to-many: a queue can subscribe to several topics, and unsubscribing detaches routing without touching the queue |
| Fan-out atomicity | Independent, best-effort per subscriber | Matches SNS. The same message may be acknowledged on one subscriber and dead-lettered on another |
| Subscribing an unregistered queue | Rejected at subscribe time | The alternative is a subscription that silently never delivers — fail where the mistake is |
| Lock discipline | Resolve subscribers into a snapshot, then publish | Same rule Phase 4.2 established: never hold one queue's monitor while publishing to another |
| Subscription durability | Logged to `_topics.log` | A topic whose subscriber list was lost still accepts publishes and delivers to nobody — a failure that looks perfectly healthy |

### The Limitation That Defines It

**A topic keeps no history.** A queue that subscribes later sees only messages published after it joined — there is nothing to backfill from. That is not an oversight; it is what separates fan-out from a log, and it is exactly what Phase 6 exists to fix.

### API

| Method | Path | Response |
|---|---|---|
| `GET` | `/topics` | `200` `{"topics":["orders-events"]}` |
| `POST` | `/topics/{name}` | `201` `{"name":"orders-events"}` |
| `DELETE` | `/topics/{name}` | `204`, `404` if absent |
| `GET` | `/topics/{name}/subscriptions` | `200` `{"subscribers":["billing"]}` |
| `POST` | `/topics/{name}/subscriptions/{queue}` | `201`, `404` if topic or queue absent |
| `DELETE` | `/topics/{name}/subscriptions/{queue}` | `204` |
| `POST` | `/topics/{name}/messages` | `201` `{"messageId":"42","delivered":"2"}` |

There is deliberately **no** `GET /topics/{name}/messages` — it returns `405`, not `404`. The path exists; the operation does not. You consume from a subscriber queue, never from the topic. Making that impossible in the API is the clearest way to say that a topic routes rather than stores.

---

## Phase 5.1 — Long Polling Without a Thread, and Durable by Default

Two things Phase 4 and 5 left half-finished.

### Long polling used to cost a thread per waiting client

`handleConsume` called the blocking `consume(timeout)`, which parks a Jetty pool thread for the entire wait. Forty idle clients meant forty parked threads, and the 20-second cap on `waitSeconds` existed purely to stop a handful of slow clients from starving the pool. The cap was treating the symptom.

Jetty 12's `handle()` returning `true` means *"I will complete the callback"*, not *"I have completed it"* — so the response can be finished later, from another thread entirely. The request now registers interest and returns immediately:

```java
Waiter waiter = queue.consumeAsync(receipt -> { ... respond 200 ... });
timeouts.schedule(() -> { if (waiter.cancel()) ...respond 204... }, waitSeconds, SECONDS);
```

`MessageQueue` gained `consumeAsync(Consumer<Receipt>)`, which either delivers inline if a message is available or queues a waiter. `publish` — along with `nack` and the visibility-timeout requeue — hands the message straight to a waiting consumer rather than enqueuing it. Whichever of delivery and timeout arrives first wins an `AtomicBoolean` and owns the response.

Waiting now costs a scheduled task on **one** shared daemon thread, so the cap rose from 20 seconds to 300.

**The test that proves it:** 40 simultaneous 2-second long polls against a server with a 6-thread pool. Measured on the blocking implementation it took **16,192ms** — roughly seven batches deep. On the async one, under 6,000ms. A test that passes both before and after the change would have proved nothing, so this one was run against both.

**The delicate part:** `MessageQueue` hands a claimed message to its waiter only *after* releasing the monitor, because the handler is caller code that could do anything — including publishing to another queue. That is Phase 4.2's dead-letter rule generalised: never run someone else's code while holding this lock.

### Durability was per-queue, so it was easy to lose

A queue created without an explicit `logDirectory` silently lost its messages on restart. Over HTTP that meant every create body needed one, and an **auto-created dead letter queue could never be durable at all** — which defeats the point of a DLQ, since it exists to hold messages for later inspection.

`QueueManager` now takes an optional default applying to any queue created without its own, DLQs included. An explicit `logDirectory` still wins. No default means no persistence, so nothing changed for existing callers.

```bash
mvn compile exec:java -Dexec.args="--log-dir=/var/lib/javaqueue"
# Persistence: /var/lib/javaqueue
```

Note the asymmetry this exposes: queue *contents* survive a restart, but the *registry* does not. Topics persist their subscriber lists; queues do not persist their own existence, so the application must recreate them on startup. Worth closing at some point.

### Try It

```bash
curl -X POST localhost:8080/topics/orders-events
curl -X POST localhost:8080/queues/billing
curl -X POST localhost:8080/queues/analytics
curl -X POST localhost:8080/topics/orders-events/subscriptions/billing
curl -X POST localhost:8080/topics/orders-events/subscriptions/analytics

curl -X POST localhost:8080/topics/orders-events/messages -d '{"payload":"Order #1"}'
# {"messageId":"0","delivered":"2"}

curl localhost:8080/queues/billing/messages     # same messageId,
curl localhost:8080/queues/analytics/messages   # different receiptHandle
```

---

## Phase 6 — The Retained Log

### The Problem Phase 5 Left Open

Fan-out gets you multiple consumer groups by *duplicating* each message into N queues. A group that joins later gets nothing, because the topic keeps no history — and nobody can ever re-read anything.

A log fixes both by not deleting on read.

### Destructive vs Non-Destructive Read

| | `MessageQueue` (Phases 1–5) | `MessageLog` (Phase 6) |
|---|---|---|
| Read | `messages.poll()` — removal **is** consumption | `records.get(offset)` — read mutates nothing |
| Deletion | On acknowledge | On retention policy only |
| Multiple groups | Duplicate the message per group | One copy, one cursor per group |
| Progress | A receipt handle per message | One integer per group |
| Replay | Impossible | Seek to any retained offset |

Every other difference follows from that first row. `MessageLog` sits **beside** `MessageQueue`, not replacing it — having both models in one codebase is the point of having built fan-out first.

### What a Log Deliberately Does Not Have

No visibility timeout. No NACK. No dead letter queue. No in-flight tracking.

A log has exactly one progress mechanism — the committed offset — so none of the Phase 2 machinery applies. This is stated in the class javadoc rather than left to be discovered, because it is not a gap to fill in later. It is the difference between the two models.

A consequence worth feeling: a poison record blocks its group until somebody skips past it. That is a real Kafka operational problem, and it exists precisely because there is no DLQ to divert it to.

### The Central Idea: Two Cursors

Each group has a **position** (in-memory, advanced by `poll`, lost on restart) and a **committed** offset (durable, moved only by `commit`). The distance between them is the entirety of delivery semantics:

| Commit timing | Crash behaviour | Guarantee |
|---|---|---|
| Commit before processing | Records never seen again — lost | At-most-once |
| Commit after processing | Records reprocessed | At-least-once |

Keeping them as separate operations is what makes that choice visible instead of hidden, and two tests in `ConsumerGroupTest` pin both halves. The second one **asserts the data loss** — demonstrating the failure is the lesson, not something to design around.

There is a related off-by-one the API tries to prevent: commit `nextOffset`, not the offset of the last record you read. Committing the latter redelivers that record forever, and there is a test for it.

### Retention, and Losing Data on Purpose

Retention is the only thing that removes records — by count or by age, via a `RetentionScanner` daemon mirroring `VisibilityScanner`.

It is deliberately **blind to consumer progress**: a slow group does not hold records back. That is exactly what makes offset-out-of-range reachable, and all three responses are implemented:

| Policy | Behaviour |
|---|---|
| `EARLIEST` | Rewind to the oldest retained record — reprocess what is left |
| `LATEST` | Jump to the end — skip the gap and accept the loss |
| `ERROR` | Refuse, and make the operator decide |

A slow consumer group silently losing data is one of the genuine hazards of running Kafka. Making all three reachable and testing each is most of the value of implementing retention at all.

Trimming advances `baseOffset` rather than renumbering, so an offset always means the same record for the life of the log and is never reused.

### Persistence: The File *Is* The Log

Phase 3's WAL was a side-record of queue state, replayed to rebuild it. Here the file is not a record of the data — it **is** the data, and replay is simply reading it back.

That is the same realisation from the other direction: **a Kafka topic is a write-ahead log that nobody deletes from.** Phase 3 built a WAL to make a queue durable; Phase 6 notices that if you stop deleting, you have Kafka.

Committed offsets live in a shared `_offsets.log`, mirroring Kafka's `__consumer_offsets` — which is itself just another log.

### API

| Method | Path | Response |
|---|---|---|
| `GET` | `/logs` | `200` `{"logs":["events"]}` |
| `POST` | `/logs/{name}` | `201` — optional `retentionMs`, `maxRecords`, `resetPolicy` |
| `DELETE` | `/logs/{name}` | `204` — takes the log's committed offsets with it |
| `POST` | `/logs/{name}/records` | `201` `{"offset":"42"}` |
| `GET` | `/logs/{name}/records?group=g&max=n` | `200` records + offsets, `204` if nothing new |
| `POST` | `/logs/{name}/groups/{g}/commit` | `204` — body `{"offset":42}` |
| `GET` | `/logs/{name}/groups/{g}` | `200` committed, beginOffset, endOffset, **lag** |
| `POST` | `/logs/{name}/groups/{g}/seek` | `204` — `{"offset":n}` or `{"position":"earliest"}` |

Note what is **absent**: no acknowledge, no receipt handle. Progress is one integer, committed explicitly. The shape of the API is itself the lesson.

### Try It

```bash
curl -X POST localhost:8080/logs/events
curl -X POST localhost:8080/logs/events/records -d '{"payload":"e1"}'
curl -X POST localhost:8080/logs/events/records -d '{"payload":"e2"}'

curl 'localhost:8080/logs/events/records?group=billing'    # both records
curl 'localhost:8080/logs/events/records?group=analytics'  # the SAME records

curl localhost:8080/logs/events/groups/billing             # lag: 2
curl -X POST localhost:8080/logs/events/groups/billing/commit -d '{"offset":2}'
curl localhost:8080/logs/events/groups/billing             # lag: 0

# rewind and read it all again
curl -X POST localhost:8080/logs/events/groups/billing/seek -d '{"position":"earliest"}'
```

### Known Limitation

A poll response contains an array of records, and `JsonUtils` is a flat parser that rejects nesting by design. This is the first place in the project where the hand-written parser has met its limit — a log batch is inherently a list. The server writes the nested response correctly; the parser cannot read it back. Logged as tech debt rather than papered over.

---

## The Dashboard

`http://localhost:8080/` serves an operator view of everything the server is doing — the three models side by side, refreshing once a second.

- **Queues** — depth, in-flight, parked long-poll waiters, configured DLQ
- **Topics** — subscribers per topic, and a visible warning when a topic has none, since publishes then reach nobody
- **Logs** — retained offset range, retention policy, and **per-group lag as a bar** next to committed and read position

The interesting behaviours here are all visual over time, which is why they are worth a page rather than a log line: lag climbing and dropping the moment a group commits, one publish landing in two subscriber queues at once, `beginOffset` marching forward as retention trims, a group falling off the back of the log and being reset by its policy.

Buttons publish, consume, append, commit and rewind, so the whole system is drivable without a terminal. The activity feed shows every request and response, including failures.

It is served by the same Jetty as the API, so the page is same-origin and needs no cross-origin arrangement. Everything is inlined — no CDN, no external stylesheet — matching the project's no-dependencies philosophy, and it works offline.

`GET /stats` returns the whole snapshot in one call. That is deliberate: a request per resource would mean N+1 round trips and a page rendering torn state, with queues from one instant beside logs from another.

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

# with persistence and a custom port
mvn compile exec:java -Dexec.args="--log-dir=/var/lib/javaqueue --port=9090"
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
Tests run: 306, Failures: 0, Errors: 0, Skipped: 0

├── MessageTest                    4 tests  — value object correctness, concurrent ID uniqueness
├── MessageQueueTest              12 tests  — blocking consume, timed consume, ACK, concurrency
├── QueueManagerTest              10 tests  — create, delete, config, DLQ wiring, scanner shutdown
├── DeliveryGuaranteesTest        10 tests  — timeout requeue, NACK, retry limit, DLQ, close()
├── LogEntryTest                  19 tests  — WAL record escaping, field spoofing, legacy format
├── WalTest                       10 tests  — read/write/compact, torn writes, punctuated payloads
├── PersistenceTest               11 tests  — survive restart, compaction, retry count preserved
├── JsonUtilsTest                 25 tests  — parser round-trips, escapes, malformed input rejection
├── TopicTest                     17 tests  — registry, subscription lifecycle, ownership boundary
├── FanOutTest                    18 tests  — delivery to N groups, no backfill, DLQ independence
├── TopicPersistenceTest          12 tests  — subscriptions survive restart, compaction, torn writes
├── AsyncConsumeTest              14 tests  — callback consume, cancellation, publish/cancel races
├── DefaultLogDirectoryTest        6 tests  — server-wide persistence default, DLQ inheritance
├── MessageLogTest                21 tests  — append, offsets, non-destructive read, seek, lag
├── ConsumerGroupTest             14 tests  — position vs committed, at-least-once vs at-most-once
├── RetentionTest                 13 tests  — trimming, baseOffset advance, all three reset policies
├── LogPersistenceTest            17 tests  — records and offsets survive restart, trims do not undo
├── QueueServerTest               22 tests  — HTTP round-trip, long polling under a starved thread pool
├── TopicServerTest               20 tests  — topic endpoints, fan-out over HTTP, the 405 asymmetry
├── LogServerTest                 23 tests  — log endpoints, commit and lag, absence of acknowledge
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

**Phase 5**
- Fan-out versus a shared log — two ways to build consumer groups, and what each one cannot do
- Why immutability lets N consumers share one object instead of N copies
- Binding by name rather than by reference, and the stale-reference bug that motivates it
- Persisting configuration, not just data — a lost subscriber list fails silently while looking healthy
- Modelling an operation's absence in the API (405, not 404) as a design statement

**Phase 5.1**
- Asynchronous request completion — finishing an HTTP response from a thread that is not the request thread
- Why a cap on wait time was treating the symptom, and what fixing the cause allows
- Writing a test that fails on the old implementation, because one that passes on both proves nothing
- Handing work to a callback outside the lock — the dead-letter rule generalised to all caller code
- Defaults as a durability strategy: the safe thing should not require remembering

**Phase 6**
- Destructive versus non-destructive read, and why every other difference follows from it
- Position versus committed offset — where at-most-once and at-least-once actually come from
- Why commit-the-next-offset, not the last one you read
- Retention as the only deletion, and deliberately blind to consumer progress
- Offset out of range, and that a reset policy is a choice about acceptable data loss
- That a Kafka topic is simply a write-ahead log nobody deletes from
- Recognising when a format (flat JSON) has outgrown its parser