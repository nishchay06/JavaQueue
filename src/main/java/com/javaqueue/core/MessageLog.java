package com.javaqueue.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.javaqueue.exception.OffsetOutOfRangeException;

/**
 * A retained, offset-indexed log — the Kafka model, beside the SQS-style
 * MessageQueue rather than replacing it.
 *
 * The defining difference is that <b>reading does not remove</b>. A queue's
 * consume() polls the message off; here a read only advances a cursor. One
 * copy of each record serves every consumer group, and records leave only when
 * retention trims them.
 *
 * <h2>What a log deliberately does not have</h2>
 *
 * No visibility timeout, no NACK, no dead letter queue, no in-flight tracking.
 * A log has exactly one progress mechanism — the committed offset — so none of
 * the Phase 2 delivery machinery applies. That is not a gap to be filled in
 * later; it is the difference between the two models.
 *
 * <h2>Two cursors per group</h2>
 *
 * <ul>
 * <li><b>position</b> — in-memory, advanced by poll, lost on restart</li>
 * <li><b>committed</b> — durable, advanced only by commit</li>
 * </ul>
 *
 * The gap between them is where at-most-once and at-least-once come from:
 * commit before processing and a crash loses records, commit after processing
 * and a crash reprocesses them. Keeping them as separate operations is what
 * makes that choice visible instead of hidden.
 *
 * <h2>Concurrency</h2>
 *
 * One intrinsic lock guards records, baseOffset, and the group table — the
 * same single-lock design as MessageQueue.
 */
public class MessageLog {

    public final String name;
    private final LogConfig config;

    // The retained records. Index 0 holds the record at baseOffset.
    //
    // Deliberate simplification: the whole log lives in heap. Real Kafka mmaps
    // segment files and never holds a full partition in memory. Fine for
    // understanding the model; not how you would store terabytes.
    private final List<Entry> records = new ArrayList<>();

    /** A record plus when it was appended, so age-based retention can work. */
    private record Entry(Message message, long appendedAtMs) {
    }

    // Offset of records.get(0). Advances as retention trims from the head, so
    // offsets stay monotonic for the life of the log and are never reused.
    private long baseOffset = 0;

    private final Map<String, GroupState> groups = new HashMap<>();

    private final Thread retentionThread;

    // Null when no log directory is configured — no persistence, same
    // convention as QueueConfig.logDirectory.
    private RecordStore recordStore;
    private OffsetStore offsetStore;

    /** A consumer group's two cursors. Guarded by the enclosing monitor. */
    private static final class GroupState {
        private long position;
        private long committed;

        GroupState(long start) {
            this.position = start;
            this.committed = start;
        }
    }

    public MessageLog(String name) {
        this(name, LogConfig.defaults());
    }

    public MessageLog(String name, LogConfig config) {
        this(name, config, 1000);
    }

    public MessageLog(String name, LogConfig config, long scanIntervalMs) {
        this.name = name;
        this.config = config;

        if (config.getLogDirectory() != null) {
            synchronized (this) {
                replay(RecordStore.read(config.getLogDirectory(), name));
            }
            try {
                this.recordStore = new RecordStore(config.getLogDirectory(), name);
                compactRecordFile();
            } catch (IOException e) {
                System.err.println("WARNING: Could not open log file for '" + name
                        + "': " + e.getMessage());
            }
        }

        RetentionScanner scanner = new RetentionScanner(this, scanIntervalMs);
        this.retentionThread = new Thread(scanner, "retention-" + name);
        this.retentionThread.setDaemon(true);
        this.retentionThread.start();
    }

    public String getName() {
        return name;
    }

    // ── Producing ─────────────────────────────────────────────────────────────

    /** Appends a record and returns the offset it was assigned. */
    public long append(Message message) {
        synchronized (this) {
            long offset = baseOffset + records.size();
            records.add(new Entry(message, System.currentTimeMillis()));

            if (recordStore != null) {
                recordStore.append(RecordStore.Entry.appended(offset, message));
            }

            // Size is enforced immediately so maxRecords is a hard bound
            // rather than one that holds only between scanner ticks. Age is
            // left to the scanner, since nothing about an append makes an
            // older record expire.
            trimToMaxRecords();

            notifyAll();
            return offset;
        }
    }

    // ── Observing ─────────────────────────────────────────────────────────────

    /** The oldest offset still retained. */
    public long beginOffset() {
        synchronized (this) {
            return baseOffset;
        }
    }

    /** The next offset that will be assigned — exclusive, not the last record. */
    public long endOffset() {
        synchronized (this) {
            return baseOffset + records.size();
        }
    }

    /**
     * How far behind the end a group is, measured from its <b>committed</b>
     * offset rather than its read position: records that have been read but
     * not committed are still outstanding work.
     *
     * This is the one number operators actually watch.
     */
    public long lag(String group) {
        synchronized (this) {
            return endOffset() - groupState(group).committed;
        }
    }

    // ── Consuming ─────────────────────────────────────────────────────────────

    /**
     * Reads up to maxRecords from the group's position and advances it.
     *
     * Does not touch the committed offset — that is the caller's decision, and
     * when they make it determines their delivery guarantee.
     */
    public LogRecords poll(String group, int maxRecords) {
        synchronized (this) {
            GroupState state = groupState(group);
            long start = resolveInRange(state.position);
            state.position = start;

            int from = (int) (start - baseOffset);
            if (from >= records.size() || maxRecords <= 0) {
                return LogRecords.empty(start);
            }

            int to = Math.min(from + maxRecords, records.size());

            // Copy before returning: the caller must never hold a view onto
            // internal state that retention could trim underneath them.
            List<Message> batch = records.subList(from, to).stream()
                    .map(Entry::message)
                    .collect(java.util.stream.Collectors.toList());
            long next = baseOffset + to;
            state.position = next;

            return new LogRecords(batch, start, next);
        }
    }

    /**
     * Records durable progress. Commit the {@code nextOffset} of the batch you
     * finished, not the offset of its last record — committing the latter
     * redelivers that record forever.
     *
     * Committing backwards is allowed: it is how you deliberately reprocess.
     */
    public void commit(String group, long offset) {
        synchronized (this) {
            if (offset < baseOffset || offset > endOffset()) {
                throw new OffsetOutOfRangeException(name, offset, baseOffset, endOffset());
            }
            groupState(group).committed = offset;

            if (offsetStore != null) {
                offsetStore.record(name, group, offset);
            }
        }
    }

    public long committed(String group) {
        synchronized (this) {
            return groupState(group).committed;
        }
    }

    /** Where the next poll will read from. In-memory only. */
    public long position(String group) {
        synchronized (this) {
            return resolveInRange(groupState(group).position);
        }
    }

    public void seek(String group, long offset) {
        synchronized (this) {
            groupState(group).position = resolveInRange(offset);
        }
    }

    public void seekToBeginning(String group) {
        synchronized (this) {
            groupState(group).position = baseOffset;
        }
    }

    public void seekToEnd(String group) {
        synchronized (this) {
            groupState(group).position = endOffset();
        }
    }

    /**
     * Drops the in-memory position back to the committed offset, exactly as a
     * consumer restart would. Everything read but not committed comes back.
     */
    public void resetPositionToCommitted(String group) {
        synchronized (this) {
            GroupState state = groupState(group);
            state.position = state.committed;
        }
    }

    /**
     * Applies the retention policy. Called by the scanner, and safe to call
     * directly in tests.
     *
     * Retention is deliberately blind to consumer progress: a slow group does
     * not hold records back. That is what makes offset-out-of-range reachable,
     * and it is exactly how Kafka behaves.
     */
    public void applyRetention() {
        synchronized (this) {
            trimToMaxRecords();
            trimExpired();
        }
    }

    public void close() {
        retentionThread.interrupt();
        try {
            retentionThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (recordStore != null) {
            try {
                recordStore.close();
            } catch (IOException e) {
                System.err.println("WARNING: Could not close log file for '" + name
                        + "': " + e.getMessage());
            }
        }
    }

    // ── Internals ─────────────────────────────────────────────────────────────

    // Caller MUST hold the monitor.
    private void trimToMaxRecords() {
        int max = config.getMaxRecords();
        if (max > 0 && records.size() > max) {
            dropOldest(records.size() - max);
        }
    }

    // Caller MUST hold the monitor.
    private void trimExpired() {
        long retentionMs = config.getRetentionMs();
        if (retentionMs <= 0) {
            return;
        }

        long cutoff = System.currentTimeMillis() - retentionMs;
        int expired = 0;
        while (expired < records.size() && records.get(expired).appendedAtMs() < cutoff) {
            expired++;
        }
        dropOldest(expired);
    }

    // Removes the oldest n records and advances baseOffset by the same amount,
    // so the offsets of everything remaining are unchanged. Offsets are never
    // renumbered and never reused.
    // Caller MUST hold the monitor.
    private void dropOldest(int count) {
        if (count <= 0) {
            return;
        }
        records.subList(0, count).clear();
        baseOffset += count;

        // Record the trim so a restart does not resurrect what retention
        // removed. One small line, rather than rewriting the whole file on
        // every trim.
        if (recordStore != null) {
            recordStore.append(RecordStore.Entry.trimmed(baseOffset));
        }
    }

    // Rebuilds records and baseOffset from the file. Caller MUST hold the
    // monitor. A TRIM sets the base offset and drops anything before it.
    private void replay(List<RecordStore.Entry> entries) {
        for (RecordStore.Entry entry : entries) {
            if (entry.append()) {
                records.add(new Entry(new Message(entry.messageId(), entry.payload()),
                        System.currentTimeMillis()));
                continue;
            }
            long newBase = entry.offset();
            int drop = (int) Math.min(Math.max(newBase - baseOffset, 0), records.size());
            records.subList(0, drop).clear();
            baseOffset = newBase;
        }
    }

    // Collapses the file to current state after replay, so trims and rewrites
    // do not accumulate across restarts.
    private void compactRecordFile() {
        synchronized (this) {
            List<RecordStore.Entry> survivors = new ArrayList<>(records.size());
            for (int i = 0; i < records.size(); i++) {
                survivors.add(RecordStore.Entry.appended(baseOffset + i, records.get(i).message()));
            }
            recordStore.compact(baseOffset, survivors);
        }
    }

    // Wires up durable offsets and restores what a group had committed.
    // Called by LogManager immediately after construction.
    void attachOffsetStore(OffsetStore store) {
        synchronized (this) {
            this.offsetStore = store;
        }
    }

    void restoreCommitted(String group, long offset) {
        synchronized (this) {
            GroupState state = groupState(group);
            state.committed = offset;
            state.position = offset;
        }
    }

    // Caller MUST hold the monitor. Groups are created on first use, starting
    // wherever the reset policy says — no explicit "create group" call, same
    // as Kafka.
    private GroupState groupState(String group) {
        return groups.computeIfAbsent(group, g -> new GroupState(defaultStartOffset()));
    }

    private long defaultStartOffset() {
        return config.getResetPolicy() == OffsetResetPolicy.LATEST
                ? endOffset()
                : baseOffset;
    }

    /**
     * Brings an offset back into the retained range, applying the reset policy
     * if it has fallen off the front — Kafka's OffsetOutOfRange handling.
     *
     * Caller MUST hold the monitor.
     */
    private long resolveInRange(long offset) {
        long end = baseOffset + records.size();

        if (offset >= baseOffset && offset <= end) {
            return offset;
        }

        // Ahead of the end is always an error: there is nothing there to read,
        // and silently rewinding would hide the caller's mistake.
        if (offset > end) {
            throw new OffsetOutOfRangeException(name, offset, baseOffset, end);
        }

        return switch (config.getResetPolicy()) {
            case EARLIEST -> baseOffset;
            case LATEST -> end;
            case ERROR -> throw new OffsetOutOfRangeException(name, offset, baseOffset, end);
        };
    }
}
