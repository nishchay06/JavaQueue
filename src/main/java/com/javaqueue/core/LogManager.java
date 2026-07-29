package com.javaqueue.core;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.javaqueue.exception.LogNotFoundException;

/**
 * Registry of named logs — the same shape as QueueManager, including the
 * server-wide default log directory added in Phase 5.1.
 *
 * It also owns the shared offset store, because committed offsets span every
 * log and belong to the deployment rather than to any single one.
 */
public class LogManager {

    private final ConcurrentHashMap<String, MessageLog> logs = new ConcurrentHashMap<>();
    private final String defaultLogDirectory;

    // Null when there is no log directory. Guarded by this object's monitor
    // together with restoredOffsets.
    private OffsetStore offsetStore;
    private final Map<OffsetStore.Key, Long> restoredOffsets = new LinkedHashMap<>();

    public LogManager() {
        this(null);
    }

    public LogManager(String defaultLogDirectory) {
        this.defaultLogDirectory = defaultLogDirectory;

        if (defaultLogDirectory != null) {
            restoredOffsets.putAll(OffsetStore.read(defaultLogDirectory));
            try {
                offsetStore = new OffsetStore(defaultLogDirectory);
                offsetStore.compact(restoredOffsets);
            } catch (IOException e) {
                System.err.println("WARNING: Could not open offset store: " + e.getMessage());
            }
        }
    }

    public MessageLog createLog(String name) {
        return createLog(name, LogConfig.defaults());
    }

    public MessageLog createLog(String name, LogConfig config) {
        LogConfig effective = applyDefaultLogDirectory(config);

        return logs.computeIfAbsent(name, n -> {
            MessageLog log = new MessageLog(n, effective);
            log.attachOffsetStore(offsetStore);
            restoreOffsetsFor(n, log);
            return log;
        });
    }

    public MessageLog getLog(String name) {
        MessageLog log = logs.get(name);
        if (log == null) {
            throw new LogNotFoundException(name);
        }
        return log;
    }

    /**
     * Removes the log, its file, and every group's progress on it.
     *
     * Unlike Phase 5, where a topic's subscriber queues outlive it, a
     * committed offset has no meaning without the log it points into.
     */
    public void deleteLog(String name) {
        MessageLog log = logs.remove(name);
        if (log == null) {
            return;
        }
        log.close();

        if (defaultLogDirectory != null) {
            RecordStore.delete(defaultLogDirectory, name);
            synchronized (this) {
                restoredOffsets.keySet().removeIf(key -> key.log().equals(name));
                if (offsetStore != null) {
                    offsetStore.compact(restoredOffsets);
                }
            }
        }
    }

    public Set<String> listLogs() {
        return new LinkedHashSet<>(logs.keySet());
    }

    public void close() {
        logs.values().forEach(MessageLog::close);
        if (offsetStore != null) {
            try {
                offsetStore.close();
            } catch (IOException e) {
                System.err.println("WARNING: Could not close offset store: " + e.getMessage());
            }
        }
    }

    private void restoreOffsetsFor(String name, MessageLog log) {
        synchronized (this) {
            restoredOffsets.forEach((key, offset) -> {
                if (key.log().equals(name)) {
                    log.restoreCommitted(key.group(), offset);
                }
            });
        }
    }

    // An explicit logDirectory is a deliberate override and always wins.
    private LogConfig applyDefaultLogDirectory(LogConfig config) {
        if (defaultLogDirectory == null || config.getLogDirectory() != null) {
            return config;
        }
        return new LogConfig(config.getRetentionMs(), config.getMaxRecords(),
                config.getResetPolicy(), defaultLogDirectory);
    }
}
