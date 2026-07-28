package com.javaqueue.core;

import java.util.Map;

import com.javaqueue.json.JsonUtils;

public class LogEntry {

    private final LogOperation op;
    private final String msgId;
    private final String payload;
    private final String handle;
    private final int retryCount;
    private final long timestamp;

    private LogEntry(LogOperation op, String msgId, String payload,
            String handle, int retryCount) {
        this(op, msgId, payload, handle, retryCount, System.currentTimeMillis());
    }

    // Replay constructor — keeps the timestamp the entry was written with
    // rather than stamping it with the time of the restart.
    private LogEntry(LogOperation op, String msgId, String payload,
            String handle, int retryCount, long timestamp) {
        this.op = op;
        this.msgId = msgId;
        this.payload = payload;
        this.handle = handle;
        this.retryCount = retryCount;
        this.timestamp = timestamp;
    }

    public static LogEntry publish(Message message) {
        return new LogEntry(LogOperation.PUBLISH,
                message.getId(), message.getPayload(), null, 0);
    }

    public static LogEntry consume(Message message, String handle, int retryCount) {
        return new LogEntry(LogOperation.CONSUME,
                message.getId(), null, handle, retryCount);
    }

    public static LogEntry ack(String handle) {
        return new LogEntry(LogOperation.ACK, null, null, handle, 0);
    }

    public static LogEntry nack(String handle) {
        return new LogEntry(LogOperation.NACK, null, null, handle, 0);
    }

    // The object literal is built by hand rather than via JsonUtils.toJson so
    // that retryCount and ts stay unquoted numbers — that keeps the on-disk
    // format identical to what Phase 3 wrote, so existing logs still replay.
    // Every string field goes through JsonUtils.escape().
    public String toJson() {
        return "{\"op\":\"" + op + "\""
                + ",\"msgId\":" + quoted(msgId)
                + ",\"payload\":" + quoted(payload)
                + ",\"handle\":" + quoted(handle)
                + ",\"retryCount\":" + retryCount
                + ",\"ts\":" + timestamp
                + "}";
    }

    // A field that does not apply to this operation is written as JSON null,
    // which keeps it distinguishable from a genuinely empty payload.
    private static String quoted(String value) {
        return value == null ? "null" : "\"" + JsonUtils.escape(value) + "\"";
    }

    public static LogEntry fromJson(String line) {
        Map<String, String> fields = JsonUtils.fromJson(line);

        String op = fields.get("op");
        if (op == null) {
            throw new IllegalArgumentException("WAL entry has no 'op' field: " + line);
        }

        return new LogEntry(
                LogOperation.valueOf(op),
                fields.get("msgId"),
                fields.get("payload"),
                fields.get("handle"),
                intField(fields, "retryCount"),
                longField(fields, "ts"));
    }

    private static int intField(Map<String, String> fields, String key) {
        String value = fields.get(key);
        return value == null ? 0 : Integer.parseInt(value);
    }

    private static long longField(Map<String, String> fields, String key) {
        String value = fields.get(key);
        return value == null ? System.currentTimeMillis() : Long.parseLong(value);
    }

    public LogOperation getOp() {
        return op;
    }

    public String getMsgId() {
        return msgId;
    }

    public String getPayload() {
        return payload;
    }

    public String getHandle() {
        return handle;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public long getTimestamp() {
        return timestamp;
    }
}
