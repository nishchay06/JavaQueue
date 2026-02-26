package com.javaqueue.core;

public class LogEntry {

    private final LogOperation op;
    private final String msgId;
    private final String payload;
    private final String handle;
    private final int retryCount;
    private final long timestamp;

    private LogEntry(LogOperation op, String msgId, String payload,
            String handle, int retryCount) {
        this.op = op;
        this.msgId = msgId;
        this.payload = payload;
        this.handle = handle;
        this.retryCount = retryCount;
        this.timestamp = System.currentTimeMillis();
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

    public String toJson() {
        return String.format(
                "{\"op\":\"%s\",\"msgId\":\"%s\",\"payload\":\"%s\",\"handle\":\"%s\",\"retryCount\":%d,\"ts\":%d}",
                op,
                msgId != null ? msgId : "",
                payload != null ? payload : "",
                handle != null ? handle : "",
                retryCount,
                timestamp);
    }

    // Extracts value for a given key from a flat JSON string.
    // Works for our controlled format — not a general JSON parser.
    private static String extractField(String json, String key) {
        String search = "\"" + key + "\":";
        int start = json.indexOf(search);
        if (start == -1)
            return "";
        start += search.length();

        if (json.charAt(start) == '"') {
            // String value
            start++;
            int end = json.indexOf('"', start);
            return json.substring(start, end);
        } else {
            // Numeric value
            int end = json.indexOf(',', start);
            if (end == -1)
                end = json.indexOf('}', start);
            return json.substring(start, end).trim();
        }
    }

    public static LogEntry fromJson(String line) {
        String op = extractField(line, "op");
        String msgId = extractField(line, "msgId");
        String payload = extractField(line, "payload");
        String handle = extractField(line, "handle");
        int retryCount = Integer.parseInt(extractField(line, "retryCount"));

        return new LogEntry(
                LogOperation.valueOf(op),
                msgId.isEmpty() ? null : msgId,
                payload.isEmpty() ? null : payload,
                handle.isEmpty() ? null : handle,
                retryCount);
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
