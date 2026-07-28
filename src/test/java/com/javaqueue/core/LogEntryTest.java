package com.javaqueue.core;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Direct serializer tests for the WAL record format.
 *
 * Everything the WAL writes must come back byte-identical, whatever the
 * payload contains. Phase 3 only ever tested alphanumeric payloads, which
 * hid the fact that the format has no escaping at all.
 */
public class LogEntryTest {

    // Round-trips a payload through toJson/fromJson and asserts it survives.
    private void assertPayloadSurvives(String payload) {
        Message message = new Message(payload);
        String json = LogEntry.publish(message).toJson();
        LogEntry parsed = LogEntry.fromJson(json);

        assertEquals(payload, parsed.getPayload(),
                "payload did not survive the round trip; serialized form was: " + json);
    }

    // ── The bug: structural characters break the format ───────────────────────

    @Test
    void testPayloadWithQuotesRoundTrips() {
        assertPayloadSurvives("Order \"urgent\"");
    }

    @Test
    void testPayloadWithCommaRoundTrips() {
        assertPayloadSurvives("Order #1, second part");
    }

    @Test
    void testPayloadWithQuotesAndCommasRoundTrips() {
        assertPayloadSurvives("Order #1, \"urgent\"");
    }

    @Test
    void testPayloadWithBackslashRoundTrips() {
        assertPayloadSurvives("C:\\orders\\inbound");
    }

    @Test
    void testPayloadWithBracesRoundTrips() {
        assertPayloadSurvives("{\"nested\":\"json\"}");
    }

    @Test
    void testPayloadWithColonRoundTrips() {
        assertPayloadSurvives("priority:high");
    }

    // A newline is the nastiest case: the WAL is line-delimited, so an
    // unescaped newline splits one record across two lines and desynchronises
    // every entry after it.
    @Test
    void testPayloadWithNewlineRoundTrips() {
        assertPayloadSurvives("line one\nline two");
    }

    @Test
    void testPayloadWithTabAndCarriageReturnRoundTrips() {
        assertPayloadSurvives("col1\tcol2\r\n");
    }

    // A payload that imitates the log format itself — the parser must not be
    // fooled into reading the payload's fake fields as real ones.
    @Test
    void testPayloadImitatingLogFormatRoundTrips() {
        assertPayloadSurvives("\",\"handle\":\"spoofed\",\"retryCount\":99,\"x\":\"");
    }

    @Test
    void testPayloadWithUnicodeRoundTrips() {
        assertPayloadSurvives("caffè — 配送 🚚");
    }

    // ── Structural integrity ──────────────────────────────────────────────────

    // A serialized entry must stay on exactly one line, or WalReader's
    // line-by-line read can never reassemble it.
    @Test
    void testSerializedEntryIsSingleLine() {
        Message message = new Message("line one\nline two\rline three");
        String json = LogEntry.publish(message).toJson();

        assertFalseContains(json, "\n");
        assertFalseContains(json, "\r");
    }

    private void assertFalseContains(String haystack, String needle) {
        assertTrue(!haystack.contains(needle),
                "serialized entry must not contain a raw " + needle.replace("\n", "\\n").replace("\r", "\\r")
                        + ", but was: " + haystack);
    }

    // ── Empty vs absent ───────────────────────────────────────────────────────

    // An empty payload is a legitimate message body. It must not come back as
    // null, which is how the format encodes "this field does not apply".
    @Test
    void testEmptyPayloadIsPreservedNotNulled() {
        Message message = new Message("");
        LogEntry parsed = LogEntry.fromJson(LogEntry.publish(message).toJson());

        assertNotNull(parsed.getPayload(), "empty payload became null");
        assertEquals("", parsed.getPayload());
    }

    // ── Other fields ──────────────────────────────────────────────────────────

    @Test
    void testAllFieldsRoundTripOnConsume() {
        Message message = new Message("some, \"payload\"");
        LogEntry parsed = LogEntry.fromJson(
                LogEntry.consume(message, "handle-abc-123", 7).toJson());

        assertEquals(LogOperation.CONSUME, parsed.getOp());
        assertEquals(message.getId(), parsed.getMsgId());
        assertEquals("handle-abc-123", parsed.getHandle());
        assertEquals(7, parsed.getRetryCount());
    }

    @Test
    void testAckAndNackRoundTrip() {
        LogEntry ack = LogEntry.fromJson(LogEntry.ack("handle-1").toJson());
        assertEquals(LogOperation.ACK, ack.getOp());
        assertEquals("handle-1", ack.getHandle());

        LogEntry nack = LogEntry.fromJson(LogEntry.nack("handle-2").toJson());
        assertEquals(LogOperation.NACK, nack.getOp());
        assertEquals("handle-2", nack.getHandle());
    }

    @Test
    void testTimestampIsPositive() {
        LogEntry entry = LogEntry.publish(new Message("x"));
        assertTrue(entry.getTimestamp() > 0);
    }

    // The timestamp records when the entry was *written*. Parsing must restore
    // it from the log, not re-stamp it with the time of the restart.
    @Test
    void testTimestampIsRestoredFromLogNotRegenerated() throws InterruptedException {
        LogEntry original = LogEntry.publish(new Message("x"));
        String json = original.toJson();

        Thread.sleep(5);
        LogEntry parsed = LogEntry.fromJson(json);

        assertEquals(original.getTimestamp(), parsed.getTimestamp(),
                "replayed entry was re-stamped with the current time");
    }

    // A payload must never be able to forge other fields of the record.
    @Test
    void testPayloadCannotSpoofOtherFields() {
        Message message = new Message("\",\"handle\":\"spoofed\",\"retryCount\":99,\"x\":\"");
        LogEntry parsed = LogEntry.fromJson(LogEntry.publish(message).toJson());

        assertEquals(0, parsed.getRetryCount(), "payload injected a retryCount");
        assertEquals(null, parsed.getHandle(), "payload injected a handle");
        assertEquals(message.getId(), parsed.getMsgId());
    }

    // Fields that do not apply to an operation stay null, and are not confused
    // with a genuinely empty string.
    @Test
    void testInapplicableFieldsAreNull() {
        LogEntry ack = LogEntry.fromJson(LogEntry.ack("handle-1").toJson());

        assertEquals(null, ack.getPayload());
        assertEquals(null, ack.getMsgId());
    }

    // Logs written by Phase 3 use "" for inapplicable fields rather than null.
    // They must still replay after the format change.
    @Test
    void testLegacyFormatWithEmptyStringFieldsStillParses() {
        String legacy = "{\"op\":\"ACK\",\"msgId\":\"\",\"payload\":\"\",\"handle\":\"abc-123\","
                + "\"retryCount\":0,\"ts\":1700000003000}";

        LogEntry parsed = LogEntry.fromJson(legacy);

        assertEquals(LogOperation.ACK, parsed.getOp());
        assertEquals("abc-123", parsed.getHandle());
        assertEquals(1700000003000L, parsed.getTimestamp());
    }
}
