package com.javaqueue.json;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Direct tests for the hand-written JSON parser. Until now it was only
 * exercised through HTTP, which made failures hard to localise.
 */
public class JsonUtilsTest {

    private void assertRoundTrips(String value) {
        String json = JsonUtils.toJson(Map.of("payload", value));
        assertEquals(value, JsonUtils.fromJson(json).get("payload"),
                "value did not survive the round trip; serialized form was: " + json);
    }

    // ── Round-tripping ────────────────────────────────────────────────────────

    @Test
    void testPlainValueRoundTrips() {
        assertRoundTrips("Order1");
    }

    @Test
    void testValueWithQuotesRoundTrips() {
        assertRoundTrips("say \"hello\"");
    }

    @Test
    void testValueWithCommasRoundTrips() {
        assertRoundTrips("a,b,c");
    }

    @Test
    void testValueWithBackslashRoundTrips() {
        assertRoundTrips("C:\\path\\to\\file");
    }

    @Test
    void testValueWithControlCharactersRoundTrips() {
        assertRoundTrips("line1\nline2\ttabbed\r\n");
    }

    @Test
    void testValueWithBracesRoundTrips() {
        assertRoundTrips("{\"looks\":\"nested\"}");
    }

    @Test
    void testValueWithUnicodeRoundTrips() {
        assertRoundTrips("caffè — 配送 🚚");
    }

    @Test
    void testEmptyValueRoundTrips() {
        assertRoundTrips("");
    }

    // ── Parsing ───────────────────────────────────────────────────────────────

    @Test
    void testParsesMultipleFields() {
        Map<String, String> parsed = JsonUtils.fromJson(
                "{\"a\":\"1\",\"b\":\"2\",\"c\":\"3\"}");

        assertEquals(3, parsed.size());
        assertEquals("1", parsed.get("a"));
        assertEquals("3", parsed.get("c"));
    }

    @Test
    void testParsesUnquotedNumbersAndBooleans() {
        Map<String, String> parsed = JsonUtils.fromJson(
                "{\"maxRetries\":7,\"visibilityTimeoutMs\":1000,\"flag\":true}");

        assertEquals("7", parsed.get("maxRetries"));
        assertEquals("1000", parsed.get("visibilityTimeoutMs"));
        assertEquals("true", parsed.get("flag"));
    }

    @Test
    void testParsesNullAsNullValue() {
        Map<String, String> parsed = JsonUtils.fromJson("{\"deadLetterQueueName\":null}");

        assertNull(parsed.get("deadLetterQueueName"));
        assertTrue(parsed.containsKey("deadLetterQueueName"),
                "an explicit null must be distinguishable from an absent key");
    }

    @Test
    void testToleratesWhitespace() {
        Map<String, String> parsed = JsonUtils.fromJson(
                "  {  \"a\" : \"1\" ,  \"b\" : 2  }  ");

        assertEquals("1", parsed.get("a"));
        assertEquals("2", parsed.get("b"));
    }

    @Test
    void testParsesEmptyObject() {
        assertTrue(JsonUtils.fromJson("{}").isEmpty());
    }

    @Test
    void testNullAndBlankInputReturnEmptyMap() {
        assertTrue(JsonUtils.fromJson(null).isEmpty());
        assertTrue(JsonUtils.fromJson("").isEmpty());
        assertTrue(JsonUtils.fromJson("   ").isEmpty());
    }

    @Test
    void testParsesUnicodeEscape() {
        assertEquals("A", JsonUtils.fromJson("{\"x\":\"\\u0041\"}").get("x"));
    }

    // ── Rejection ─────────────────────────────────────────────────────────────
    // Failing loudly beats silently mangling — a wrong body should be a 400,
    // not a message with a quietly truncated payload.

    @Test
    void testRejectsTruncatedInput() {
        assertThrows(JsonUtils.JsonParseException.class,
                () -> JsonUtils.fromJson("{\"payload\":"));
    }

    @Test
    void testRejectsUnterminatedString() {
        assertThrows(JsonUtils.JsonParseException.class,
                () -> JsonUtils.fromJson("{\"payload\":\"unclosed"));
    }

    @Test
    void testRejectsMissingOpeningBrace() {
        assertThrows(JsonUtils.JsonParseException.class,
                () -> JsonUtils.fromJson("\"payload\":\"x\"}"));
    }

    @Test
    void testRejectsUnquotedKey() {
        assertThrows(JsonUtils.JsonParseException.class,
                () -> JsonUtils.fromJson("{payload:\"x\"}"));
    }

    @Test
    void testRejectsNestedObject() {
        assertThrows(JsonUtils.JsonParseException.class,
                () -> JsonUtils.fromJson("{\"config\":{\"a\":\"1\"}}"));
    }

    @Test
    void testRejectsArray() {
        assertThrows(JsonUtils.JsonParseException.class,
                () -> JsonUtils.fromJson("{\"items\":[1,2]}"));
    }

    // ── Serializing ───────────────────────────────────────────────────────────

    @Test
    void testToJsonWritesNullAsLiteralNotString() {
        Map<String, String> fields = new LinkedHashMap<>();
        fields.put("dlq", null);

        assertEquals("{\"dlq\":null}", JsonUtils.toJson(fields));
    }

    @Test
    void testToJsonPreservesInsertionOrder() {
        Map<String, String> fields = new LinkedHashMap<>();
        fields.put("messageId", "1");
        fields.put("payload", "x");
        fields.put("receiptHandle", "h");

        assertEquals("{\"messageId\":\"1\",\"payload\":\"x\",\"receiptHandle\":\"h\"}",
                JsonUtils.toJson(fields));
    }

    @Test
    void testErrorJsonEscapesMessage() {
        String json = JsonUtils.errorJson("bad \"input\"");

        assertEquals("bad \"input\"", JsonUtils.fromJson(json).get("error"));
    }

    @Test
    void testKeysAreEscapedToo() {
        Map<String, String> fields = Map.of("odd\"key", "v");

        assertEquals("v", JsonUtils.fromJson(JsonUtils.toJson(fields)).get("odd\"key"));
    }
}
