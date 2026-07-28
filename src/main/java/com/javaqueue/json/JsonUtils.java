package com.javaqueue.json;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Hand-written JSON for flat, single-level objects.
 *
 * No Jackson, no Gson. Writing the parser by hand is the point — it forces you
 * to deal with quoting, escaping, and the fact that you cannot split a JSON
 * document on commas and hope for the best.
 *
 * Supports: string values, numbers, booleans, null. Not supported: nesting,
 * arrays. Those are rejected rather than silently mangled.
 *
 * Lives in its own package because two layers depend on it: the write-ahead
 * log in {@code core} and the HTTP API in {@code server}. They used to have
 * separate implementations, and the WAL's could not survive a quote or a
 * comma in a payload.
 */
public class JsonUtils {

    /** Thrown when a body is not a flat JSON object we can parse. */
    public static class JsonParseException extends RuntimeException {
        public JsonParseException(String message) {
            super(message);
        }
    }

    // Builds a flat JSON object from a string map.
    // Keys and values are escaped, so payloads may contain quotes and commas.
    // A null value is written as the JSON literal null, not the string "null".
    public static String toJson(Map<String, String> fields) {
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, String> entry : fields.entrySet()) {
            if (!first) {
                sb.append(",");
            }
            sb.append("\"").append(escape(entry.getKey())).append("\":");
            if (entry.getValue() == null) {
                sb.append("null");
            } else {
                sb.append("\"").append(escape(entry.getValue())).append("\"");
            }
            first = false;
        }
        sb.append("}");
        return sb.toString();
    }

    // Parses a flat JSON object into a string map.
    // e.g. {"payload":"a,b \"c\""} → {"payload" -> a,b "c"}
    // Numbers and booleans are kept as their literal text; null becomes a null
    // value, so callers can distinguish "absent" from "explicitly null".
    public static Map<String, String> fromJson(String json) {
        Map<String, String> result = new LinkedHashMap<>();
        if (json == null || json.isBlank()) {
            return result;
        }

        Cursor c = new Cursor(json);
        c.skipWhitespace();
        c.expect('{');
        c.skipWhitespace();

        if (c.peek() == '}') {
            return result;
        }

        while (true) {
            c.skipWhitespace();
            if (c.peek() != '"') {
                throw new JsonParseException("Expected a quoted key at position " + c.pos);
            }
            String key = c.readString();

            c.skipWhitespace();
            c.expect(':');
            c.skipWhitespace();

            result.put(key, c.readValue());

            c.skipWhitespace();
            if (c.peek() == ',') {
                c.pos++;
                continue;
            }
            c.expect('}');
            break;
        }
        return result;
    }

    // Convenience method — returns {"error":"<message>"}
    public static String errorJson(String message) {
        return "{\"error\":\"" + escape(message) + "\"}";
    }

    /**
     * Escapes a string for inclusion inside JSON double quotes. Public because
     * LogEntry builds its own object literal to keep numeric fields unquoted,
     * preserving the on-disk WAL format.
     */
    public static String escape(String raw) {
        StringBuilder sb = new StringBuilder(raw.length() + 8);
        for (int i = 0; i < raw.length(); i++) {
            char ch = raw.charAt(i);
            switch (ch) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                case '\b' -> sb.append("\\b");
                case '\f' -> sb.append("\\f");
                default -> {
                    if (ch < 0x20) {
                        sb.append(String.format("\\u%04x", (int) ch));
                    } else {
                        sb.append(ch);
                    }
                }
            }
        }
        return sb.toString();
    }

    /** Single-pass character scanner over the document. */
    private static final class Cursor {
        private final String src;
        private int pos;

        Cursor(String src) {
            this.src = src;
        }

        char peek() {
            if (pos >= src.length()) {
                throw new JsonParseException("Unexpected end of JSON input");
            }
            return src.charAt(pos);
        }

        void expect(char expected) {
            if (peek() != expected) {
                throw new JsonParseException(
                        "Expected '" + expected + "' at position " + pos + " but found '" + peek() + "'");
            }
            pos++;
        }

        void skipWhitespace() {
            while (pos < src.length() && Character.isWhitespace(src.charAt(pos))) {
                pos++;
            }
        }

        // Reads a quoted string starting at the opening quote, resolving escapes.
        String readString() {
            expect('"');
            StringBuilder sb = new StringBuilder();
            while (true) {
                char ch = peek();
                pos++;
                if (ch == '"') {
                    return sb.toString();
                }
                if (ch != '\\') {
                    sb.append(ch);
                    continue;
                }
                char esc = peek();
                pos++;
                switch (esc) {
                    case '"' -> sb.append('"');
                    case '\\' -> sb.append('\\');
                    case '/' -> sb.append('/');
                    case 'n' -> sb.append('\n');
                    case 'r' -> sb.append('\r');
                    case 't' -> sb.append('\t');
                    case 'b' -> sb.append('\b');
                    case 'f' -> sb.append('\f');
                    case 'u' -> {
                        if (pos + 4 > src.length()) {
                            throw new JsonParseException("Truncated \\u escape at position " + pos);
                        }
                        sb.append((char) Integer.parseInt(src.substring(pos, pos + 4), 16));
                        pos += 4;
                    }
                    default -> throw new JsonParseException("Invalid escape '\\" + esc + "' at position " + pos);
                }
            }
        }

        // Reads a string, number, boolean, or null. Nesting is rejected.
        String readValue() {
            char ch = peek();
            if (ch == '"') {
                return readString();
            }
            if (ch == '{' || ch == '[') {
                throw new JsonParseException("Nested objects and arrays are not supported (position " + pos + ")");
            }
            int start = pos;
            while (pos < src.length()) {
                char here = src.charAt(pos);
                if (here == ',' || here == '}' || Character.isWhitespace(here)) {
                    break;
                }
                pos++;
            }
            String literal = src.substring(start, pos);
            if (literal.isEmpty()) {
                throw new JsonParseException("Empty value at position " + start);
            }
            return literal.equals("null") ? null : literal;
        }
    }
}
