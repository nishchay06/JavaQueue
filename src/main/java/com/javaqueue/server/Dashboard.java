package com.javaqueue.server;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * Loads the operator dashboard from the classpath.
 *
 * Read once and cached — the page is static, and re-reading it on every
 * request would be a needless file hit on a path a browser polls.
 */
final class Dashboard {

    private static final String RESOURCE = "/dashboard.html";
    private static volatile String cached;

    private Dashboard() {
    }

    static String html() {
        String local = cached;
        if (local != null) {
            return local;
        }

        try (InputStream in = Dashboard.class.getResourceAsStream(RESOURCE)) {
            if (in == null) {
                return fallback("dashboard.html was not found on the classpath");
            }
            local = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            return fallback("Could not read dashboard.html: " + e.getMessage());
        }

        cached = local;
        return local;
    }

    private static String fallback(String message) {
        return "<!doctype html><meta charset=\"utf-8\"><title>JavaQueue</title>"
                + "<body style=\"font-family:system-ui;padding:2rem\">"
                + "<h1>JavaQueue</h1><p>" + message + "</p>"
                + "<p>The HTTP API is unaffected — try <code>GET /stats</code>.</p></body>";
    }
}
