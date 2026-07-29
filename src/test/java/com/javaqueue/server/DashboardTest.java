package com.javaqueue.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.javaqueue.core.LogManager;
import com.javaqueue.core.Message;
import com.javaqueue.core.QueueConfig;
import com.javaqueue.core.QueueManager;
import com.javaqueue.core.TopicManager;

/**
 * The operator dashboard and the snapshot that feeds it.
 *
 * Served from the same origin as the API — that is what lets a browser page
 * call it without any cross-origin arrangement.
 */
class DashboardTest {

    private QueueManager queues;
    private TopicManager topics;
    private LogManager logs;
    private QueueServer server;
    private HttpClient client;
    private String baseUrl;

    @BeforeEach
    void setUp() throws Exception {
        queues = new QueueManager();
        topics = new TopicManager(queues);
        logs = new LogManager();
        server = new QueueServer(queues, topics, logs, 0);
        server.start();
        baseUrl = "http://localhost:" + server.getPort();
        client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    }

    @AfterEach
    void tearDown() throws Exception {
        server.stop();
        logs.close();
    }

    private HttpResponse<String> get(String path) throws Exception {
        return client.send(HttpRequest.newBuilder(URI.create(baseUrl + path)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
    }

    // ── The page ──────────────────────────────────────────────────────────────

    // "/".split("/") yields an empty array, so the root path has to be handled
    // before the segment-count routing — which it originally was not.
    @Test
    void testRootServesTheDashboard() throws Exception {
        HttpResponse<String> response = get("/");

        assertEquals(200, response.statusCode());
        assertTrue(response.headers().firstValue("Content-Type").orElse("").startsWith("text/html"));
        assertTrue(response.body().contains("<title>JavaQueue</title>"));
    }

    @Test
    void testDashboardPathAlsoServesThePage() throws Exception {
        assertEquals(200, get("/dashboard").statusCode());
    }

    // Everything is inlined: no CDN, no external stylesheet, matching the
    // project's no-dependencies philosophy and working offline.
    @Test
    void testDashboardIsSelfContained() throws Exception {
        String html = get("/").body();

        assertTrue(!html.contains("http://") || html.indexOf("http://") > html.length(),
                "no external http resources");
        assertTrue(!html.contains("cdn."), "no CDN references");
        assertTrue(html.contains("<style>") && html.contains("<script>"),
                "styles and script are inlined");
    }

    // ── The snapshot ──────────────────────────────────────────────────────────

    @Test
    void testStatsOnAnEmptyServer() throws Exception {
        HttpResponse<String> response = get("/stats");

        assertEquals(200, response.statusCode());
        assertEquals("{\"queues\":[],\"topics\":[],\"logs\":[]}", response.body());
    }

    @Test
    void testStatsReportsQueueDepthAndInFlight() throws Exception {
        queues.createQueue("orders", new QueueConfig(30_000, 3, "orders-dlq", null));
        queues.getQueue("orders").publish(new Message("a"));
        queues.getQueue("orders").publish(new Message("b"));
        queues.getQueue("orders").consume(1000);

        String body = get("/stats").body();

        assertTrue(body.contains("\"name\":\"orders\""));
        assertTrue(body.contains("\"depth\":\"1\""), "one message still waiting: " + body);
        assertTrue(body.contains("\"inFlight\":\"1\""), "one consumed but unacked: " + body);
        assertTrue(body.contains("\"deadLetterQueue\":\"orders-dlq\""));
    }

    @Test
    void testStatsReportsTopicSubscribers() throws Exception {
        topics.createTopic("orders-events").subscribe(queues.createQueue("billing"));

        String body = get("/stats").body();

        assertTrue(body.contains("\"name\":\"orders-events\""));
        assertTrue(body.contains("\"subscribers\":[\"billing\"]"));
    }

    // Lag per group is the number an operator actually watches, so it has to
    // be in the snapshot rather than requiring a call per group.
    @Test
    void testStatsReportsPerGroupLag() throws Exception {
        var log = logs.createLog("events");
        for (int i = 0; i < 5; i++) {
            log.append(new Message("e" + i));
        }
        log.poll("fast", 5);
        log.commit("fast", 5);
        log.poll("slow", 1);

        String body = get("/stats").body();

        assertTrue(body.contains("\"beginOffset\":0"), body);
        assertTrue(body.contains("\"endOffset\":5"), body);
        assertTrue(body.contains("\"name\":\"fast\",\"committed\":5,\"position\":5,\"lag\":0"), body);
        assertTrue(body.contains("\"name\":\"slow\",\"committed\":0,\"position\":1,\"lag\":5"), body);
    }

    // Names are user-supplied, so they go through the same escaping as
    // everything else the server writes.
    @Test
    void testStatsEscapesNames() throws Exception {
        queues.createQueue("odd\"name");

        assertTrue(get("/stats").body().contains("odd\\\"name"));
    }

    @Test
    void testStatsRejectsNonGetMethods() throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + "/stats"))
                .method("DELETE", HttpRequest.BodyPublishers.noBody()).build();

        assertEquals(405, client.send(request, HttpResponse.BodyHandlers.ofString()).statusCode());
    }
}
