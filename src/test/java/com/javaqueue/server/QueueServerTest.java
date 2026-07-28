package com.javaqueue.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.javaqueue.core.Message;
import com.javaqueue.core.QueueManager;

/**
 * End-to-end tests over real HTTP — a real socket, a real Jetty, a real client.
 * Port 0 lets the OS pick a free port, so parallel runs never collide.
 */
class QueueServerTest {

    private QueueManager manager;
    private QueueServer server;
    private HttpClient client;
    private String baseUrl;

    @BeforeEach
    void setUp() throws Exception {
        manager = new QueueManager();
        server = new QueueServer(manager, 0);
        server.start();
        baseUrl = "http://localhost:" + server.getPort();
        client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
    }

    @AfterEach
    void tearDown() throws Exception {
        server.stop();
    }

    // ---------- queue lifecycle ----------

    @Test
    void testServerStartsOnRandomPort() {
        assertTrue(server.getPort() > 0, "port 0 should resolve to a real bound port");
        assertTrue(server.isRunning());
    }

    @Test
    void testCreateQueueReturns201() throws Exception {
        HttpResponse<String> response = post("/queues/orders", null);

        assertEquals(201, response.statusCode());
        assertEquals("orders", field(response.body(), "name"));
        assertNotNull(manager.getQueue("orders"));
    }

    @Test
    void testCreateQueueWithConfigBody() throws Exception {
        String body = "{\"visibilityTimeoutMs\":1000,\"maxRetries\":7,\"deadLetterQueueName\":\"orders-dlq\"}";
        HttpResponse<String> response = post("/queues/orders", body);

        assertEquals(201, response.statusCode());
        // DLQ is auto-created by QueueManager when configured
        assertTrue(manager.listQueues().contains("orders-dlq"));
    }

    @Test
    void testListQueues() throws Exception {
        post("/queues/orders", null);
        post("/queues/payments", null);

        HttpResponse<String> response = get("/queues");

        assertEquals(200, response.statusCode());
        assertTrue(response.body().contains("\"orders\""));
        assertTrue(response.body().contains("\"payments\""));
    }

    @Test
    void testDeleteQueueReturns204() throws Exception {
        post("/queues/orders", null);

        HttpResponse<String> response = delete("/queues/orders");

        assertEquals(204, response.statusCode());
        assertTrue(manager.listQueues().isEmpty());
    }

    @Test
    void testDeleteUnknownQueueReturns404() throws Exception {
        HttpResponse<String> response = delete("/queues/nope");

        assertEquals(404, response.statusCode());
        assertNotNull(field(response.body(), "error"));
    }

    // ---------- publish / consume / ack ----------

    @Test
    void testPublishThenConsumeRoundTrip() throws Exception {
        post("/queues/orders", null);

        HttpResponse<String> published = post("/queues/orders/messages", "{\"payload\":\"Order1\"}");
        assertEquals(201, published.statusCode());
        assertNotNull(field(published.body(), "messageId"));

        HttpResponse<String> consumed = get("/queues/orders/messages?waitSeconds=1");
        assertEquals(200, consumed.statusCode());
        assertEquals("Order1", field(consumed.body(), "payload"));
        assertNotNull(field(consumed.body(), "receiptHandle"));
    }

    @Test
    void testAcknowledgeRemovesFromInFlight() throws Exception {
        post("/queues/orders", null);
        post("/queues/orders/messages", "{\"payload\":\"Order1\"}");

        String handle = field(get("/queues/orders/messages?waitSeconds=1").body(), "receiptHandle");
        HttpResponse<String> ack = delete("/queues/orders/messages/" + handle);

        assertEquals(204, ack.statusCode());
        // Nothing left to redeliver
        assertEquals(204, get("/queues/orders/messages?waitSeconds=0").statusCode());
    }

    @Test
    void testNackRequeuesMessage() throws Exception {
        post("/queues/orders", null);
        post("/queues/orders/messages", "{\"payload\":\"Order1\"}");

        String handle = field(get("/queues/orders/messages?waitSeconds=1").body(), "receiptHandle");
        HttpResponse<String> nack = post("/queues/orders/messages/" + handle + "/nack", null);
        assertEquals(204, nack.statusCode());

        // Same payload comes back, under a fresh receipt handle
        HttpResponse<String> redelivered = get("/queues/orders/messages?waitSeconds=1");
        assertEquals(200, redelivered.statusCode());
        assertEquals("Order1", field(redelivered.body(), "payload"));
    }

    @Test
    void testAcknowledgeWithBadHandleReturns404() throws Exception {
        post("/queues/orders", null);

        HttpResponse<String> response = delete("/queues/orders/messages/not-a-real-handle");

        assertEquals(404, response.statusCode());
    }

    @Test
    void testPublishToUnknownQueueReturns404() throws Exception {
        HttpResponse<String> response = post("/queues/ghost/messages", "{\"payload\":\"x\"}");

        assertEquals(404, response.statusCode());
    }

    @Test
    void testPublishWithoutPayloadReturns400() throws Exception {
        post("/queues/orders", null);

        HttpResponse<String> response = post("/queues/orders/messages", "{\"notPayload\":\"x\"}");

        assertEquals(400, response.statusCode());
    }

    @Test
    void testMalformedJsonReturns400() throws Exception {
        post("/queues/orders", null);

        HttpResponse<String> response = post("/queues/orders/messages", "{\"payload\":");

        assertEquals(400, response.statusCode());
    }

    // ---------- long polling ----------

    @Test
    void testLongPollReturns204WhenNoMessageArrives() throws Exception {
        post("/queues/orders", null);

        long start = System.currentTimeMillis();
        HttpResponse<String> response = get("/queues/orders/messages?waitSeconds=1");
        long elapsed = System.currentTimeMillis() - start;

        assertEquals(204, response.statusCode());
        assertTrue(elapsed >= 900, "should have held the connection for ~1s, held " + elapsed + "ms");
    }

    @Test
    void testLongPollWakesWhenMessagePublishedMidWait() throws Exception {
        post("/queues/orders", null);

        // Publish 300ms into a 10s poll — the response should come back
        // promptly, not after the full timeout.
        Thread producer = new Thread(() -> {
            try {
                Thread.sleep(300);
                manager.getQueue("orders").publish(new Message("LateOrder"));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        producer.start();

        long start = System.currentTimeMillis();
        HttpResponse<String> response = get("/queues/orders/messages?waitSeconds=10");
        long elapsed = System.currentTimeMillis() - start;
        producer.join();

        assertEquals(200, response.statusCode());
        assertEquals("LateOrder", field(response.body(), "payload"));
        assertTrue(elapsed < 5000, "should have woken on publish, took " + elapsed + "ms");
    }

    @Test
    void testWaitSecondsIsClampedToMax() throws Exception {
        post("/queues/orders", null);

        // Asking for an hour must not park a Jetty thread for an hour.
        long start = System.currentTimeMillis();
        HttpResponse<String> response = get("/queues/orders/messages?waitSeconds=0");
        long elapsed = System.currentTimeMillis() - start;

        assertEquals(204, response.statusCode());
        assertTrue(elapsed < 1000, "waitSeconds=0 should return immediately, took " + elapsed + "ms");
        assertEquals(QueueHandler.MAX_WAIT_SECONDS, Math.clamp(3600, 0, QueueHandler.MAX_WAIT_SECONDS));
    }

    // ---------- routing ----------

    @Test
    void testUnknownPathReturns404() throws Exception {
        assertEquals(404, get("/nonsense").statusCode());
    }

    @Test
    void testWrongMethodReturns405() throws Exception {
        post("/queues/orders", null);

        HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + "/queues"))
                .method("DELETE", HttpRequest.BodyPublishers.noBody())
                .build();

        assertEquals(405, client.send(request, HttpResponse.BodyHandlers.ofString()).statusCode());
    }

    // ---------- payload fidelity ----------

    @Test
    void testPayloadWithCommasAndQuotesSurvivesRoundTrip() throws Exception {
        post("/queues/orders", null);

        // The Phase 3 WAL parser could not handle this. The Phase 4 parser must.
        String payload = "a,b \"quoted\" c\\d";
        post("/queues/orders/messages", JsonUtils.toJson(Map.of("payload", payload)));

        HttpResponse<String> response = get("/queues/orders/messages?waitSeconds=1");

        assertEquals(200, response.statusCode());
        assertEquals(payload, field(response.body(), "payload"));
    }

    // ---------- helpers ----------

    private HttpResponse<String> get(String path) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + path)).GET().build();
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> post(String path, String body) throws Exception {
        HttpRequest.BodyPublisher publisher = (body == null)
                ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofString(body);
        HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + path))
                .header("Content-Type", "application/json")
                .POST(publisher)
                .build();
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> delete(String path) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + path)).DELETE().build();
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    /** Pulls one field out of a JSON response body; null if absent. */
    private String field(String json, String key) {
        return JsonUtils.fromJson(json).get(key);
    }

    @Test
    void testErrorBodiesAreJson() throws Exception {
        HttpResponse<String> response = get("/queues/ghost/messages?waitSeconds=0");

        assertEquals(404, response.statusCode());
        assertNotNull(field(response.body(), "error"));
        assertNull(field(response.body(), "payload"));
    }
}
