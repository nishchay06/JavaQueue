package com.javaqueue.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.javaqueue.core.QueueManager;
import com.javaqueue.core.TopicManager;
import com.javaqueue.json.JsonUtils;

/** HTTP surface for topics and fan-out, end to end over a real socket. */
class TopicServerTest {

    private QueueManager queues;
    private TopicManager topics;
    private QueueServer server;
    private HttpClient client;
    private String baseUrl;

    @BeforeEach
    void setUp() throws Exception {
        queues = new QueueManager();
        topics = new TopicManager(queues);
        server = new QueueServer(queues, topics, 0);
        server.start();
        baseUrl = "http://localhost:" + server.getPort();
        client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    }

    @AfterEach
    void tearDown() throws Exception {
        server.stop();
    }

    // ── Topic lifecycle ───────────────────────────────────────────────────────

    @Test
    void testCreateTopicReturns201() throws Exception {
        HttpResponse<String> response = post("/topics/orders-events", null);

        assertEquals(201, response.statusCode());
        assertEquals("orders-events", field(response.body(), "name"));
        assertNotNull(topics.getTopic("orders-events"));
    }

    @Test
    void testListTopics() throws Exception {
        post("/topics/orders-events", null);
        post("/topics/payment-events", null);

        HttpResponse<String> response = get("/topics");

        assertEquals(200, response.statusCode());
        assertTrue(response.body().contains("\"orders-events\""));
        assertTrue(response.body().contains("\"payment-events\""));
    }

    @Test
    void testListTopicsEmpty() throws Exception {
        HttpResponse<String> response = get("/topics");

        assertEquals(200, response.statusCode());
        assertEquals("{\"topics\":[]}", response.body());
    }

    @Test
    void testDeleteTopicReturns204() throws Exception {
        post("/topics/orders-events", null);

        assertEquals(204, delete("/topics/orders-events").statusCode());
        assertTrue(topics.listTopics().isEmpty());
    }

    @Test
    void testDeleteUnknownTopicReturns404() throws Exception {
        HttpResponse<String> response = delete("/topics/ghost");

        assertEquals(404, response.statusCode());
        assertNotNull(field(response.body(), "error"));
    }

    // ── Subscriptions ─────────────────────────────────────────────────────────

    @Test
    void testSubscribeReturns201() throws Exception {
        post("/topics/orders-events", null);
        post("/queues/billing", null);

        HttpResponse<String> response = post("/topics/orders-events/subscriptions/billing", null);

        assertEquals(201, response.statusCode());
        assertTrue(topics.getTopic("orders-events").listSubscribers().contains("billing"));
    }

    @Test
    void testListSubscriptions() throws Exception {
        post("/topics/orders-events", null);
        post("/queues/billing", null);
        post("/queues/analytics", null);
        post("/topics/orders-events/subscriptions/billing", null);
        post("/topics/orders-events/subscriptions/analytics", null);

        HttpResponse<String> response = get("/topics/orders-events/subscriptions");

        assertEquals(200, response.statusCode());
        assertTrue(response.body().contains("\"billing\""));
        assertTrue(response.body().contains("\"analytics\""));
    }

    @Test
    void testUnsubscribeReturns204() throws Exception {
        post("/topics/orders-events", null);
        post("/queues/billing", null);
        post("/topics/orders-events/subscriptions/billing", null);

        assertEquals(204, delete("/topics/orders-events/subscriptions/billing").statusCode());
        assertTrue(topics.getTopic("orders-events").listSubscribers().isEmpty());
    }

    @Test
    void testSubscribeToUnknownTopicReturns404() throws Exception {
        post("/queues/billing", null);

        assertEquals(404, post("/topics/ghost/subscriptions/billing", null).statusCode());
    }

    // Subscribing a queue that does not exist is the mistake the core API
    // rejects — the HTTP layer must surface that rather than record a
    // subscription that silently never delivers.
    @Test
    void testSubscribeUnknownQueueReturns404() throws Exception {
        post("/topics/orders-events", null);

        HttpResponse<String> response = post("/topics/orders-events/subscriptions/ghost", null);

        assertEquals(404, response.statusCode());
        assertTrue(topics.getTopic("orders-events").listSubscribers().isEmpty());
    }

    @Test
    void testListSubscriptionsOfUnknownTopicReturns404() throws Exception {
        assertEquals(404, get("/topics/ghost/subscriptions").statusCode());
    }

    // ── Publishing and fan-out ────────────────────────────────────────────────

    @Test
    void testPublishReportsDeliveryCount() throws Exception {
        post("/topics/orders-events", null);
        post("/queues/billing", null);
        post("/queues/analytics", null);
        post("/topics/orders-events/subscriptions/billing", null);
        post("/topics/orders-events/subscriptions/analytics", null);

        HttpResponse<String> response = post("/topics/orders-events/messages",
                "{\"payload\":\"Order1\"}");

        assertEquals(201, response.statusCode());
        assertNotNull(field(response.body(), "messageId"));
        assertEquals("2", field(response.body(), "delivered"));
    }

    @Test
    void testPublishToTopicWithNoSubscribersReportsZero() throws Exception {
        post("/topics/orders-events", null);

        HttpResponse<String> response = post("/topics/orders-events/messages",
                "{\"payload\":\"Order1\"}");

        assertEquals(201, response.statusCode());
        assertEquals("0", field(response.body(), "delivered"));
    }

    // The whole point of the phase, over the wire: one publish, two groups.
    @Test
    void testFanOutEndToEndOverHttp() throws Exception {
        post("/topics/orders-events", null);
        post("/queues/billing", null);
        post("/queues/analytics", null);
        post("/topics/orders-events/subscriptions/billing", null);
        post("/topics/orders-events/subscriptions/analytics", null);

        post("/topics/orders-events/messages", "{\"payload\":\"Order #1, \\\"urgent\\\"\"}");

        HttpResponse<String> fromBilling = get("/queues/billing/messages?waitSeconds=1");
        HttpResponse<String> fromAnalytics = get("/queues/analytics/messages?waitSeconds=1");

        assertEquals(200, fromBilling.statusCode());
        assertEquals(200, fromAnalytics.statusCode());
        assertEquals("Order #1, \"urgent\"", field(fromBilling.body(), "payload"));
        assertEquals("Order #1, \"urgent\"", field(fromAnalytics.body(), "payload"));

        // Same logical message, different deliveries
        assertEquals(field(fromBilling.body(), "messageId"), field(fromAnalytics.body(), "messageId"));
        assertTrue(!field(fromBilling.body(), "receiptHandle")
                .equals(field(fromAnalytics.body(), "receiptHandle")));
    }

    @Test
    void testPublishToUnknownTopicReturns404() throws Exception {
        assertEquals(404, post("/topics/ghost/messages", "{\"payload\":\"x\"}").statusCode());
    }

    @Test
    void testPublishWithoutPayloadReturns400() throws Exception {
        post("/topics/orders-events", null);

        assertEquals(400, post("/topics/orders-events/messages", "{\"nope\":\"x\"}").statusCode());
    }

    // ── The asymmetry ─────────────────────────────────────────────────────────

    // You cannot consume from a topic, only from a subscriber queue. The path
    // exists, the operation does not — so 405, not 404. Making it impossible
    // in the API is the clearest statement that a topic routes, not stores.
    @Test
    void testConsumingFromTopicReturns405() throws Exception {
        post("/topics/orders-events", null);

        HttpResponse<String> response = get("/topics/orders-events/messages");

        assertEquals(405, response.statusCode());
        assertNotNull(field(response.body(), "error"));
    }

    @Test
    void testWrongMethodOnTopicsCollectionReturns405() throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + "/topics"))
                .method("DELETE", HttpRequest.BodyPublishers.noBody())
                .build();

        assertEquals(405, client.send(request, HttpResponse.BodyHandlers.ofString()).statusCode());
    }

    @Test
    void testUnknownTopicSubpathReturns404() throws Exception {
        post("/topics/orders-events", null);

        assertEquals(404, get("/topics/orders-events/nonsense").statusCode());
    }

    // Queue routes must keep working unchanged alongside the new topic routes.
    @Test
    void testQueueRoutesStillWork() throws Exception {
        assertEquals(201, post("/queues/orders", null).statusCode());
        assertEquals(200, get("/queues").statusCode());
        assertEquals(201, post("/queues/orders/messages", "{\"payload\":\"x\"}").statusCode());
    }

    // ── helpers ───────────────────────────────────────────────────────────────

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

    private String field(String json, String key) {
        return JsonUtils.fromJson(json).get(key);
    }
}
