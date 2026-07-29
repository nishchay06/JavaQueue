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

import com.javaqueue.core.LogManager;
import com.javaqueue.core.QueueManager;
import com.javaqueue.core.TopicManager;
import com.javaqueue.json.JsonUtils;

/** HTTP surface for the retained log. */
class LogServerTest {

    private LogManager logs;
    private QueueServer server;
    private HttpClient client;
    private String baseUrl;

    @BeforeEach
    void setUp() throws Exception {
        QueueManager queues = new QueueManager();
        logs = new LogManager();
        server = new QueueServer(queues, new TopicManager(queues), logs, 0);
        server.start();
        baseUrl = "http://localhost:" + server.getPort();
        client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    }

    @AfterEach
    void tearDown() throws Exception {
        server.stop();
        logs.close();
    }

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    @Test
    void testCreateLogReturns201() throws Exception {
        HttpResponse<String> response = post("/logs/orders", null);

        assertEquals(201, response.statusCode());
        assertEquals("orders", field(response.body(), "name"));
        assertNotNull(logs.getLog("orders"));
    }

    @Test
    void testCreateLogWithConfigBody() throws Exception {
        HttpResponse<String> response = post("/logs/orders",
                "{\"maxRecords\":10,\"retentionMs\":60000,\"resetPolicy\":\"LATEST\"}");

        assertEquals(201, response.statusCode());
        // LATEST means a new group skips what already exists
        post("/logs/orders/records", "{\"payload\":\"before\"}");
        assertEquals(204, get("/logs/orders/records?group=g1").statusCode());
    }

    @Test
    void testListLogs() throws Exception {
        post("/logs/orders", null);
        post("/logs/payments", null);

        HttpResponse<String> response = get("/logs");

        assertEquals(200, response.statusCode());
        assertTrue(response.body().contains("\"orders\""));
        assertTrue(response.body().contains("\"payments\""));
    }

    @Test
    void testDeleteLog() throws Exception {
        post("/logs/orders", null);

        assertEquals(204, delete("/logs/orders").statusCode());
        assertTrue(logs.listLogs().isEmpty());
    }

    @Test
    void testDeleteUnknownLogReturns404() throws Exception {
        assertEquals(404, delete("/logs/ghost").statusCode());
    }

    // ── Append and read ───────────────────────────────────────────────────────

    @Test
    void testAppendReturnsAssignedOffset() throws Exception {
        post("/logs/orders", null);

        assertEquals("0", field(post("/logs/orders/records", "{\"payload\":\"a\"}").body(), "offset"));
        assertEquals("1", field(post("/logs/orders/records", "{\"payload\":\"b\"}").body(), "offset"));
    }

    @Test
    void testPollReturnsRecordsAndOffsets() throws Exception {
        post("/logs/orders", null);
        post("/logs/orders/records", "{\"payload\":\"a\"}");
        post("/logs/orders/records", "{\"payload\":\"b\"}");

        HttpResponse<String> response = get("/logs/orders/records?group=g1&max=10");

        assertEquals(200, response.statusCode());
        assertTrue(response.body().contains("\"a\""));
        assertTrue(response.body().contains("\"b\""));
        assertEquals("0", scalarField(response.body(), "startOffset"));
        assertEquals("2", scalarField(response.body(), "nextOffset"));
    }

    // The whole reason a log exists rather than a queue.
    @Test
    void testTwoGroupsBothReadEverything() throws Exception {
        post("/logs/orders", null);
        post("/logs/orders/records", "{\"payload\":\"a\"}");

        assertTrue(get("/logs/orders/records?group=billing").body().contains("\"a\""));
        assertTrue(get("/logs/orders/records?group=analytics").body().contains("\"a\""),
                "reading must not consume the record");
    }

    @Test
    void testPollWithNothingNewReturns204() throws Exception {
        post("/logs/orders", null);

        assertEquals(204, get("/logs/orders/records?group=g1").statusCode());
    }

    @Test
    void testPollWithoutGroupReturns400() throws Exception {
        post("/logs/orders", null);

        assertEquals(400, get("/logs/orders/records").statusCode());
    }

    @Test
    void testAppendWithoutPayloadReturns400() throws Exception {
        post("/logs/orders", null);

        assertEquals(400, post("/logs/orders/records", "{\"nope\":\"x\"}").statusCode());
    }

    @Test
    void testAppendToUnknownLogReturns404() throws Exception {
        assertEquals(404, post("/logs/ghost/records", "{\"payload\":\"x\"}").statusCode());
    }

    // ── Commit and lag ────────────────────────────────────────────────────────

    @Test
    void testCommitAndGroupStatus() throws Exception {
        post("/logs/orders", null);
        post("/logs/orders/records", "{\"payload\":\"a\"}");
        post("/logs/orders/records", "{\"payload\":\"b\"}");
        get("/logs/orders/records?group=billing");

        assertEquals(204, post("/logs/orders/groups/billing/commit", "{\"offset\":2}").statusCode());

        HttpResponse<String> status = get("/logs/orders/groups/billing");
        assertEquals(200, status.statusCode());
        assertEquals("2", field(status.body(), "committed"));
        assertEquals("2", field(status.body(), "endOffset"));
        assertEquals("0", field(status.body(), "lag"));
    }

    // Polling is not committing — lag must not move until the consumer says so.
    @Test
    void testPollingAloneDoesNotReduceLag() throws Exception {
        post("/logs/orders", null);
        post("/logs/orders/records", "{\"payload\":\"a\"}");
        get("/logs/orders/records?group=billing");

        assertEquals("1", field(get("/logs/orders/groups/billing").body(), "lag"));
    }

    @Test
    void testCommitBeyondEndReturns400() throws Exception {
        post("/logs/orders", null);
        post("/logs/orders/records", "{\"payload\":\"a\"}");

        assertEquals(400, post("/logs/orders/groups/g1/commit", "{\"offset\":99}").statusCode());
    }

    @Test
    void testCommitWithoutOffsetReturns400() throws Exception {
        post("/logs/orders", null);

        assertEquals(400, post("/logs/orders/groups/g1/commit", "{}").statusCode());
    }

    // ── Seek ──────────────────────────────────────────────────────────────────

    @Test
    void testSeekToOffsetReReads() throws Exception {
        post("/logs/orders", null);
        post("/logs/orders/records", "{\"payload\":\"a\"}");
        post("/logs/orders/records", "{\"payload\":\"b\"}");
        get("/logs/orders/records?group=g1");

        assertEquals(204, post("/logs/orders/groups/g1/seek", "{\"offset\":0}").statusCode());

        assertTrue(get("/logs/orders/records?group=g1").body().contains("\"a\""));
    }

    @Test
    void testSeekToNamedPositions() throws Exception {
        post("/logs/orders", null);
        post("/logs/orders/records", "{\"payload\":\"a\"}");

        assertEquals(204,
                post("/logs/orders/groups/g1/seek", "{\"position\":\"earliest\"}").statusCode());
        assertTrue(get("/logs/orders/records?group=g1").body().contains("\"a\""));

        assertEquals(204,
                post("/logs/orders/groups/g1/seek", "{\"position\":\"latest\"}").statusCode());
        assertEquals(204, get("/logs/orders/records?group=g1").statusCode());
    }

    @Test
    void testSeekWithNeitherOffsetNorPositionReturns400() throws Exception {
        post("/logs/orders", null);

        assertEquals(400, post("/logs/orders/groups/g1/seek", "{}").statusCode());
    }

    // ── The shape of the API is the lesson ────────────────────────────────────

    // There is no acknowledge and no receipt handle. Progress is one integer,
    // committed explicitly.
    @Test
    void testThereIsNoAcknowledgeEndpoint() throws Exception {
        post("/logs/orders", null);
        post("/logs/orders/records", "{\"payload\":\"a\"}");

        HttpResponse<String> polled = get("/logs/orders/records?group=g1");
        assertTrue(!polled.body().contains("receiptHandle"),
                "a log has no per-message receipts");

        assertEquals(404, delete("/logs/orders/records/0").statusCode());
    }

    @Test
    void testWrongMethodOnLogsCollectionReturns405() throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + "/logs"))
                .method("DELETE", HttpRequest.BodyPublishers.noBody()).build();

        assertEquals(405, client.send(request, HttpResponse.BodyHandlers.ofString()).statusCode());
    }

    @Test
    void testUnknownLogSubpathReturns404() throws Exception {
        post("/logs/orders", null);

        assertEquals(404, get("/logs/orders/nonsense").statusCode());
    }

    // Queue and topic routes must keep working alongside the new ones.
    @Test
    void testQueueAndTopicRoutesStillWork() throws Exception {
        assertEquals(201, post("/queues/orders", null).statusCode());
        assertEquals(201, post("/topics/orders-events", null).statusCode());
        assertEquals(200, get("/queues").statusCode());
        assertEquals(200, get("/topics").statusCode());
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private HttpResponse<String> get(String path) throws Exception {
        return client.send(HttpRequest.newBuilder(URI.create(baseUrl + path)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> post(String path, String body) throws Exception {
        HttpRequest.BodyPublisher publisher = (body == null)
                ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofString(body);
        return client.send(HttpRequest.newBuilder(URI.create(baseUrl + path))
                .header("Content-Type", "application/json").POST(publisher).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> delete(String path) throws Exception {
        return client.send(HttpRequest.newBuilder(URI.create(baseUrl + path)).DELETE().build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private String field(String json, String key) {
        return JsonUtils.fromJson(json).get(key);
    }

    /**
     * Pulls one scalar field out of a body that JsonUtils cannot parse.
     *
     * A poll response contains an array of records, and JsonUtils is a flat
     * parser that rejects nesting by design. This is the first place in the
     * project where the hand-written parser has met its limit — a log batch is
     * inherently a list. Noted as tech debt rather than papered over.
     */
    private String scalarField(String json, String key) {
        String search = "\"" + key + "\":\"";
        int start = json.indexOf(search);
        if (start == -1) {
            return null;
        }
        start += search.length();
        return json.substring(start, json.indexOf('"', start));
    }
}
