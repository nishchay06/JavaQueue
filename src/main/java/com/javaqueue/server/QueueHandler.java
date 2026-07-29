package com.javaqueue.server;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.eclipse.jetty.io.Content;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.util.Callback;

import com.javaqueue.core.MessageQueue;
import com.javaqueue.core.Message;
import com.javaqueue.core.QueueConfig;
import com.javaqueue.core.LogConfig;
import com.javaqueue.core.LogManager;
import com.javaqueue.core.LogRecords;
import com.javaqueue.core.MessageLog;
import com.javaqueue.core.OffsetResetPolicy;
import com.javaqueue.core.QueueManager;
import com.javaqueue.core.Receipt;
import com.javaqueue.core.Topic;
import com.javaqueue.core.TopicManager;
import com.javaqueue.exception.InvalidReceiptException;
import com.javaqueue.exception.LogNotFoundException;
import com.javaqueue.exception.OffsetOutOfRangeException;
import com.javaqueue.exception.QueueNotFoundException;
import com.javaqueue.exception.TopicNotFoundException;
import com.javaqueue.json.JsonUtils;

/**
 * Routes HTTP requests onto the QueueManager and TopicManager.
 *
 * | Method | Path                                       |
 * |--------|--------------------------------------------|
 * | GET    | /queues                                    |
 * | POST   | /queues/{name}                             |
 * | DELETE | /queues/{name}                             |
 * | POST   | /queues/{name}/messages                    |
 * | GET    | /queues/{name}/messages?waitSeconds=n      |
 * | DELETE | /queues/{name}/messages/{handle}           |
 * | POST   | /queues/{name}/messages/{handle}/nack      |
 * | GET    | /topics                                    |
 * | POST   | /topics/{name}                             |
 * | DELETE | /topics/{name}                             |
 * | GET    | /topics/{name}/subscriptions               |
 * | POST   | /topics/{name}/subscriptions/{queue}       |
 * | DELETE | /topics/{name}/subscriptions/{queue}       |
 * | POST   | /topics/{name}/messages                    |
 *
 * There is deliberately no GET /topics/{name}/messages. A topic routes, it
 * does not store — you consume from a subscriber queue, never from the topic.
 */
public class QueueHandler extends Handler.Abstract {

    static final int DEFAULT_WAIT_SECONDS = 5;

    // A waiting long poll no longer holds a thread, so this cap is about
    // bounding client-visible latency rather than protecting the thread pool.
    static final int MAX_WAIT_SECONDS = 300;

    private final QueueManager queueManager;
    private final TopicManager topicManager;
    private final LogManager logManager;

    // Fires the 204 when a long poll reaches its deadline. One shared daemon
    // thread for all pending polls — which is the entire point: waiting costs
    // a scheduled task, not a thread.
    private final ScheduledExecutorService timeouts = Executors.newSingleThreadScheduledExecutor(
            runnable -> {
                Thread thread = new Thread(runnable, "long-poll-timeouts");
                thread.setDaemon(true);
                return thread;
            });

    public QueueHandler(QueueManager queueManager, TopicManager topicManager,
            LogManager logManager) {
        this.queueManager = queueManager;
        this.topicManager = topicManager;
        this.logManager = logManager;
    }

    /** Shuts down the timeout scheduler. Called by QueueServer.stop(). */
    public void close() {
        timeouts.shutdownNow();
    }

    @Override
    public boolean handle(Request request, Response response, Callback callback) throws Exception {
        String path = request.getHttpURI().getPath();
        String method = request.getMethod();

        // The dashboard and its data feed, checked before the segment split:
        // "/".split("/") yields an empty array, so the root path never
        // survives the length guard below.
        //
        // Both are served from the same origin as the API, which is what lets
        // a browser page call it at all.
        if (path.equals("/") || path.equals("/dashboard")) {
            if (method.equals("GET")) {
                writeHtml(response, callback, Dashboard.html());
            } else {
                writeError(response, callback, 405, "Method not allowed");
            }
            return true;
        }
        if (path.equals("/stats")) {
            if (method.equals("GET")) {
                writeJson(response, callback, 200, buildStats());
            } else {
                writeError(response, callback, 405, "Method not allowed");
            }
            return true;
        }

        // path="/queues/orders/messages" → segments=["","queues","orders","messages"]
        String[] segments = path.split("/");

        if (segments.length < 2) {
            writeError(response, callback, 404, "Not found");
            return true;
        }

        // Exception mapping is shared across both resources, so it wraps the
        // dispatch rather than being repeated inside each one.
        try {
            switch (segments[1]) {
                case "queues" -> routeQueues(segments, method, request, response, callback);
                case "topics" -> routeTopics(segments, method, request, response, callback);
                case "logs" -> routeLogs(segments, method, request, response, callback);
                default -> writeError(response, callback, 404, "Not found");
            }
        } catch (QueueNotFoundException | TopicNotFoundException | LogNotFoundException e) {
            writeError(response, callback, 404, e.getMessage());
        } catch (InvalidReceiptException e) {
            writeError(response, callback, 404, e.getMessage());
        } catch (OffsetOutOfRangeException e) {
            writeError(response, callback, 400, e.getMessage());
        } catch (JsonUtils.JsonParseException | NumberFormatException e) {
            writeError(response, callback, 400, "Malformed request: " + e.getMessage());
        } catch (InterruptedException e) {
            // Server is shutting down under this request — restore the flag and
            // tell the client rather than swallowing it.
            Thread.currentThread().interrupt();
            writeError(response, callback, 503, "Server shutting down");
        } catch (Exception e) {
            writeError(response, callback, 500, "Internal server error");
        }

        return true;
    }

    // ── /queues ───────────────────────────────────────────────────────────────

    private void routeQueues(String[] segments, String method, Request request,
            Response response, Callback callback) throws Exception {
        {
            switch (segments.length) {
                case 2 -> { // /queues
                    if (method.equals("GET")) {
                        handleListQueues(response, callback);
                    } else {
                        writeError(response, callback, 405, "Method not allowed");
                    }
                }
                case 3 -> { // /queues/{name}
                    if (method.equals("POST")) {
                        handleCreateQueue(segments[2], request, response, callback);
                    } else if (method.equals("DELETE")) {
                        handleDeleteQueue(segments[2], response, callback);
                    } else {
                        writeError(response, callback, 405, "Method not allowed");
                    }
                }
                case 4 -> { // /queues/{name}/messages
                    if (!segments[3].equals("messages")) {
                        writeError(response, callback, 404, "Not found");
                    } else if (method.equals("POST")) {
                        handlePublish(segments[2], request, response, callback);
                    } else if (method.equals("GET")) {
                        handleConsume(segments[2], waitSecondsOf(request), response, callback);
                    } else {
                        writeError(response, callback, 405, "Method not allowed");
                    }
                }
                case 5 -> { // /queues/{name}/messages/{handle}
                    if (!segments[3].equals("messages")) {
                        writeError(response, callback, 404, "Not found");
                    } else if (method.equals("DELETE")) {
                        handleAcknowledge(segments[2], segments[4], response, callback);
                    } else {
                        writeError(response, callback, 405, "Method not allowed");
                    }
                }
                case 6 -> { // /queues/{name}/messages/{handle}/nack
                    if (!segments[3].equals("messages") || !segments[5].equals("nack")) {
                        writeError(response, callback, 404, "Not found");
                    } else if (method.equals("POST")) {
                        handleNack(segments[2], segments[4], response, callback);
                    } else {
                        writeError(response, callback, 405, "Method not allowed");
                    }
                }
                default -> writeError(response, callback, 404, "Not found");
            }
        }
    }

    // ── /topics ───────────────────────────────────────────────────────────────

    private void routeTopics(String[] segments, String method, Request request,
            Response response, Callback callback) throws Exception {
        switch (segments.length) {
            case 2 -> { // /topics
                if (method.equals("GET")) {
                    handleListTopics(response, callback);
                } else {
                    writeError(response, callback, 405, "Method not allowed");
                }
            }
            case 3 -> { // /topics/{name}
                if (method.equals("POST")) {
                    handleCreateTopic(segments[2], response, callback);
                } else if (method.equals("DELETE")) {
                    handleDeleteTopic(segments[2], response, callback);
                } else {
                    writeError(response, callback, 405, "Method not allowed");
                }
            }
            case 4 -> { // /topics/{name}/messages | /topics/{name}/subscriptions
                switch (segments[3]) {
                    case "messages" -> {
                        if (method.equals("POST")) {
                            handleTopicPublish(segments[2], request, response, callback);
                        } else if (method.equals("GET")) {
                            // A topic routes, it does not store. There is
                            // nothing to consume from here — only from a
                            // subscriber queue. The path exists, the operation
                            // does not, so this is 405 rather than 404.
                            writeError(response, callback, 405,
                                    "Cannot consume from a topic — consume from a subscriber queue instead");
                        } else {
                            writeError(response, callback, 405, "Method not allowed");
                        }
                    }
                    case "subscriptions" -> {
                        if (method.equals("GET")) {
                            handleListSubscriptions(segments[2], response, callback);
                        } else {
                            writeError(response, callback, 405, "Method not allowed");
                        }
                    }
                    default -> writeError(response, callback, 404, "Not found");
                }
            }
            case 5 -> { // /topics/{name}/subscriptions/{queue}
                if (!segments[3].equals("subscriptions")) {
                    writeError(response, callback, 404, "Not found");
                } else if (method.equals("POST")) {
                    handleSubscribe(segments[2], segments[4], response, callback);
                } else if (method.equals("DELETE")) {
                    handleUnsubscribe(segments[2], segments[4], response, callback);
                } else {
                    writeError(response, callback, 405, "Method not allowed");
                }
            }
            default -> writeError(response, callback, 404, "Not found");
        }
    }

    // GET /topics → 200 {"topics":["orders-events"]}
    private void handleListTopics(Response response, Callback callback) {
        writeJson(response, callback, 200, jsonNameArray("topics", topicManager.listTopics()));
    }

    // POST /topics/{name} → 201 {"name":"orders-events"}
    private void handleCreateTopic(String name, Response response, Callback callback) {
        topicManager.createTopic(name);
        writeJson(response, callback, 201, JsonUtils.toJson(Map.of("name", name)));
    }

    // DELETE /topics/{name} → 204
    private void handleDeleteTopic(String name, Response response, Callback callback) {
        topicManager.getTopic(name); // 404 rather than a cheerful 204
        topicManager.deleteTopic(name);
        writeEmpty(response, callback, 204);
    }

    // GET /topics/{name}/subscriptions → 200 {"subscribers":["billing"]}
    private void handleListSubscriptions(String name, Response response, Callback callback) {
        writeJson(response, callback, 200,
                jsonNameArray("subscribers", topicManager.getTopic(name).listSubscribers()));
    }

    // POST /topics/{name}/subscriptions/{queue} → 201
    private void handleSubscribe(String topicName, String queueName,
            Response response, Callback callback) {
        Topic topic = topicManager.getTopic(topicName);

        // Resolve through the QueueManager so an unknown queue is a 404 here,
        // rather than a subscription that silently never delivers.
        topic.subscribe(queueManager.getQueue(queueName));

        writeJson(response, callback, 201,
                JsonUtils.toJson(Map.of("topic", topicName, "queue", queueName)));
    }

    // DELETE /topics/{name}/subscriptions/{queue} → 204
    private void handleUnsubscribe(String topicName, String queueName,
            Response response, Callback callback) {
        topicManager.getTopic(topicName).unsubscribe(queueName);
        writeEmpty(response, callback, 204);
    }

    // POST /topics/{name}/messages → 201 {"messageId":"42","delivered":"3"}
    private void handleTopicPublish(String name, Request request,
            Response response, Callback callback) throws Exception {
        Topic topic = topicManager.getTopic(name);

        Map<String, String> fields = JsonUtils.fromJson(readBody(request));
        String payload = fields.get("payload");
        if (payload == null) {
            writeError(response, callback, 400, "Body must contain a 'payload' field");
            return;
        }

        Message message = new Message(payload);
        int delivered = topic.publish(message);

        // delivered is reported as a string like every other field — JsonUtils
        // serializes a flat string map, and the HTTP API has been consistent
        // about that since Phase 4.
        Map<String, String> body = new LinkedHashMap<>();
        body.put("messageId", message.getId());
        body.put("delivered", String.valueOf(delivered));
        writeJson(response, callback, 201, JsonUtils.toJson(body));
    }

    // ── /logs ─────────────────────────────────────────────────────────────────

    private void routeLogs(String[] segments, String method, Request request,
            Response response, Callback callback) throws Exception {
        switch (segments.length) {
            case 2 -> { // /logs
                if (method.equals("GET")) {
                    writeJson(response, callback, 200,
                            jsonNameArray("logs", logManager.listLogs()));
                } else {
                    writeError(response, callback, 405, "Method not allowed");
                }
            }
            case 3 -> { // /logs/{name}
                if (method.equals("POST")) {
                    handleCreateLog(segments[2], request, response, callback);
                } else if (method.equals("DELETE")) {
                    logManager.getLog(segments[2]); // 404 rather than a cheerful 204
                    logManager.deleteLog(segments[2]);
                    writeEmpty(response, callback, 204);
                } else {
                    writeError(response, callback, 405, "Method not allowed");
                }
            }
            case 4 -> { // /logs/{name}/records
                if (!segments[3].equals("records")) {
                    writeError(response, callback, 404, "Not found");
                } else if (method.equals("POST")) {
                    handleAppend(segments[2], request, response, callback);
                } else if (method.equals("GET")) {
                    handlePoll(segments[2], request, response, callback);
                } else {
                    writeError(response, callback, 405, "Method not allowed");
                }
            }
            case 5 -> { // /logs/{name}/groups/{group}
                if (!segments[3].equals("groups")) {
                    writeError(response, callback, 404, "Not found");
                } else if (method.equals("GET")) {
                    handleGroupStatus(segments[2], segments[4], response, callback);
                } else {
                    writeError(response, callback, 405, "Method not allowed");
                }
            }
            case 6 -> { // /logs/{name}/groups/{group}/{commit|seek}
                if (!segments[3].equals("groups") || !method.equals("POST")) {
                    writeError(response, callback, 404, "Not found");
                } else if (segments[5].equals("commit")) {
                    handleCommit(segments[2], segments[4], request, response, callback);
                } else if (segments[5].equals("seek")) {
                    handleSeek(segments[2], segments[4], request, response, callback);
                } else {
                    writeError(response, callback, 404, "Not found");
                }
            }
            default -> writeError(response, callback, 404, "Not found");
        }
    }

    // POST /logs/{name} → 201 {"name":"orders"}
    private void handleCreateLog(String name, Request request, Response response, Callback callback)
            throws Exception {
        Map<String, String> fields = JsonUtils.fromJson(readBody(request));
        LogConfig defaults = LogConfig.defaults();

        long retentionMs = fields.containsKey("retentionMs")
                ? Long.parseLong(fields.get("retentionMs"))
                : defaults.getRetentionMs();
        int maxRecords = fields.containsKey("maxRecords")
                ? Integer.parseInt(fields.get("maxRecords"))
                : defaults.getMaxRecords();
        OffsetResetPolicy policy = fields.containsKey("resetPolicy")
                ? OffsetResetPolicy.valueOf(fields.get("resetPolicy").toUpperCase())
                : defaults.getResetPolicy();

        logManager.createLog(name,
                new LogConfig(retentionMs, maxRecords, policy, fields.get("logDirectory")));
        writeJson(response, callback, 201, JsonUtils.toJson(Map.of("name", name)));
    }

    // POST /logs/{name}/records → 201 {"offset":"42"}
    private void handleAppend(String name, Request request, Response response, Callback callback)
            throws Exception {
        MessageLog log = logManager.getLog(name);

        Map<String, String> fields = JsonUtils.fromJson(readBody(request));
        String payload = fields.get("payload");
        if (payload == null) {
            writeError(response, callback, 400, "Body must contain a 'payload' field");
            return;
        }

        long offset = log.append(new Message(payload));
        writeJson(response, callback, 201,
                JsonUtils.toJson(Map.of("offset", String.valueOf(offset))));
    }

    // GET /logs/{name}/records?group=g&max=n
    //   → 200 records plus the offsets they span, 204 when there is nothing new
    private void handlePoll(String name, Request request, Response response, Callback callback) {
        MessageLog log = logManager.getLog(name);

        String group = Request.extractQueryParameters(request).getValue("group");
        if (group == null || group.isBlank()) {
            writeError(response, callback, 400, "A 'group' query parameter is required");
            return;
        }

        String maxParam = Request.extractQueryParameters(request).getValue("max");
        int max = (maxParam == null || maxParam.isBlank()) ? 10 : Integer.parseInt(maxParam);

        LogRecords batch = log.poll(group, max);
        if (batch.isEmpty()) {
            writeEmpty(response, callback, 204);
            return;
        }

        StringBuilder sb = new StringBuilder("{\"records\":[");
        boolean first = true;
        for (Message message : batch.messages()) {
            if (!first) {
                sb.append(",");
            }
            Map<String, String> record = new LinkedHashMap<>();
            record.put("messageId", message.getId());
            record.put("payload", message.getPayload());
            sb.append(JsonUtils.toJson(record));
            first = false;
        }
        sb.append("],\"startOffset\":\"").append(batch.startOffset())
                .append("\",\"nextOffset\":\"").append(batch.nextOffset()).append("\"}");

        writeJson(response, callback, 200, sb.toString());
    }

    // GET /logs/{name}/groups/{group} → committed, endOffset, lag
    private void handleGroupStatus(String name, String group, Response response, Callback callback) {
        MessageLog log = logManager.getLog(name);

        Map<String, String> body = new LinkedHashMap<>();
        body.put("group", group);
        body.put("committed", String.valueOf(log.committed(group)));
        body.put("beginOffset", String.valueOf(log.beginOffset()));
        body.put("endOffset", String.valueOf(log.endOffset()));
        body.put("lag", String.valueOf(log.lag(group)));
        writeJson(response, callback, 200, JsonUtils.toJson(body));
    }

    // POST /logs/{name}/groups/{group}/commit → 204, body {"offset":42}
    private void handleCommit(String name, String group, Request request,
            Response response, Callback callback) throws Exception {
        MessageLog log = logManager.getLog(name);

        String offset = JsonUtils.fromJson(readBody(request)).get("offset");
        if (offset == null) {
            writeError(response, callback, 400, "Body must contain an 'offset' field");
            return;
        }

        log.commit(group, Long.parseLong(offset));
        writeEmpty(response, callback, 204);
    }

    // POST /logs/{name}/groups/{group}/seek → 204
    // Body is {"offset":n} or {"position":"earliest"|"latest"}
    private void handleSeek(String name, String group, Request request,
            Response response, Callback callback) throws Exception {
        MessageLog log = logManager.getLog(name);
        Map<String, String> fields = JsonUtils.fromJson(readBody(request));

        String offset = fields.get("offset");
        if (offset != null) {
            log.seek(group, Long.parseLong(offset));
            writeEmpty(response, callback, 204);
            return;
        }

        String position = fields.get("position");
        if (position == null) {
            writeError(response, callback, 400,
                    "Body must contain either 'offset' or 'position'");
            return;
        }

        switch (position.toLowerCase()) {
            case "earliest" -> log.seekToBeginning(group);
            case "latest" -> log.seekToEnd(group);
            default -> {
                writeError(response, callback, 400,
                        "'position' must be 'earliest' or 'latest'");
                return;
            }
        }
        writeEmpty(response, callback, 204);
    }

    /**
     * One snapshot of everything, for the dashboard.
     *
     * A single call rather than a request per resource: the page would
     * otherwise need N+1 round trips and would render torn state, with queues
     * from one instant next to logs from another.
     *
     * Built by hand because this response is nested, and JsonUtils is a flat
     * parser by design. Browsers have a real JSON parser; the constraint only
     * ever bound our own reading of these bodies.
     */
    private String buildStats() {
        StringBuilder sb = new StringBuilder("{\"queues\":[");

        boolean first = true;
        for (String name : queueManager.listQueues()) {
            MessageQueue queue = queueManager.findQueue(name);
            if (queue == null) {
                continue;
            }
            if (!first) {
                sb.append(",");
            }
            Map<String, String> fields = new LinkedHashMap<>();
            fields.put("name", name);
            fields.put("depth", String.valueOf(queue.depth()));
            fields.put("inFlight", String.valueOf(queue.inFlightCount()));
            fields.put("waiters", String.valueOf(queue.waiterCount()));
            fields.put("deadLetterQueue", queue.getDeadLetterQueueName());
            fields.put("persistent", String.valueOf(queue.getConfig().getLogDirectory() != null));
            sb.append(JsonUtils.toJson(fields));
            first = false;
        }

        sb.append("],\"topics\":[");
        first = true;
        for (String name : topicManager.listTopics()) {
            if (!first) {
                sb.append(",");
            }
            sb.append("{\"name\":\"").append(JsonUtils.escape(name)).append("\",\"subscribers\":[");
            boolean firstSub = true;
            for (String subscriber : topicManager.getTopic(name).listSubscribers()) {
                if (!firstSub) {
                    sb.append(",");
                }
                sb.append("\"").append(JsonUtils.escape(subscriber)).append("\"");
                firstSub = false;
            }
            sb.append("]}");
            first = false;
        }

        sb.append("],\"logs\":[");
        first = true;
        for (String name : logManager.listLogs()) {
            MessageLog log = logManager.getLog(name);
            if (!first) {
                sb.append(",");
            }
            sb.append("{\"name\":\"").append(JsonUtils.escape(name)).append("\"")
                    .append(",\"beginOffset\":").append(log.beginOffset())
                    .append(",\"endOffset\":").append(log.endOffset())
                    .append(",\"records\":").append(log.recordCount())
                    .append(",\"maxRecords\":").append(log.getConfig().getMaxRecords())
                    .append(",\"retentionMs\":").append(log.getConfig().getRetentionMs())
                    .append(",\"resetPolicy\":\"").append(log.getConfig().getResetPolicy())
                    .append("\",\"groups\":[");
            boolean firstGroup = true;
            for (String group : log.groups()) {
                if (!firstGroup) {
                    sb.append(",");
                }
                sb.append("{\"name\":\"").append(JsonUtils.escape(group)).append("\"")
                        .append(",\"committed\":").append(log.committed(group))
                        .append(",\"position\":").append(log.position(group))
                        .append(",\"lag\":").append(log.lag(group)).append("}");
                firstGroup = false;
            }
            sb.append("]}");
            first = false;
        }

        return sb.append("]}").toString();
    }

    private void writeHtml(Response response, Callback callback, String html) {
        response.getHeaders().put("Content-Type", "text/html; charset=utf-8");
        response.setStatus(200);
        response.write(true, ByteBuffer.wrap(html.getBytes(StandardCharsets.UTF_8)), callback);
    }

    // Renders {"<key>":["a","b"]} with names escaped.
    // Fully qualified: this class extends Handler.Abstract, which inherits a
    // nested Handler.Collection that would otherwise shadow java.util.Collection.
    private String jsonNameArray(String key, java.util.Collection<String> names) {
        StringBuilder sb = new StringBuilder("{\"").append(key).append("\":[");
        boolean first = true;
        for (String name : names) {
            if (!first) {
                sb.append(",");
            }
            sb.append("\"").append(JsonUtils.escape(name)).append("\"");
            first = false;
        }
        return sb.append("]}").toString();
    }

    // GET /queues → 200 {"queues":["orders","payments"]}
    private void handleListQueues(Response response, Callback callback) {
        StringBuilder sb = new StringBuilder("{\"queues\":[");
        boolean first = true;
        for (String name : queueManager.listQueues()) {
            if (!first) {
                sb.append(",");
            }
            sb.append("\"").append(JsonUtils.escape(name)).append("\"");
            first = false;
        }
        sb.append("]}");
        writeJson(response, callback, 200, sb.toString());
    }

    // POST /queues/{name} → 201 {"name":"orders"}
    // Optional JSON body: visibilityTimeoutMs, maxRetries, deadLetterQueueName, logDirectory
    private void handleCreateQueue(String name, Request request, Response response, Callback callback)
            throws Exception {
        Map<String, String> fields = JsonUtils.fromJson(readBody(request));

        QueueConfig defaults = QueueConfig.defaults();
        long visibilityTimeout = fields.containsKey("visibilityTimeoutMs")
                ? Long.parseLong(fields.get("visibilityTimeoutMs"))
                : defaults.getVisibilityTimeoutMs();
        int maxRetries = fields.containsKey("maxRetries")
                ? Integer.parseInt(fields.get("maxRetries"))
                : defaults.getMaxRetries();
        String dlqName = fields.get("deadLetterQueueName");
        String logDirectory = fields.get("logDirectory");

        queueManager.createQueue(name,
                new QueueConfig(visibilityTimeout, maxRetries, dlqName, logDirectory));

        writeJson(response, callback, 201, JsonUtils.toJson(Map.of("name", name)));
    }

    // DELETE /queues/{name} → 204
    private void handleDeleteQueue(String name, Response response, Callback callback) {
        // getQueue throws QueueNotFoundException — deleteQueue alone is silent
        // on a missing queue, and a DELETE of something that never existed
        // should be a 404, not a cheerful 204.
        queueManager.getQueue(name);
        queueManager.deleteQueue(name);
        writeEmpty(response, callback, 204);
    }

    // POST /queues/{name}/messages → 201 {"messageId":"42"}
    private void handlePublish(String name, Request request, Response response, Callback callback)
            throws Exception {
        MessageQueue queue = queueManager.getQueue(name);

        Map<String, String> fields = JsonUtils.fromJson(readBody(request));
        String payload = fields.get("payload");
        if (payload == null) {
            writeError(response, callback, 400, "Body must contain a 'payload' field");
            return;
        }

        Message message = new Message(payload);
        queue.publish(message);
        writeJson(response, callback, 201, JsonUtils.toJson(Map.of("messageId", message.getId())));
    }

    /**
     * GET /queues/{name}/messages?waitSeconds=n
     *   → 200 {"messageId":..,"payload":..,"receiptHandle":..} when a message arrives
     *   → 204 when the wait expires with the queue still empty
     *
     * This does not block. Blocking here would hold a Jetty pool thread for the
     * whole wait, so N idle long-pollers would consume N threads and a few slow
     * clients could starve the pool — which is why the wait used to be capped.
     *
     * Instead the request registers interest and returns; the response is
     * completed later, either by the publish that hands over a message or by
     * the scheduled timeout. Jetty allows this because handle() returning true
     * means "I will complete the callback", not "I have completed it".
     */
    private void handleConsume(String name, int waitSeconds, Response response, Callback callback) {
        MessageQueue queue = queueManager.getQueue(name);

        // Whichever of delivery and timeout gets here first owns the response.
        AtomicBoolean responded = new AtomicBoolean();
        AtomicReference<ScheduledFuture<?>> timeout = new AtomicReference<>();

        MessageQueue.Waiter waiter = queue.consumeAsync(receipt -> {
            if (responded.compareAndSet(false, true)) {
                ScheduledFuture<?> scheduled = timeout.get();
                if (scheduled != null) {
                    scheduled.cancel(false);
                }
                writeReceipt(response, callback, receipt);
            }
        });

        // consumeAsync runs the handler inline when a message was already
        // available, so this may already be settled.
        if (responded.get()) {
            return;
        }

        if (waitSeconds <= 0) {
            if (waiter.cancel() && responded.compareAndSet(false, true)) {
                writeEmpty(response, callback, 204);
            }
            return;
        }

        timeout.set(timeouts.schedule(() -> {
            if (waiter.cancel() && responded.compareAndSet(false, true)) {
                writeEmpty(response, callback, 204);
            }
        }, waitSeconds, TimeUnit.SECONDS));
    }

    private void writeReceipt(Response response, Callback callback, Receipt receipt) {
        // LinkedHashMap, not Map.of — response field order should be stable.
        Map<String, String> body = new LinkedHashMap<>();
        body.put("messageId", receipt.getMessage().getId());
        body.put("payload", receipt.getMessage().getPayload());
        body.put("receiptHandle", receipt.getReceiptHandle());
        writeJson(response, callback, 200, JsonUtils.toJson(body));
    }

    // DELETE /queues/{name}/messages/{handle} → 204
    private void handleAcknowledge(String name, String handle, Response response, Callback callback) {
        queueManager.getQueue(name).acknowledge(handle);
        writeEmpty(response, callback, 204);
    }

    // POST /queues/{name}/messages/{handle}/nack → 204
    private void handleNack(String name, String handle, Response response, Callback callback) {
        queueManager.getQueue(name).nack(handle);
        writeEmpty(response, callback, 204);
    }

    // waitSeconds query param, clamped to [0, MAX_WAIT_SECONDS].
    private int waitSecondsOf(Request request) {
        String param = Request.extractQueryParameters(request).getValue("waitSeconds");
        if (param == null || param.isBlank()) {
            return DEFAULT_WAIT_SECONDS;
        }
        int requested = Integer.parseInt(param);
        return Math.clamp(requested, 0, MAX_WAIT_SECONDS);
    }

    // Reads the full request body. Content.Source.asString blocks until the
    // last chunk arrives — a single request.read() only returns whatever chunk
    // happens to be available, which is not the same thing.
    private String readBody(Request request) throws Exception {
        return Content.Source.asString(request, StandardCharsets.UTF_8);
    }

    // Sets Content-Type, status code, and writes the JSON body
    private void writeJson(Response response, Callback callback, int status, String json) {
        response.getHeaders().put("Content-Type", "application/json");
        response.setStatus(status);
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        response.write(true, ByteBuffer.wrap(bytes), callback);
    }

    // Status-only response with no body (204s).
    private void writeEmpty(Response response, Callback callback, int status) {
        response.setStatus(status);
        response.write(true, ByteBuffer.allocate(0), callback);
    }

    // Writes an error JSON response with the given status code
    private void writeError(Response response, Callback callback, int status, String message) {
        writeJson(response, callback, status, JsonUtils.errorJson(message));
    }
}
