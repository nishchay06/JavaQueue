package com.javaqueue.server;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

import org.eclipse.jetty.io.Content;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.util.Callback;

import com.javaqueue.core.MessageQueue;
import com.javaqueue.core.Message;
import com.javaqueue.core.QueueConfig;
import com.javaqueue.core.QueueManager;
import com.javaqueue.core.Receipt;
import com.javaqueue.exception.InvalidReceiptException;
import com.javaqueue.exception.QueueNotFoundException;
import com.javaqueue.json.JsonUtils;

/**
 * Routes HTTP requests onto the QueueManager.
 *
 * | Method | Path                                    |
 * |--------|-----------------------------------------|
 * | GET    | /queues                                 |
 * | POST   | /queues/{name}                          |
 * | DELETE | /queues/{name}                          |
 * | POST   | /queues/{name}/messages                 |
 * | GET    | /queues/{name}/messages?waitSeconds=n   |
 * | DELETE | /queues/{name}/messages/{handle}        |
 * | POST   | /queues/{name}/messages/{handle}/nack   |
 */
public class QueueHandler extends Handler.Abstract {

    static final int DEFAULT_WAIT_SECONDS = 5;

    // Long polling parks a Jetty pool thread for the whole wait. Left uncapped,
    // a handful of slow clients would starve the pool — so the wait is bounded.
    static final int MAX_WAIT_SECONDS = 20;

    private final QueueManager queueManager;

    public QueueHandler(QueueManager manager) {
        this.queueManager = manager;
    }

    @Override
    public boolean handle(Request request, Response response, Callback callback) throws Exception {
        String path = request.getHttpURI().getPath();
        String method = request.getMethod();

        // path="/queues/orders/messages" → segments=["","queues","orders","messages"]
        String[] segments = path.split("/");

        if (segments.length < 2 || !segments[1].equals("queues")) {
            writeError(response, callback, 404, "Not found");
            return true;
        }

        try {
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
        } catch (QueueNotFoundException e) {
            writeError(response, callback, 404, e.getMessage());
        } catch (InvalidReceiptException e) {
            writeError(response, callback, 404, e.getMessage());
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

    // GET /queues/{name}/messages?waitSeconds=n
    //   → 200 {"messageId":..,"payload":..,"receiptHandle":..} when a message arrives
    //   → 204 when the wait expires with the queue still empty
    private void handleConsume(String name, int waitSeconds, Response response, Callback callback)
            throws InterruptedException {
        MessageQueue queue = queueManager.getQueue(name);

        Receipt receipt = queue.consume(waitSeconds * 1000L);
        if (receipt == null) {
            writeEmpty(response, callback, 204);
            return;
        }

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
