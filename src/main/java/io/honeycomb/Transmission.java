package io.honeycomb;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Queue;
import java.util.concurrent.*;

/**
 * Sends messages and receives responses with honeycomb.io.
 */
public class Transmission {
    /**
     * Request queue contains HoneyEvents that will soon be sent as HTTP requests.
     * Response queue contains JSONObjects that were recently received as HTTP responses.
     */
    private ArrayBlockingQueue<Object> requestQueue;
    private ArrayBlockingQueue<JSONObject> responseQueue;
    private final ExecutorService executor;

    // Metadata
    private final String apiHost;
    private final int maxConcurrentBranches;
    private final boolean blockOnSend;
    private final boolean blockOnResponse;
    private final int closeTimeout;

    // Logging
    private final Log log = LogFactory.getLog(Transmission.class);


    // For closing all threads
    private final Object POISON_PILL = new Object();

    /**
     * Constructs a Transmission from a Transmission.Builder.
     * Initializes and dispatches a number of threads based on builder.maxConcurrentBranches.
     * @param builder the builder to build this Transmission
     */
    private Transmission(Builder builder) {
        this.apiHost = builder.apiHost;
        this.maxConcurrentBranches = builder.maxConcurrentBranches;
        this.blockOnSend = builder.blockOnSend;
        this.blockOnResponse = builder.blockOnResponse;
        this.closeTimeout = builder.closeTimeout;
        this.requestQueue = new ArrayBlockingQueue<>(builder.requestQueueLength);
        this.responseQueue = new ArrayBlockingQueue<>(builder.responseQueueLength);

        /**
         * Blocks on requestQueue.take(), handling and usually sending a request when it is taken
         */
        this.executor = Executors.newFixedThreadPool(this.maxConcurrentBranches);
        for (int i = 0; i < maxConcurrentBranches; i++) {
            this.executor.submit(() -> {
                try {
                    while (!Thread.currentThread().isInterrupted()) {
                        Object request = requestQueue.take();
                        if (request == POISON_PILL) {
                            log.debug("killing thread " + Thread.currentThread().getId());
                            this.enqueueRequest(POISON_PILL);
                            return;
                        }
                        this.send((HoneyEvent) request);
                    }
                } catch (Exception e) {
                    log.error(e);
                }
            });
        }
    }

    /**
     * Transmission.Builder
     */
    public static class Builder {
        private String apiHost;
        private int maxConcurrentBranches;
        private boolean blockOnSend;
        private boolean blockOnResponse;
        private int closeTimeout;
        private int requestQueueLength;
        private int responseQueueLength;

        // Passed in global state
        public Builder(LibHoney libhoney) {
            this.apiHost = libhoney.getApiHost();
            this.maxConcurrentBranches = libhoney.getMaxConcurrentBranches();
            this.blockOnSend = libhoney.getBlockOnSend();
            this.blockOnResponse = libhoney.getBlockOnResponse();
            this.closeTimeout = libhoney.getCloseTimeout();
            this.requestQueueLength = libhoney.getRequestQueueLength();
            this.responseQueueLength = libhoney.getResponseQueueLength();
        }

        public Builder apiHost(String apiHost) {
            this.apiHost = apiHost;
            return this;
        }

        public Builder maxConcurrentBranches(int maxConcurrentBranches) {
            this.maxConcurrentBranches = maxConcurrentBranches;
            return this;
        }

        public Builder blockOnSend(boolean blockOnSend) {
            this.blockOnSend = blockOnSend;
            return this;
        }

        public Builder blockOnResponse(boolean blockOnResponse) {
            this.blockOnResponse = blockOnResponse;
            return this;
        }

        public Builder closeTimeout(int closeTimeout) {
            this.closeTimeout = closeTimeout;
            return this;
        }

        public Builder requestQueueLength(int requestQueueLength) {
            this.requestQueueLength = requestQueueLength;
            return this;
        }

        public Builder getResponseQueueLength(int getResponseQueueLength) {
            this.responseQueueLength = getResponseQueueLength;
            return this;
        }

        public Transmission build() {
            return new Transmission(this);
        }
    }

    /**
     * Closes Transmission by enqueuing a POISON_PILL which causes each thread to return,
     * then shuts down the executor and awaits this.closeTimeout seconds before timing out.
     */
    public void close() {
        this.enqueueRequest(POISON_PILL);
        this.executor.shutdown();
        try {
            this.executor.awaitTermination(this.closeTimeout, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.error(e);
        }
    }

    /**
     * Returns an HTTP POST request built from the specified HoneyEvent.
     *
     * @param honeyEvent the data to be sent in an HTTP POST request
     * @return an HTTP POST request
     */
    private HttpPost createHttpRequest(HoneyEvent honeyEvent) {
        HttpPost post = new HttpPost(this.apiHost + "/1/events/" + honeyEvent.getDataSet());

        post.setHeader("User-Agent", "libhoney-java/" + Constants.LIBHONEY_VERSION);
        post.setHeader("X-Honeycomb-Team", honeyEvent.getWriteKey());
        post.setHeader("X-Honeycomb-SampleRate", Integer.toString(honeyEvent.getSampleRate()));
        post.setHeader("X-Honeycomb-Event-Time", honeyEvent.getCreatedAt());
        post.setHeader("X-Honeycomb-Samplerate", Integer.toString(honeyEvent.getSampleRate()));
        try {
            post.setEntity( new StringEntity(honeyEvent.getFieldHolder().toJson().getString("fields"),
                    ContentType.APPLICATION_JSON));
        } catch (JSONException e) {
            log.error(e);
        }

        return post;
    }

    /**
     * Returns a JSONObject indicating that a specified error has occurred.
     *
     * @param error string describing specified error
     * @param metadata metadata string used for debugging
     * @return a JSONObject indicating that a specified error has occurred
     */
    protected JSONObject createJsonError(String error, String metadata) {
        JSONObject json = new JSONObject();
        try {
            json.put("status_code", 0);
            json.put("duration", 0);
            json.put("metadata", metadata);
            json.put("body", "");
            json.put("error", error);
        } catch (Exception e) {
            log.error(e);
        }
        return json;
    }

    /**
     * Returns a JSONObject based on the HTTP response received from honeycomb.io.
     * @param httpResponse HTTP response received
     * @param metadata metadata string used for debugging
     * @param start current time in ms when the request started
     * @return a JSONObject based on the HTTP response received from honeycomb.io
     */
    private JSONObject createJsonResponse(HttpResponse httpResponse, String metadata, long start)  {
        long end = System.currentTimeMillis();
        BufferedReader rd;
        StringBuilder result = new StringBuilder();
        String line;
        try {
            rd = new BufferedReader(new InputStreamReader(httpResponse.getEntity().getContent()));
            while ((line = rd.readLine()) != null) {
                result.append(line);
            }
        } catch (IOException e) {
            log.error(e);
        }

        if (httpResponse.getStatusLine().getStatusCode() == 200) {
            log.debug("messages sent");
        } else {
            log.debug("send_errors");
        }

        JSONObject json = new JSONObject();
        try {
            json.put("status_code", httpResponse.getStatusLine().getStatusCode());
            json.put("duration", end - start);
            json.put("metadata", metadata);
            json.put("body", result.toString());
            json.put("error", "");
        } catch (JSONException e) {
            log.error(e);
        }
        return json;
    }

    /**
     * Adds a HoneyEvent to this Transmission's request queue.
     *
     * @param honeyEvent HoneyEvent to be enqueued
     */
    public void enqueueRequest(Object honeyEvent) {
        if (this.blockOnSend) {
            try {
                this.requestQueue.put(honeyEvent);
            } catch (InterruptedException e) {
                log.error(e);
            }
        } else {
            try {
                this.requestQueue.add(honeyEvent);
            } catch (IllegalStateException e) {
                log.debug("queue_overflow");
                if (honeyEvent.getClass() == HoneyEvent.class) {
                    this.enqueueResponse(this.createJsonError("event dropped; queue overflow",
                            ((HoneyEvent) honeyEvent).getMetadata()));
                } else {
                    this.enqueueResponse(this.createJsonError("event dropped; queue overflow",
                            ""));
                }
            }
        }
    }

    /**
     * Adds a JSONObject to this Transmission's response queue.
     *
     * @param json JSONObject to be enqueued
     */
    public void enqueueResponse(JSONObject json) {
        if (this.blockOnResponse) {
            try {
                responseQueue.put(json);
            } catch (InterruptedException e) {
                log.error(e);
            }
        } else {
            if (responseQueue.offer(json)) {
                log.debug("message_queued");
            } else {
                log.debug("queue_overflow");
            }
        }
    }

    /**
     * Returns the API host for this Transmission.
     * @return the API host for this Transmission
     */
    public String getApiHost() {
        return this.apiHost;
    }

    /**
     * Returns true if this Transmission should block on response.
     * @return true if this Transmission should block on response
     */
    public boolean getBlockOnResponse() {
        return this.blockOnResponse;
    }

    /**
     * Returns true if this Transmission should block on send.
     * @return true if this Transmission should block on send
     */
    public boolean getBlockOnSend() {
        return this.blockOnSend;
    }

    /**
     * Returns number of seconds Transmission's close method will wait before timing out.
     * @return number of seconds Transmission's close method will wait before timing out
     */
    public int getCloseTimeout() {
        return this.closeTimeout;
    }

    /**
     * Returns this Transmission's thread executor.
     * @return this Transmission's thread executor
     */
    public Executor getExecutor() {
        return this.executor;
    }

    /**
     * Returns this Transmission's queue of requests to be sent.
     * @return this Transmission's queue of requests to be sent.
     */
    public Queue getRequestQueue() {
        return this.requestQueue;
    }

    /**
     * Return this Transmission's queue of received responses.
     * @return this Transmission's queue of received responses.
     */
    public Queue getResponseQueue() {
        return this.responseQueue;
    }

    /**
     * Returns the number of threads to be instantiated on construction.
     * @return the number of threads to be instantiated on construction
     */
    public int getMaxConcurrentBranches() {
        return this.maxConcurrentBranches;
    }

    /**
     * Returns true if all threads are shutdown.
     * @return true if all threads are shutdown
     */
    public boolean isShutdown() {
        return this.executor.isShutdown();
    }

    /**
     * Send an HTTP request based on a HoneyEvent, wait for a response, then enqueue the response.
     *
     * @param honeyEvent HonyEvent from which the HTTP request is built
     */
    protected void send(HoneyEvent honeyEvent) {
        long start = System.currentTimeMillis();

        // Configure request
        HttpPost post = this.createHttpRequest(honeyEvent);

        // Execute request
        HttpResponse response = null;
        try {
            response = (new DefaultHttpClient()).execute(post);
        } catch (IOException e) {
            log.error(e);
        }

        // Interpret response
        JSONObject json = this.createJsonResponse(response, honeyEvent.getMetadata(), start);

        // Enqueue response
        this.enqueueResponse(json);
    }

    /**
     * Sets the request queue (for debugging purposes)
     * @param requestQueue request queue
     */
    protected void setRequestQueue(ArrayBlockingQueue requestQueue) {
        this.requestQueue = requestQueue;
    }

    /**
     * Sets the response queue (for debugging purposes)
     * @param responseQueue response queue
     */
    protected void setResponseQueue(ArrayBlockingQueue responseQueue) {
        this.responseQueue = responseQueue;
    }

    /**
     * Enqueue a response indicating that a HoneyEvent was dropped due to sample rate, including its metadata string.
     * @param metadata metadata string used for debugging
     */
    public void sendDroppedResponse(String metadata) {
        JSONObject json = this.createJsonError("event dropped due to sampling", metadata);
        this.enqueueResponse(json);
    }

    /**
     * Returns a JSON representation of this Transmission.
     * @return a JSON representation of this Transmission
     */
    public JSONObject toJson() {
        JSONObject json = new JSONObject();
        try {
            json.put("apiHost", this.apiHost);
            json.put("maxConcurrentBranches", this.maxConcurrentBranches);
            json.put("blockOnSend", this.blockOnSend);
            json.put("blockOnResponse", this.blockOnResponse);
            json.put("requestQueue", this.requestQueue);
            json.put("responseQueue", this.responseQueue);
        } catch (JSONException e) {
            log.error(e);
        }
        return json;
    }

    /**
     * Returns a string representation of this Transmission.
     * @return a string representation of this Transmission
     */
    @Override
    public String toString() {
        return this.toJson().toString();
    }
}
