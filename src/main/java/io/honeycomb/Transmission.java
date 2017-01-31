package io.honeycomb;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.*;

/**
 * Sends messages and receives responses with honeycomb.io.
 */
public class Transmission {
    /**
     * Request queue contains Events that will soon be sent as HTTP requests.
     * Response queue contains JSONObjects that were recently received as HTTP responses.
     */
    private ArrayBlockingQueue<Object> requestQueue;
    private ArrayBlockingQueue<JSONObject> responseQueue;
    private final ExecutorService executor;
    private final Object POISON_PILL = new Object();

    // Metadata
    private String apiHost;
    private boolean blockOnSend;
    private boolean blockOnResponse;
    private int closeTimeout;
    private final int maxConcurrentBranches;
    private String userAgent;

    // Logging
    private final Log log = LogFactory.getLog(Transmission.class);

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
        this.userAgent = builder.userAgent;

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
                        this.send((Event) request);
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
        private String userAgent;

        // Passed in global state
        public Builder(LibHoney libhoney) {
            this.apiHost = libhoney.getApiHost();
            this.maxConcurrentBranches = libhoney.getMaxConcurrentBranches();
            this.blockOnSend = libhoney.getBlockOnSend();
            this.blockOnResponse = libhoney.getBlockOnResponse();
            this.closeTimeout = libhoney.getCloseTimeout();
            this.requestQueueLength = libhoney.getRequestQueueLength();
            this.responseQueueLength = libhoney.getResponseQueueLength();
            this.userAgent = libhoney.getUserAgent();
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

        public Builder responseQueueLength(int responseQueueLength) {
            this.responseQueueLength = responseQueueLength;
            return this;
        }

        public Builder userAgent(String agent) {
            this.userAgent = agent;
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
        this.executor.shutdown();
        try {
            this.requestQueue.offer(POISON_PILL); // Does not acknowledge blockOnSend
            this.executor.awaitTermination(this.closeTimeout, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.error(e);
        } finally {
            this.executor.shutdownNow();
        }
    }

    /**
     * Returns an HTTP POST request built from the specified Event.
     *
     * @param event the data to be sent in an HTTP POST request
     * @return an HTTP POST request
     */
    private HttpPost createHttpRequest(Event event) {
        HttpPost post = new HttpPost(this.apiHost + "/1/events/" + event.getDataSet());

        post.setHeader("User-Agent", this.userAgent);
        post.setHeader("X-Honeycomb-Team", event.getWriteKey());
        post.setHeader("X-Honeycomb-SampleRate", Integer.toString(event.getSampleRate()));
        post.setHeader("X-Honeycomb-Event-Time", event.getCreatedAt());
        post.setEntity(new StringEntity(new JSONObject(event.getFields()).toString(), ContentType.APPLICATION_JSON));

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

        JSONObject json = new JSONObject();
        try {
            json.put("status_code", httpResponse.getStatusLine().getStatusCode());
            json.put("duration", end - start);
            json.put("metadata", metadata);
            json.put("body", EntityUtils.toString(httpResponse.getEntity()));
            json.put("error", "");
        } catch (Exception e) {
            log.error(e);
        }

        return json;
    }

    /**
     * Adds a Event to this Transmission's request queue.
     *
     * @param event Event to be enqueued
     */
    public void enqueueRequest(Object event) {
        if (this.blockOnSend) {
            try {
                this.requestQueue.put(event);
            } catch (InterruptedException e) {
                log.error(e);
            }
        } else {
            try {
                this.requestQueue.add(event);
            } catch (IllegalStateException e) {
                log.debug("queue_overflow");
                if (event.getClass() == Event.class) {
                    this.enqueueResponse(this.createJsonError("event dropped; queue overflow",
                            ((Event) event).getMetadata()));
                } else {
                    this.enqueueResponse(this.createJsonError("event dropped; queue overflow", ""));
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
     * Send an HTTP request based on a Event, wait for a response, then enqueue the response.
     *
     * @param event HonyEvent from which the HTTP request is built
     */
    protected void send(Event event) {
        long start = System.currentTimeMillis();

        // Configure request
        HttpPost post = this.createHttpRequest(event);

        // Execute request
        HttpResponse response = null;
        try {
            response = (new DefaultHttpClient()).execute(post);
        } catch (IOException e) {
            log.error(e);
        }

        // Interpret response
        JSONObject json = this.createJsonResponse(response, event.getMetadata(), start);

        // Enqueue response
        this.enqueueResponse(json);
    }

    /**
     * Enqueue a response indicating that a Event was dropped due to sample rate, including its metadata string.
     * @param metadata metadata string used for debugging
     */
    public void sendDroppedResponse(String metadata) {
        JSONObject json = this.createJsonError("event dropped due to sampling", metadata);
        this.enqueueResponse(json);
    }

    /**
     * Sets the api host
     * @param apiHost api host
     */
    public void setApiHost(String apiHost) {
        this.apiHost = apiHost;
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
     * Sets if threads should block on response
     * @param blockOnResponse block on response
     */
    public void setBlockOnResponse(boolean blockOnResponse) {
        this.blockOnResponse = blockOnResponse;
    }

    /**
     * Sets if threads should block on send
     * @param blockOnSend block on send
     */
    public void setBlockOnSend(boolean blockOnSend) {
        this.blockOnSend = blockOnSend;
    }

    /**
     * Sets the thread .close timeout
     * @param closeTimeout .close timeout
     */
    public void setCloseTimeout(int closeTimeout) {
        this.closeTimeout = closeTimeout;
    }

    /**
     * Sets the user agent
     * @param userAgent userAgent
     */
    public void setUserAgent(String userAgent) {
        this.userAgent = userAgent;
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
