package io.honeycomb;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.http.HttpHost;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.json.JSONException;
import org.json.JSONObject;

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

    // Metadata
    private String apiHost;
    private boolean blockOnSend;
    private boolean blockOnResponse;
    private int closeTimeout;
    private final int maxConcurrentBranches;
    private String userAgent;

    private CloseableHttpClient httpClient;
    private PoolingHttpClientConnectionManager conMgr;
    private AtomicBoolean conMgrActive = new AtomicBoolean(false);

    // Logging
    private final Log log = LogFactory.getLog(Transmission.class);

    private void init() {

        // set a bunch of other stuff like header tweaks, threading and gzip here
        conMgr = new PoolingHttpClientConnectionManager();
        // TODO configurable
        conMgr.setMaxTotal(10);
        // TODO configurable? may have different port number?
        HttpRoute route = new HttpRoute(new HttpHost(apiHost, 80));
        // TODO configurabe? but look into perRoute vs. threadPool size interplay
        // let's make it match now since we only have one route
        conMgr.setMaxPerRoute(route,10);

        httpClient = HttpClients.custom()
                .setConnectionManager(conMgr)
                .build();
        conMgrActive.compareAndSet(false, true);

    }

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
     * Wrapper around {@link CloseableHttpClient#close()} which logs any IOExceptions
     * and returns.
     */
    public void close() {
        try {
            httpClient.close();
            conMgr.shutdown();
            conMgrActive.compareAndSet(true, false);
        } catch (IOException ioe) {
            log.error("There was a problem closing the underying HttpClient: ", ioe);
        }
    }

    /**
     * Returns an HTTP POST request built from the specified Event.
     *
     * @param event the data to be sent in an HTTP POST request
     * @return an HTTP POST request
     */
    private HttpPost createHttpRequest(Event event) {
        // TODO we dont need most of this as it's primed in connection manager
        // TODO most of this can be moved up to connection manager as well1
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
     *
     * @return true if {@link #close()} has been called
     */
    public boolean isShutdown() {
        return conMgrActive.get();
    }

    /**
     * Send an HTTP request based on a Event, wait for a response, then enqueue the response.
     *
     * @param event Event from which the HTTP request is built
     */
    protected void send(Event event) {
        long start = System.currentTimeMillis();

        // Configure request
        HttpPost post = this.createHttpRequest(event);

        JSONObject json = new JSONObject();

        // response is AutoCloseable so we try-with-resource
        try (CloseableHttpResponse response = (httpClient).execute(post)) {

            long end = System.currentTimeMillis();

            try {
                json.put("status_code", response.getStatusLine().getStatusCode());
                json.put("duration", end - start);
                json.put("metadata", event.getMetadata());
                json.put("body", EntityUtils.toString(response.getEntity()));
                json.put("error", "");
            } catch (Exception e) {
                log.error(e);
            }
        } catch (IOException e) {
            log.error(e);
        }

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
