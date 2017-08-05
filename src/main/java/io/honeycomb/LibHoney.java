package io.honeycomb;

import java.io.IOException;
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
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Configuration for connections to Honeycomb.io
 */
public final class LibHoney {

    private Transmitter transmission;

    // Metadata
    private final String writeKey;
    private final String dataSet;
    private final int sampleRate;
    private final String apiHost;
    private final int closeTimeout;
    private final String userAgent;

    // Logging
    private final Log log = LogFactory.getLog(LibHoney.class);

    /**
     * Constructs a LibHoney from a LibHoney.Builder
     * @param builder the builder to build this LibHoney
     */
    private LibHoney(Builder builder) {
        this.writeKey = builder.writeKey;
        this.dataSet = builder.dataSet;
        this.sampleRate = builder.sampleRate;
        this.apiHost = builder.apiHost;
        this.closeTimeout = builder.closeTimeout;
        this.userAgent = builder.userAgent;
        this.transmission = new Transmitter();
        transmission.init();
    }

    /**
     * LibHoney.Builder
     */
    public static class Builder {
        private String writeKey = Constants.DEFAULT_WRITE_KEY;
        private String dataSet = Constants.DEFAULT_DATA_SET;
        private int sampleRate = Constants.DEFAULT_SAMPLE_RATE;
        private String apiHost = Constants.DEFAULT_API_HOST;
        private int closeTimeout = Constants.DEFAULT_CLOSE_TIMEOUT;
        private String userAgent = Constants.DEFAULT_USER_AGENT;

        public Builder writeKey(String writeKey) {
            this.writeKey = writeKey;
            return this;
        }

        public Builder dataSet(String dataSet) {
            this.dataSet = dataSet;
            return this;
        }

        public Builder sampleRate(int sampleRate) {
            this.sampleRate = sampleRate;
            return this;
        }

        public Builder apiHost(String apiHost) {
            this.apiHost = apiHost;
            return this;
        }

        public Builder closeTimeout(int closeTimeout) {
            this.closeTimeout = closeTimeout;
            return this;
        }

        public Builder userAgent(String agent) {
            this.userAgent = agent;
            return this;
        }

        public LibHoney build() {
            return new LibHoney(this);
        }
    }


    /**
     * Closes Transmission
     */
    public void close() {
        this.transmission.close();
    }


    /**
     * Creates a Event from this LibHoney's Builder.
     *
     * @return a Event from this LibHoney's Builder
     */
    public Event newEvent() {
        return new Event(dataSet);
    }

    public void send(Event event) {
        this.transmission.send(event);
    }

    /**
     * Returns a JSON representation of this LibHoney.
     * @return a JSON representation of this LibHoney
     */
    public JSONObject toJson() {
        JSONObject json = new JSONObject();
        try {
            json.put("writeKey", this.writeKey);
            json.put("dataSet", this.dataSet);
            json.put("sampleRate", this.sampleRate);
            json.put("apiHost", this.apiHost);
            json.put("closeTimeout", this.closeTimeout);
        } catch (JSONException e) {
            log.error(e);
        }
        return json;
    }

    /**
     * Returns a string representation of this LibHoney.
     * @return a string representation of this LibHoney
     */
    @Override
    public String toString() {
        return this.toJson().toString();
    }

    class Transmitter {

        private CloseableHttpClient httpClient;
        private PoolingHttpClientConnectionManager conMgr;
        private AtomicBoolean conMgrActive = new AtomicBoolean(false);

        private void init() {

            // TODO maybe we just use a single HttpConnection and add back ArrayBlockingQueue
            // - could use Guava's future management as well.
            // - either way, need to take httpSubmission out of the hot path

            // set a bunch of other stuff like header tweaks, threading and gzip here
            conMgr = new PoolingHttpClientConnectionManager();
            // TODO configurable
            conMgr.setMaxTotal(4);
            // TODO configurable? may have different port number?
            HttpRoute route = new HttpRoute(new HttpHost(apiHost, 80));
            // TODO configurabe? but look into perRoute vs. threadPool size interplay
            // let's make it match now since we only have one route
            conMgr.setMaxPerRoute(route,4);
            httpClient = HttpClients.custom()
                    .setConnectionManager(conMgr)
                    .build();
            conMgrActive.compareAndSet(false, true);

        }

        /**
         * Shutdown the underlying connection manager and call close
         * on the HttpClient.
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
            HttpPost post = new HttpPost(apiHost + "/1/events/" + event.getDataSet());

            post.setHeader("User-Agent", userAgent);
            post.setHeader("X-Honeycomb-Team", writeKey);
            post.setHeader("X-Honeycomb-SampleRate", Integer.toString(sampleRate));
            post.setHeader("X-Honeycomb-Event-Time", event.getCreatedAt());
            post.setEntity(new StringEntity(event.toJson().toString(), ContentType.APPLICATION_JSON));

            return post;
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

            // response is AutoCloseable so we try-with-resource
            try (CloseableHttpResponse response = (httpClient).execute(post)) {
                // TODO do something with response status code checking
                // - log on non-200

            } catch (IOException e) {
                log.error(e);
            }

        }
    }
}
