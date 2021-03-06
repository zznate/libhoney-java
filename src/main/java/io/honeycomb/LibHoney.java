package io.honeycomb;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Callable;

/**
 * Stores default values for Builders and Transmission.
 */
public final class LibHoney {
    /**
     * Builder contains the default mappings for Builders.
     * Transmission contains the global instance of Transmission.
     * All other metadata is used as default values for Events and Transmission.
     */
    private HashMap<String, Object> fields;
    private HashMap<String, Callable> dynFields;
    private Transmission transmission;

    // Metadata
    private final String writeKey;
    private final String dataSet;
    private final int sampleRate;
    private final String apiHost;
    private final int maxConcurrentBranches;
    private final boolean blockOnSend;
    private final boolean blockOnResponse;
    private final int closeTimeout;
    private final int requestQueueLength;
    private final int responseQueueLength;
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
        this.maxConcurrentBranches = builder.maxConcurrentBranches;
        this.blockOnSend = builder.blockOnSend;
        this.blockOnResponse = builder.blockOnResponse;
        this.closeTimeout = builder.closeTimeout;
        this.requestQueueLength = builder.requestQueueLength;
        this.responseQueueLength = builder.responseQueueLength;
        this.userAgent = builder.userAgent;

        this.fields = new HashMap<>();
        this.dynFields = new HashMap<>();
        this.transmission = new Transmission.Builder(this).build();
    }

    /**
     * LibHoney.Builder
     */
    public static class Builder {
        private String writeKey = Constants.DEFAULT_WRITE_KEY;
        private String dataSet = Constants.DEFAULT_DATA_SET;
        private int sampleRate = Constants.DEFAULT_SAMPLE_RATE;
        private String apiHost = Constants.DEFAULT_API_HOST;
        private int maxConcurrentBranches = Constants.DEFAULT_MAX_CONCURRENT_BRANCHES;
        private boolean blockOnSend = Constants.DEFAULT_BLOCK_ON_SEND;
        private boolean blockOnResponse = Constants.DEFAULT_BLOCK_ON_RESPONSE;
        private int closeTimeout = Constants.DEFAULT_CLOSE_TIMEOUT;
        private int requestQueueLength = Constants.DEFAULT_REQUEST_QUEUE_LENGTH;
        private int responseQueueLength = Constants.DEFAULT_RESPONSE_QUEUE_LENGTH;
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

        public LibHoney build() {
            return new LibHoney(this);
        }
    }

    /**
     * Copies all of the field mappings from the specified map to this LibHoney.
     * @param fields field mappings to be added to this LibHoney
     */
    public void add(Map<String, Object> fields) {
        this.fields.putAll(fields);
    }

    /**
     *
     * Associates the specified function with the specified key in the dynamic fields map.
     *
     * @param key key with which the specified function is to be associated
     * @param function function to be associated with the specified key
     */
    public void addDynField(String key, Callable function) {
        this.dynFields.put(key, function);
    }

    /**
     * Copies all of the dynamic field mappings from the specified map to this LibHoney.
     * @param dynFields dynamic field mappings to be added this LibHoney
     */
    public void addDynFields(Map<String, Callable> dynFields) {
        this.dynFields.putAll(dynFields);
    }

    /**
     * Associates the specified value with the specified key in the fields map.
     *
     * @param key key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     */
    public void addField(String key, Object value) {
        this.fields.put(key, value);
    }

    /**
     * Closes Transmission
     */
    public void close() {
        this.transmission.close();
    }

    /**
     * Returns the API host for this LibHoney.
     * @return the API host for this LibHoney
     */
    public String getApiHost() {
        return this.apiHost;
    }

    /**
     * Returns true if this LibHoney should block on response.
     * @return true if this LibHoney should block on response
     */
    public boolean getBlockOnResponse() {
        return this.blockOnResponse;
    }

    /**
     * Returns true if this LibHoney should block on send.
     * @return true if this LibHoney should block on send
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
     * Returns the data set identifier for this LibHoney.
     * @return the data set identifier for this LibHoney
     */
    public String getDataSet() {
        return this.dataSet;
    }

    /**
     * Returns dynamic fields for this LibHoney.
     * @return dynamic fields for this LibHoney
     */
    public Map <String, Callable> getDynFields() {
        return this.dynFields;
    }

    /**
     * Returns fields for this LibHoney.
     * @return fields for this LibHoney
     */
    public Map<String, Object> getFields() {
        return this.fields;
    }

    /**
     * Returns the maximum number of concurrent branches for this LibHoney.
     * @return the maximum number of concurrent branches for this LibHoney
     */
    public int getMaxConcurrentBranches() {
        return this.maxConcurrentBranches;
    }

    /**
     * Returns the request queue for this LibHoney.
     * @return the request queue for this LibHoney
     */
    public Queue getRequestQueue() {
        return this.transmission.getRequestQueue();
    }

    /**
     * Returns the response queue length for this LibHoney.
     * @return the response queue length for this LibHoney
     */
    public int getRequestQueueLength() {
        return this.requestQueueLength;
    }

    /**
     * Returns the response queue for this LibHoney
     * @return the response queue for this LibHoney
     */
    public Queue getResponseQueue() {
        return this.transmission.getResponseQueue();
    }

    /**
     * Returns the request queue length for this LibHoney.
     * @return the request queue length for this LibHoney
     */
    public int getResponseQueueLength() {
        return this.responseQueueLength;
    }

    /**
     * Returns the sample rate for this LibHoney.
     * @return the sample rate for this LibHoney
     */
    public int getSampleRate() {
        return this.sampleRate;
    }

    /**
     * Returns the Transmission for this LibHoney.
     * @return the Transmission for this LibHoney
     */
    protected Transmission getTransmission() {
        return this.transmission;
    }

    public String getUserAgent() {
        return this.userAgent;
    }

    /**
     * Returns the write key for this LibHoney.
     * @return the write key for this LibHoney.
     */
    public String getWriteKey() {
        return this.writeKey;
    }

    /**
     * Creates a Builder from this LibHoney's fields.
     *
     * @return a Builder from this LibHoney's fields
     */
    public io.honeycomb.Builder newBuilder() {
        return new io.honeycomb.Builder(this);
    }

    /**
     * Creates a Event from this LibHoney's Builder.
     *
     * @return a Event from this LibHoney's Builder
     */
    public Event newEvent() {
        return newBuilder().newEvent();
    }

    /**
     * Creates an Event, then sends with Transmission as a request, or as a dropped response if it should be dropped.
     *
     * @throws HoneyException if there is something wrong with the request
     */
    public void send() throws HoneyException {
        this.newBuilder().send();
    }

    /**
     * Immediately send a map of keys and values as a Event
     *
     * @param fields field key
     * @throws HoneyException if there is something wrong with the request
     */
    public void sendNow(Map<String, Object> fields) throws HoneyException {
        io.honeycomb.Builder builder = this.newBuilder();
        builder.add(fields);
        builder.send();
    }

    /**
     * Sets the reference to transmission
     * @param transmission reference to transmission
     */
    public void setTransmission(Transmission transmission) {
        this.transmission = transmission;
    }

    /**
     * Returns a JSON representation of this LibHoney.
     * @return a JSON representation of this LibHoney
     */
    public JSONObject toJson() {
        JSONObject json = new JSONObject();
        try {
            json.put("fields", this.fields);
            json.put("dynFields", this.fields);
            json.put("writeKey", this.writeKey);
            json.put("dataSet", this.dataSet);
            json.put("sampleRate", this.sampleRate);
            json.put("apiHost", this.apiHost);
            json.put("maxConcurrentBranches", this.maxConcurrentBranches);
            json.put("blockOnSend", this.blockOnSend);
            json.put("blockOnResponse", this.blockOnResponse);
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
}
