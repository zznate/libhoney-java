package io.honeycomb;

import java.util.HashMap;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * Stores a Builder and some metadata, executes all dynamic fields upon instantiation,
 * and can enqueue HTTP requests to Transmission.
 */
public class Event {
    /**
     * Builder contains all the fields and dynamic fields.
     * Created at, write key, data set, and sample rate are all necessary to create a Event.
     * Values are typically passed in by Builder.
     */
    private HashMap<String, Object> fields;

    // Metadata
    private final String dataSet;
    private String createdAt;
    private String metadata;
    private int sampleRate;
    private String writeKey;

    // Logging
    private final Log log = LogFactory.getLog(Event.class);

    Event(String dataSet) {
        this.dataSet = dataSet;
        fields = new HashMap<>();
    }


    public String getDataSet() {
        return dataSet;
    }

    public String getCreatedAt() {
        return createdAt;
    }


    public Event with(String key, String value) {
        fields.put(key, value);
        return this;
    }

    /**
     * Returns true if this Event should be dropped (due to the sample rate).
     * @return true if this Event should be dropped
     */
    public boolean shouldSendEvent() {
        return this.sampleRate == 1 || ((new Random()).nextInt(this.sampleRate) == 0);
    }

    /**
     * Returns a JSON representation of this Event.
     * @return a JSON representation of this Event
     */
    public JSONObject toJson() {
        JSONObject json = new JSONObject();
        try {
            json.put("fields", this.fields);
            json.put("createdAt", this.createdAt);
            json.put("writeKey", this.writeKey);
            json.put("dataSet", this.dataSet);
            json.put("sampleRate", this.sampleRate);
            json.put("metadata", this.metadata);
        } catch (JSONException e) {
            log.error(e);
        }
        return json;
    }

    /**
     * Returns a string representation of this Event.
     * @return a string representation of this Event
     */
    @Override
    public String toString() {
        return this.toJson().toString();
    }
}
