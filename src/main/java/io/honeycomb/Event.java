package io.honeycomb;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONException;
import org.json.JSONObject;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;

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
    private final Transmission transmission;

    // Metadata
    private final String createdAt;
    private String dataSet;
    private String metadata;
    private int sampleRate;
    private String writeKey;

    // Logging
    private final Log log = LogFactory.getLog(Event.class);

    /**
     * Constructs a new Event with a default metadata string inherited from LibHoney.
     *
     * @param libhoney LibHoney
     * @param builder builder from which this Event is built
     */
    public Event(LibHoney libhoney, Builder builder) {
        this(libhoney, builder, "");
    }

    /**
     * Constructs a new Event from a Builder, executing all dynamic fields and storing them as fields.
     *
     * @param libhoney LibHoney
     * @param builder builder from which this Event is built
     * @param metadata metadata for debugging purposes
     */
    public Event(LibHoney libhoney, Builder builder, String metadata) {
        this.fields = new HashMap(builder.getFields());
        this.createdAt = ZonedDateTime.now().format(DateTimeFormatter.ISO_INSTANT);
        this.writeKey = builder.getWriteKey();
        this.dataSet = builder.getDataSet();
        this.sampleRate = builder.getSampleRate();
        this.metadata = metadata;
        this.transmission = libhoney.getTransmission();

        // Execute all dynamic field functions
        for (Object o : builder.getDynFields().entrySet()) {
            HashMap.Entry entry = (HashMap.Entry) o;
            try {
                this.fields.put((String) entry.getKey(), ((Callable) entry.getValue()).call());
            } catch (Exception e) {
                log.error(e);
            }
        }
    }

    /**
     * Copies all of the field mappings from the specified map to this Builder.
     * @param fields field mappings to be added to this Builder
     */
    public void add(Map<String, Object> fields) {
        this.fields.putAll(fields);
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
     * Returns the time when this Event was created.
     * @return the time when this Event was created
     */
    public String getCreatedAt() {
        return this.createdAt;
    }

    /**
     * Returns the data set identifier for this Event.
     * @return the data set identifier for this Event
     */
    public String getDataSet() {
        return this.dataSet;
    }

    /**
     * Returns fields for this Event.
     * @return fields for this Event
     */
    public Map<String, Object> getFields() {
        return this.fields;
    }

    /**
     * Returns the metadata string for this Event.
     * @return the metadata string for this Event
     */
    public String getMetadata() {
        return this.metadata;
    }

    /**
     * Returns the sample rate for this Event.
     * @return the sample rate for this Event
     */
    public int getSampleRate() {
        return this.sampleRate;
    }

    /**
     * Returns the write key for this Event.
     * @return the write key for this Event.
     */
    public String getWriteKey() {
        return this.writeKey;
    }

    /**
     * Enqueues this Event with Transmission as a request, or as a dropped response if it should be dropped.
     *
     * @throws HoneyException if there is something wrong with the request
     */
    public void send() throws HoneyException {
        if (this.shouldSendEvent()) {
            if (this.fields.isEmpty()) {
                throw new HoneyException("No metrics added to event. Won't send empty event.");
            } else if (this.transmission.getApiHost().equals("")) {
                throw new HoneyException("No APIHost for Honeycomb. Can't send to the Great Unknown.");
            } else if (this.writeKey.equals("")) {
                throw new HoneyException("No WriteKey specified. Can't send event.");
            } else if (this.dataSet.equals("")) {
                throw new HoneyException("No Dataset for Honeycomb. Can't send datasetless.");
            }

            transmission.enqueueRequest(this);
        } else {
            log.debug("sampled");
            transmission.sendDroppedResponse(this.metadata);
        }
    }

    /**
     * Sets the data set
     * @param dataSet data set
     */
    public void setDataSet(String dataSet) {
        this.dataSet = dataSet;
    }

    /**
     * Sets the metadata string
     * @param metadata metadata string
     */
    public void setMetadata(String metadata) {
        this.metadata = metadata;
    }

    /**
     * Sets the sample rate
     * @param sampleRate sample rate
     */
    public void setSampleRate(int sampleRate) {
        this.sampleRate = sampleRate;
    }

    /**
     * Sets the write key
     * @param writeKey write key
     */
    public void setWriteKey(String writeKey) {
        this.writeKey = writeKey;
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
