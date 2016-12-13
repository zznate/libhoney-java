package io.honeycomb;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONException;
import org.json.JSONObject;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.Callable;

/**
 * Stores a FieldHolder and some metadata, executes all dynamic fields upon instantiation,
 * and can enqueue HTTP requests to Transmission.
 */
public class HoneyEvent {
    /**
     * FieldHolder contains all the fields and dynamic fields.
     * Created at, write key, data set, and sample rate are all necessary to create a HoneyEvent.
     * Values are typically passed in by FieldBuilder.
     */
    private final FieldHolder fieldHolder;
    private final Random random;
    private final Transmission transmission;

    // Metadata
    private final String createdAt;
    private final String writeKey;
    private final String dataSet;
    private final int sampleRate;
    private final String metadata;

    // Logging
    private final Log log = LogFactory.getLog(HoneyEvent.class);

    /**
     * Constructs a new HoneyEvent with a default metadata string inherited from LibHoney.
     *
     * @param libhoney LibHoney
     * @param fieldBuilder fieldBuilder from which this HoneyEvent is built
     */
    public HoneyEvent(LibHoney libhoney, FieldBuilder fieldBuilder) {
        this(libhoney, fieldBuilder, libhoney.getMetadata());
    }

    /**
     * Constructs a new HoneyEvent from a FieldBuilder, executing all dynamic fields and storing them as fields.
     *
     * @param libhoney LibHoney
     * @param fieldBuilder fieldBuilder from which this HoneyEvent is built
     * @param metadata metadata for debugging purposes
     */
    public HoneyEvent(LibHoney libhoney, FieldBuilder fieldBuilder, String metadata) {
        this.fieldHolder = new FieldHolder(fieldBuilder.getFieldHolder());

        this.createdAt = ZonedDateTime.now().format(DateTimeFormatter.ISO_INSTANT);
        this.writeKey = fieldBuilder.getWriteKey();
        this.dataSet = fieldBuilder.getDataSet();
        this.sampleRate = fieldBuilder.getSampleRate();
        this.metadata = metadata;
        this.random = new Random();
        this.transmission = libhoney.getTransmission();

        // Execute all dynamic field functions
        for (Object o : this.fieldHolder.getDynFields().entrySet()) {
            HashMap.Entry entry = (HashMap.Entry) o;
            try {
                this.fieldHolder.addField((String) entry.getKey(), ((Callable) entry.getValue()).call());
            } catch (Exception e) {
                log.error(e);
            }
        }
    }

    /**
     * Returns the time when this HoneyEvent was created.
     * @return the time when this HoneyEvent was created
     */
    public String getCreatedAt() {
        return this.createdAt;
    }

    /**
     * Returns the data set identifier for this HoneyEvent.
     * @return the data set identifier for this HoneyEvent
     */
    public String getDataSet() {
        return this.dataSet;
    }

    /**
     * Returns the FieldHolder for this HoneyEvent.
     * @return the FieldHolder for this HoneyEvent
     */
    public FieldHolder getFieldHolder() {
        return this.fieldHolder;
    }

    /**
     * Returns the metadata string for this HoneyEvent.
     * @return the metadata string for this HoneyEvent
     */
    public String getMetadata() {
        return this.metadata;
    }

    /**
     * Returns the sample rate for this HoneyEvent.
     * @return the sample rate for this HoneyEvent
     */
    public int getSampleRate() {
        return this.sampleRate;
    }

    /**
     * Returns the write key for this HoneyEvent.
     * @return the write key for this HoneyEvent.
     */
    public String getWriteKey() {
        return this.writeKey;
    }

    /**
     * Enqueues this HoneyEvent with Transmission as a request, or as a dropped response if it should be dropped.
     *
     * @throws HoneyException if there is something wrong with the request
     */
    public void send() throws HoneyException {
        if (this.shouldSendEvent()) {
            if (this.fieldHolder.isEmpty()) {
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
     * Returns true if this HoneyEvent should be dropped (due to the sample rate).
     * @return true if this HoneyEvent should be dropped
     */
    public boolean shouldSendEvent() {
        return this.sampleRate == 1 || (this.random.nextInt(this.sampleRate) == 0);
    }

    /**
     * Returns a JSON representation of this HoneyEvent.
     * @return a JSON representation of this HoneyEvent
     */
    public JSONObject toJson() {
        JSONObject json = new JSONObject();
        try {
            json.put("fieldHolder", this.fieldHolder);
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
     * Returns a string representation of this HoneyEvent.
     * @return a string representation of this HoneyEvent
     */
    @Override
    public String toString() {
        return this.toJson().toString();
    }
}
