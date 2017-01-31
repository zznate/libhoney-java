package io.honeycomb;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;

/**
 * Stores a Builder and some metadata, and can create a Event.
 */
public final class Builder {
    /**
     * Builder contains all the fields and dynamic fields.
     * Write key, data set, and sample rate are necessary to create a Event.
     * Default values are inherited from LibHoney.
     */
    private HashMap<String, Object> fields;
    private HashMap<String, Callable> dynFields;

    // Metadata
    private String writeKey;
    private String dataSet;
    private int sampleRate;

    // Logging
    private final Log log = LogFactory.getLog(Builder.class);

    // Reference to LibHoney
    private LibHoney libhoney;

    protected Builder() {
        this.fields = new HashMap();
        this.dynFields = new HashMap();
    }

    public Builder(LibHoney libhoney) {
        this();
        this.linkLibHoney(libhoney);
    }

    /**
     * Constructs a new Builder with the same mappings and metadata as the specified Builder.
     *
     * @param other the Builder whose mappings are to be stored in this map
     */
    public Builder(Builder other) {
        this.fields = new HashMap(other.getFields());
        this.dynFields = new HashMap(other.getDynFields());
        this.writeKey = other.getWriteKey();
        this.dataSet = other.getDataSet();
        this.sampleRate = other.getSampleRate();
        this.libhoney = other.getLibHoney();
    }

    /**
     * Copies all of the field mappings from the specified map to this Builder.
     * @param fields field mappings to be added to this Builder
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
     * Copies all of the dynamic field mappings from the specified map to this Builder.
     * @param dynFields dynamic field mappings to be added this Builder
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
     * Copies all of the field and dynamic field mappings from the specified Builder to this Builder.
     *
     * @param other the Builder whose mappings are to be added to this map
     */
    public void addFromBuilder(Builder other) {
        this.fields.putAll(other.fields);
        this.dynFields.putAll(other.dynFields);
    }

    /**
     * Creates a Event from this Builder's fields.
     *
     * @return a Event from this Builder's fields
     */
    protected Event newEvent() {
        return new Event(this.libhoney, this);
    }

    /**
     * Compares the specified object with this Builder for equality.  Returns true if the given object is also a
     * Builder and the two Builders contain equal fields and metadata.
     *
     * @param obj Object to be compared for equality with this Builder
     * @return true if the specified object is equal to this Builder
     */
    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (this.getClass() != obj.getClass()) {
            return false;
        }

        Builder other = (Builder) obj;

        return this.fields.equals(other.getFields())
                && this.dynFields.equals(other.getDynFields())
                && this.writeKey.equals(other.writeKey)
                && this.dataSet.equals(other.dataSet)
                && this.sampleRate == other.sampleRate;
    }

    /**
     * Returns the data set identifier for this Builder.
     * @return the data set identifier for this Builder
     */
    public String getDataSet() {
        return this.dataSet;
    }

    /**
     * Returns dynamic fields for this Builder.
     * @return dynamic fields for this Builder
     */
    public Map<String, Callable> getDynFields() {
        return this.dynFields;
    }

    /**
     * Returns fields for this Builder.
     * @return fields for this Builder
     */
    public Map<String, Object> getFields() {
        return this.fields;
    }

    /**
     * Returns a reference to LibHoney.
     * @return a reference to LibHoney.
     */
    public LibHoney getLibHoney() {
        return this.libhoney;
    }

    /**
     * Returns the sample rate for this Builder.
     * @return the sample rate for this Builder
     */
    public int getSampleRate() {
        return this.sampleRate;
    }

    /**
     * Returns the write key for this Builder.
     * @return the write key for this Builder
     */
    public String getWriteKey() {
        return this.writeKey;
    }

    /**
     * Returns the hash code value for this Builder.
     * @return the hash code value for this Builder
     */
    @Override
    public int hashCode() {
        return Objects.hash(this.fields, this.dynFields, this.writeKey, this.dataSet, this.sampleRate);
    }

    /**
     * Returns true if this Builder contains no fields.  Does not check dynamic fields.
     * @return true if this Builder contains no fields
     */
    public boolean isEmpty() {
        return this.fields.isEmpty();
    }

    /**
     * Stores a reference to LibHoney and applies LibHoney's metadata to this Builder.
     * @param libhoney reference to LibHoney
     */
    protected void linkLibHoney(LibHoney libhoney) {
        this.libhoney = libhoney;
        this.fields.putAll(libhoney.getFields());
        this.dynFields.putAll(libhoney.getDynFields());
        this.writeKey = libhoney.getWriteKey();
        this.dataSet = libhoney.getDataSet();
        this.sampleRate = libhoney.getSampleRate();
    }

    public void send() throws HoneyException {
        this.newEvent().send();
    }

    /**
     * Immediately send a map of keys and values as a Event
     *
     * @param fields field key
     * @throws HoneyException if there is something wrong with the request
     */
    public void sendNow(Map<String, Object> fields) throws HoneyException {
        this.fields.putAll(fields);
        this.send();
    }

    /**
     * Sets the data set
     * @param dataSet data set
     */
    public void setDataSet(String dataSet) {
        this.dataSet = dataSet;
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
     * Returns a JSON representation of this Builder.
     * @return a JSON representation of this Builder
     */
    public JSONObject toJson() {
        JSONObject json = new JSONObject();
        try {
            json.put("fields", this.fields);
            json.put("dynFields", this.dynFields);
            json.put("writeKey", this.writeKey);
            json.put("dataSet", this.dataSet);
            json.put("sampleRate", this.sampleRate);
        } catch (JSONException e) {
            log.error(e);
        }
        return json;
    }

    /**
     * Returns a string representation of this Builder.
     * @return a string representation of this Builder
     */
    @Override
    public String toString() {
        return this.toJson().toString();
    }
}
