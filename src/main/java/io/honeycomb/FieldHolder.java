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
 * Stores a FieldHolder and some metadata, and can create a HoneyEvent.
 */
public final class FieldHolder {
    /**
     * FieldHolder contains all the fields and dynamic fields.
     * Write key, data set, and sample rate are necessary to create a HoneyEvent.
     * Default values are inherited from LibHoney.
     */
    private HashMap<String, Object> fields;
    private HashMap<String, Callable> dynFields;

    // Metadata
    private String writeKey;
    private String dataSet;
    private int sampleRate;

    // Logging
    private final Log log = LogFactory.getLog(FieldHolder.class);

    // Reference to LibHoney
    private LibHoney libhoney;

    protected FieldHolder() {
        this.fields = new HashMap();
        this.dynFields = new HashMap();
    }

    public FieldHolder(LibHoney libhoney) {
        this();
        this.linkLibHoney(libhoney);
    }

    /**
     * Constructs a new FieldHolder with the same mappings and metadata as the specified FieldHolder.
     *
     * @param other the FieldHolder whose mappings are to be stored in this map
     */
    public FieldHolder(FieldHolder other) {
        this.fields = new HashMap(other.getFields());
        this.dynFields = new HashMap(other.getDynFields());
        this.writeKey = other.getWriteKey();
        this.dataSet = other.getDataSet();
        this.sampleRate = other.getSampleRate();
        this.libhoney = other.getLibHoney();
    }

    /**
     * Copies all of the field and dynamic field mappings from the specified FieldHolder to this FieldHolder.
     *
     * @param other the FieldHolder whose mappings are to be added to this map
     */
    public void add(FieldHolder other) {
        this.fields.putAll(other.fields);
        this.dynFields.putAll(other.dynFields);
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
     * Copies all of the dynamic field mappings from the specified map to this FieldHolder.
     * @param dynFields dynamic field mappings to be added this FieldHolder
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
     * Copies all of the field mappings from the specified map to this FieldHolder.
     * @param fields field mappings to be added to this FieldHolder
     */
    public void addFields(Map<String, Object> fields) {
        this.fields.putAll(fields);
    }


    /**
     * Creates a HoneyEvent from this FieldHolder's fields.
     *
     * @return a HoneyEvent from this FieldHolder's fields
     */
    public HoneyEvent createEvent() {
        return new HoneyEvent(this.libhoney, this);
    }

    /**
     * Compares the specified object with this FieldHolder for equality.  Returns true if the given object is also a
     * FieldHolder and the two FieldHolders contain equal FieldHolders and metadata.
     *
     * @param obj Object to be compared for equality with this FieldHolder
     * @return true if the specified object is equal to this FieldHolder
     */
    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (this.getClass() != obj.getClass()) {
            return false;
        }

        FieldHolder other = (FieldHolder) obj;

        return this.fields.equals(other.getFields())
                && this.dynFields.equals(other.getDynFields())
                && this.writeKey.equals(other.writeKey)
                && this.dataSet.equals(other.dataSet)
                && this.sampleRate == other.sampleRate;
    }

    /**
     * Returns the data set identifier for this FieldHolder.
     * @return the data set identifier for this FieldHolder
     */
    public String getDataSet() {
        return this.dataSet;
    }

    public Map<String, Callable> getDynFields() {
        return this.dynFields;
    }

    public Map<String, Object> getFields() {
        return this.fields;
    }

    public LibHoney getLibHoney() {
        return this.libhoney;
    }

    /**
     * Returns the sample rate for this FieldHolder.
     * @return the sample rate for this FieldHolder
     */
    public int getSampleRate() {
        return this.sampleRate;
    }

    /**
     * Returns the write key for this FieldHolder.
     * @return the write key for this FieldHolder
     */
    public String getWriteKey() {
        return this.writeKey;
    }

    /**
     * Returns the hash code value for this FieldHolder.
     * @return the hash code value for this FieldHolder
     */
    @Override
    public int hashCode() {
        return Objects.hash(this.fields, this.dynFields, this.writeKey, this.dataSet, this.sampleRate);
    }

    /**
     * Returns true if this FieldHolder contains no fields.  Does not check dynamic fields.
     * @return true if this FieldHolder contains no fields
     */
    public boolean isEmpty() {
        return this.fields.isEmpty();
    }

    protected void linkLibHoney(LibHoney libhoney) {
        this.libhoney = libhoney;
        this.fields = new HashMap(libhoney.getDefaultFields());
        this.dynFields = new HashMap(libhoney.getDefaultDynFields());
        this.writeKey = libhoney.getWriteKey();
        this.dataSet = libhoney.getDataSet();
        this.sampleRate = libhoney.getSampleRate();
    }

    public void setDataSet(String dataSet) {
        this.dataSet = dataSet;
    }

    public void setDynFields(HashMap<String, Callable> dynFields) {
        this.dynFields = dynFields;
    }

    public void setFields(HashMap<String, Object> fields) {
        this.fields = fields;
    }

    public void setSampleRate(int sampleRate) {
        this.sampleRate = sampleRate;
    }

    public void setWriteKey(String writeKey) {
        this.writeKey = writeKey;
    }

    /**
     * Returns a JSON representation of this FieldHolder.
     * @return a JSON representation of this FieldHolder
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
     * Returns a string representation of this FieldHolder.
     * @return a string representation of this FieldHolder
     */
    @Override
    public String toString() {
        return this.toJson().toString();
    }
}
