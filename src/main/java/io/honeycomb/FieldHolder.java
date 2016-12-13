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
 * Stores fields and dynamic fields.
 */
public final class FieldHolder {
    /**
     * Fields are key value pairs.
     * Dynamic fields are functions that return key value pairs.  When a HoneyEvent is constructed, all dynamic fields
     * are executed and the results are stored as fields.
     */
    private final HashMap<String, Object> fields;
    private final HashMap<String, Callable> dynFields;

    // Logging
    private final Log log = LogFactory.getLog(FieldHolder.class);

    /**
     * Constructs an empty FieldHolder.
     */
    public FieldHolder() {
        this.fields = new HashMap<>();
        this.dynFields = new HashMap<>();
    }

    /**
     * Constructs a new FieldHolder with mappings from the specified fields and dynamic fields Maps.
     *
     * @param fields field mappings to be stored in this FieldHolder
     * @param dynFields dynamic field mappings to be stored in this FieldHolder
     */
    public FieldHolder(Map<String, Object> fields, Map<String, Callable> dynFields) {
        this.fields = new HashMap<>(fields);
        this.dynFields = new HashMap<>(dynFields);
    }

    /**
     * Constructs a new FieldHolder with the same mappings as the specified FieldHolder.
     *
     * @param other the FieldHolder whose mappings are to be stored in this map
     */
    public FieldHolder(FieldHolder other) {
        this.fields = new HashMap<>(other.fields);
        this.dynFields = new HashMap<>(other.dynFields);
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
     * Compares the specified object with this FieldHolder for equality.  Returns true if the given object is also a
     * FieldHolder and the two FieldHolders contain equal fields and dynamic fields.
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

        return this.fields.equals(other.fields) && this.dynFields.equals(other.dynFields);
    }

    /**
     * Returns a Map of this FieldHolder's dynamic fields
     * @return a Map of this FieldHolder's dynamic fields
     */
    public Map<String, Callable> getDynFields() {
        return this.dynFields;
    }

    /**
     * Returns a Map of this FieldHolder's fields
     * @return a Map of this FieldHolder's fields
     */
    public Map<String, Object> getFields() {
        return this.fields;
    }

    /**
     * Returns the hash code value for this FieldHolder.
     * @return the hash code value for this FieldHolder
     */
    @Override
    public int hashCode() {
        return Objects.hash(this.fields, this.dynFields);
    }

    /**
     * Returns true if this FieldHolder contains no fields.  Does not check dynamic fields.
     * @return true if this FieldHolder contains no fields
     */
    public boolean isEmpty() {
        return this.fields.isEmpty();
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
