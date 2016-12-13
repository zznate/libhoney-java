package io.honeycomb;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;

/**
 * Stores a FieldHolder and some metadata, and can create a HoneyEvent.
 */
public final class FieldBuilder {
    /**
     * FieldHolder contains all the fields and dynamic fields.
     * Write key, data set, and sample rate are necessary to create a HoneyEvent.
     * Default values are inherited from LibHoney.
     */
    private final FieldHolder fieldHolder;

    // Metadata
    private final String writeKey;
    private final String dataSet;
    private final int sampleRate;

    // Logging
    private final Log log = LogFactory.getLog(FieldBuilder.class);

    // Reference to LibHoney
    private final LibHoney libhoney;

    /**
     * Constructs a FieldBuilder from a FieldBuilder.Builder
     * @param builder the builder to build this FieldBuilder
     */
    private FieldBuilder(FieldBuilder.Builder builder) {
        this.fieldHolder = builder.fieldHolder;
        this.writeKey = builder.writeKey;
        this.dataSet = builder.dataSet;
        this.sampleRate = builder.sampleRate;
        this.libhoney = builder.libhoney;
    }

    /**
     * Constructs a new FieldBuilder with the same mappings and metadata as the specified FieldBuilder.
     *
     * @param other the FieldHolder whose mappings are to be stored in this map
     */
    public FieldBuilder(FieldBuilder other) {
        this.fieldHolder = new FieldHolder(other.getFieldHolder());
        this.writeKey = other.writeKey;
        this.dataSet = other.dataSet;
        this.sampleRate = other.sampleRate;
        this.libhoney = other.libhoney;
    }

    /**
     * FieldBuilder.Builder
     */
    public static class Builder {
        private FieldHolder fieldHolder;
        private String writeKey;
        private String dataSet;
        private int sampleRate;
        private final LibHoney libhoney;

        // Passed in global state
        public Builder(LibHoney libhoney) {
            this.fieldHolder = new FieldHolder(libhoney.getFieldHolder());
            this.writeKey = libhoney.getWriteKey();
            this.dataSet = libhoney.getDataSet();
            this.sampleRate = libhoney.getSampleRate();
            this.libhoney = libhoney;
        }

        public Builder fields(Map<String, Object> fields) {
            this.fieldHolder.addFields(fields);
            return this;
        }

        public Builder dynFields(Map<String, Callable> dynFields) {
            this.fieldHolder.addDynFields(dynFields);
            return this;
        }

        public Builder fieldHolder(FieldHolder fieldHolder) {
            this.fieldHolder = new FieldHolder(fieldHolder);
            return this;
        }

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

        public FieldBuilder build() {
            return new FieldBuilder(this);
        }
    }

    /**
     * Creates a HoneyEvent from this FieldBuilder's fields.
     *
     * @return a HoneyEvent from this FieldBuilder's fields
     */
    public HoneyEvent createEvent() {
        return new HoneyEvent(this.libhoney, this);
    }

    /**
     * Compares the specified object with this FieldBuilder for equality.  Returns true if the given object is also a
     * FieldBuilder and the two FieldHolders contain equal FieldHolders and metadata.
     *
     * @param obj Object to be compared for equality with this FieldBuilder
     * @return true if the specified object is equal to this FieldBuilder
     */
    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (this.getClass() != obj.getClass()) {
            return false;
        }

        FieldBuilder other = (FieldBuilder) obj;

        return this.fieldHolder.equals(other.getFieldHolder())
                && this.writeKey.equals(other.writeKey)
                && this.dataSet.equals(other.dataSet)
                && this.sampleRate == other.sampleRate;
    }

    /**
     * Returns the data set identifier for this FieldBuilder.
     * @return the data set identifier for this FieldBuilder
     */
    public String getDataSet() {
        return this.dataSet;
    }

    /**
     * Returns the FieldHolder for this FieldBuilder.
     * @return the FieldHolder for this FieldBuilder
     */
    public FieldHolder getFieldHolder() {
        return this.fieldHolder;
    }

    /**
     * Returns the sample rate for this FieldBuilder.
     * @return the sample rate for this FieldBuilder
     */
    public int getSampleRate() {
        return this.sampleRate;
    }

    /**
     * Returns the write key for this FieldBuilder.
     * @return the write key for this FieldBuilder
     */
    public String getWriteKey() {
        return this.writeKey;
    }

    /**
     * Returns the hash code value for this FieldBuilder.
     * @return the hash code value for this FieldBuilder
     */
    @Override
    public int hashCode() {
        return Objects.hash(this.fieldHolder, this.writeKey, this.dataSet, this.sampleRate);
    }

    /**
     * Returns a JSON representation of this FieldBuilder.
     * @return a JSON representation of this FieldBuilder
     */
    public JSONObject toJson() {
        JSONObject json = new JSONObject();
        try {
            json.put("fieldHolder", this.fieldHolder);
            json.put("writeKey", this.writeKey);
            json.put("dataSet", this.dataSet);
            json.put("sampleRate", this.sampleRate);
        } catch (JSONException e) {
            log.error(e);
        }
        return json;
    }

    /**
     * Returns a string representation of this FieldBuilder.
     * @return a string representation of this FieldBuilder
     */
    @Override
    public String toString() {
        return this.toJson().toString();
    }
}
