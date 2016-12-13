package io.honeycomb;

import org.junit.Test;

import java.util.HashMap;
import java.util.concurrent.Callable;

import static org.junit.Assert.*;

public class FieldHolderTest {
    private final Callable sampleDynField = () -> "foobar";

    @Test
    public void testAddField() throws Exception {
        FieldHolder fieldHolder = new FieldHolder();
        HashMap expectedFields = new HashMap<String, Object>();

        assertEquals(expectedFields, fieldHolder.getFields());
        assertEquals(true, fieldHolder.isEmpty());

        fieldHolder.addField("foo", 4);
        expectedFields.put("foo", 4);
        assertEquals(expectedFields, fieldHolder.getFields());
        assertEquals(false, fieldHolder.isEmpty());

        fieldHolder.addField("bar", "baz");
        expectedFields.put("bar", "baz");
        assertEquals(expectedFields, fieldHolder.getFields());

        fieldHolder.addField("foo", 6);
        expectedFields.put("foo", 6);
        assertEquals(expectedFields, fieldHolder.getFields());
    }

    @Test
    public void testAddDynField() throws Exception {
        FieldHolder fieldHolder = new FieldHolder();
        HashMap expectedDynFields = new HashMap<String, Callable>();

        assertEquals(expectedDynFields, fieldHolder.getDynFields());

        fieldHolder.addDynField("sampleDynField", this.sampleDynField);
        expectedDynFields.put("sampleDynField", this.sampleDynField);
        assertEquals(expectedDynFields, fieldHolder.getDynFields());

        // Adding a second time should still only have one element
        fieldHolder.addDynField("sampleDynField", this.sampleDynField);
        expectedDynFields.put("sampleDynField", this.sampleDynField);
        assertEquals(expectedDynFields, fieldHolder.getDynFields());
    }
}
