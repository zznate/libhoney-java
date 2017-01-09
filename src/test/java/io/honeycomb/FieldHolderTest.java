package io.honeycomb;

import org.junit.Test;

import java.util.HashMap;
import java.util.concurrent.Callable;

import static org.junit.Assert.*;

public class FieldHolderTest {
    private final Callable sampleDynField = () -> "foobar";

    @Test
    public void testConstructor() throws Exception {
        LibHoney libhoney = new LibHoney.Builder().build();
        // TODO test that FieldHolder inherits from LibHoney maybe
    }

    @Test
    public void testAdd() throws Exception {
        LibHoney libhoney = new LibHoney.Builder().build();
        FieldHolder fieldHolder = libhoney.createFieldHolder();
        HashMap<String, Object> expectedFields = new HashMap<>();

        expectedFields.put("a", 1);
        expectedFields.put("b", 3);
        FieldHolder other = libhoney.createFieldHolder();
        other.addFields(expectedFields);
        fieldHolder.add(other);
        assertEquals(expectedFields, fieldHolder.getFields());

        expectedFields.put("c", 3);
        expectedFields.put("d", 4);
        other.addFields(expectedFields);
        fieldHolder.add(other);
        assertEquals(expectedFields, fieldHolder.getFields());
    }

    @Test
    public void testAddField() throws Exception {
        LibHoney libhoney = new LibHoney.Builder().build();
        FieldHolder fieldHolder = libhoney.createFieldHolder();
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
        LibHoney libhoney = new LibHoney.Builder().build();
        FieldHolder fieldHolder = libhoney.createFieldHolder();
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

    @Test
    public void testClone() throws Exception {
        LibHoney libhoney = new LibHoney.Builder()
                .dataSet("newds")
                .build();
        HashMap<String, Object> expectedFields = new HashMap<>();
        expectedFields.put("e", 9);
        FieldHolder fieldHolder = libhoney.createFieldHolder();
        fieldHolder.addField("e", 9);
        fieldHolder.addDynField("sampleDynField", sampleDynField);
        FieldHolder clonedHolder = new FieldHolder(fieldHolder);

        assertEquals(fieldHolder, clonedHolder);

        expectedFields.put("f", 10);
        fieldHolder.addField("f", 10);
        assertEquals(expectedFields, fieldHolder.getFields());

        expectedFields.remove("f");
        expectedFields.put("g", 11);
        clonedHolder.addField("g", 11);
        assertEquals(expectedFields, clonedHolder.getFields());

        assertEquals("newds", clonedHolder.getDataSet());
    }

    @Test
    public void testCreateEvent() throws Exception {
        LibHoney libhoney = new LibHoney.Builder()
                .sampleRate(5)
                .build();
        FieldHolder fieldHolder = libhoney.createFieldHolder();
        HashMap<String, Object> expectedFields = new HashMap<>();
        expectedFields.put("a", 1);
        expectedFields.put("b", 3);
        fieldHolder.addFields(expectedFields);
        HoneyEvent honeyEvent = fieldHolder.createEvent();

        assertEquals(fieldHolder.getFields(), honeyEvent.getFieldHolder().getFields());
        assertEquals(fieldHolder.getSampleRate(), 5);

        honeyEvent.getFieldHolder().addField("3", "c");
        assertNotEquals(fieldHolder.getFields(), honeyEvent.getFieldHolder().getFields());
    }
}
