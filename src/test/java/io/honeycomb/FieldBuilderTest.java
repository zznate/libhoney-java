package io.honeycomb;

import org.junit.Test;

import java.util.HashMap;
import java.util.concurrent.Callable;

import static org.junit.Assert.*;

public class FieldBuilderTest {
    private final Callable sampleDynField = () -> "foobar";

    private final Callable sampleDynField2 = () -> 2;

    @Test
    public void testConstructor() throws Exception {
        LibHoney libhoney = new LibHoney.Builder().build();
        FieldBuilder fieldBuilder = new FieldBuilder.Builder(libhoney).build();
        HashMap<String, Object> expectedFields = new HashMap<>();
        HashMap<String, Callable> expectedDynFields = new HashMap<>();

        // New builder, no arguments
        assertEquals(expectedFields, fieldBuilder.getFieldHolder().getFields());
        assertEquals(expectedDynFields, fieldBuilder.getFieldHolder().getDynFields());

        // New builder, passed in fields and dynFields
        expectedFields.put("aa", 1);
        expectedDynFields.put("sampleDynField", this.sampleDynField);
        fieldBuilder = new FieldBuilder.Builder(libhoney)
                .fields(expectedFields)
                .dynFields(expectedDynFields)
                .build();
        assertEquals(expectedFields, fieldBuilder.getFieldHolder().getFields());
        assertEquals(expectedDynFields, fieldBuilder.getFieldHolder().getDynFields());

        // New builder, inherited data and dynFields
        libhoney.getFieldHolder().addFields(expectedFields);
        libhoney.getFieldHolder().addDynFields(expectedDynFields);
        fieldBuilder = new FieldBuilder.Builder(libhoney)
                .build();
        assertEquals(expectedFields, fieldBuilder.getFieldHolder().getFields());
        assertEquals(expectedDynFields, fieldBuilder.getFieldHolder().getDynFields());

        // New builder, merge inherited data and dynFields and arguments
        HashMap<String, Object> newFields = new HashMap<>();
        HashMap<String, Callable> newDynFields = new HashMap<>();
        newFields.put("b", 2);
        newDynFields.put("sampleDynField2", this.sampleDynField2);
        expectedFields.putAll(newFields);
        expectedDynFields.putAll(newDynFields);
        fieldBuilder = new FieldBuilder.Builder(libhoney)
                .fields(newFields)
                .dynFields(newDynFields)
                .build();
        assertEquals(expectedFields, fieldBuilder.getFieldHolder().getFields());
        assertEquals(expectedDynFields, fieldBuilder.getFieldHolder().getDynFields());
    }

    @Test
    public void testAdd() throws Exception {
        LibHoney libhoney = new LibHoney.Builder().build();
        FieldBuilder fieldBuilder = new FieldBuilder.Builder(libhoney).build();
        HashMap<String, Object> expectedFields = new HashMap<>();
        HashMap<String, Callable> expectedDynFields = new HashMap<>();

        expectedFields.put("a", 1);
        expectedFields.put("b", 3);
        fieldBuilder.getFieldHolder().add(new FieldHolder(expectedFields, expectedDynFields));
        assertEquals(expectedFields, fieldBuilder.getFieldHolder().getFields());

        expectedFields.put("c", 3);
        expectedFields.put("d", 4);
        fieldBuilder.getFieldHolder().add(new FieldHolder(expectedFields, expectedDynFields));
        assertEquals(expectedFields, fieldBuilder.getFieldHolder().getFields());
    }

    @Test
    public void testAddField() throws Exception {
        LibHoney libhoney = new LibHoney.Builder().build();
        FieldBuilder fieldBuilder = new FieldBuilder.Builder(libhoney).build();
        HashMap<String, Object> expectedFields = new HashMap<>();

        assertEquals(expectedFields, fieldBuilder.getFieldHolder().getFields());

        fieldBuilder.getFieldHolder().addField("foo", 4);
        expectedFields.put("foo", 4);
        assertEquals(expectedFields, fieldBuilder.getFieldHolder().getFields());

        fieldBuilder.getFieldHolder().addField("bar", "baz");
        expectedFields.put("bar", "baz");
        assertEquals(expectedFields, fieldBuilder.getFieldHolder().getFields());

        fieldBuilder.getFieldHolder().addField("foo", 6);
        expectedFields.put("foo", 6);
        assertEquals(expectedFields, fieldBuilder.getFieldHolder().getFields());
    }

    @Test
    public void testAddDynField() throws Exception {
        LibHoney libhoney = new LibHoney.Builder().build();
        FieldBuilder fieldBuilder = new FieldBuilder.Builder(libhoney).build();
        HashMap<String, Callable> expectedDynFields = new HashMap<>();

        assertEquals(expectedDynFields, fieldBuilder.getFieldHolder().getDynFields());

        fieldBuilder.getFieldHolder().addDynField("sampleDynField", this.sampleDynField);
        expectedDynFields.put("sampleDynField", this.sampleDynField);
        assertEquals(expectedDynFields, fieldBuilder.getFieldHolder().getDynFields());

        // Adding a second time should still only have one element
        fieldBuilder.getFieldHolder().addDynField("sampleDynField", this.sampleDynField);
        expectedDynFields.put("sampleDynField", this.sampleDynField);
        assertEquals(expectedDynFields, fieldBuilder.getFieldHolder().getDynFields());
    }

    @Test
    public void testClone() throws Exception {
        LibHoney libhoney = new LibHoney.Builder().build();
        HashMap<String, Object> expectedFields = new HashMap<>();
        expectedFields.put("e", 9);
        FieldBuilder fieldBuilder = new FieldBuilder.Builder(libhoney)
                .fields(expectedFields)
                .dataSet("newds")
                .build();
        fieldBuilder.getFieldHolder().addDynField("sampleDynField", sampleDynField);
        FieldBuilder clonedBuilder = new FieldBuilder(fieldBuilder);

        assertEquals(fieldBuilder, clonedBuilder);

        expectedFields.put("f", 10);
        fieldBuilder.getFieldHolder().addField("f", 10);
        assertEquals(expectedFields, fieldBuilder.getFieldHolder().getFields());

        expectedFields.remove("f");
        expectedFields.put("g", 11);
        clonedBuilder.getFieldHolder().addField("g", 11);
        assertEquals(expectedFields, clonedBuilder.getFieldHolder().getFields());

        assertEquals("newds", clonedBuilder.getDataSet());
    }

    @Test
    public void testCreateEvent() throws Exception {
        LibHoney libhoney = new LibHoney.Builder().build();
        FieldBuilder fieldBuilder = new FieldBuilder.Builder(libhoney)
                .sampleRate(5)
                .build();
        HashMap<String, Object> expectedFields = new HashMap<>();
        expectedFields.put("a", 1);
        expectedFields.put("b", 3);
        fieldBuilder.getFieldHolder().addFields(expectedFields);
        HoneyEvent honeyEvent = fieldBuilder.createEvent();

        assertEquals(fieldBuilder.getFieldHolder().getFields(), honeyEvent.getFieldHolder().getFields());
        assertEquals(fieldBuilder.getSampleRate(), 5);

        honeyEvent.getFieldHolder().addField("3", "c");
        assertNotEquals(fieldBuilder.getFieldHolder().getFields(), honeyEvent.getFieldHolder().getFields());
    }

}
