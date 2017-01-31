package io.honeycomb;

import org.junit.Test;

import java.util.HashMap;
import java.util.concurrent.Callable;

import static org.junit.Assert.*;

public class BuilderTest {
    private final Callable sampleDynField = () -> "foobar";
    private final Callable sampleDynField2 = () -> "fizzbuzz";

    @Test
    public void testConstructor() throws Exception {
        // new builder, no arguments
        Builder builder = new Builder();
        HashMap<String, Object> expectedFields = new HashMap<>();
        assertEquals(expectedFields, builder.getFields());

        // new builder, passed in data and dynfields
        expectedFields.put("aa", 1);
        HashMap<String, Callable> expectedDynFields = new HashMap<>();
        expectedDynFields.put("sample", sampleDynField);
        builder.addField("aa", 1);
        builder.addDynField("sample", sampleDynField);
        assertEquals(expectedFields, builder.getFields());
        assertEquals(expectedDynFields, builder.getDynFields());

        // new builder, inherited data and dyn_fields
        LibHoney libhoney = new LibHoney.Builder().build();
        libhoney.add(expectedFields);
        libhoney.addDynFields(expectedDynFields);
        builder = libhoney.newBuilder();
        assertEquals(expectedFields, builder.getFields());
        assertEquals(expectedDynFields, builder.getDynFields());

        // new builder, merge inherited data and dyn_fields and arguments
        expectedFields.put("b", 2);
        expectedDynFields.put("fizzzzz", sampleDynField2);
        builder = libhoney.newBuilder();
        builder.addField("b", 2);
        builder.addDynField("fizzzzz", sampleDynField2);
        assertEquals(expectedFields, builder.getFields());
        assertEquals(expectedDynFields, builder.getDynFields());
    }

    @Test
    public void testAdd() throws Exception {
        LibHoney libhoney = new LibHoney.Builder().build();
        Builder builder = libhoney.newBuilder();
        HashMap<String, Object> expectedFields = new HashMap<>();

        expectedFields.put("a", 1);
        expectedFields.put("b", 3);
        Builder other = libhoney.newBuilder();
        other.add(expectedFields);
        builder.addFromBuilder(other);
        assertEquals(expectedFields, builder.getFields());

        expectedFields.put("c", 3);
        expectedFields.put("d", 4);
        other.add(expectedFields);
        builder.addFromBuilder(other);
        assertEquals(expectedFields, builder.getFields());
    }

    @Test
    public void testAddField() throws Exception {
        LibHoney libhoney = new LibHoney.Builder().build();
        Builder builder = libhoney.newBuilder();
        HashMap expectedFields = new HashMap<String, Object>();

        assertEquals(expectedFields, builder.getFields());
        assertEquals(true, builder.isEmpty());

        builder.addField("foo", 4);
        expectedFields.put("foo", 4);
        assertEquals(expectedFields, builder.getFields());
        assertEquals(false, builder.isEmpty());

        builder.addField("bar", "baz");
        expectedFields.put("bar", "baz");
        assertEquals(expectedFields, builder.getFields());

        builder.addField("foo", 6);
        expectedFields.put("foo", 6);
        assertEquals(expectedFields, builder.getFields());
    }

    @Test
    public void testAddDynField() throws Exception {
        LibHoney libhoney = new LibHoney.Builder().build();
        Builder builder = libhoney.newBuilder();
        HashMap expectedDynFields = new HashMap<String, Callable>();

        assertEquals(expectedDynFields, builder.getDynFields());

        builder.addDynField("sampleDynField", this.sampleDynField);
        expectedDynFields.put("sampleDynField", this.sampleDynField);
        assertEquals(expectedDynFields, builder.getDynFields());

        // Adding a second time should still only have one element
        builder.addDynField("sampleDynField", this.sampleDynField);
        expectedDynFields.put("sampleDynField", this.sampleDynField);
        assertEquals(expectedDynFields, builder.getDynFields());
    }

    @Test
    public void testClone() throws Exception {
        LibHoney libhoney = new LibHoney.Builder()
                .dataSet("newds")
                .build();
        HashMap<String, Object> expectedFields = new HashMap<>();
        expectedFields.put("e", 9);
        Builder builder = libhoney.newBuilder();
        builder.addField("e", 9);
        builder.addDynField("sampleDynField", sampleDynField);
        Builder clonedBuilder = new Builder(builder);

        assertEquals(builder, clonedBuilder);

        expectedFields.put("f", 10);
        builder.addField("f", 10);
        assertEquals(expectedFields, builder.getFields());

        expectedFields.remove("f");
        expectedFields.put("g", 11);
        clonedBuilder.addField("g", 11);
        assertEquals(expectedFields, clonedBuilder.getFields());

        assertEquals("newds", clonedBuilder.getDataSet());
    }

    @Test
    public void testNewEvent() throws Exception {
        LibHoney libhoney = new LibHoney.Builder()
                .sampleRate(5)
                .build();
        Builder builder = libhoney.newBuilder();
        HashMap<String, Object> expectedFields = new HashMap<>();
        expectedFields.put("a", 1);
        expectedFields.put("b", 3);
        builder.add(expectedFields);
        Event event = builder.newEvent();

        assertEquals(builder.getFields(), event.getFields());
        assertEquals(builder.getSampleRate(), 5);

        event.addField("3", "c");
        assertNotEquals(builder.getFields(), event.getFields());
    }
}
