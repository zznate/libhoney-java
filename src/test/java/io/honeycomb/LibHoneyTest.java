package io.honeycomb;

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.Callable;

import static org.junit.Assert.*;

public class LibHoneyTest {
    private final Callable sampleDynField = () -> "foobar";

    LibHoney libhoney;

    @Before
    public void setUp() throws Exception {
        libhoney = new LibHoney.Builder()
                .writeKey("499e56a6f613dc79e68afd742153750e")
                .dataSet("ds")
                .sampleRate(3)
                .maxConcurrentBranches(5)
                .blockOnSend(true)
                .blockOnResponse(true)
                .build();
    }

    @Test
    public void testConstructor() throws Exception {
        assertEquals("499e56a6f613dc79e68afd742153750e", libhoney.getWriteKey());
        assertEquals("ds", libhoney.getDataSet());
        assertEquals(3, libhoney.getSampleRate());
        assertEquals(5, libhoney.getMaxConcurrentBranches());
        assertEquals(true, libhoney.getBlockOnResponse());
    }

    @Test
    public void testClose() throws Exception {
        // Fields for Builder
        HashMap<String, Object> fields = new HashMap<>();
        fields.put("foo", "bar");

        // Dynamic fields for Builder
        HashMap<String, Callable> dynFields = new HashMap<>();
        Callable randomUUID = () -> UUID.randomUUID().toString();
        dynFields.put("exampleDynField", randomUUID);

        // Create a Builder
        Builder builder = libhoney.newBuilder();
        builder.add(fields);
        builder.addDynFields(dynFields);
        builder.setSampleRate(1);

        // Send an event, verify running
        builder.newEvent().send();
        assertEquals(false, libhoney.getTransmission().isShutdown());

        // Close, verify not running
        libhoney.close();
        assertEquals(true, libhoney.getTransmission().isShutdown());
    }

    @Test
    public void testAddField() {
        HashMap<String, Object> ed = new HashMap<>();
        ed.put("whomp", true);
        libhoney.addField("whomp", true);
        assertEquals(ed, libhoney.getFields());
    }

    @Test
    public void testAddDynField() {
        HashMap<String, Callable> ed = new HashMap<>();
        ed.put("baz", sampleDynField);
        libhoney.addDynField("baz", sampleDynField);
        assertEquals(ed, libhoney.getDynFields());
    }

    @Test
    public void testAdd() {
        HashMap<String, Object> ed = new HashMap<>();
        ed.put("whomp", true);
        libhoney.add(ed);
        assertEquals(ed, libhoney.getFields());
    }
}