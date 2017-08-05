package io.honeycomb;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;

public final class EventTest {
    @Test
    public void testConstructor() throws Exception {
        LibHoney libhoney = new LibHoney.Builder()
                .apiHost("uuu")
                .maxConcurrentBranches(5)
                .dataSet("dz")
                .blockOnSend(true)
                .blockOnResponse(true)
                .closeTimeout(21)
                .writeKey("wk")
                .build();
        Event event = libhoney.newEvent();

        assertEquals("dz", event.getDataSet());
        //assertEquals("", event.getMetadata());
        //assertEquals("wk", event.getWriteKey());
    }

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    @Ignore
    public void testShouldSend() throws Exception {
        LibHoney libhoney = new LibHoney.Builder()
                .apiHost("uuu")
                .maxConcurrentBranches(5)
                .dataSet("dz")
                .blockOnSend(true)
                .blockOnResponse(true)
                .closeTimeout(21)
                .writeKey("wk")
                .build();
        /*
        Transmission transmission = mock(Transmission.class);
        libhoney.setTransmission(transmission);
        Builder builder = libhoney.newBuilder();
        Event event = spy(new Event(libhoney, builder));

        when(event.shouldSendEvent()).thenReturn(false);
        event.send();
        verify(transmission, never()).enqueueRequest(anyObject());

        when(event.shouldSendEvent()).thenReturn(true);
        exception.expect(HoneyException.class);
        event.send();
        */
    }

}