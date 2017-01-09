package io.honeycomb;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public final class HoneyEventTest {
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
        HoneyEvent honeyEvent = libhoney.createFieldHolder().createEvent();

        assertEquals("dz", honeyEvent.getDataSet());
        assertEquals("", honeyEvent.getMetadata());
        assertEquals("wk", honeyEvent.getWriteKey());
    }

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
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
        Transmission transmission = mock(Transmission.class);
        libhoney.setTransmission(transmission);
        FieldHolder fieldHolder = libhoney.createFieldHolder();
        HoneyEvent event = spy(new HoneyEvent(libhoney, fieldHolder));

        when(event.shouldSendEvent()).thenReturn(false);
        event.send();
        verify(transmission, never()).enqueueRequest(anyObject());

        when(event.shouldSendEvent()).thenReturn(true);
        exception.expect(HoneyException.class);
        event.send();
    }

}