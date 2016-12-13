package io.honeycomb;

import org.json.JSONObject;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.concurrent.ArrayBlockingQueue;

import static org.mockito.Mockito.*;

import static org.junit.Assert.*;

public class TransmissionTest {
    @Test
    public void testConstructor() throws Exception {
        LibHoney libhoney = new LibHoney.Builder()
                .apiHost("uuu")
                .maxConcurrentBranches(5)
                .blockOnSend(true)
                .blockOnResponse(true)
                .closeTimeout(21)
                .build();
        Transmission transmission = libhoney.getTransmission();

        assertEquals("uuu", transmission.getApiHost());
        assertEquals(5, transmission.getMaxConcurrentBranches());
        assertEquals(true, transmission.getBlockOnSend());
        assertEquals(true, transmission.getBlockOnResponse());
        assertEquals(21, transmission.getCloseTimeout());
    }

    @Test
    public void testBlockOnResponse() throws Exception {
        LibHoney libhoney = new LibHoney.Builder()
                .writeKey("wk")
                .dataSet("ds")
                .blockOnResponse(true)
                .build();
        ArrayBlockingQueue responseQueue = spy((ArrayBlockingQueue) libhoney.getTransmission().getResponseQueue());
        libhoney.getTransmission().setResponseQueue(responseQueue);

        libhoney.getTransmission().enqueueResponse(new JSONObject());
        verify(responseQueue, never()).add(anyObject());
        verify(responseQueue, times(1)).put(anyObject());
        libhoney.close();
    }

    @Test
    public void testBlockOnSend() throws Exception {
        LibHoney libhoney = new LibHoney.Builder()
                .writeKey("wk")
                .dataSet("ds")
                .blockOnSend(true)
                .build();
        ArrayBlockingQueue requestQueue = spy((ArrayBlockingQueue) libhoney.getTransmission().getRequestQueue());
        libhoney.getTransmission().setRequestQueue(requestQueue);

        HoneyEvent honeyEvent = libhoney.createFieldBuilder().build().createEvent();
        libhoney.getTransmission().enqueueRequest(honeyEvent);
        verify(requestQueue, never()).add(anyObject());
        verify(requestQueue, times(1)).put(anyObject());
        libhoney.close();
    }

    @Test
    public void testQueueOverflow() throws Exception {
        FieldHolder fieldHolder = new FieldHolder();
        fieldHolder.addField("foo", 4);
        LibHoney libhoney = new LibHoney.Builder()
                .writeKey("wk")
                .dataSet("ds")
                .maxConcurrentBranches(1)
                .fieldHolder(fieldHolder)
                .build();
        Transmission transmission = spy(libhoney.getTransmission());
        transmission.setRequestQueue(new ArrayBlockingQueue<>(1));
        libhoney.setTransmission(transmission);
        libhoney.createFieldBuilder().build().createEvent().send();
        libhoney.createFieldBuilder().build().createEvent().send();
        verify(transmission, times(1)).createJsonError(eq("event dropped; queue overflow"), anyString());
        libhoney.close();
    }

    @Test
    public void testSend() throws Exception {
        FieldHolder fieldHolder = new FieldHolder();
        fieldHolder.addField("foo", "bar");
        LibHoney libhoney = new LibHoney.Builder()
                .writeKey("writeme")
                .dataSet("datame")
                .apiHost("http://urlme")
                .metadata("metame")
                .sampleRate(1)
                .fieldHolder(fieldHolder)
                .build();

        Transmission transmission = spy(libhoney.getTransmission());
        libhoney.setTransmission(transmission);

        HoneyEvent honeyEvent = libhoney.createFieldBuilder().build().createEvent();
        honeyEvent.send();

        verify(transmission, times(1)).enqueueRequest(honeyEvent);
    }
}