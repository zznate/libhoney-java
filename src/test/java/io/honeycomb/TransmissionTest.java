package io.honeycomb;

import org.json.JSONObject;
import org.junit.Test;

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
                .apiHost("uuu")
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
                .apiHost("uuu")
                .writeKey("wk")
                .dataSet("ds")
                .closeTimeout(1) // seconds
                .blockOnSend(true)
                .build();
        ArrayBlockingQueue requestQueue = spy((ArrayBlockingQueue) libhoney.getTransmission().getRequestQueue());
        libhoney.getTransmission().setRequestQueue(requestQueue);

        Event event = libhoney.newBuilder().newEvent();
        libhoney.getTransmission().enqueueRequest(event);
        verify(requestQueue, never()).add(anyObject());
        verify(requestQueue, times(1)).put(anyObject());
        libhoney.close();
    }

    @Test
    public void testQueueOverflow() throws Exception {
        LibHoney libhoney = new LibHoney.Builder()
                .apiHost("uuu")
                .writeKey("wk")
                .dataSet("ds")
                .closeTimeout(1) // seconds
                .maxConcurrentBranches(1)
                .build();
        libhoney.addField("foo", 4);
        Transmission transmission = spy(libhoney.getTransmission());
        transmission.setRequestQueue(new ArrayBlockingQueue<>(1));
        libhoney.setTransmission(transmission);
        libhoney.newBuilder().newEvent().send();
        libhoney.newBuilder().newEvent().send();
        verify(transmission, times(1)).createJsonError(eq("event dropped; queue overflow"), anyString());
        libhoney.close();
    }

    @Test
    public void testSend() throws Exception {
        LibHoney libhoney = new LibHoney.Builder()
                .writeKey("writeme")
                .dataSet("datame")
                .apiHost("http://urlme")
                .sampleRate(1)
                .build();
        Builder builder = new Builder(libhoney);
        libhoney.addField("foo", "bar");

        Transmission transmission = spy(libhoney.getTransmission());
        libhoney.setTransmission(transmission);

        Event event = libhoney.newBuilder().newEvent();
        event.send();

        verify(transmission, times(1)).enqueueRequest(event);
    }
}
