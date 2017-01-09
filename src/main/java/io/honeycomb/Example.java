package io.honeycomb;

import java.util.UUID;

public class Example {

    public static final String DATA_SET = "example";
    public static final String WRITE_KEY = "499e56a6f613dc79e68afd742153750e";

    public static void main(String[] args) throws InterruptedException {

        // Initialize LibHoney.  All default values are inherited from LibHoney.
        LibHoney libhoney = new LibHoney.Builder()
                .writeKey(WRITE_KEY)
                .dataSet(DATA_SET)
                .maxConcurrentBranches(1)
                .sampleRate(1)
                .blockOnResponse(true)
                .build();

        // You can use LibHoney in several different ways.
        // (1) SendNow is the simplest way to send single key-value pairs.
        try {
            libhoney.sendNow("key", "value");
        } catch (HoneyException e) {
            // Something went wrong
        }

        // (2) To send more complex data, first create a FieldHolder...
        FieldHolder exampleHolder = libhoney.createFieldHolder();
        exampleHolder.addField("foo", "bar");
        exampleHolder.addField("fizz", "buzz");
        exampleHolder.addDynField("randomUUID", () -> UUID.randomUUID().toString());

        try {
            for (int i = 0; i < 5; i++) {
                // Then create and send an event!
                exampleHolder.createEvent().send();
            }
        } catch (HoneyException e) {
            // Something went wrong
        }

        // (3) If you only ever need one FieldHolder, you can just use LibHoney's.
        // Note: all FieldHolders inherit default values from LibHoney.
        libhoney.addDefaultField("added to", "all new fieldholders");
        try {
            libhoney.createFieldHolder().createEvent().send();
        } catch (HoneyException e) {
            // Something went wrong
        }

        // All HTTP responses are dropped by default.
        // If you want to keep responses, create a thread that removes responses as they are received.
        // (This code is not multi-threaded.)
        while (libhoney.getTransmission().getResponseQueue().peek() != null) {
            libhoney.getTransmission().getResponseQueue().remove();
        }

        // Shutdown threads gracefully
        libhoney.close();
    }
}
