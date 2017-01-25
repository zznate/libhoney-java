package io.honeycomb;

import java.util.HashMap;
import java.util.UUID;

public class Example {

    public static final String DATA_SET = "example";
    public static final String WRITE_KEY = "499e56a6f613dc79e68afd742153750e"; // TODO delete

    public static void main(String[] args) throws HoneyException, InterruptedException {

        // Initialize LibHoney.  All default values are inherited from LibHoney.
        LibHoney libhoney = new LibHoney.Builder()
                .writeKey(WRITE_KEY)
                .dataSet(DATA_SET)
                .build();

        // Set up some example data
        HashMap<String, Object> data = new HashMap();
        data.put("foo", "bar");
        data.put("fizz", "buzz");

        // (1) Maps can be sent with libhoney.sendNow
        libhoney.sendNow(data);

        // (2) To use dynamic fields or send multiple times, add data to libhoney and then call libhoney.send
        libhoney.addFields(data);
        libhoney.addDynField("randomUUID", () -> UUID.randomUUID().toString());
        for (int i = 0; i < 5; i++) {
            libhoney.send();
        }

        // (3) FieldBuilders can be instantiated for more advanced use cases
        // Note: new Builders inherit default values from LibHoney
        Builder builder = libhoney.createBuilder();
        builder.addField("added to builder", "but not libhoney");
        builder.send();

        // All HTTP responses are dropped by default.
        // If you want to keep responses, create a thread that removes responses as they are received.
        // (This code is not multi-threaded.)
        while (libhoney.getResponseQueue().peek() != null) {
            libhoney.getResponseQueue().remove();
        }

        // Shutdown threads gracefully
        libhoney.close();
    }
}
