package io.honeycomb;

import java.util.HashMap;
import java.util.UUID;

public class Example {

    public static final String DATA_SET = "example";
    public static final String WRITE_KEY = "499e56a6f613dc79e68afd742153750e"; // TODO delete

    public static void main(String[] args) throws HoneyException, InterruptedException {
        // initialize libhoney
        LibHoney libhoney = new LibHoney.Builder()
                .writeKey(WRITE_KEY)
                .dataSet(DATA_SET)
                .build();

        // you can add simple key/value pairs:
        libhoney.addField("baseKey", "foo");

        // or maps, all at once:
        HashMap<String, Object> dataLH = new HashMap();
        dataLH.put("baseMap1", 1);
        dataLH.put("baseMap2", 2);
        libhoney.add(dataLH);

        // sendNow takes a map and
        HashMap<String, Object> dataSendNow = new HashMap();
        dataSendNow.put("now", "nowValue");
        // results in keys "baseKey", "baseMap1", "baseMap2", and "now" being sent
        libhoney.sendNow(dataSendNow);

        // Events represent the individual blob of data being sent to Honeycomb, and will
        // inherit any relevant fields from libhoney.
        Event event = libhoney.newEvent();
        event.addField("eventKey", "bar");
        // results in keys "baseKey", "baseMap1", "baseMap2", "eventKey" being sent
        event.send();

        // Builders can be instantiated as intermediate data structures for more
        // advanced use cases
        Builder builder = libhoney.newBuilder();
        builder.addField("builderKey", "baz");
        // results in keys "baseKey", "baseMap1", "baseMap2", "builderKey", and "now" being sent
        builder.sendNow(dataSendNow);

        // And builders, too, can spawn child Events
        Event event2 = builder.newEvent();
        event2.addField("event2Key", "zerg");
        // results in keys "baseKey", "baseMap1", "baseMap2", "builderKey", and "event2Key" being sent
        event2.send();

        // Dynamic fields can be useful to delay the evaluation of a parameter until an
        // event is spawned
        long start = System.nanoTime();
        builder.addDynField("responseTimeNanos", () -> System.nanoTime() - start);
        Event event3 = builder.newEvent();
        // ... do work
        // results in keys "baseKey", "baseMap1", "baseMap2", "builderKey", and "responseTimeNanos" being sent
        event3.send();

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
