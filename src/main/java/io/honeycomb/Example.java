package io.honeycomb;

public class Example {

    public static final String DATA_SET = "example";
    public static final String WRITE_KEY = "499e56a6f613dc79e68afd742153750e"; // TODO delete

    public static void main(String[] args) throws HoneyException, InterruptedException {
        // initialize libhoney
        LibHoney libhoney = new LibHoney.Builder()
                .writeKey(WRITE_KEY)
                .dataSet(DATA_SET)
                .build();

        Event event = libhoney.newEvent()
                .with("someKey","someValue")
                .with("differentKey","value2");

        libhoney.send(event);

        // Events represent the individual blob of data being sent to Honeycomb, and will
        // inherit any relevant fields from libhoney.
        Event event2 = libhoney.newEvent();
        event2.with("eventKey", "bar");
        // results in keys "baseKey", "baseMap1", "baseMap2", "eventKey" being sent
        libhoney.send(event2);



        // Shutdown threads gracefully
        libhoney.close();
    }
}
