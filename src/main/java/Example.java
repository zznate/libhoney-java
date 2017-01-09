import io.honeycomb.FieldBuilder;;
import io.honeycomb.HoneyException;
import io.honeycomb.LibHoney;

import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.UUID;

public class Example {

    public static final String DATA_SET = "example";
    public static final String WRITE_KEY = "abcabc123123defdef456456";

    public static void main(String[] args) throws InterruptedException {

        // Initialize LibHoney.  All default values are inherited from LibHoney.
        LibHoney libhoney = new LibHoney.Builder()
                .writeKey(WRITE_KEY)
                .dataSet(DATA_SET)
                .maxConcurrentBranches(1)
                .sampleRate(1)
                .blockOnResponse(true)
                .build();

        /*
            There are two ways to use LibHoney.  If you want multiple builder, instantiate them outside of LibHoney.
         */
        // Create our first builder
        HashMap<String, Object> fields = new HashMap<>();
        fields.put("foo", "bar");

        FieldBuilder exampleBuilderOne = libhoney.createFieldBuilder()
                .fields(fields)
                .build();

        // Create our second builder
        fields.clear();
        fields.put("fizz", "buzz");

        HashMap<String, Callable> dynFields = new HashMap<>();           // Dynamic field functions are executed upon
        dynFields.put("randomUUID", () -> UUID.randomUUID().toString()); // event instantiation

        FieldBuilder exampleBuilderTwo = libhoney.createFieldBuilder()
                .fields(fields)
                .dynFields(dynFields)
                .build();

        System.out.println(exampleBuilderOne.createEvent().toString());
        System.out.println(exampleBuilderTwo.createEvent().toString());

        // Create and send some events!
        try {
            for (int i = 0; i < 3; i++) {
                exampleBuilderOne.createEvent().send();
                exampleBuilderTwo.createEvent().send();
            }
        } catch (HoneyException e) {
            // Something went wrong
        }

        /*
            If you only want a single builder, you can do everything through LibHoney.
         */
        fields.clear();
        fields.put("numbers", 123);
        try {
            libhoney.createFieldBuilder().fields(fields).build() // Create a builder,
                    .createEvent().send();                       // then create and send an event
        } catch (HoneyException e) {
            // Something went wrong
        }

        /*
            You can also use LibHoney's FieldBuilder.
            Anything added to LibHoney's FieldBuilder will be passed down to new FieldBuilders.
         */
        fields.clear();
        libhoney.getFieldHolder().addField("added to", "all new builders");
        try {
            libhoney.createFieldBuilder().build() // Contains the field with key "added to"
                    .createEvent().send();
        } catch (HoneyException e) {
            // Something went wrong
        }

        /*
            HTTP responses are stored in Transmission's response queue, but we don't do anything with them!
            However, you can easily set up a thread waiting for JSONObjects to appear in the response queue.
            Here's one way to interact with the queue:
         */

        while (libhoney.getTransmission().getResponseQueue().peek() != null) {
            libhoney.getTransmission().getResponseQueue().remove();
        }

        // Shutdown threads gracefully
        libhoney.close();
    }
}
