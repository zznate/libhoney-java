# libhoney [![Build Status](https://travis-ci.org/honeycombio/libhoney-java.svg?branch=master)](https://travis-ci.org/honeycombio/libhoney-java)

Java library for sending events to [Honeycomb](https://honeycomb.io). (For more information, see the [documentation](https://honeycomb.io/docs).)


## Documentation

An API reference is available at https://honeycombio.github.io/libhoney-java

## Example Usage

```java
import io.honeycomb.*;

// ...

// Initialize a LibHoney instance
LibHoney libhoney = new LibHoney.Builder().writeKey(WRITE_KEY).dataSet(DATA_SET).build();

// Populate an Event and send it immediately
HashMap<String, Object> data = new HashMap();
data.put("durationMs", new Float(153.12));
data.put("method", "get");
data.put("hostname", "appserver15");
data.put("payloadLength", new Integer(27));
libhoney.sendNow(data);

// Call close() to flush any pending calls to Honeycomb and shut down gracefully
libhoney.close();
```

You can find a complete runnable example demonstrating usage in [`Example.java`](src/main/java/io/honeycomb/Example.java)

## Contributions

Features, bug fixes and other changes to libhoney are gladly accepted. Please
open issues or a pull request with your change. Remember to add your name to the
CONTRIBUTORS file!

All contributions will be released under the Apache License 2.0.
