A Java library for sending data to Honeycomb (http://honeycomb.io)
========================================================

## Summary

libhoney is written to ease the process of sending data to Honeycomb from within
your Java code.

For an overview of how to use a honeycomb library, see our documentation at
https://honeycomb.io/docs/send-data/sdks/

For specifics on the python libhoney, check out the
[javadoc](https://honeycomb.io/docs/send-data/sdks/java/)

## Basic usage:

* Initialize LibHoney with your Honeycomb writekey and dataset
  name
* Create a FieldBuilder from LibHoney, then a HoneyEvent from that FieldBuilder
* Send that HoneyEvent
* Close LibHoney when you are finished

## Example

You can find an example demonstrating usage in `Example.java`

## Contributions

Features, bug fixes and other changes to libhoney are gladly accepted. Please
open issues or a pull request with your change. Remember to add your name to the
CONTRIBUTORS file!

All contributions will be released under the Apache License 2.0.
