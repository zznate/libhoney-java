package io.honeycomb;

abstract class Constants {

    public static final String LIBHONEY_VERSION = "0.0.1";

    public static final String DEFAULT_API_HOST = "https://api.honeycomb.io";
    public static final int DEFAULT_CLOSE_TIMEOUT = 10; // seconds
    public static final String DEFAULT_DATA_SET = "";
    public static final int DEFAULT_SAMPLE_RATE = 1;
    public static final String DEFAULT_USER_AGENT = "libhoney-java/" + Constants.LIBHONEY_VERSION;
    public static final String DEFAULT_WRITE_KEY = "";

    private Constants() {
        throw new IllegalAccessError("Utility class");
    }
}
