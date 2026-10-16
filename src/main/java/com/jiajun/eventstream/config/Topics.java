package com.jiajun.eventstream.config;

/**
 * Topic naming conventions. Every registered stream gets its own Kafka topic
 * ({@code events.<stream>}) and a matching dead-letter topic ({@code dlq.events.<stream>}).
 */
public final class Topics {

    public static final String EVENT_PREFIX = "events";
    public static final String DLQ_PREFIX = "dlq";

    /** Regex used for pattern subscriptions. Kafka anchors the pattern (full match). */
    public static final String EVENT_TOPIC_PATTERN = EVENT_PREFIX + "\\.[a-z][a-z0-9-]+";
    public static final String DLQ_TOPIC_PATTERN = DLQ_PREFIX + "\\." + EVENT_PREFIX + "\\.[a-z][a-z0-9-]+";

    private Topics() {
    }

    public static String eventTopic(String stream) {
        return EVENT_PREFIX + "." + stream;
    }

    public static String dlqTopic(String eventTopic) {
        return DLQ_PREFIX + "." + eventTopic;
    }

    public static String streamFromTopic(String topic) {
        int idx = topic.lastIndexOf('.');
        return idx < 0 ? topic : topic.substring(idx + 1);
    }
}
