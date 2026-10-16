package com.jiajun.eventstream.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Application settings bound from the {@code app.*} namespace in application.yml.
 */
@ConfigurationProperties(prefix = "app")
public record AppProperties(Kafka kafka, Streams streams) {

    public record Kafka(String topicPrefix, String dlqPrefix, short replicationFactor, int publishTimeoutSeconds) {
    }

    public record Streams(int defaultPartitions, int defaultRetentionHours) {
    }
}
