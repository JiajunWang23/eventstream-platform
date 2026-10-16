package com.jiajun.eventstream.domain;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;

/**
 * Metadata for a registered stream. The stream name doubles as the Mongo {@code _id}.
 */
@Document(collection = "streams")
public record StreamDefinition(
        @Id String name,
        String description,
        String topic,
        int partitions,
        int retentionHours,
        Instant createdAt) {
}
