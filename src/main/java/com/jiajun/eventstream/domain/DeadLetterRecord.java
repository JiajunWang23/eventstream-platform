package com.jiajun.eventstream.domain;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;

/**
 * A message that failed processing and was routed to a dead-letter topic.
 */
@Document(collection = "dead_letters")
public record DeadLetterRecord(
        @Id String id,
        @Indexed String stream,
        String originalTopic,
        String dlqTopic,
        String key,
        String payload,
        String reason,
        Instant failedAt) {
}
