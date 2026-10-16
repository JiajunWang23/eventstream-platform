package com.jiajun.eventstream.messaging;

import java.time.Instant;
import java.util.Map;

/**
 * Wire format of an event on Kafka. Serialized as JSON.
 */
public record EventEnvelope(
        String eventId,
        String stream,
        String type,
        String source,
        Map<String, Object> payload,
        Instant occurredAt) {
}
