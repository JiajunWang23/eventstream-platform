package com.jiajun.eventstream.domain;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.CompoundIndexes;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;
import java.util.Map;

/**
 * An event after it has been consumed from Kafka and persisted. The id is the producer-assigned
 * event id, so re-delivered records upsert instead of duplicating.
 */
@Document(collection = "events")
@CompoundIndexes({
        @CompoundIndex(name = "stream_occurred", def = "{'stream': 1, 'occurredAt': -1}"),
        @CompoundIndex(name = "stream_type_occurred", def = "{'stream': 1, 'type': 1, 'occurredAt': -1}")
})
public record EventRecord(
        @Id String id,
        String stream,
        String type,
        String source,
        String key,
        Map<String, Object> payload,
        Instant occurredAt,
        Instant ingestedAt,
        String topic,
        int partition,
        long offset) {
}
