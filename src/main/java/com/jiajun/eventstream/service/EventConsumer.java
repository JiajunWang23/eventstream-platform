package com.jiajun.eventstream.service;

import com.jiajun.eventstream.config.Topics;
import com.jiajun.eventstream.domain.EventRecord;
import com.jiajun.eventstream.messaging.EventEnvelope;
import com.jiajun.eventstream.repository.EventRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.time.Instant;

/**
 * Consumes every {@code events.*} topic, persists the event to MongoDB and fans it out to
 * GraphQL subscribers. Persisting is an upsert keyed by event id, so redelivery is idempotent.
 * Anything thrown here goes through the retry/dead-letter error handler in {@code KafkaConfig}.
 */
@Service
public class EventConsumer {

    private static final Logger log = LoggerFactory.getLogger(EventConsumer.class);

    private final EventRepository eventRepository;
    private final EventBroadcaster broadcaster;
    private final Counter consumedCounter;

    public EventConsumer(EventRepository eventRepository, EventBroadcaster broadcaster, MeterRegistry meterRegistry) {
        this.eventRepository = eventRepository;
        this.broadcaster = broadcaster;
        this.consumedCounter = meterRegistry.counter("eventstream.events.consumed");
    }

    @KafkaListener(id = "event-consumer", idIsGroup = false, topicPattern = Topics.EVENT_TOPIC_PATTERN)
    public void onEvent(ConsumerRecord<String, EventEnvelope> record) {
        EventRecord event = toRecord(record);
        eventRepository.save(event);
        broadcaster.broadcast(event);
        consumedCounter.increment();
        log.debug("Consumed event {} from {}-{}@{}", event.id(), record.topic(), record.partition(), record.offset());
    }

    EventRecord toRecord(ConsumerRecord<String, EventEnvelope> record) {
        EventEnvelope envelope = record.value();
        if (envelope == null) {
            throw new IllegalArgumentException("Record " + record.topic() + "@" + record.offset() + " has no payload");
        }
        if (envelope.eventId() == null || envelope.eventId().isBlank()) {
            throw new IllegalArgumentException("Envelope is missing eventId");
        }
        if (envelope.type() == null || envelope.type().isBlank()) {
            throw new IllegalArgumentException("Envelope is missing type");
        }
        String stream = envelope.stream() != null ? envelope.stream() : Topics.streamFromTopic(record.topic());
        Instant occurredAt = envelope.occurredAt() != null ? envelope.occurredAt() : Instant.now();
        return new EventRecord(
                envelope.eventId(),
                stream,
                envelope.type(),
                envelope.source(),
                record.key(),
                envelope.payload(),
                occurredAt,
                Instant.now(),
                record.topic(),
                record.partition(),
                record.offset());
    }
}
