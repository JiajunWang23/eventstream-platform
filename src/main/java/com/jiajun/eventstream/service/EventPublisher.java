package com.jiajun.eventstream.service;

import com.jiajun.eventstream.config.AppProperties;
import com.jiajun.eventstream.domain.StreamDefinition;
import com.jiajun.eventstream.exception.PublishException;
import com.jiajun.eventstream.messaging.EventEnvelope;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Produces events onto a stream's Kafka topic. The call blocks until the broker acknowledges
 * (acks=all, idempotent producer) so the GraphQL caller gets a real partition/offset back.
 */
@Service
public class EventPublisher {

    private static final Logger log = LoggerFactory.getLogger(EventPublisher.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final StreamService streamService;
    private final AppProperties properties;
    private final Counter publishedCounter;
    private final Counter failedCounter;

    public EventPublisher(KafkaTemplate<String, Object> kafkaTemplate,
                          StreamService streamService,
                          AppProperties properties,
                          MeterRegistry meterRegistry) {
        this.kafkaTemplate = kafkaTemplate;
        this.streamService = streamService;
        this.properties = properties;
        this.publishedCounter = meterRegistry.counter("eventstream.events.published");
        this.failedCounter = meterRegistry.counter("eventstream.events.publish_failed");
    }

    public PublishReceipt publish(String stream, String type, String source, String key,
                                  Map<String, Object> payload, Instant occurredAt) {
        if (type == null || type.isBlank()) {
            throw new IllegalArgumentException("Event type must not be blank");
        }
        if (payload == null) {
            throw new IllegalArgumentException("Event payload must not be null");
        }
        StreamDefinition definition = streamService.get(stream);

        String eventId = UUID.randomUUID().toString();
        String partitionKey = (key == null || key.isBlank()) ? eventId : key;
        EventEnvelope envelope = new EventEnvelope(
                eventId, stream, type, source, payload, occurredAt == null ? Instant.now() : occurredAt);

        try {
            SendResult<String, Object> result = kafkaTemplate
                    .send(definition.topic(), partitionKey, envelope)
                    .get(properties.kafka().publishTimeoutSeconds(), TimeUnit.SECONDS);
            RecordMetadata metadata = result.getRecordMetadata();
            publishedCounter.increment();
            log.debug("Published event {} to {}-{}@{}", eventId, metadata.topic(), metadata.partition(), metadata.offset());
            return new PublishReceipt(eventId, stream, metadata.topic(), metadata.partition(), metadata.offset(), Instant.now());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            failedCounter.increment();
            throw new PublishException("Interrupted while publishing to " + definition.topic(), e);
        } catch (ExecutionException | TimeoutException e) {
            failedCounter.increment();
            throw new PublishException("Failed to publish to " + definition.topic() + ": " + e.getMessage(), e);
        }
    }
}
