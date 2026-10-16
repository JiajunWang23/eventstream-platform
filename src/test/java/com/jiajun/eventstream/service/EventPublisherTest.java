package com.jiajun.eventstream.service;

import com.jiajun.eventstream.config.AppProperties;
import com.jiajun.eventstream.domain.StreamDefinition;
import com.jiajun.eventstream.exception.PublishException;
import com.jiajun.eventstream.messaging.EventEnvelope;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class EventPublisherTest {

    @Mock
    KafkaTemplate<String, Object> kafkaTemplate;

    @Mock
    StreamService streamService;

    SimpleMeterRegistry meterRegistry;
    EventPublisher publisher;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        AppProperties properties = new AppProperties(
                new AppProperties.Kafka("events", "dlq", (short) 1, 2),
                new AppProperties.Streams(3, 168));
        publisher = new EventPublisher(kafkaTemplate, streamService, properties, meterRegistry);
        lenient().when(streamService.get("orders")).thenReturn(
                new StreamDefinition("orders", null, "events.orders", 3, 168, Instant.now()));
    }

    @Test
    void publishesEnvelopeAndReturnsReceipt() {
        ArgumentCaptor<Object> valueCaptor = ArgumentCaptor.forClass(Object.class);
        when(kafkaTemplate.send(eq("events.orders"), eq("order-42"), valueCaptor.capture()))
                .thenAnswer(inv -> CompletableFuture.completedFuture(sendResult("events.orders", 1, 99L)));

        Instant occurredAt = Instant.parse("2026-10-16T12:00:00Z");
        PublishReceipt receipt = publisher.publish("orders", "OrderCreated", "checkout", "order-42",
                Map.of("orderId", 42, "total", 19.99), occurredAt);

        assertThat(receipt.stream()).isEqualTo("orders");
        assertThat(receipt.topic()).isEqualTo("events.orders");
        assertThat(receipt.partition()).isEqualTo(1);
        assertThat(receipt.offset()).isEqualTo(99L);
        assertThat(receipt.eventId()).isNotBlank();

        EventEnvelope envelope = (EventEnvelope) valueCaptor.getValue();
        assertThat(envelope.eventId()).isEqualTo(receipt.eventId());
        assertThat(envelope.stream()).isEqualTo("orders");
        assertThat(envelope.type()).isEqualTo("OrderCreated");
        assertThat(envelope.source()).isEqualTo("checkout");
        assertThat(envelope.payload()).containsEntry("orderId", 42);
        assertThat(envelope.occurredAt()).isEqualTo(occurredAt);
        assertThat(meterRegistry.counter("eventstream.events.published").count()).isEqualTo(1.0);
    }

    @Test
    void defaultsPartitionKeyToEventIdAndOccurredAtToNow() {
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        when(kafkaTemplate.send(eq("events.orders"), keyCaptor.capture(), any()))
                .thenAnswer(inv -> CompletableFuture.completedFuture(sendResult("events.orders", 0, 1L)));

        PublishReceipt receipt = publisher.publish("orders", "OrderCreated", null, null, Map.of(), null);

        assertThat(keyCaptor.getValue()).isEqualTo(receipt.eventId());
    }

    @Test
    void wrapsBrokerFailures() {
        when(kafkaTemplate.send(anyString(), anyString(), any()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("broker down")));

        assertThatThrownBy(() -> publisher.publish("orders", "OrderCreated", null, null, Map.of(), null))
                .isInstanceOf(PublishException.class)
                .hasMessageContaining("events.orders");
        assertThat(meterRegistry.counter("eventstream.events.publish_failed").count()).isEqualTo(1.0);
    }

    @Test
    void rejectsBlankTypeBeforeTouchingKafka() {
        assertThatThrownBy(() -> publisher.publish("orders", " ", null, null, Map.of(), null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static SendResult<String, Object> sendResult(String topic, int partition, long offset) {
        RecordMetadata metadata = new RecordMetadata(new TopicPartition(topic, partition), offset, 0, 0L, 0, 0);
        return new SendResult<>(new ProducerRecord<String, Object>(topic, "k", "v"), metadata);
    }
}
