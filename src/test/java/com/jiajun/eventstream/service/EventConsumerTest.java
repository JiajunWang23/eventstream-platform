package com.jiajun.eventstream.service;

import com.jiajun.eventstream.domain.EventRecord;
import com.jiajun.eventstream.messaging.EventEnvelope;
import com.jiajun.eventstream.repository.EventRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class EventConsumerTest {

    @Mock
    EventRepository eventRepository;

    EventBroadcaster broadcaster;
    EventConsumer consumer;

    @BeforeEach
    void setUp() {
        broadcaster = new EventBroadcaster();
        consumer = new EventConsumer(eventRepository, broadcaster, new SimpleMeterRegistry());
    }

    @Test
    void persistsAndBroadcastsEvent() {
        Instant occurredAt = Instant.parse("2026-10-16T12:00:00Z");
        EventEnvelope envelope = new EventEnvelope("evt-1", "orders", "OrderCreated", "checkout",
                Map.of("orderId", 42), occurredAt);
        ConsumerRecord<String, EventEnvelope> record = new ConsumerRecord<>("events.orders", 2, 17L, "order-42", envelope);

        StepVerifier.create(broadcaster.stream("orders", null).take(1))
                .then(() -> consumer.onEvent(record))
                .assertNext(event -> {
                    assertThat(event.id()).isEqualTo("evt-1");
                    assertThat(event.key()).isEqualTo("order-42");
                })
                .verifyComplete();

        ArgumentCaptor<EventRecord> saved = ArgumentCaptor.forClass(EventRecord.class);
        verify(eventRepository).save(saved.capture());
        EventRecord event = saved.getValue();
        assertThat(event.id()).isEqualTo("evt-1");
        assertThat(event.stream()).isEqualTo("orders");
        assertThat(event.type()).isEqualTo("OrderCreated");
        assertThat(event.occurredAt()).isEqualTo(occurredAt);
        assertThat(event.topic()).isEqualTo("events.orders");
        assertThat(event.partition()).isEqualTo(2);
        assertThat(event.offset()).isEqualTo(17L);
        assertThat(event.ingestedAt()).isAfterOrEqualTo(occurredAt);
    }

    @Test
    void broadcasterFiltersByStreamAndType() {
        EventEnvelope orders = new EventEnvelope("evt-1", "orders", "OrderCreated", null, Map.of(), Instant.now());
        EventEnvelope payments = new EventEnvelope("evt-2", "payments", "PaymentTaken", null, Map.of(), Instant.now());
        EventEnvelope cancelled = new EventEnvelope("evt-3", "orders", "OrderCancelled", null, Map.of(), Instant.now());

        StepVerifier.create(broadcaster.stream("orders", "OrderCancelled").take(1))
                .then(() -> {
                    consumer.onEvent(new ConsumerRecord<>("events.orders", 0, 0L, null, orders));
                    consumer.onEvent(new ConsumerRecord<>("events.payments", 0, 0L, null, payments));
                    consumer.onEvent(new ConsumerRecord<>("events.orders", 0, 1L, null, cancelled));
                })
                .assertNext(event -> assertThat(event.id()).isEqualTo("evt-3"))
                .expectComplete()
                .verify(Duration.ofSeconds(5));
    }

    @Test
    void fallsBackToTopicDerivedStreamAndNow() {
        EventEnvelope envelope = new EventEnvelope("evt-9", null, "Ping", null, Map.of(), null);

        EventRecord event = consumer.toRecord(new ConsumerRecord<>("events.health", 0, 0L, null, envelope));

        assertThat(event.stream()).isEqualTo("health");
        assertThat(event.occurredAt()).isNotNull();
    }

    @Test
    void rejectsMalformedEnvelopesSoTheyAreDeadLettered() {
        EventEnvelope noId = new EventEnvelope(null, "orders", "OrderCreated", null, Map.of(), Instant.now());
        EventEnvelope noType = new EventEnvelope("evt-1", "orders", "", null, Map.of(), Instant.now());

        assertThatThrownBy(() -> consumer.onEvent(new ConsumerRecord<>("events.orders", 0, 0L, null, noId)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> consumer.onEvent(new ConsumerRecord<>("events.orders", 0, 0L, null, noType)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> consumer.onEvent(new ConsumerRecord<>("events.orders", 0, 0L, null, null)))
                .isInstanceOf(IllegalArgumentException.class);
        verify(eventRepository, never()).save(org.mockito.ArgumentMatchers.any());
    }
}
