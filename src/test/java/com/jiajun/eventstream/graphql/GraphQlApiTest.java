package com.jiajun.eventstream.graphql;

import com.jiajun.eventstream.config.GraphQlConfig;
import com.jiajun.eventstream.domain.EventRecord;
import com.jiajun.eventstream.domain.StreamDefinition;
import com.jiajun.eventstream.exception.NotFoundException;
import com.jiajun.eventstream.service.EventBroadcaster;
import com.jiajun.eventstream.service.EventPublisher;
import com.jiajun.eventstream.service.EventQueryService;
import com.jiajun.eventstream.service.PublishReceipt;
import com.jiajun.eventstream.service.StreamService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.graphql.GraphQlTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.graphql.execution.ErrorType;
import org.springframework.graphql.test.tester.GraphQlTester;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.when;

/**
 * Exercises the GraphQL schema, scalars, controllers and error mapping with mocked services.
 */
@GraphQlTest(controllers = {StreamController.class, EventController.class})
@Import({GraphQlConfig.class, GraphQlExceptionResolver.class})
class GraphQlApiTest {

    private static final Instant NOW = Instant.parse("2026-10-16T12:00:00Z");

    @Autowired
    GraphQlTester graphQlTester;

    @MockBean
    StreamService streamService;

    @MockBean
    EventQueryService eventQueryService;

    @MockBean
    EventPublisher eventPublisher;

    @MockBean
    EventBroadcaster broadcaster;

    @Test
    void listsStreamsWithBatchedEventCounts() {
        when(streamService.list()).thenReturn(List.of(
                new StreamDefinition("orders", "Orders", "events.orders", 3, 168, NOW),
                new StreamDefinition("payments", null, "events.payments", 6, 24, NOW)));
        when(eventQueryService.countByStream(any())).thenReturn(Map.of("orders", 12L, "payments", 0L));

        graphQlTester.document("""
                        { streams { name topic partitions createdAt eventCount } }
                        """)
                .execute()
                .path("streams").entityList(Object.class).hasSize(2)
                .path("streams[0].name").entity(String.class).isEqualTo("orders")
                .path("streams[0].createdAt").entity(String.class).isEqualTo("2026-10-16T12:00:00Z")
                .path("streams[0].eventCount").entity(Integer.class).isEqualTo(12)
                .path("streams[1].eventCount").entity(Integer.class).isEqualTo(0);
    }

    @Test
    void createStreamMutation() {
        when(streamService.create("orders", "Order lifecycle", 6, null))
                .thenReturn(new StreamDefinition("orders", "Order lifecycle", "events.orders", 6, 168, NOW));
        when(eventQueryService.countByStream(any())).thenReturn(Map.of("orders", 0L));

        graphQlTester.document("""
                        mutation {
                          createStream(input: { name: "orders", description: "Order lifecycle", partitions: 6 }) {
                            name topic partitions retentionHours eventCount
                          }
                        }
                        """)
                .execute()
                .path("createStream.topic").entity(String.class).isEqualTo("events.orders")
                .path("createStream.partitions").entity(Integer.class).isEqualTo(6)
                .path("createStream.retentionHours").entity(Integer.class).isEqualTo(168);
    }

    @Test
    void publishEventWithInlineJsonPayload() {
        when(eventPublisher.publish(eq("orders"), eq("OrderCreated"), eq("checkout"), eq("order-42"), any(), isNull()))
                .thenReturn(new PublishReceipt("evt-1", "orders", "events.orders", 1, 99L, NOW));

        graphQlTester.document("""
                        mutation {
                          publishEvent(input: {
                            stream: "orders", type: "OrderCreated", source: "checkout", key: "order-42",
                            payload: { orderId: 42, items: ["sku-1", "sku-2"], rush: true }
                          }) { eventId topic partition offset acceptedAt }
                        }
                        """)
                .execute()
                .path("publishEvent.eventId").entity(String.class).isEqualTo("evt-1")
                .path("publishEvent.partition").entity(Integer.class).isEqualTo(1)
                .path("publishEvent.offset").entity(String.class).isEqualTo("99")
                .path("publishEvent.acceptedAt").entity(String.class).isEqualTo("2026-10-16T12:00:00Z");
    }

    @Test
    void publishEventWithVariablePayloadAndOccurredAt() {
        when(eventPublisher.publish(eq("orders"), eq("OrderCreated"), isNull(), isNull(), any(), eq(NOW)))
                .thenReturn(new PublishReceipt("evt-2", "orders", "events.orders", 0, 1L, NOW));

        graphQlTester.document("""
                        mutation Publish($input: PublishEventInput!) {
                          publishEvent(input: $input) { eventId }
                        }
                        """)
                .variable("input", Map.of(
                        "stream", "orders",
                        "type", "OrderCreated",
                        "payload", Map.of("orderId", 7),
                        "occurredAt", "2026-10-16T12:00:00Z"))
                .execute()
                .path("publishEvent.eventId").entity(String.class).isEqualTo("evt-2");
    }

    @Test
    void pagesEventsAndReturnsJsonPayload() {
        EventRecord event = new EventRecord("evt-1", "orders", "OrderCreated", "checkout", "order-42",
                Map.of("orderId", 42, "nested", Map.of("ok", true)), NOW, NOW, "events.orders", 1, 99L);
        when(streamService.get("orders")).thenReturn(new StreamDefinition("orders", null, "events.orders", 3, 168, NOW));
        when(eventQueryService.events("orders", "OrderCreated", 0, 10))
                .thenReturn(new PageImpl<>(List.of(event), PageRequest.of(0, 10), 25));

        graphQlTester.document("""
                        { events(stream: "orders", type: "OrderCreated", size: 10) {
                            totalElements totalPages hasNext
                            content { id payload occurredAt offset }
                        } }
                        """)
                .execute()
                .path("events.totalElements").entity(Integer.class).isEqualTo(25)
                .path("events.totalPages").entity(Integer.class).isEqualTo(3)
                .path("events.hasNext").entity(Boolean.class).isEqualTo(true)
                .path("events.content[0].payload.orderId").entity(Integer.class).isEqualTo(42)
                .path("events.content[0].payload.nested.ok").entity(Boolean.class).isEqualTo(true)
                .path("events.content[0].offset").entity(String.class).isEqualTo("99");
    }

    @Test
    void streamStatsQuery() {
        when(streamService.get("orders")).thenReturn(new StreamDefinition("orders", null, "events.orders", 3, 168, NOW));
        when(eventQueryService.stats("orders")).thenReturn(new EventQueryService.StreamStats(
                "orders", 100, 40, 2, List.of(new EventQueryService.TypeCount("OrderCreated", 60),
                new EventQueryService.TypeCount("OrderCancelled", 40))));

        graphQlTester.document("""
                        { streamStats(stream: "orders") { totalEvents eventsLast24h deadLetters byType { type count } } }
                        """)
                .execute()
                .path("streamStats.totalEvents").entity(Integer.class).isEqualTo(100)
                .path("streamStats.deadLetters").entity(Integer.class).isEqualTo(2)
                .path("streamStats.byType[0].type").entity(String.class).isEqualTo("OrderCreated")
                .path("streamStats.byType[1].count").entity(Integer.class).isEqualTo(40);
    }

    @Test
    void unknownStreamMapsToNotFound() {
        when(streamService.get("ghost")).thenThrow(new NotFoundException("Stream 'ghost' does not exist"));

        graphQlTester.document("""
                        { streamStats(stream: "ghost") { totalEvents } }
                        """)
                .execute()
                .errors()
                .satisfy(errors -> {
                    assertThat(errors).hasSize(1);
                    assertThat(errors.get(0).getErrorType()).isEqualTo(ErrorType.NOT_FOUND);
                    assertThat(errors.get(0).getMessage()).contains("ghost");
                });
    }

    @Test
    void validationErrorsMapToBadRequest() {
        when(streamService.create(eq("Bad Name"), isNull(), isNull(), isNull()))
                .thenThrow(new IllegalArgumentException("Stream name must be lowercase"));

        graphQlTester.document("""
                        mutation { createStream(input: { name: "Bad Name" }) { name } }
                        """)
                .execute()
                .errors()
                .satisfy(errors -> {
                    assertThat(errors).hasSize(1);
                    assertThat(errors.get(0).getErrorType()).isEqualTo(ErrorType.BAD_REQUEST);
                });
    }

    @Test
    void subscriptionStreamsLiveEvents() {
        when(streamService.get("orders")).thenReturn(new StreamDefinition("orders", null, "events.orders", 3, 168, NOW));
        EventRecord first = new EventRecord("evt-1", "orders", "OrderCreated", null, null, Map.of(), NOW, NOW, "events.orders", 0, 0L);
        EventRecord second = new EventRecord("evt-2", "orders", "OrderCreated", null, null, Map.of(), NOW, NOW, "events.orders", 0, 1L);
        when(broadcaster.stream("orders", null)).thenReturn(Flux.just(first, second));

        Flux<String> ids = graphQlTester.document("""
                        subscription { eventFeed(stream: "orders") { id type } }
                        """)
                .executeSubscription()
                .toFlux("eventFeed.id", String.class);

        StepVerifier.create(ids)
                .expectNext("evt-1", "evt-2")
                .verifyComplete();
    }
}
