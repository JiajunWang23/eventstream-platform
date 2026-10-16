package com.jiajun.eventstream;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.graphql.test.tester.GraphQlTester;
import org.springframework.graphql.test.tester.HttpGraphQlTester;
import org.springframework.test.context.TestPropertySource;
import org.springframework.web.reactive.function.client.WebClient;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * End-to-end: GraphQL mutation -> Kafka topic -> consumer -> MongoDB -> GraphQL query.
 * Runs against real Kafka and MongoDB containers; skipped automatically when Docker is unavailable.
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestPropertySource(properties = {
        "spring.kafka.consumer.properties.metadata.max.age.ms=2000",
        "spring.kafka.listener.concurrency=1"
})
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class EventStreamIntegrationTest {

    @Container
    @ServiceConnection
    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1")).withKraft();

    @Container
    @ServiceConnection
    static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

    @LocalServerPort
    int port;

    private GraphQlTester tester() {
        WebClient client = WebClient.create("http://localhost:" + port + "/graphql");
        return HttpGraphQlTester.create(client);
    }

    @Test
    @Order(1)
    void registersStreamPublishesAndQueriesEvents() {
        GraphQlTester tester = tester();

        tester.document("""
                        mutation { createStream(input: { name: "orders", description: "it", partitions: 2 }) { name topic partitions } }
                        """)
                .execute()
                .path("createStream.topic").entity(String.class).isEqualTo("events.orders")
                .path("createStream.partitions").entity(Integer.class).isEqualTo(2);

        // Pattern-subscribed consumer needs a metadata refresh to notice the new topic; publishing retries meanwhile.
        String eventId = tester.document("""
                        mutation {
                          publishEvent(input: {
                            stream: "orders", type: "OrderCreated", source: "it", key: "order-1",
                            payload: { orderId: 1, items: ["sku-1"], total: 19.99 }
                          }) { eventId topic offset }
                        }
                        """)
                .execute()
                .path("publishEvent.topic").entity(String.class).isEqualTo("events.orders")
                .path("publishEvent.eventId").entity(String.class).get();

        tester.document("""
                        mutation { publishEvent(input: { stream: "orders", type: "OrderCancelled", payload: { orderId: 1 } }) { eventId } }
                        """)
                .execute()
                .path("publishEvent.eventId").entity(String.class).satisfies(id -> assertThat(id).isNotBlank());

        await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofSeconds(1)).untilAsserted(() ->
                tester.document("""
                                { streamStats(stream: "orders") { totalEvents } }
                                """)
                        .execute()
                        .path("streamStats.totalEvents").entity(Integer.class).isEqualTo(2));

        tester.document("""
                        query($id: ID!) { event(id: $id) { id stream type key payload partition } }
                        """)
                .variable("id", eventId)
                .execute()
                .path("event.id").entity(String.class).isEqualTo(eventId)
                .path("event.type").entity(String.class).isEqualTo("OrderCreated")
                .path("event.key").entity(String.class).isEqualTo("order-1")
                .path("event.payload.orderId").entity(Integer.class).isEqualTo(1)
                .path("event.payload.items").entityList(String.class).isEqualTo(List.of("sku-1"));

        tester.document("""
                        { events(stream: "orders", type: "OrderCancelled") { totalElements content { type } } }
                        """)
                .execute()
                .path("events.totalElements").entity(Integer.class).isEqualTo(1)
                .path("events.content[0].type").entity(String.class).isEqualTo("OrderCancelled");

        tester.document("""
                        { streams { name eventCount } streamStats(stream: "orders") { byType { type count } } }
                        """)
                .execute()
                .path("streams[0].eventCount").entity(Integer.class).isEqualTo(2)
                .path("streamStats.byType").entityList(Map.class).hasSize(2);
    }

    @Test
    @Order(2)
    void rejectsUnknownStreamAndDuplicateRegistration() {
        GraphQlTester tester = tester();

        tester.document("""
                        mutation { publishEvent(input: { stream: "nope", type: "X", payload: {} }) { eventId } }
                        """)
                .execute()
                .errors().satisfy(errors -> assertThat(errors).singleElement()
                        .satisfies(e -> assertThat(e.getMessage()).contains("nope")));

        tester.document("""
                        mutation { createStream(input: { name: "orders" }) { name } }
                        """)
                .execute()
                .errors().satisfy(errors -> assertThat(errors).singleElement()
                        .satisfies(e -> assertThat(e.getMessage()).contains("already exists")));
    }
}
