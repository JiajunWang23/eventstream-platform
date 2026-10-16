# EventStream Platform

[![CI](https://github.com/JiajunWang23/eventstream-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/JiajunWang23/eventstream-platform/actions/workflows/ci.yml)

A scalable, event-driven platform that exposes **GraphQL APIs over Kafka streams**, keeps stream metadata and the event history in **MongoDB**, and ships through an automated **Jenkins CI/CD** pipeline.

Clients register named streams, publish events to them with GraphQL mutations, query and aggregate history, and subscribe to live events over a GraphQL WebSocket. Kafka is the source of truth for ordering and durability; MongoDB is the queryable read model.

**Stack:** Java 21 · Spring Boot 3.3 · Spring for GraphQL · Spring Kafka · Spring Data MongoDB · Micrometer/Prometheus · Testcontainers · Docker · Jenkins · Kubernetes

---

## Architecture

```
                ┌──────────────────────── EventStream Platform (Spring Boot) ────────────────────────┐
                │                                                                                     │
 GraphQL client │  Mutation.createStream ──► StreamService ──► KafkaAdmin: events.<name>, dlq.events.<name>
 (HTTP / WS)    │                                       └────► MongoDB  streams                       │
   ───────────► │                                                                                     │
                │  Mutation.publishEvent ──► EventPublisher ──► Kafka events.<name>  (acks=all, idempotent)
                │                                                     │                               │
                │                                   pattern listener  ▼  events\..*                   │
                │                              EventConsumer ──► MongoDB events  (upsert by eventId)  │
                │                                    │       └──► EventBroadcaster (Reactor sink)     │
                │                    retry ×2, then │                     │                         │
                │                                   ▼                     ▼                           │
                │                     Kafka dlq.events.<name>   Subscription.eventFeed (WebSocket)    │
                │                                   │                                                 │
                │                    DeadLetterConsumer ──► MongoDB dead_letters                      │
                │                                                                                     │
                │  Query.events / streamStats / deadLetters ◄── EventQueryService (indexed + $group)   │
                └─────────────────────────────────────────────────────────────────────────────────────┘
```

### Design notes

- **One topic per stream.** `createStream` creates `events.<name>` with its own partition count and retention, plus a matching `dlq.events.<name>`. Topic auto-creation is disabled on the broker, so a typo can't create a stray topic.
- **Pattern subscription.** One `@KafkaListener` subscribes to `events\.[a-z][a-z0-9-]+`, so a new stream starts flowing without a redeploy (Kafka picks it up on the next metadata refresh, `metadata.max.age.ms`).
- **Acknowledged publishes.** `publishEvent` blocks until the broker acks (`acks=all`, idempotent producer) and returns the real partition and offset. Events with the same `key` keep their order within a partition.
- **Idempotent consumption.** The producer assigns the event id, and the consumer upserts by it, so redelivery after a rebalance or retry never duplicates history.
- **Retry, then dead-letter.** `DefaultErrorHandler` retries a failed record twice, then publishes it to the stream's DLQ. Validation failures skip retries. Poison messages that fail JSON deserialization go to the DLQ byte-for-byte through a separate byte-array template, and a raw-string DLQ consumer records them in MongoDB, where they can be queried with `deadLetters`.
- **No N+1.** `Stream.eventCount` is a `@BatchMapping` resolved with a single `$group` aggregation for the whole result set.
- **Indexed reads.** Compound indexes on `(stream, occurredAt)` and `(stream, type, occurredAt)` back paging and type filters.
- **Live feed.** Persisted events go out through a multicast Reactor sink to GraphQL subscriptions. A slow subscriber is dropped instead of slowing down the Kafka consumer.
- **Typed errors.** Domain exceptions map to GraphQL `NOT_FOUND` / `BAD_REQUEST` classifications instead of a generic internal error.
- **Observability.** Actuator health/readiness probes and Prometheus metrics, including `eventstream.events.published`, `.consumed`, `.publish_failed` and `.dead_lettered`.

---

## Running locally

Requirements: Docker. JDK 21 only if you want to run outside Docker.

```bash
docker compose up --build        # Kafka (KRaft) + MongoDB + the app
open http://localhost:8080/graphiql
```

Or run the app against the compose infrastructure from your IDE:

```bash
docker compose up -d kafka mongo
./gradlew bootRun
```

| Variable                  | Default                                  |
|---------------------------|------------------------------------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092`                         |
| `MONGODB_URI`             | `mongodb://localhost:27017/eventstream`  |
| `KAFKA_REPLICATION_FACTOR`| `1`                                      |
| `KAFKA_LISTENER_CONCURRENCY` | `3`                                   |
| `PORT`                    | `8080`                                   |

---

## API tour

Full schema: [`src/main/resources/graphql/schema.graphqls`](src/main/resources/graphql/schema.graphqls). GraphiQL is at `/graphiql`.

**Register a stream**

```graphql
mutation {
  createStream(input: { name: "orders", description: "Order lifecycle", partitions: 6, retentionHours: 72 }) {
    name topic partitions retentionHours createdAt
  }
}
```

**Publish an event** (`payload` is free-form JSON)

```graphql
mutation {
  publishEvent(input: {
    stream: "orders", type: "OrderCreated", source: "checkout", key: "order-1042",
    payload: { orderId: 1042, total: 58.40, items: ["sku-17", "sku-22"] }
  }) {
    eventId topic partition offset acceptedAt
  }
}
```

**Query history and stats**

```graphql
{
  events(stream: "orders", type: "OrderCreated", page: 0, size: 20) {
    totalElements hasNext
    content { id type key payload occurredAt partition offset }
  }
  streamStats(stream: "orders") {
    totalEvents eventsLast24h deadLetters
    byType { type count }
  }
  streams { name partitions eventCount }
}
```

**Subscribe to live events** (graphql-transport-ws on `ws://localhost:8080/graphql`)

```graphql
subscription {
  eventFeed(stream: "orders", type: "OrderCancelled") { id type payload occurredAt }
}
```

**Inspect dead letters**

```graphql
{ deadLetters(stream: "orders", size: 10) { originalTopic key payload reason failedAt } }
```

---

## Testing

```bash
./gradlew test              # unit + GraphQL slice tests (no Docker needed)
./gradlew integrationTest   # end-to-end with real Kafka + MongoDB via Testcontainers
./gradlew build             # compile, unit tests, JaCoCo report (build/reports/jacoco)
```

- **Unit tests:** stream validation and topic provisioning, publisher acks and failure handling, consumer mapping, rejection of malformed envelopes, subscription filtering, and the JSON/DateTime scalars.
- **GraphQL slice tests** (`@GraphQlTest`): every query, mutation and subscription against the real schema, including inline JSON literals, variables, batch-loaded fields and error classification.
- **Integration test:** GraphQL mutation → Kafka → consumer → MongoDB → GraphQL query, run against real containers.

`scripts/smoke-test.sh <base-url>` checks that a deployed instance is healthy and that an event round-trips through Kafka.

---

## CI/CD

**Jenkins** ([`Jenkinsfile`](Jenkinsfile)) runs a declarative multibranch pipeline:

1. **Checkout**
2. **Build & Unit Test.** JUnit results and the JaCoCo HTML report are published.
3. **Integration Tests.** Testcontainers runs on the agent's Docker daemon, for `main` and PRs.
4. **Build Image.** A multi-stage Docker build produces a non-root JRE image with a healthcheck.
5. **Push Image** to GHCR, tagged with branch and commit SHA, plus `latest`.
6. **Deploy.** The pipeline SSHes to the host and rolls the `app` service with `docker compose up -d`.
7. **Smoke Test** against the deployed instance.

Credentials it expects: `docker-registry` (username/password) and `deploy-ssh` (SSH key).

**GitHub Actions** ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) runs the build, unit and integration tests, and the image build on every push and PR.

**Kubernetes** ([`k8s/eventstream.yaml`](k8s/eventstream.yaml)) contains a Deployment with readiness/liveness probes and Prometheus scrape annotations, plus a Service and an HPA that scales from 2 to 8 replicas at 70% CPU.

---

## Project layout

```
src/main/java/com/jiajun/eventstream
├── config/        Kafka error handling + DLQ wiring, GraphQL scalars, topic naming, app properties
├── domain/        Mongo documents: StreamDefinition, EventRecord, DeadLetterRecord
├── messaging/     EventEnvelope (Kafka wire format)
├── repository/    Spring Data Mongo repositories
├── service/       StreamService, EventPublisher, EventConsumer, DeadLetterConsumer,
│                  EventBroadcaster, EventQueryService
└── graphql/       Controllers, DTOs, JSON/DateTime scalars, exception resolver
```
