package com.jiajun.eventstream.service;

import com.jiajun.eventstream.domain.EventRecord;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

/**
 * In-process fan-out of persisted events to GraphQL subscribers. Multicast, best-effort:
 * a slow subscriber is dropped rather than slowing the Kafka consumer.
 */
@Component
public class EventBroadcaster {

    private final Sinks.Many<EventRecord> sink = Sinks.many().multicast().directBestEffort();

    public void broadcast(EventRecord event) {
        sink.tryEmitNext(event);
    }

    public Flux<EventRecord> stream(String streamName, String type) {
        return sink.asFlux()
                .filter(event -> event.stream().equals(streamName))
                .filter(event -> type == null || type.isBlank() || type.equals(event.type()));
    }

    /** Current number of live subscribers (exposed for tests and metrics). */
    public int subscriberCount() {
        return sink.currentSubscriberCount();
    }
}
