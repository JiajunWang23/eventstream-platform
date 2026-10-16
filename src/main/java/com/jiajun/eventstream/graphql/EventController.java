package com.jiajun.eventstream.graphql;

import com.jiajun.eventstream.domain.DeadLetterRecord;
import com.jiajun.eventstream.domain.EventRecord;
import com.jiajun.eventstream.graphql.dto.EventPage;
import com.jiajun.eventstream.graphql.dto.PublishEventInput;
import com.jiajun.eventstream.service.EventBroadcaster;
import com.jiajun.eventstream.service.EventPublisher;
import com.jiajun.eventstream.service.EventQueryService;
import com.jiajun.eventstream.service.PublishReceipt;
import com.jiajun.eventstream.service.StreamService;
import org.springframework.graphql.data.method.annotation.Argument;
import org.springframework.graphql.data.method.annotation.MutationMapping;
import org.springframework.graphql.data.method.annotation.QueryMapping;
import org.springframework.graphql.data.method.annotation.SubscriptionMapping;
import org.springframework.stereotype.Controller;
import reactor.core.publisher.Flux;

import java.util.List;

@Controller
public class EventController {

    private final EventPublisher eventPublisher;
    private final EventQueryService eventQueryService;
    private final EventBroadcaster broadcaster;
    private final StreamService streamService;

    public EventController(EventPublisher eventPublisher,
                           EventQueryService eventQueryService,
                           EventBroadcaster broadcaster,
                           StreamService streamService) {
        this.eventPublisher = eventPublisher;
        this.eventQueryService = eventQueryService;
        this.broadcaster = broadcaster;
        this.streamService = streamService;
    }

    @QueryMapping
    public EventPage events(@Argument String stream, @Argument String type, @Argument int page, @Argument int size) {
        streamService.get(stream);
        return EventPage.from(eventQueryService.events(stream, type, page, size));
    }

    @QueryMapping
    public EventRecord event(@Argument String id) {
        return eventQueryService.event(id).orElse(null);
    }

    @QueryMapping
    public EventQueryService.StreamStats streamStats(@Argument String stream) {
        streamService.get(stream);
        return eventQueryService.stats(stream);
    }

    @QueryMapping
    public List<DeadLetterRecord> deadLetters(@Argument String stream, @Argument int size) {
        streamService.get(stream);
        return eventQueryService.deadLetters(stream, size);
    }

    @MutationMapping
    public PublishReceipt publishEvent(@Argument PublishEventInput input) {
        return eventPublisher.publish(input.stream(), input.type(), input.source(), input.key(),
                input.payload(), input.occurredAt());
    }

    @SubscriptionMapping
    public Flux<EventRecord> eventFeed(@Argument String stream, @Argument String type) {
        streamService.get(stream);
        return broadcaster.stream(stream, type);
    }
}
