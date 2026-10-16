package com.jiajun.eventstream.graphql;

import com.jiajun.eventstream.domain.StreamDefinition;
import com.jiajun.eventstream.graphql.dto.CreateStreamInput;
import com.jiajun.eventstream.service.EventQueryService;
import com.jiajun.eventstream.service.StreamService;
import org.springframework.graphql.data.method.annotation.Argument;
import org.springframework.graphql.data.method.annotation.BatchMapping;
import org.springframework.graphql.data.method.annotation.MutationMapping;
import org.springframework.graphql.data.method.annotation.QueryMapping;
import org.springframework.stereotype.Controller;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Controller
public class StreamController {

    private final StreamService streamService;
    private final EventQueryService eventQueryService;

    public StreamController(StreamService streamService, EventQueryService eventQueryService) {
        this.streamService = streamService;
        this.eventQueryService = eventQueryService;
    }

    @QueryMapping
    public List<StreamDefinition> streams() {
        return streamService.list();
    }

    @QueryMapping
    public StreamDefinition stream(@Argument String name) {
        return streamService.exists(name) ? streamService.get(name) : null;
    }

    @MutationMapping
    public StreamDefinition createStream(@Argument CreateStreamInput input) {
        return streamService.create(input.name(), input.description(), input.partitions(), input.retentionHours());
    }

    /** Resolves {@code Stream.eventCount} for a whole page of streams with one Mongo aggregation. */
    @BatchMapping(typeName = "Stream", field = "eventCount")
    public Map<StreamDefinition, Long> eventCount(List<StreamDefinition> streams) {
        Map<String, Long> counts = eventQueryService.countByStream(
                streams.stream().map(StreamDefinition::name).toList());
        return streams.stream().collect(Collectors.toMap(s -> s, s -> counts.getOrDefault(s.name(), 0L)));
    }
}
