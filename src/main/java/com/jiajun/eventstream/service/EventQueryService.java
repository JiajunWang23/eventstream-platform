package com.jiajun.eventstream.service;

import com.jiajun.eventstream.domain.DeadLetterRecord;
import com.jiajun.eventstream.domain.EventRecord;
import com.jiajun.eventstream.repository.DeadLetterRepository;
import com.jiajun.eventstream.repository.EventRepository;
import org.bson.Document;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Read side: paged event queries and aggregated statistics from MongoDB.
 */
@Service
public class EventQueryService {

    private static final int MAX_PAGE_SIZE = 200;

    private final EventRepository eventRepository;
    private final DeadLetterRepository deadLetterRepository;
    private final MongoTemplate mongoTemplate;

    public EventQueryService(EventRepository eventRepository,
                             DeadLetterRepository deadLetterRepository,
                             MongoTemplate mongoTemplate) {
        this.eventRepository = eventRepository;
        this.deadLetterRepository = deadLetterRepository;
        this.mongoTemplate = mongoTemplate;
    }

    public Page<EventRecord> events(String stream, String type, int page, int size) {
        Pageable pageable = PageRequest.of(
                Math.max(page, 0),
                Math.min(Math.max(size, 1), MAX_PAGE_SIZE),
                Sort.by(Sort.Direction.DESC, "occurredAt"));
        if (type == null || type.isBlank()) {
            return eventRepository.findByStream(stream, pageable);
        }
        return eventRepository.findByStreamAndType(stream, type, pageable);
    }

    public Optional<EventRecord> event(String id) {
        return eventRepository.findById(id);
    }

    public List<DeadLetterRecord> deadLetters(String stream, int size) {
        return deadLetterRepository.findByStreamOrderByFailedAtDesc(
                stream, PageRequest.of(0, Math.min(Math.max(size, 1), MAX_PAGE_SIZE)));
    }

    public StreamStats stats(String stream) {
        long total = eventRepository.countByStream(stream);
        long last24h = eventRepository.countByStreamAndOccurredAtAfter(stream, Instant.now().minus(Duration.ofHours(24)));
        long deadLetters = deadLetterRepository.countByStream(stream);
        List<TypeCount> byType = countByField("type", Criteria.where("stream").is(stream)).entrySet().stream()
                .map(e -> new TypeCount(e.getKey(), e.getValue()))
                .sorted((a, b) -> Long.compare(b.count(), a.count()))
                .toList();
        return new StreamStats(stream, total, last24h, deadLetters, byType);
    }

    /** One aggregation query for the event counts of many streams (used by the GraphQL batch loader). */
    public Map<String, Long> countByStream(Collection<String> streams) {
        Map<String, Long> counts = countByField("stream", Criteria.where("stream").in(streams));
        for (String stream : streams) {
            counts.putIfAbsent(stream, 0L);
        }
        return counts;
    }

    private Map<String, Long> countByField(String field, Criteria criteria) {
        Aggregation aggregation = Aggregation.newAggregation(
                Aggregation.match(criteria),
                Aggregation.group(field).count().as("count"));
        List<Document> results = mongoTemplate.aggregate(aggregation, "events", Document.class).getMappedResults();
        Map<String, Long> counts = new HashMap<>();
        for (Document doc : results) {
            Object id = doc.get("_id");
            Object count = doc.get("count");
            if (id != null && count instanceof Number number) {
                counts.put(id.toString(), number.longValue());
            }
        }
        return counts;
    }

    public record StreamStats(String stream, long totalEvents, long eventsLast24h, long deadLetters, List<TypeCount> byType) {
    }

    public record TypeCount(String type, long count) {
    }
}
