package com.jiajun.eventstream.repository;

import com.jiajun.eventstream.domain.EventRecord;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.time.Instant;

public interface EventRepository extends MongoRepository<EventRecord, String> {

    Page<EventRecord> findByStream(String stream, Pageable pageable);

    Page<EventRecord> findByStreamAndType(String stream, String type, Pageable pageable);

    long countByStream(String stream);

    long countByStreamAndOccurredAtAfter(String stream, Instant after);
}
