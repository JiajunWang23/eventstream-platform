package com.jiajun.eventstream.repository;

import com.jiajun.eventstream.domain.DeadLetterRecord;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.List;

public interface DeadLetterRepository extends MongoRepository<DeadLetterRecord, String> {

    List<DeadLetterRecord> findByStreamOrderByFailedAtDesc(String stream, Pageable pageable);

    long countByStream(String stream);
}
