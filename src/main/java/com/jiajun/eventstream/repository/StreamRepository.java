package com.jiajun.eventstream.repository;

import com.jiajun.eventstream.domain.StreamDefinition;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.List;

public interface StreamRepository extends MongoRepository<StreamDefinition, String> {

    List<StreamDefinition> findAllByOrderByCreatedAtAsc();
}
