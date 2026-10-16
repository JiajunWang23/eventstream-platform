package com.jiajun.eventstream.service;

import com.jiajun.eventstream.config.AppProperties;
import com.jiajun.eventstream.config.Topics;
import com.jiajun.eventstream.domain.StreamDefinition;
import com.jiajun.eventstream.exception.ConflictException;
import com.jiajun.eventstream.exception.NotFoundException;
import com.jiajun.eventstream.repository.StreamRepository;
import org.apache.kafka.common.config.TopicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Registers streams: validates the name, creates the backing Kafka topic and persists metadata.
 */
@Service
public class StreamService {

    private static final Logger log = LoggerFactory.getLogger(StreamService.class);
    private static final Pattern NAME = Pattern.compile("^[a-z][a-z0-9-]{1,48}$");

    private final StreamRepository streamRepository;
    private final KafkaAdmin kafkaAdmin;
    private final AppProperties properties;

    public StreamService(StreamRepository streamRepository, KafkaAdmin kafkaAdmin, AppProperties properties) {
        this.streamRepository = streamRepository;
        this.kafkaAdmin = kafkaAdmin;
        this.properties = properties;
    }

    public List<StreamDefinition> list() {
        return streamRepository.findAllByOrderByCreatedAtAsc();
    }

    public StreamDefinition get(String name) {
        return streamRepository.findById(name)
                .orElseThrow(() -> new NotFoundException("Stream '" + name + "' does not exist"));
    }

    public boolean exists(String name) {
        return streamRepository.existsById(name);
    }

    public StreamDefinition create(String name, String description, Integer partitions, Integer retentionHours) {
        if (name == null || !NAME.matcher(name).matches()) {
            throw new IllegalArgumentException(
                    "Stream name must be 2-49 chars of lowercase letters, digits and dashes, starting with a letter");
        }
        if (streamRepository.existsById(name)) {
            throw new ConflictException("Stream '" + name + "' already exists");
        }
        int resolvedPartitions = partitions == null ? properties.streams().defaultPartitions() : partitions;
        int resolvedRetention = retentionHours == null ? properties.streams().defaultRetentionHours() : retentionHours;
        if (resolvedPartitions < 1 || resolvedPartitions > 64) {
            throw new IllegalArgumentException("partitions must be between 1 and 64");
        }
        if (resolvedRetention < 1) {
            throw new IllegalArgumentException("retentionHours must be positive");
        }

        String topic = Topics.eventTopic(name);
        long retentionMs = Duration.ofHours(resolvedRetention).toMillis();
        kafkaAdmin.createOrModifyTopics(
                TopicBuilder.name(topic)
                        .partitions(resolvedPartitions)
                        .replicas(properties.kafka().replicationFactor())
                        .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(retentionMs))
                        .build(),
                TopicBuilder.name(Topics.dlqTopic(topic))
                        .partitions(1)
                        .replicas(properties.kafka().replicationFactor())
                        .build());

        StreamDefinition definition = new StreamDefinition(
                name, description, topic, resolvedPartitions, resolvedRetention, Instant.now());
        StreamDefinition saved = streamRepository.save(definition);
        log.info("Registered stream '{}' backed by topic '{}' ({} partitions, {}h retention)",
                name, topic, resolvedPartitions, resolvedRetention);
        return saved;
    }
}
