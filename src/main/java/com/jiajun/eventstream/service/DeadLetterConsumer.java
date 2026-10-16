package com.jiajun.eventstream.service;

import com.jiajun.eventstream.config.Topics;
import com.jiajun.eventstream.domain.DeadLetterRecord;
import com.jiajun.eventstream.repository.DeadLetterRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

/**
 * Records dead-lettered messages in MongoDB so operators can inspect them through GraphQL.
 * Reads values as raw strings: the message may be the very thing that failed JSON deserialization.
 */
@Service
public class DeadLetterConsumer {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterConsumer.class);

    private final DeadLetterRepository deadLetterRepository;
    private final Counter deadLetterCounter;

    public DeadLetterConsumer(DeadLetterRepository deadLetterRepository, MeterRegistry meterRegistry) {
        this.deadLetterRepository = deadLetterRepository;
        this.deadLetterCounter = meterRegistry.counter("eventstream.events.dead_lettered");
    }

    @KafkaListener(id = "dlq-consumer", idIsGroup = false, topicPattern = Topics.DLQ_TOPIC_PATTERN,
            containerFactory = "dlqListenerContainerFactory")
    public void onDeadLetter(ConsumerRecord<String, String> record) {
        String originalTopic = header(record, KafkaHeaders.DLT_ORIGINAL_TOPIC);
        if (originalTopic == null) {
            originalTopic = record.topic().substring(Topics.DLQ_PREFIX.length() + 1);
        }
        String reason = header(record, KafkaHeaders.DLT_EXCEPTION_MESSAGE);
        DeadLetterRecord deadLetter = new DeadLetterRecord(
                record.topic() + "-" + record.partition() + "-" + record.offset(),
                Topics.streamFromTopic(originalTopic),
                originalTopic,
                record.topic(),
                record.key(),
                record.value(),
                reason,
                Instant.now());
        deadLetterRepository.save(deadLetter);
        deadLetterCounter.increment();
        log.warn("Dead letter recorded for stream '{}' from {}: {}", deadLetter.stream(), originalTopic, reason);
    }

    private static String header(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null || header.value() == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
