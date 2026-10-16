package com.jiajun.eventstream.service;

import com.jiajun.eventstream.config.AppProperties;
import com.jiajun.eventstream.domain.StreamDefinition;
import com.jiajun.eventstream.exception.ConflictException;
import com.jiajun.eventstream.exception.NotFoundException;
import com.jiajun.eventstream.repository.StreamRepository;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StreamServiceTest {

    @Mock
    StreamRepository streamRepository;

    @Mock
    KafkaAdmin kafkaAdmin;

    StreamService service;

    @BeforeEach
    void setUp() {
        AppProperties properties = new AppProperties(
                new AppProperties.Kafka("events", "dlq", (short) 1, 10),
                new AppProperties.Streams(3, 168));
        service = new StreamService(streamRepository, kafkaAdmin, properties);
    }

    @Test
    void createsTopicAndPersistsMetadataWithDefaults() {
        when(streamRepository.existsById("orders")).thenReturn(false);
        when(streamRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        StreamDefinition created = service.create("orders", "Order lifecycle", null, null);

        assertThat(created.name()).isEqualTo("orders");
        assertThat(created.topic()).isEqualTo("events.orders");
        assertThat(created.partitions()).isEqualTo(3);
        assertThat(created.retentionHours()).isEqualTo(168);
        assertThat(created.createdAt()).isNotNull();

        ArgumentCaptor<NewTopic> topics = ArgumentCaptor.forClass(NewTopic.class);
        verify(kafkaAdmin).createOrModifyTopics(topics.capture(), topics.capture());
        assertThat(topics.getAllValues()).extracting(NewTopic::name)
                .containsExactly("events.orders", "dlq.events.orders");
        assertThat(topics.getAllValues().get(0).numPartitions()).isEqualTo(3);
        assertThat(topics.getAllValues().get(0).configs()).containsEntry("retention.ms", String.valueOf(168L * 3600 * 1000));
    }

    @Test
    void rejectsInvalidNames() {
        assertThatThrownBy(() -> service.create("Orders", null, null, null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> service.create("1orders", null, null, null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> service.create("a", null, null, null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> service.create("orders.v2", null, null, null)).isInstanceOf(IllegalArgumentException.class);
        verify(kafkaAdmin, never()).createOrModifyTopics(any());
    }

    @Test
    void rejectsDuplicateStream() {
        when(streamRepository.existsById("orders")).thenReturn(true);

        assertThatThrownBy(() -> service.create("orders", null, null, null)).isInstanceOf(ConflictException.class);
        verify(kafkaAdmin, never()).createOrModifyTopics(any());
    }

    @Test
    void rejectsOutOfRangePartitions() {
        when(streamRepository.existsById("orders")).thenReturn(false);

        assertThatThrownBy(() -> service.create("orders", null, 0, null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> service.create("orders", null, 65, null)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void getThrowsWhenMissing() {
        when(streamRepository.findById("ghost")).thenReturn(Optional.empty());

        assertThatThrownBy(() -> service.get("ghost"))
                .isInstanceOf(NotFoundException.class)
                .hasMessageContaining("ghost");
    }
}
