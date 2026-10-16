package com.jiajun.eventstream.graphql.dto;

import java.time.Instant;
import java.util.Map;

public record PublishEventInput(String stream, String type, String source, String key,
                                Map<String, Object> payload, Instant occurredAt) {
}
