package com.jiajun.eventstream.service;

import java.time.Instant;

/** Returned to the caller once Kafka has acknowledged the write. */
public record PublishReceipt(String eventId, String stream, String topic, int partition, long offset, Instant acceptedAt) {
}
