package com.jiajun.eventstream.graphql.dto;

public record CreateStreamInput(String name, String description, Integer partitions, Integer retentionHours) {
}
