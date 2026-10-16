package com.jiajun.eventstream.graphql.dto;

import com.jiajun.eventstream.domain.EventRecord;
import org.springframework.data.domain.Page;

import java.util.List;

public record EventPage(List<EventRecord> content, int page, int size, long totalElements, int totalPages, boolean hasNext) {

    public static EventPage from(Page<EventRecord> page) {
        return new EventPage(page.getContent(), page.getNumber(), page.getSize(),
                page.getTotalElements(), page.getTotalPages(), page.hasNext());
    }
}
