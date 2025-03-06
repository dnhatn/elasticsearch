/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute;

import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public final class ExecutionTime {
    public static final Logger LOG = LogManager.getLogger(ExecutionTime.class);
    public static final ExecutionTime INSTANCE = new ExecutionTime();
    private final Map<String, List<Long>> events = ConcurrentCollections.newConcurrentMap();
    private long startTime = System.nanoTime();
    public final AtomicInteger shards = new AtomicInteger();
    public String query = "";

    public void startQuery() {
        shards.set(0);
        events.clear();
        startTime = System.nanoTime();
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public void trackExecutionTime(String task, long timeInNanos) {
        events.computeIfAbsent(task, k -> Collections.synchronizedList(new ArrayList<>())).add(timeInNanos);
    }

    public void logExecutionTime() {
        LOG.info("--> query [{}] took {} shards {} ", query, TimeValue.timeValueNanos(System.nanoTime() - startTime), shards.get());
        for (Map.Entry<String, List<Long>> e : events.entrySet()) {
            List<Long> values = e.getValue();
            long total = values.stream().mapToLong(Long::longValue).sum();
            LOG.info("--> {} executed {} times took {} ", e.getKey(), values.size(), TimeValue.timeValueNanos(total));
        }
        for (Map.Entry<String, List<Long>> e : events.entrySet()) {
            List<Long> values = e.getValue();
            for (Long v : values) {
                LOG.info("--> {} took [{}] ", e.getKey(), TimeValue.timeValueNanos(v));
            }
        }
    }
}
