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
    private static final org.apache.logging.log4j.Logger log = org.apache.logging.log4j.LogManager.getLogger(ExecutionTime.class);
    private final Map<String, List<Long>> events = ConcurrentCollections.newConcurrentMap();
    private final Map<String, List<Long>> rules = ConcurrentCollections.newConcurrentMap();

    private long startTime = System.nanoTime();
    public final AtomicInteger shards = new AtomicInteger();
    public String query = "";

    public void startQuery() {
        clear();
        startTime = System.nanoTime();
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public void trackExecutionTime(String task, long timeInNanos) {
        events.computeIfAbsent(task, k -> Collections.synchronizedList(new ArrayList<>())).add(timeInNanos);
    }

    public void trackRule(String rule, long timeInNanos) {
        rules.computeIfAbsent(rule, k -> Collections.synchronizedList(new ArrayList<>())).add(timeInNanos);
    }

    public void logExecutionTime() {
        LOG.info("--> query [{}] took {} shards {} ", query, TimeValue.timeValueNanos(System.nanoTime() - startTime), shards.get());
        for (Map.Entry<String, List<Long>> e : events.entrySet()) {
            List<Long> values = e.getValue();
            long total = values.stream().mapToLong(Long::longValue).sum();
            LOG.info("--> {} executed {} times took {} ", e.getKey(), values.size(), TimeValue.timeValueNanos(total));
        }
        LOG.info("--> rules ");
        rules.entrySet()
            .stream()
            .map(e -> Map.entry(e.getKey(), e.getValue().stream().mapToLong(Long::longValue).sum()))
            .sorted((a, b) -> Long.compare(b.getValue(), a.getValue()))
            .forEach(e -> {
                LOG.info("--> rule {} took [{}] ", e.getKey(), TimeValue.timeValueNanos(e.getValue()));
            });
        for (Map.Entry<String, List<Long>> e : events.entrySet()) {
            List<Long> values = e.getValue();
            for (Long v : values) {
                LOG.info("--> {} took [{}] ", e.getKey(), TimeValue.timeValueNanos(v));
            }
        }
        clear();
    }

    public void startEven(String label) {
        long elapsed = System.nanoTime() - startTime;
        log.info("[{}] after [{}]", label, TimeValue.timeValueNanos(elapsed));
    }

    private void clear() {
        shards.set(0);
        events.clear();
        rules.clear();
    }
}
