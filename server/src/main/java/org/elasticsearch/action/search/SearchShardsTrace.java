/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Temporary instrumentation: collects nanoTime marks for one search_shards round trip, keyed by the root task id,
 * and prints a single line of deltas once the trip is complete so the prints themselves stay out of the measured windows.
 */
public final class SearchShardsTrace {
    public static final int DETECT_START = 0;
    public static final int DETECT_END = 1;
    public static final int SEND = 2;
    public static final int EXECUTE = 3;
    public static final int REWRITE_START = 4;
    public static final int REWRITE_END = 5;
    public static final int READY = 6;
    public static final int RECEIVED = 7;
    public static final int MAPPED = 8;

    private static final String[] NAMES = { "detect", "ctor", "dispatch", "resolve", "rewrite", "shards", "response", "map" };

    private static final ConcurrentHashMap<Long, long[]> TRACES = new ConcurrentHashMap<>();

    private SearchShardsTrace() {}

    public static void start(long rootTaskId) {
        long[] marks = new long[MAPPED + 1];
        marks[DETECT_START] = System.nanoTime();
        TRACES.put(rootTaskId, marks);
    }

    public static void mark(long rootTaskId, int slot) {
        long[] marks = TRACES.get(rootTaskId);
        if (marks != null) {
            marks[slot] = System.nanoTime();
        }
    }

    public static void finish(long rootTaskId) {
        long[] marks = TRACES.remove(rootTaskId);
        if (marks == null) {
            return;
        }
        marks[MAPPED] = System.nanoTime();
        StringBuilder sb = new StringBuilder("--> search_shards trace [us]:");
        for (int i = 0; i < NAMES.length; i++) {
            sb.append(' ').append(NAMES[i]).append('=').append((marks[i + 1] - marks[i]) / 1000);
        }
        sb.append(" took=").append((marks[MAPPED] - marks[SEND]) / 1000);
        sb.append(" total=").append((marks[MAPPED] - marks[DETECT_START]) / 1000);
        System.err.println(sb);
    }
}
