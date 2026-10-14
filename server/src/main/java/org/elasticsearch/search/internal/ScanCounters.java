/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.internal;

import org.elasticsearch.core.Releasable;

import java.util.concurrent.atomic.LongAdder;

/**
 * The bytes a search moved, counted where they were moved so that a reader of them can be told apart from the one
 * that decoded them and the one that looked at them. Divided by the time the same place reports, each is a rate, and
 * a rate is what carries from one corpus to the next.
 *
 * <p>Two counts, which answer different questions and must not be added together: {@link #decoded} is what a read
 * turned into, the volume handed on, and {@link #processed} is the part of it a predicate walked. What was taken
 * from storage is counted already, by whoever took it.
 *
 * <p>A counter is accumulated by whoever moves the bytes, and the whole of a count is one place's business: a
 * caller reports all the bytes it moved or none of them, since a part of them is a rate that reads as fact and is
 * wrong.
 *
 * <p>Counting is cheap enough to leave on. The adders take the flush of a document or a block, not of a value, so
 * the hot loop stays a local sum; several of a search's leaves may run at once, which is what the adders are for.
 */
public final class ScanCounters {

    /** Counts nothing, for a caller with nowhere to report. */
    public static final ScanCounters NONE = new ScanCounters();

    /**
     * The counters work done on this thread is reported to. Whoever runs the work says where its counts go, so that
     * a reader of a column, a query over an index and an expression over a page all report the same way without
     * being handed anything: each asks for {@link #reporting} and adds to what it finds.
     */
    private static final ThreadLocal<ScanCounters> REPORTING = ThreadLocal.withInitial(() -> NONE);

    /** Where work on this thread reports what it moved, which counts nothing where nobody is listening. */
    public static ScanCounters reporting() {
        return REPORTING.get();
    }

    /**
     * Reports work on this thread to these counters until the returned handle is closed, which puts back whatever
     * was listening before. The caller that opens it is the one that knows which work it covers.
     */
    public Releasable reportHere() {
        final ScanCounters reportedBefore = REPORTING.get();
        REPORTING.set(this);
        return () -> REPORTING.set(reportedBefore);
    }

    private final LongAdder decoded = new LongAdder();
    private final LongAdder processed = new LongAdder();
    private final LongAdder decodeNanos = new LongAdder();

    /** The bytes storage gave up turned into, which is the volume handed on. */
    public void decoded(long bytes) {
        decoded.add(bytes);
    }

    /** The bytes a predicate walked, of those it was handed. */
    public void processed(long bytes) {
        processed.add(bytes);
    }

    public long decoded() {
        return decoded.sum();
    }

    public long processed() {
        return processed.sum();
    }

    /** Adds the time a decode took, which a rate over the bytes it left is taken against. */
    public void decodeNanos(long nanos) {
        decodeNanos.add(nanos);
    }

    public long decodeNanos() {
        return decodeNanos.sum();
    }

    /** What has been counted so far, as a report that can be carried. */
    public ScanCounts snapshot() {
        return new ScanCounts(decoded(), processed(), decodeNanos());
    }

    @Override
    public String toString() {
        return "ScanCounters" + snapshot();
    }
}
