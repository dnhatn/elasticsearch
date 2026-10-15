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
 * Counts what a search moved: the bytes it decoded, the bytes something looked at, the time decoding took, and the
 * documents it covered and read. Each count belongs to the place that did the work, so dividing it by the time that
 * place reports gives a rate.
 *
 * <p>The counts are tiers of the same work rather than parts of a total, so they are not added together. Bytes taken
 * from storage are counted by whoever took them, and the volume that flowed between two places is counted by
 * whoever moved it.
 *
 * <p>A caller reports all of what it moved or none of it: half a count reads as fact and is wrong.
 *
 * <p>Counting is left on. The adders take a document or a block at a time rather than a value, and several leaves of
 * one search may report at once, which is what the adders are for.
 */
public final class ScanCounters {

    /** Counts nothing, for work with nowhere to report. */
    public static final ScanCounters NONE = new ScanCounters();

    /**
     * The counters work on this thread reports to. Whoever runs the work installs them, so a column reader, a query
     * over an index and an expression over a page all report without being passed anything.
     */
    private static final ThreadLocal<ScanCounters> REPORTING = ThreadLocal.withInitial(() -> NONE);

    /** The counters work on this thread reports to, which count nothing when none are installed. */
    public static ScanCounters reporting() {
        return REPORTING.get();
    }

    /**
     * Installs these counters for this thread until the returned handle is closed, which puts back whichever were
     * installed before.
     */
    public Releasable reportHere() {
        final ScanCounters reportedBefore = REPORTING.get();
        REPORTING.set(this);
        return () -> REPORTING.set(reportedBefore);
    }

    private final LongAdder decoded = new LongAdder();
    private final LongAdder processed = new LongAdder();
    private final LongAdder decodeNanos = new LongAdder();
    private final LongAdder documentsScanned = new LongAdder();
    private final LongAdder documentsProcessed = new LongAdder();

    /** Adds bytes a read turned into, which is the volume handed on. */
    public void decoded(long bytes) {
        decoded.add(bytes);
    }

    /** Adds bytes something looked at, of those it was handed. */
    public void processed(long bytes) {
        processed.add(bytes);
    }

    public long decoded() {
        return decoded.sum();
    }

    public long processed() {
        return processed.sum();
    }

    /** Adds the time a decode took, which the bytes it produced are divided by. */
    public void decodeNanos(long nanos) {
        decodeNanos.add(nanos);
    }

    public long decodeNanos() {
        return decodeNanos.sum();
    }

    /** Adds documents a search was asked to cover, reported for a range rather than for each document. */
    public void documentsScanned(long documents) {
        documentsScanned.add(documents);
    }

    public long documentsScanned() {
        return documentsScanned.sum();
    }

    /** Adds documents something read to decide or to produce, of those it was asked to cover. */
    public void documentsProcessed(long documents) {
        documentsProcessed.add(documents);
    }

    public long documentsProcessed() {
        return documentsProcessed.sum();
    }

    /** The counts so far, as a report that can be carried. */
    public ScanCounts snapshot() {
        return new ScanCounts(0, 0, decoded(), processed(), decodeNanos(), documentsScanned(), documentsProcessed());
    }

    @Override
    public String toString() {
        return "ScanCounters" + snapshot();
    }
}
