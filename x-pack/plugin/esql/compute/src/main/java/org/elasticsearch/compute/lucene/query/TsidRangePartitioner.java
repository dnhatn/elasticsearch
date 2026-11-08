/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;

import java.io.IOException;

/**
 * Cuts a time-series shard into {@code _tsid} ranges, so that a time-series aggregation can run one range at a time: a
 * series is whole inside one range, so every function stays exact.
 * <p>
 *     The cuts come from the shard's reference segment, the leaf with the most series. With {@code count} series there and
 *     {@code seriesPerRange} series per range, there are {@code R = ceil(count / seriesPerRange)} ranges, cut at every
 *     {@code seriesPerRange}-th series of that segment; the first and last range are open. A range then holds at most
 *     {@code seriesPerRange} of the reference segment's series, plus the series of the other segments that fall in the same
 *     range but are not in the reference segment, such as short-lived ones. A range index at or past {@code R} is an empty
 *     range, which has the setup cost of a range and matches nothing.
 * </p>
 * <p>
 *     The planner decides how many ranges a node's shards are cut into from the shard with the most series in one segment,
 *     see {@link #numRanges}, and the ranges are resolved per shard, where the segments are, with {@link #range}.
 * </p>
 */
public final class TsidRangePartitioner {
    private TsidRangePartitioner() {}

    /**
     * The number of series in the segment of {@code reader} that has the most, or {@code 0} if no segment has a {@code _tsid}.
     */
    public static int seriesInLargestSegment(IndexReader reader) throws IOException {
        SortedDocValues reference = referenceSegment(reader);
        return reference == null ? 0 : reference.getValueCount();
    }

    /**
     * The number of ranges a shard whose largest segment has {@code seriesInLargestSegment} series is cut into, at least one.
     */
    public static int numRanges(int seriesInLargestSegment, int seriesPerRange) {
        if (seriesPerRange <= 0) {
            throw new IllegalArgumentException("series per range must be positive, got [" + seriesPerRange + "]");
        }
        return Math.max(1, Math.ceilDiv(seriesInLargestSegment, seriesPerRange));
    }

    /**
     * The query for range {@code rangeIndex} of the shard behind {@code reader}. The range is open below for the first range
     * and open above for the last; it is empty when {@code rangeIndex} is at or past the shard's number of ranges. A shard
     * without a {@code _tsid} has one open range, which matches nothing there since no doc has a {@code _tsid}.
     */
    public static TsidRangeQuery range(IndexReader reader, int seriesPerRange, int rangeIndex, TsidRangeQuery.Steps steps)
        throws IOException {
        if (rangeIndex < 0) {
            throw new IllegalArgumentException("range index must not be negative, got [" + rangeIndex + "]");
        }
        SortedDocValues reference = referenceSegment(reader);
        if (reference == null) {
            return new TsidRangeQuery(null, null, steps);
        }
        int ranges = numRanges(reference.getValueCount(), seriesPerRange);
        if (rangeIndex >= ranges) {
            BytesRef empty = BytesRef.deepCopyOf(reference.lookupOrd(0));
            return new TsidRangeQuery(empty, empty, steps);
        }
        BytesRef lower = rangeIndex == 0 ? null : cut(reference, seriesPerRange, rangeIndex);
        BytesRef upper = rangeIndex == ranges - 1 ? null : cut(reference, seriesPerRange, rangeIndex + 1);
        return new TsidRangeQuery(lower, upper, steps);
    }

    /**
     * The first {@code _tsid} of range {@code rangeIndex}, which is the {@code rangeIndex * seriesPerRange}-th series of the
     * reference segment.
     */
    private static BytesRef cut(SortedDocValues reference, int seriesPerRange, int rangeIndex) throws IOException {
        return BytesRef.deepCopyOf(reference.lookupOrd(Math.toIntExact((long) rangeIndex * seriesPerRange)));
    }

    /**
     * The {@code _tsid} doc values of the leaf with the most series, or {@code null} if no leaf has any.
     */
    @Nullable
    private static SortedDocValues referenceSegment(IndexReader reader) throws IOException {
        SortedDocValues reference = null;
        for (LeafReaderContext leaf : reader.leaves()) {
            SortedDocValues tsid = leaf.reader().getSortedDocValues(TimeSeriesIdFieldMapper.NAME);
            if (tsid != null && tsid.getValueCount() > 0 && (reference == null || tsid.getValueCount() > reference.getValueCount())) {
                reference = tsid;
            }
        }
        return reference;
    }
}
