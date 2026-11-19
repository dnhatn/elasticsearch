/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.elasticsearch.action.fieldcaps.TimeSeriesStats;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.compute.lucene.PartialLeafReaderContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;

import java.io.IOException;
import java.util.IdentityHashMap;
import java.util.Map;

/**
 * Estimates the {@code (_tsid, time bucket)} groups of a time-series aggregation before it reads any doc, to bound its memory.
 */
public final class TimeSeriesGroups {
    /**
     * Coordinator bytes per group of the {@code rate} final state, the largest of the time-series aggregations; about 75 measured.
     */
    public static final long ESTIMATED_BYTES_PER_GROUP = 135;
    /**
     * Coordinator bytes per group when every aggregation keeps a few numeric values per group, as sum, count, min and max do;
     * about 35 measured for {@code avg_over_time}.
     */
    public static final long ESTIMATED_BYTES_PER_FIXED_WIDTH_GROUP = 55;
    /**
     * Bytes every group adds for each dimension it is grouped by, since the dimensions are packed with each group; about 25 measured
     * for {@code host.name}.
     */
    public static final long ESTIMATED_BYTES_PER_PACKED_DIMENSION = 45;
    private static final long NO_BUCKET = 0;

    private TimeSeriesGroups() {}

    public static long bytes(long groups, long bytesPerGroup) {
        return groups > Long.MAX_VALUE / bytesPerGroup ? Long.MAX_VALUE : groups * bytesPerGroup;
    }

    /**
     * {@link TimeSeriesStats#timestampRange} in the resolution of the {@code @timestamp} field type, milliseconds unless it is
     * a date field.
     */
    @Nullable
    public static long[] timestampRange(LeafReader reader, @Nullable MappedFieldType timestamp) throws IOException {
        DateFieldMapper.Resolution resolution = timestamp instanceof DateFieldMapper.DateFieldType date
            ? date.resolution()
            : DateFieldMapper.Resolution.MILLISECONDS;
        return TimeSeriesStats.timestampRange(reader, resolution);
    }

    /**
     * The width of the bucket holding {@code timestamp}, which must be in the range the rounding was prepared for.
     */
    public static long bucketMillis(@Nullable Rounding.Prepared rounding, long timestamp) {
        if (rounding == null) {
            return NO_BUCKET;
        }
        long first = rounding.round(timestamp);
        return rounding.nextRoundingValue(first) - first;
    }

    /**
     * The groups of the series whose series-milliseconds spread over {@code spanMillis}: each series counts the buckets its
     * time span covers, plus one for the bucket it starts in.
     */
    public static long groups(long seriesMillis, long spanMillis, long bucketMillis) {
        long series = Math.ceilDiv(seriesMillis, spanMillis);
        return bucketMillis == NO_BUCKET ? series : Math.ceilDiv(seriesMillis, bucketMillis) + series;
    }

    /**
     * Whether the source reads one {@code _tsid} range of a split query.
     */
    public static boolean readsTsidRange(LuceneOperator.Factory source) {
        final LuceneSliceQueue queue = source.sliceQueue;
        for (int i = 0; i < queue.totalSlices(); i++) {
            if (TsidPrefixRangeQuery.find(queue.slice(i).weight().getQuery()) != null) {
                return true;
            }
        }
        return false;
    }

    /**
     * The most groups one slice of the source covers, before any slice is taken. A slice's series-milliseconds sum, over its
     * pieces, the series between the piece's first and last doc times the {@code @timestamp} span of the piece's segment.
     */
    public static long maxGroupsPerSlice(LuceneOperator.Factory source, @Nullable Rounding.Prepared timeBucket) throws IOException {
        final LuceneSliceQueue queue = source.sliceQueue;
        final Map<LeafReaderContext, Leaf> leaves = new IdentityHashMap<>();
        long maxGroups = 0;
        for (int i = 0; i < queue.totalSlices(); i++) {
            LuceneSlice slice = queue.slice(i);
            long seriesMillis = 0;
            long minTimestamp = Long.MAX_VALUE;
            long maxTimestamp = Long.MIN_VALUE;
            for (PartialLeafReaderContext piece : slice.leaves()) {
                Leaf leaf = leaves.get(piece.leafReaderContext());
                if (leaf == null) {
                    MappedFieldType timestamp = slice.shardContext().fieldType(DataStream.TIMESTAMP_FIELD_NAME);
                    leaf = new Leaf(piece.leafReaderContext().reader(), timestamp);
                    leaves.put(piece.leafReaderContext(), leaf);
                }
                if (leaf.tsid == null || leaf.timestamps == null) {
                    continue;
                }
                int firstOrd = leaf.ord(piece.minDoc());
                int endOrd = leaf.ord(piece.maxDoc());
                seriesMillis += (long) (endOrd - firstOrd) * (leaf.timestamps[1] - leaf.timestamps[0] + 1);
                minTimestamp = Math.min(minTimestamp, leaf.timestamps[0]);
                maxTimestamp = Math.max(maxTimestamp, leaf.timestamps[1]);
            }
            if (minTimestamp <= maxTimestamp) {
                long bucketMillis = bucketMillis(timeBucket, minTimestamp);
                maxGroups = Math.max(maxGroups, groups(seriesMillis, maxTimestamp - minTimestamp + 1, bucketMillis));
            }
        }
        return maxGroups;
    }

    private static final class Leaf {
        private final LeafReader reader;
        @Nullable
        private final long[] timestamps;
        @Nullable
        private SortedDocValues tsid;
        private int lastDoc = -1;
        private int lastOrd;

        Leaf(LeafReader reader, @Nullable MappedFieldType timestamp) throws IOException {
            this.reader = reader;
            this.timestamps = timestampRange(reader, timestamp);
            this.tsid = reader.getSortedDocValues(TimeSeriesIdFieldMapper.NAME);
        }

        int ord(int doc) throws IOException {
            if (doc != lastDoc) {
                if (doc < tsid.docID()) {
                    tsid = reader.getSortedDocValues(TimeSeriesIdFieldMapper.NAME);
                }
                lastOrd = doc >= reader.maxDoc() ? tsid.getValueCount() : ordAt(doc);
                lastDoc = doc;
            }
            return lastOrd;
        }

        private int ordAt(int doc) throws IOException {
            final boolean found = tsid.advanceExact(doc);
            assert found : "every time-series doc has a _tsid";
            return tsid.ordValue();
        }
    }
}
