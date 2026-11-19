/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.fieldcaps;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SortedDocValues;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;

import java.io.IOException;

/**
 * The size of a time-series index from segment metadata alone, so a query can estimate how many {@code (_tsid, time bucket)}
 * groups it builds before reading any document. {@code seriesMillis} sums, over the segments, the distinct series in the
 * segment times the segment's time span; dividing it by a bucket width estimates the buckets the series cover.
 */
public record TimeSeriesStats(long seriesMillis, long minTimestamp, long maxTimestamp) implements Writeable {

    public TimeSeriesStats(StreamInput in) throws IOException {
        this(in.readVLong(), in.readLong(), in.readLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(seriesMillis);
        out.writeLong(minTimestamp);
        out.writeLong(maxTimestamp);
    }

    /**
     * The stats of an index estimated from one of its {@code numberOfShards} shards, or {@code null} when a segment lacks the
     * {@code @timestamp} range.
     */
    @Nullable
    public static TimeSeriesStats ofShard(IndexReader reader, DateFieldMapper.Resolution resolution, int numberOfShards)
        throws IOException {
        long seriesMillis = 0;
        long minTimestamp = Long.MAX_VALUE;
        long maxTimestamp = Long.MIN_VALUE;
        for (LeafReaderContext leaf : reader.leaves()) {
            SortedDocValues tsid = leaf.reader().getSortedDocValues(TimeSeriesIdFieldMapper.NAME);
            if (tsid == null) {
                continue;
            }
            long[] range = timestampRange(leaf.reader(), resolution);
            if (range == null) {
                return null;
            }
            seriesMillis += tsid.getValueCount() * (range[1] - range[0] + 1);
            minTimestamp = Math.min(minTimestamp, range[0]);
            maxTimestamp = Math.max(maxTimestamp, range[1]);
        }
        return new TimeSeriesStats(seriesMillis * numberOfShards, minTimestamp, maxTimestamp);
    }

    /**
     * The {@code @timestamp} range of a segment in milliseconds, read from its doc values skipper or points, or {@code null}
     * if it has neither.
     */
    @Nullable
    public static long[] timestampRange(LeafReader reader, DateFieldMapper.Resolution resolution) throws IOException {
        long min;
        long max;
        DocValuesSkipper skipper = reader.getDocValuesSkipper(DataStream.TIMESTAMP_FIELD_NAME);
        if (skipper != null) {
            min = skipper.minValue();
            max = skipper.maxValue();
        } else {
            PointValues points = reader.getPointValues(DataStream.TIMESTAMP_FIELD_NAME);
            if (points == null) {
                return null;
            }
            min = LongPoint.decodeDimension(points.getMinPackedValue(), 0);
            max = LongPoint.decodeDimension(points.getMaxPackedValue(), 0);
        }
        return new long[] { resolution.roundDownToMillis(min), resolution.roundUpToMillis(max) };
    }
}
