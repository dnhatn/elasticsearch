/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Limiter;
import org.elasticsearch.core.RefCounted;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Extension of {@link LuceneSourceOperator} that appends the potential max timestamp can be appeared in the next pages.
 */
public class TimeSeriesSourceOperator extends LuceneSourceOperator {

    public TimeSeriesSourceOperator(
        List<? extends RefCounted> shardContextCounters,
        BlockFactory blockFactory,
        int maxPageSize,
        LuceneSliceQueue sliceQueue,
        int limit,
        Limiter limiter,
        boolean needsScore
    ) {
        super(shardContextCounters, blockFactory, maxPageSize, sliceQueue, limit, limiter, needsScore);
    }

    private final Map<Integer, Long> cachedTimestamps = new HashMap<>();

    @Override
    public Page getCheckedOutput() throws IOException {
        Page page = super.getCheckedOutput();
        if (page == null) {
            return null;
        }
        boolean success = false;
        try {
            LuceneSlice slice = currentSlice();
            long maxTimestamp = Long.MIN_VALUE;
            for (int i = indexOfCurrentSlice() + 1; i < slice.leaves().size(); i++) {
                LeafReaderContext leafContext = slice.getLeaf(i).leafReaderContext();
                Long timestamp = cachedTimestamps.get(leafContext.ord);
                if (timestamp == null) {
                    timestamp = readMaxTimestamp(leafContext.reader());
                    cachedTimestamps.put(leafContext.ord, timestamp);
                }
                maxTimestamp = Math.max(maxTimestamp, timestamp);
            }
            Block[] blocks = new Block[page.getBlockCount() + 1];
            blocks[0] = page.getBlock(0);
            blocks[1] = blockFactory.newConstantLongVector(maxTimestamp, page.getPositionCount()).asBlock();
            for (int i = 1; i < page.getBlockCount(); i++) {
                blocks[i + 1] = page.getBlock(i);
            }
            success = true;
            return new Page(blocks);
        } finally {
            if (success == false) {
                page.releaseBlocks();
            }
        }
    }

    static long readMaxTimestamp(LeafReader reader) throws IOException {
        FieldInfo info = reader.getFieldInfos().fieldInfo(DataStream.TIMESTAMP_FIELD_NAME);
        if (info != null) {
            if (info.docValuesSkipIndexType() == DocValuesSkipIndexType.RANGE) {
                DocValuesSkipper skipper = reader.getDocValuesSkipper(DataStream.TIMESTAMP_FIELD_NAME);
                assert skipper != null;
                return skipper.maxValue();
            } else {
                PointValues pointValues = reader.getPointValues(DataStream.TIMESTAMP_FIELD_NAME);
                assert pointValues != null;
                return LongPoint.decodeDimension(pointValues.getMaxPackedValue(), 0);
            }
        } else {
            return Long.MAX_VALUE;
        }
    }
}
