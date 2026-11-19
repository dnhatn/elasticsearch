/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.PartitionedHashTable;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.RandomBlock;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class TimeSeriesPartitionedBlockHashTests extends PartitionedBlockHashTestCase {
    private final boolean routeByHash = randomBoolean();

    @Override
    protected List<ElementType> keyTypes() {
        return randomBoolean() ? List.of(ElementType.BYTES_REF, ElementType.LONG) : List.of(ElementType.LONG, ElementType.BYTES_REF);
    }

    @Override
    protected PartitionedBlockHash newBlockHash(List<BlockHash.GroupSpec> groups, BlockFactory blockFactory, int emitBatchSize) {
        if (groups.get(0).elementType() == ElementType.BYTES_REF) {
            return new TimeSeriesBlockHash(0, 1, false, randomBoolean(), routeByHash, blockFactory);
        }
        return new TimeSeriesBlockHash(1, 0, true, randomBoolean(), routeByHash, blockFactory);
    }

    /**
     * Packed dimensions end with the last dimension's value, so keys like {@code host-0} to {@code host-9999} share ten last bytes.
     */
    public void testRouteByHashSpreadsKeysSharingLastBytes() {
        assertThat(nonEmptyPartitions(true), greaterThan(PartitionedHashTable.NUM_PARTITIONS / 2));
        assertThat(nonEmptyPartitions(false), lessThanOrEqualTo(10));
    }

    private int nonEmptyPartitions(boolean routeByHash) {
        BlockFactory blockFactory = blockFactory();
        int numKeys = 2_000;
        try (
            TimeSeriesBlockHash hash = new TimeSeriesBlockHash(0, 1, false, false, routeByHash, blockFactory);
            BytesRefVector.Builder keys = blockFactory.newBytesRefVectorBuilder(numKeys);
            LongVector.Builder buckets = blockFactory.newLongVectorBuilder(numKeys)
        ) {
            for (int i = 0; i < numKeys; i++) {
                keys.appendBytesRef(new BytesRef("host-" + i));
                buckets.appendLong(i % 3);
            }
            Page page = new Page(keys.build().asBlock(), buckets.build().asBlock());
            try {
                hash.add(page, new GroupingAggregatorFunction.AddInput() {
                    @Override
                    public void add(int positionOffset, IntArrayBlock groupIds) {}

                    @Override
                    public void add(int positionOffset, IntBigArrayBlock groupIds) {}

                    @Override
                    public void add(int positionOffset, IntVector groupIds) {}

                    @Override
                    public void close() {}
                });
            } finally {
                page.releaseBlocks();
            }
            CircuitBreaker breaker = blockFactory.breaker();
            PartitionedHashTable.PartitionedHashKeys partitioned = hash.splitPartition(
                breaker,
                new PartitionedHashTable.PartitionSplitter() {
                    @Override
                    public void split(int firstId, short[] shiftedIds, int batchSize, int[] batchPartitionCounts, int[] partitionOffsets) {}

                    @Override
                    public void release(CircuitBreaker b) {}
                }
            );
            try {
                int nonEmpty = 0;
                for (int p = 0; p < PartitionedHashTable.NUM_PARTITIONS; p++) {
                    if (partitioned.keysInPartition(p) > 0) {
                        nonEmpty++;
                    }
                }
                return nonEmpty;
            } finally {
                partitioned.releaseAll(breaker);
            }
        }
    }

    @Override
    protected List<Page> randomPages(BlockFactory blockFactory, List<BlockHash.GroupSpec> groups) {
        final int tsidChannel = groups.get(0).elementType() == ElementType.BYTES_REF ? 0 : 1;
        final boolean fixedLength = randomBoolean();
        BytesRef[] tsids = new BytesRef[between(1, 128)];
        for (int i = 0; i < tsids.length; i++) {
            tsids[i] = new BytesRef(randomByteArrayOfLength(fixedLength ? 16 : between(1, 24)));
        }
        long[] timestamps = new long[between(1, 32)];
        for (int i = 0; i < timestamps.length; i++) {
            timestamps[i] = randomNonNegativeLong();
        }
        int numPages = between(1, 20);
        List<Page> pages = new ArrayList<>(numPages);
        for (int i = 0; i < numPages; i++) {
            int positionCount = between(1, 1_000);
            Block[] blocks = new Block[3];
            try (
                var tsidBuilder = blockFactory.newBytesRefBlockBuilder(positionCount);
                var timestampBuilder = blockFactory.newLongBlockBuilder(positionCount)
            ) {
                for (int p = 0; p < positionCount; p++) {
                    tsidBuilder.appendBytesRef(randomFrom(tsids));
                    timestampBuilder.appendLong(timestamps[randomIntBetween(0, timestamps.length - 1)]);
                }
                blocks[tsidChannel] = tsidBuilder.build();
                blocks[1 - tsidChannel] = timestampBuilder.build();
            }
            blocks[2] = RandomBlock.randomBlock(blockFactory, ElementType.INT, positionCount, false, 0, 2, 0, 0).block();
            pages.add(new Page(blocks));
        }
        return pages;
    }
}
