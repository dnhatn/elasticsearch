/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.OrdinalBytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.util.Holder;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class TimeSeriesBlockHashTests extends BlockHashTestCase {

    List<Page> randomPages(long endTime) {
        int numPages = between(1, 100);
        List<Page> pages = new ArrayList<>(numPages);
        int globalTsid = -1;
        long timestamp = endTime;
        for (int p = 0; p < numPages; p++) {
            int numRows = between(1, 1000);
            if (randomBoolean()) {
                timestamp -= between(0, 100);
            }
            try (
                BytesRefVector.Builder dictBuilder = blockFactory.newBytesRefVectorBuilder(numRows);
                IntVector.Builder ordinalBuilder = blockFactory.newIntVectorBuilder(numRows);
                LongVector.Builder timestampsBuilder = blockFactory.newLongVectorBuilder(numRows)
            ) {
                int perPageOrd = -1;
                for (int i = 0; i < numRows; i++) {
                    boolean newGroup = globalTsid == -1 || randomInt(100) < 10;
                    if (newGroup) {
                        globalTsid++;
                        timestamp = endTime - between(0, 100);
                    }
                    if (perPageOrd == -1 || newGroup) {
                        perPageOrd++;
                        dictBuilder.appendBytesRef(new BytesRef(String.format(Locale.ROOT, "id-%06d", globalTsid)));
                    }
                    ordinalBuilder.appendInt(perPageOrd);
                    if (randomInt(100) < 20) {
                        timestamp -= between(1, 10);
                    }
                    timestampsBuilder.appendLong(timestamp);
                }

                var tsidBlock = new OrdinalBytesRefVector(ordinalBuilder.build(), dictBuilder.build()).asBlock();
                var timestampBlock = timestampsBuilder.build().asBlock();
                pages.add(new Page(tsidBlock, timestampBlock));
            }
        }
        return pages;
    }

    public void testRandomKeys() throws Exception {
        var hash1 = new TimeSeriesBlockHash(0, 1, blockFactory);
        var hash2 = BlockHash.build(
            List.of(new BlockHash.GroupSpec(0, ElementType.BYTES_REF), new BlockHash.GroupSpec(1, ElementType.LONG)),
            blockFactory,
            32 * 1024,
            randomBoolean()
        );
        try (hash1; hash2) {
            long endTime = randomLongBetween(1000_000, 2000_000);
            for (Page page : randomPages(endTime)) {
                try (IntVector ords1 = addPage(hash1, page); IntVector ords2 = addPage(hash2, page)) {
                    assertThat("input=" + page, ords1, equalTo(ords2));
                } finally {
                    page.releaseBlocks();
                }
            }
            Block[] keys1 = null;
            Block[] keys2 = null;
            try {
                keys1 = hash1.getKeys();
                keys2 = hash2.getKeys();
                assertThat(keys1, equalTo(keys2));
            } finally {
                Releasables.close(keys1);
                Releasables.close(keys2);
            }
        }
    }

    public void testRandomPartition() {
        var hash1 = new TimeSeriesBlockHash(0, 1, blockFactory);
        var hash2 = BlockHash.build(
            List.of(new BlockHash.GroupSpec(0, ElementType.BYTES_REF), new BlockHash.GroupSpec(1, ElementType.LONG)),
            blockFactory,
            32 * 1024,
            randomBoolean()
        );
        try (hash1; hash2) {
            long endTime = randomLongBetween(1000_000, 2_000_000);
            for (Page page : randomPages(endTime)) {
                try (IntVector ords1 = addPage(hash1, page); IntVector ords2 = addPage(hash2, page)) {
                    assertThat("input=" + page, ords1, equalTo(ords2));
                } finally {
                    page.releaseBlocks();
                }
            }
            long bucketEndTime = randomBoolean() ? randomLongBetween(1000_000, endTime) : randomLongBetween(2000_000, 3000_000);
            long bucketStartTime = randomBoolean() ? between(0, 1000_000) : randomLongBetween(1000_000, endTime);
            var partitions = hash1.partitionBuckets(bucketStartTime, bucketEndTime);
            Block[] allKeys = hash2.getKeys();
            try (var finalKeys = partitions.v1(); var partialKeys = partitions.v2()) {
                BytesRefBlock tsids = (BytesRefBlock) allKeys[0];
                LongBlock timestamps = (LongBlock) allKeys[1];
                BytesRef spare1 = new BytesRef();
                BytesRef spare2 = new BytesRef();
                assertTrue(finalKeys.tsids().areAllValuesNull());
                for (int p = 0; p < finalKeys.positionCount(); p++) {
                    int ord = finalKeys.selected().getInt(p);
                    long timestamp = finalKeys.timeBuckets().getLong(p);
                    assertThat(timestamp, both(greaterThan(bucketStartTime)).and(lessThan(bucketEndTime)));
                    assertThat(timestamp, equalTo(timestamps.getLong(ord)));
                }
                for (int p = 0; p < partialKeys.positionCount(); p++) {
                    int ord = partialKeys.selected().getInt(p);
                    long timestamp = partialKeys.timeBuckets().getLong(p);
                    assertThat(timestamp, either(lessThanOrEqualTo(bucketStartTime)).or(greaterThanOrEqualTo(bucketEndTime)));
                    assertThat(timestamp, equalTo(timestamps.getLong(ord)));
                    assertThat(partialKeys.tsids().getBytesRef(ord, spare1), equalTo(tsids.getBytesRef(ord, spare2)));
                }
            } finally {
                Releasables.closeExpectNoException(allKeys);
            }
        }
    }

    private IntVector addPage(BlockHash hash, Page page) {
        Holder<IntVector> holder = new Holder<>();
        hash.add(page, new GroupingAggregatorFunction.AddInput() {
            @Override
            public void add(int positionOffset, IntBlock groupIds) {
                IntVector vector = groupIds.asVector();
                assertNotNull("should emit a vector", vector);
                add(positionOffset, vector);
            }

            @Override
            public void add(int positionOffset, IntVector groupIds) {
                groupIds.incRef();
                holder.set(groupIds);
            }

            @Override
            public void close() {

            }
        });
        assertNotNull(holder.get());
        return holder.get();
    }
}
