/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;

import java.util.ArrayList;
import java.util.List;

public class LongBytesRefPartitionedBlockHashTests extends PartitionedBlockHashTestCase {
    @Override
    protected List<ElementType> keyTypes() {
        return randomBoolean() ? List.of(ElementType.LONG, ElementType.BYTES_REF) : List.of(ElementType.BYTES_REF, ElementType.LONG);
    }

    @Override
    protected PartitionedBlockHash newBlockHash(List<BlockHash.GroupSpec> groups, BlockFactory blockFactory, int emitBatchSize) {
        boolean reverseOutput = groups.get(0).elementType() == ElementType.BYTES_REF;
        return new LongBytesRefBlockHash(groups, blockFactory, emitBatchSize, reverseOutput);
    }

    /**
     * Half the time, skews the bytes key the way real keyword columns do: most rows share one value (an empty string, a default)
     * drawn from a tiny dictionary, with occasional nulls. Every pair partition then references the same few dictionary entries, which
     * exercises the shared dictionary slices, their reference counts and the per-partition bitsets.
     */
    @Override
    protected List<Page> randomPages(BlockFactory blockFactory, List<BlockHash.GroupSpec> groups) {
        List<Page> pages = super.randomPages(blockFactory, groups);
        if (randomBoolean()) {
            return pages;
        }
        int bytesChannel = groups.get(0).elementType() == ElementType.BYTES_REF ? 0 : 1;
        BytesRef dominant = new BytesRef(randomBoolean() ? "" : randomAlphaOfLength(5));
        BytesRef[] rare = new BytesRef[between(1, 8)];
        for (int i = 0; i < rare.length; i++) {
            rare[i] = new BytesRef(randomAlphaOfLengthBetween(1, 20));
        }
        List<Page> skewed = new ArrayList<>(pages.size());
        for (Page page : pages) {
            int positionCount = page.getPositionCount();
            Block[] blocks = new Block[page.getBlockCount()];
            for (int b = 0; b < blocks.length; b++) {
                blocks[b] = page.getBlock(b);
            }
            try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(positionCount)) {
                for (int p = 0; p < positionCount; p++) {
                    int roll = between(0, 99);
                    if (roll < 87) {
                        builder.appendBytesRef(dominant);
                    } else if (roll < 97) {
                        builder.appendBytesRef(randomFrom(rare));
                    } else {
                        builder.appendNull();
                    }
                }
                blocks[bytesChannel].close();
                blocks[bytesChannel] = builder.build();
            }
            skewed.add(new Page(blocks));
        }
        return skewed;
    }
}
