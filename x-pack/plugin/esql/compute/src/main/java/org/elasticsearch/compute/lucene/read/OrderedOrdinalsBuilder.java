/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.read;

import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.mapper.BlockLoader;

import java.io.IOException;
import java.io.UncheckedIOException;

public class OrderedOrdinalsBuilder implements BlockLoader.SingletonOrdinalsBuilder, Releasable, Block.Builder {
    private final BlockFactory blockFactory;
    private final SortedDocValues docValues;
    private final IntBlock.Builder ordsBuilder;
    private final BytesRefVector.Builder dictBuilder;
    private int lastOrd = -1;
    private int mappedOrd = -1;

    public OrderedOrdinalsBuilder(BlockFactory blockFactory, SortedDocValues docValues, int count) {
        this.blockFactory = blockFactory;
        this.docValues = docValues;
        blockFactory.adjustBreaker(ordsSize(count));
        this.ordsBuilder = blockFactory.newIntBlockBuilder(count).mvOrdering(Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING);
        this.dictBuilder = blockFactory.newBytesRefVectorBuilder(count);
    }

    @Override
    public OrderedOrdinalsBuilder appendNull() {
        ordsBuilder.appendNull();
        return this;
    }

    @Override
    public OrderedOrdinalsBuilder appendOrd(int ord) {
        if (lastOrd != ord) {
            final BytesRef term;
            try {
                term = docValues.lookupOrd(ord);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            dictBuilder.appendBytesRef(term);
            mappedOrd++;
            lastOrd = ord;
        }
        ordsBuilder.appendInt(mappedOrd);
        return this;
    }

    @Override
    public OrderedOrdinalsBuilder beginPositionEntry() {
        throw new UnsupportedOperationException("should only have one value per doc");
    }

    @Override
    public OrderedOrdinalsBuilder endPositionEntry() {
        throw new UnsupportedOperationException("should only have one value per doc");
    }



    @Override
    public long estimatedBytes() {
        return ordsBuilder.estimatedBytes();
    }

    @Override
    public BytesRefBlock build() {
        return new OrdinalBytesRefBlock(ordsBuilder.build(), dictBuilder.build());
    }


    @Override
    public void close() {
        ordsBuilder.close();
    }

    @Override
    public Block.Builder copyFrom(Block block, int beginInclusive, int endExclusive) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Block.Builder mvOrdering(Block.MvOrdering mvOrdering) {
        throw new UnsupportedOperationException();
    }

    private static long ordsSize(int ordsCount) {
        return RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + ordsCount * Integer.BYTES;
    }
}
