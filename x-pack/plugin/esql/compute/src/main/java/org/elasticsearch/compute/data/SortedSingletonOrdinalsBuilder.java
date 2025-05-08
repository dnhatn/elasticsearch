/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.index.SortedDocValues;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.BlockLoader;

import java.io.IOException;
import java.io.UncheckedIOException;

public class SortedSingletonOrdinalsBuilder implements BlockLoader.SingletonOrdinalsBuilder, Releasable, Block.Builder {
    private final BlockFactory blockFactory;
    private final SortedDocValues docValues;
    private final IntBlock.Builder ordsBuilder;
    private int totalOrds;
    private int lastOrd;

    public SortedSingletonOrdinalsBuilder(BlockFactory blockFactory, SortedDocValues docValues, int count) {
        this.blockFactory = blockFactory;
        this.docValues = docValues;
        this.ordsBuilder = blockFactory.newIntBlockBuilder(count);
    }

    @Override
    public SortedSingletonOrdinalsBuilder appendNull() {
        ordsBuilder.appendNull();
        return this;
    }

    @Override
    public SortedSingletonOrdinalsBuilder appendOrd(int newOrd) {
        ordsBuilder.appendInt(newOrd);
        if (lastOrd != newOrd) {
            lastOrd = newOrd;
            totalOrds++;
        }
        return this;
    }

    @Override
    public SortedSingletonOrdinalsBuilder beginPositionEntry() {
        throw new UnsupportedOperationException("should only have one value per doc");
    }

    @Override
    public SortedSingletonOrdinalsBuilder endPositionEntry() {
        throw new UnsupportedOperationException("should only have one value per doc");
    }

    BytesRefBlock buildOrdinal(IntBlock ordinalsBlock) {
        BytesRefVector dictVector = null;
        boolean success = false;
        try (BytesRefVector.Builder bytesBuilder = blockFactory.newBytesRefVectorBuilder(totalOrds)) {
            int lastOrd = -1;
            for (int p = 0; p < ordinalsBlock.getPositionCount(); p++) {
                if (ordinalsBlock.isNull(p)) {
                    continue;
                }
                int ord = ordinalsBlock.getInt(ordinalsBlock.getFirstValueIndex(p));
                if (ord != lastOrd) {
                    lastOrd = ord;
                    bytesBuilder.appendBytesRef(docValues.lookupOrd(ord));
                }
            }
            dictVector = bytesBuilder.build();
            var result = new OrdinalBytesRefBlock(ordinalsBlock, dictVector);
            success = true;
            return result;
        } catch (IOException e) {
            throw new UncheckedIOException("error resolving ordinals", e);
        } finally {
            if (success == false) {
                Releasables.close(ordinalsBlock, dictVector);
            }
        }
    }

    @Override
    public long estimatedBytes() {
        /*
         * This is a *terrible* estimate because we have no idea how big the
         * values in the ordinals are.
         */
        return ordsBuilder.estimatedBytes() + (totalOrds * 50L);
    }

    @Override
    public BytesRefBlock build() {
        return buildOrdinal(ordsBuilder.build());
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
}
