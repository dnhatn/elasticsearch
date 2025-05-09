/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.BlockLoader;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;

public class SingletonOrdinalsBuilder implements BlockLoader.SingletonOrdinalsBuilder, Releasable, Block.Builder {
    private final BlockFactory blockFactory;
    private final SortedDocValues docValues;
    private final int[] ords;
    private int uniqueCount;
    private final int totalCount;
    private final int[] positions;
    private int count;

    public SingletonOrdinalsBuilder(BlockFactory blockFactory, SortedDocValues docValues, int totalCount, Buffers buffer) {
        this.blockFactory = blockFactory;
        this.docValues = docValues;
        this.totalCount = totalCount;
        this.positions = buffer.getPositions(totalCount);
        this.ords = buffer.getOrds(docValues.getValueCount());
        Arrays.fill(ords, -1);
    }

    @Override
    public SingletonOrdinalsBuilder appendNull() {
        positions[count++] = -1; // real ords can't be < 0, so we use -1 as null
        return this;
    }

    @Override
    public SingletonOrdinalsBuilder appendOrd(int ord) {
        positions[count++] = ord;
        if (ords[ord] == -1) {
            ords[ord] = 0;
            uniqueCount++;
        }
        return this;
    }

    @Override
    public SingletonOrdinalsBuilder beginPositionEntry() {
        throw new UnsupportedOperationException("should only have one value per doc");
    }

    @Override
    public SingletonOrdinalsBuilder endPositionEntry() {
        throw new UnsupportedOperationException("should only have one value per doc");
    }

    BytesRefBlock buildOrdinal() {
        BytesRefVector bytesVector = null;
        IntBlock ordinalBlock = null;
        int valueCount = docValues.getValueCount();
        try {
            // resolve the ordinals and remaps the ordinals
            int nextOrd = -1;
            try (BytesRefVector.Builder bytesBuilder = blockFactory.newBytesRefVectorBuilder(Math.min(valueCount, totalCount))) {
                for (int i = 0; i < valueCount; i++) {
                    if (ords[i] != -1) {
                        ords[i] = ++nextOrd;
                        bytesBuilder.appendBytesRef(docValues.lookupOrd(i));
                    }
                }
                bytesVector = bytesBuilder.build();
            } catch (IOException e) {
                throw new UncheckedIOException("error resolving ordinals", e);
            }
            try (IntBlock.Builder ordinalsBuilder = blockFactory.newIntBlockBuilder(totalCount)) {
                for (int i = 0; i < totalCount; i++) {
                    int ord = positions[i];
                    if (ord == -1) {
                        ordinalsBuilder.appendNull();
                    } else {
                        ordinalsBuilder.appendInt(ords[ord]);
                    }
                }
                ordinalBlock = ordinalsBuilder.build();
            }
            final OrdinalBytesRefBlock result = new OrdinalBytesRefBlock(ordinalBlock, bytesVector);
            bytesVector = null;
            ordinalBlock = null;
            return result;
        } finally {
            Releasables.close(ordinalBlock, bytesVector);
        }
    }

    BytesRefBlock buildRegularBlock() {
        try {
            long breakerSize = ordsSize(positions.length);
            // Increment breaker for sorted ords.
            blockFactory.adjustBreaker(breakerSize);
            try {
                try (BreakingBytesRefBuilder copies = new BreakingBytesRefBuilder(blockFactory.breaker(), "ords")) {
                    long offsetsAndLength = RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (uniqueCount + 1) * Integer.BYTES;
                    blockFactory.adjustBreaker(offsetsAndLength);
                    breakerSize += offsetsAndLength;
                    int nextOrd = -1;
                    int[] offsets = new int[uniqueCount + 1];
                    for (int o = 0; o < docValues.getValueCount(); o++) {
                        if (ords[o] != -1) {
                            int ord = ++nextOrd;
                            ords[o] = ord;
                            BytesRef v = docValues.lookupOrd(ord);
                            offsets[o] = copies.length();
                            copies.append(v);
                        }
                    }
                    offsets[uniqueCount] = copies.length();
                    BytesRef scratch = new BytesRef();
                    scratch.bytes = copies.bytes();
                    try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(totalCount)) {
                        for (int i = 0; i < totalCount; i++) {
                            int ord = positions[i];
                            if (ord == -1) {
                                builder.appendNull();
                                continue;
                            }
                            int pos = ords[ord];
                            scratch.offset = offsets[pos];
                            scratch.length = offsets[pos + 1] - scratch.offset;
                            builder.appendBytesRef(scratch);
                        }
                        return builder.build();
                    }
                }
            } finally {
                blockFactory.adjustBreaker(-breakerSize);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("error resolving ordinals", e);
        }
    }

    @Override
    public long estimatedBytes() {
        /*
         * This is a *terrible* estimate because we have no idea how big the
         * values in the ordinals are.
         */
        long overhead = shouldBuildOrdinalsBlock() ? 5 : 20;
        return positions.length * overhead;
    }

    @Override
    public BytesRefBlock build() {
        return shouldBuildOrdinalsBlock() ? buildOrdinal() : buildRegularBlock();
    }

    boolean shouldBuildOrdinalsBlock() {
        return OrdinalBytesRefBlock.isDense(totalCount, uniqueCount);
    }

    @Override
    public void close() {

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

    public static class Buffers {
        private int[] ords;
        private int[] positions;

        int[] getOrds(int numOrds) {
            if (ords == null || ords.length < numOrds) {
                return this.ords = new int[numOrds];
            } else {
                return ords;
            }
        }

        int[] getPositions(int numPositions) {
            if (positions == null || positions.length < numPositions) {
                return this.positions = new int[numPositions];
            } else {
                return positions;
            }
        }
    }
}
