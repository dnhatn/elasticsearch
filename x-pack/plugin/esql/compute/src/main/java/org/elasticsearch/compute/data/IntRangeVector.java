/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.ReleasableIterator;

public final class IntRangeVector extends AbstractVector implements IntVector {
    public static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(IntRangeVector.class);

    private final int lowerInclusive;
    private final int upperExclusive;

    public IntRangeVector(BlockFactory blockFactory, int lowerInclusive, int upperExclusive) {
        super(upperExclusive - lowerInclusive, blockFactory);
        this.lowerInclusive = lowerInclusive;
        this.upperExclusive = upperExclusive;
    }

    @Override
    public int getInt(int position) {
        return lowerInclusive + position;
    }

    @Override
    public IntBlock asBlock() {
        return new IntVectorBlock(this);
    }

    @Override
    public IntVector filter(int... positions) {
        try (FixedBuilder builder = blockFactory().newIntVectorFixedBuilder(positions.length)) {
            for (int position : positions) {
                builder.appendInt(getInt(position));
            }
            return builder.build();
        }
    }

    @Override
    public IntBlock keepMask(BooleanVector mask) {
        try (IntBlock.Builder builder = blockFactory().newIntBlockBuilder(getPositionCount())) {
            for (int p = 0; p < getPositionCount(); p++) {
                if (mask.getBoolean(p)) {
                    builder.appendInt(getInt(p));
                } else {
                    builder.appendNull();
                }
            }
            return builder.build();
        }
    }

    @Override
    public ReleasableIterator<? extends IntBlock> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        return new IntLookup(asBlock(), positions, targetBlockSize);
    }

    @Override
    public int min() {
        return lowerInclusive;
    }

    @Override
    public int max() {
        return upperExclusive - 1;
    }

    @Override
    public ElementType elementType() {
        return ElementType.INT;
    }

    @Override
    public boolean isConstant() {
        return upperExclusive - lowerInclusive == 1;
    }

    @Override
    public long ramBytesUsed() {
        return BASE_RAM_BYTES_USED;
    }
}
