/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.ReleasableIterator;

public final class IntRangeVector extends AbstractVector implements IntVector {
    private final int fromInclusive;
    private final int toExclusive;

    public IntRangeVector(BlockFactory blockFactory, int fromInclusive, int toExclusive) {
        super(toExclusive - fromInclusive, blockFactory);
        this.fromInclusive = fromInclusive;
        this.toExclusive = toExclusive;
    }

    @Override
    public int getInt(int position) {
        return position - fromInclusive;
    }

    @Override
    public IntBlock asBlock() {
        throw new UnsupportedOperationException();
    }

    @Override
    public IntVector filter(int... positions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public IntBlock keepMask(BooleanVector mask) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ReleasableIterator<? extends IntBlock> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int min() {
        return fromInclusive;
    }

    @Override
    public int max() {
        return toExclusive - 1;
    }

    @Override
    public ElementType elementType() {
        return ElementType.INT;
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public long ramBytesUsed() {
        return 0;
    }
}
