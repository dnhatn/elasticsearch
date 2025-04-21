/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.ReleasableIterator;

public final class FilterLongBlock extends AbstractNonThreadSafeRefCounted implements LongBlock {
    private final LongBlock block;
    private final int[] mapped;

    public FilterLongBlock(LongBlock block, int[] mapped) {
        this.block = block;
        this.mapped = mapped;
        block.incRef();
    }

    @Override
    protected void closeInternal() {
        block.decRef();
    }

    @Override
    public long getLong(int valueIndex) {
        return block.getLong(valueIndex);
    }

    @Override
    public LongVector asVector() {
        return null;
    }

    @Override
    public LongBlock filter(int... positions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public LongBlock keepMask(BooleanVector mask) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ReleasableIterator<? extends LongBlock> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        throw new UnsupportedOperationException();
    }

    @Override
    public LongBlock expand() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getTotalValueCount() {
        return block.getTotalValueCount();
    }

    @Override
    public int getPositionCount() {
        return block.getPositionCount();
    }

    @Override
    public int getFirstValueIndex(int position) {
        return block.getFirstValueIndex(mapped[position]);
    }

    @Override
    public int getValueCount(int position) {
        return block.getValueCount(mapped[position]);
    }

    @Override
    public ElementType elementType() {
        return block.elementType();
    }

    @Override
    public BlockFactory blockFactory() {
        return block.blockFactory();
    }

    @Override
    public void allowPassingToDifferentDriver() {
        block.allowPassingToDifferentDriver();
    }

    @Override
    public boolean isNull(int position) {
        return block.isNull(mapped[position]);
    }

    @Override
    public boolean mayHaveNulls() {
        return block.mayHaveNulls();
    }

    @Override
    public boolean areAllValuesNull() {
        return block.areAllValuesNull();
    }

    @Override
    public boolean mayHaveMultivaluedFields() {
        return block.mayHaveMultivaluedFields();
    }

    @Override
    public boolean doesHaveMultivaluedFields() {
        return block.doesHaveMultivaluedFields();
    }

    @Override
    public MvOrdering mvOrdering() {
        return block.mvOrdering();
    }

    @Override
    public long ramBytesUsed() {
        return block.ramBytesUsed();
    }
}
