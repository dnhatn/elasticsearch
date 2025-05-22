/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.BlockLoader;

/**
 * A {@link BytesRefBlock} consists of a pair: an {@link IntBlock} for ordinals and a {@link BytesRefVector} for the dictionary.
 * Compared to the regular {@link BytesRefBlock}, this block is slower due to indirect access and consume more memory because of
 * the additional ordinals block. However, they offer significant speed improvements and reduced memory usage when byte values are
 * frequently repeated
 */
public final class OrdinalBytesRefBlockBuilder implements BlockLoader.OrdinalBytesRefBuilder {
    private final IntBlock.Builder ordinalBuilder;
    private final BytesRefVector.Builder valuesBuilder;
    private int maxOrd = -1;

    public OrdinalBytesRefBlockBuilder(BlockFactory blockFactory, int estimatedSize) {
        var ordinals = blockFactory.newIntBlockBuilder(estimatedSize);
        BytesRefVector.Builder values = null;
        try {
            values = blockFactory.newBytesRefVectorBuilder(estimatedSize);
        } finally {
            if (values == null) {
                Releasables.close(ordinals);
            }
        }
        this.ordinalBuilder = ordinals;
        this.valuesBuilder = values;
    }

    @Override
    public int appendBytesRef(BytesRef bytes) {
        valuesBuilder.appendBytesRef(bytes);
        return ++maxOrd;
    }

    @Override
    public BlockLoader.Block build() {
        BytesRefBlock result = null;
        var ordinals = ordinalBuilder.build();
        BytesRefVector values = null;
        try {
            values = valuesBuilder.build();
            result = new OrdinalBytesRefBlock(ordinals, values);
            return result;
        } finally {
            if (result == null) {
                Releasables.closeExpectNoException(ordinals, values);
            }
        }
    }

    @Override
    public BlockLoader.Builder appendNull() {
        ordinalBuilder.appendNull();
        return this;
    }

    @Override
    public BlockLoader.Builder beginPositionEntry() {
        ordinalBuilder.beginPositionEntry();
        return this;
    }

    @Override
    public BlockLoader.Builder endPositionEntry() {
        ordinalBuilder.endPositionEntry();
        return this;
    }

    @Override
    public void appendOrd(int ord) {
        assert 0 <= ord && ord <= maxOrd : "ord=" + ord + " maxOrd=" + maxOrd;
        ordinalBuilder.appendInt(ord);
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(ordinalBuilder, valuesBuilder);
    }
}
