/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.ByteArray;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.LongLongHashTable;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBigArrayVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBigArrayVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

/**
 * Maps a {@link LongBlock} column paired with a {@link BytesRefBlock} column to group ids.
 */
public final class LongIntBlockHash extends BlockHash {
    private final int longChannel;
    private final int intChannel;
    private final boolean reverseOutput;
    private final int emitBatchSize;
    private LongLongHashTable directHash;
    private MultipleHash multipleHash;
    private PackedValuesBlockHash packedHash;

    LongIntBlockHash(BlockFactory blockFactory, int longChannel, int intChannel, boolean reverseOutput, int emitBatchSize) {
        super(blockFactory);
        this.longChannel = longChannel;
        this.intChannel = intChannel;
        this.reverseOutput = reverseOutput;
        this.emitBatchSize = emitBatchSize;
        this.directHash = HashImplFactory.newLongLongHash(blockFactory);
    }

    @Override
    public void close() {
        Releasables.close(directHash, multipleHash, packedHash);
    }

    @Override
    public void add(Page page, GroupingAggregatorFunction.AddInput addInput) {
        if (directHash != null || multipleHash != null) {
            LongBlock longs = page.getBlock(longChannel);
            IntBlock ints = page.getBlock(intChannel);
            LongVector longsVector = longs.asVector();
            IntVector intsVector = ints.asVector();
            if (longsVector != null && intsVector != null) {
                addDirect(longsVector, intsVector, addInput);
                return;
            }
        }
        if (packedHash == null) {
            switchToPackedHash();
        }
        packedHash.add(page, addInput);
    }

    private void switchToPackedHash() {
        // TODO:
        // copy keys from direct to packed
        // allocate 1 byte for null, 8 bytes, and 4 bytes
        // then fill all values
        // then add to packedHash
        throw new AssertionError();
    }

    void addDirect(LongVector longs, IntVector ints, GroupingAggregatorFunction.AddInput addInput) {
        if (directHash != null && directHash.size() + longs.getPositionCount() > 100_000) {
            multipleHash = new MultipleHash(blockFactory, directHash);
            directHash = null;
        }
        int positionCount = longs.getPositionCount();
        if (directHash != null) {
            try (var ordsBuilder = blockFactory.newIntVectorFixedBuilder(positionCount)) {
                for (int p = 0; p < positionCount; p++) {
                    long longKey = longs.getLong(p);
                    int intKey = ints.getInt(p);
                    long ord = hashOrdToGroup(directHash.add(longKey, intKey));
                    ordsBuilder.appendInt(Math.toIntExact(ord));
                }
                try (IntVector ords = ordsBuilder.build()) {
                    addInput.add(0, ords);
                }
            }
        } else {
            try (var ordsBuilder = blockFactory.newIntVectorFixedBuilder(positionCount)) {
                for (int p = 0; p < positionCount; p++) {
                    long longKey = longs.getLong(p);
                    int intKey = ints.getInt(p);
                    long ord = hashOrdToGroup(multipleHash.addKey(longKey, intKey));
                    ordsBuilder.appendInt(Math.toIntExact(ord));
                }
                try (IntVector ords = ordsBuilder.build()) {
                    addInput.add(0, ords);
                }
            }
        }
    }

    @Override
    public ReleasableIterator<IntBlock> lookup(Page page, ByteSizeValue targetBlockSize) {
        if (packedHash != null) {
            return packedHash.lookup(page, targetBlockSize);
        }
        // TODO:
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public Block[] getKeys() {
        if (packedHash != null) {
            return packedHash.getKeys();
        }
        if (multipleHash != null) {
            return multipleHash.getKeys(reverseOutput);
        }
        int positions = (int) directHash.size();
        if (positions < BlockFactory.DEFAULT_MAX_BLOCK_PRIMITIVE_ARRAY_SIZE.getBytes() / Long.BYTES) {
            return getSmallKeys(positions);
        } else {
            return getBigKeys(positions);
        }
    }

    private Block[] getSmallKeys(int positions) {
        LongVector k1 = null;
        IntVector k2 = null;
        try (
            var longKeys = blockFactory.newLongVectorFixedBuilder(positions);
            var intKeys = blockFactory.newIntVectorFixedBuilder(positions)
        ) {
            for (int id = 0; id < positions; id++) {
                longKeys.appendLong(id, directHash.getKey1(id));
                intKeys.appendInt(id, Math.toIntExact(directHash.getKey2(id)));
            }
            k1 = longKeys.build();
            k2 = intKeys.build();
            final Block[] blocks;
            if (reverseOutput) {
                blocks = new Block[] { k2.asBlock(), k1.asBlock() };
            } else {
                blocks = new Block[] { k1.asBlock(), k2.asBlock() };
            }
            k1 = null;
            k2 = null;
            return blocks;
        } finally {
            Releasables.close(k1, k2);
        }
    }

    private Block[] getBigKeys(int positions) {
        LongArray longKeys = blockFactory.bigArrays().newLongArray(positions);
        IntArray intKeys = blockFactory.bigArrays().newIntArray(positions);
        for (int id = 0; id < positions; id++) {
            longKeys.set(id, directHash.getKey1(id));
            intKeys.set(id, Math.toIntExact(directHash.getKey2(id)));
        }
        LongBlock longBlock = new LongBigArrayVector(longKeys, positions, blockFactory).asBlock();
        IntBlock intBlock = new IntBigArrayVector(intKeys, positions, blockFactory).asBlock();
        final Block[] blocks;
        if (reverseOutput) {
            blocks = new Block[] { intBlock, longBlock };
        } else {
            blocks = new Block[] { longBlock, intBlock };
        }
        return blocks;
    }

    public long size() {
        if (directHash != null) {
            return directHash.size();
        }
        if (multipleHash != null) {
            return multipleHash.tableIds.size();
        }
        return 0;
    }

    @Override
    public BitArray seenGroupIds(BigArrays bigArrays) {
        if (packedHash != null) {
            return packedHash.seenGroupIds(bigArrays);
        }
        if (multipleHash != null) {
            return new Range(0, Math.toIntExact(multipleHash.tableIds.size())).seenGroupIds(bigArrays);
        } else {
            return new Range(0, Math.toIntExact(directHash.size())).seenGroupIds(bigArrays);
        }
    }

    @Override
    public IntVector nonEmpty() {
        if (packedHash != null) {
            return packedHash.nonEmpty();
        }
        if (multipleHash != null) {
            return IntVector.range(0, Math.toIntExact(multipleHash.tableIds.size()), blockFactory);
        } else {
            return IntVector.range(0, Math.toIntExact(directHash.size()), blockFactory);
        }
    }

    static class MultipleHash implements Releasable {
        final LongLongHashTable[] hashes;
        final BlockFactory blockFactory;
        final ByteArray tableIds;

        MultipleHash(BlockFactory blockFactory, LongLongHashTable current) {
            this.hashes = new LongLongHashTable[256];
            for (int i = 0; i < hashes.length; i++) {
                this.hashes[i] = HashImplFactory.newLongLongHash(blockFactory);
            }
            this.blockFactory = blockFactory;
            this.tableIds = blockFactory.bigArrays().newByteArray(current.size() * 2);
            // copy the current table
            for (long id = 0; id < current.size(); id++) {
                long key1 = current.getKey1(id);
                int key2 = (int) current.getKey2(id);
                addKey(key1, key2);
            }
        }

        long addKey(long longKey, int intKey) {
            int tableIndex = (int)(longKey & 0xFF);
            final LongLongHashTable hash = hashes[tableIndex];
            long key = hash.add(longKey, intKey);
            if (key >= 0) {
                tableIds.set(key, (byte) tableIndex);
            }
            return key;
        }


        private Block[] getKeys(boolean reverseOutput) {
            LongVector k1 = null;
            IntVector k2 = null;
            int positions = Math.toIntExact(tableIds.size());
            try (
                var longKeys = blockFactory.newLongVectorFixedBuilder(positions);
                var intKeys = blockFactory.newIntVectorFixedBuilder(positions)
            ) {
                for (int id = 0; id < positions; id++) {
                    int tableIndex = tableIds.get(id);
                    LongLongHashTable directHash = hashes[tableIndex];
                    longKeys.appendLong(id, directHash.getKey1(id));
                    intKeys.appendInt(id, Math.toIntExact(directHash.getKey2(id)));
                }
                k1 = longKeys.build();
                k2 = intKeys.build();
                final Block[] blocks;
                if (reverseOutput) {
                    blocks = new Block[] { k2.asBlock(), k1.asBlock() };
                } else {
                    blocks = new Block[] { k1.asBlock(), k2.asBlock() };
                }
                k1 = null;
                k2 = null;
                return blocks;
            } finally {
                Releasables.close(k1, k2);
            }
        }

        @Override
        public void close() {
            Releasables.close(hashes);
        }
    }

    @Override
    public String toString() {
        if (packedHash != null) {
            return packedHash.toString();
        }
        long size = directHash.size();
        long memoryUsed = directHash.size() * (3 * Long.BYTES);
        return "LongIntBlockHash{keys=[long[channel="
            + longChannel
            + "], int[channel="
            + intChannel
            + "]], entries="
            + size
            + ", size="
            + memoryUsed
            + "b}";
    }
}
