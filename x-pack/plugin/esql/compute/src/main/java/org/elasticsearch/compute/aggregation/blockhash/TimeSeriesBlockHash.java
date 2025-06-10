/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.util.List;

/**
 * An optimized block hash that receives two blocks: tsid and timestamp, which are sorted.
 * Since the incoming data is sorted, this block hash appends the incoming data to the internal arrays without lookup.
 */
public final class TimeSeriesBlockHash extends BlockHash {

    private final int tsHashChannel;
    private final int timestampIntervalChannel;

    private int lastTsidPosition = 0;
    private final BytesRefArrayWithSize tsidArray;

    private long lastTimestamp;
    private final LongArrayWithSize timestampArray;

    private int currentTimestampCount;
    private final IntArrayWithSize perTsidCountArray;
    private final List<GroupSpec> dimensionFields;
    private final Block.Builder[] dimensionCollectors;

    public TimeSeriesBlockHash(
        int tsHashChannel,
        int timestampIntervalChannel,
        List<GroupSpec> dimensionFields,
        BlockFactory blockFactory
    ) {
        super(blockFactory);
        this.tsHashChannel = tsHashChannel;
        this.timestampIntervalChannel = timestampIntervalChannel;
        this.dimensionFields = dimensionFields;
        boolean success = false;
        try {
            this.tsidArray = new BytesRefArrayWithSize(blockFactory);
            this.timestampArray = new LongArrayWithSize(blockFactory);
            this.perTsidCountArray = new IntArrayWithSize(blockFactory);
            this.dimensionCollectors = new Block.Builder[dimensionFields.size()];
            for (int i = 0; i < dimensionFields.size(); i++) {
                dimensionCollectors[i] = dimensionFields.get(i).elementType().newBlockBuilder(1, blockFactory);
            }
            success = true;
        } finally {
            if (success == false) {
                this.close();
            }
        }
    }

    @Override
    public void close() {
        Releasables.close(tsidArray, timestampArray, perTsidCountArray);
    }

    private OrdinalBytesRefVector getTsidVector(Page page) {
        BytesRefBlock block = page.getBlock(tsHashChannel);
        var ordinalBlock = block.asOrdinals();
        if (ordinalBlock == null) {
            throw new IllegalStateException("expected ordinal block for tsid");
        }
        var ordinalVector = ordinalBlock.asVector();
        if (ordinalVector == null) {
            throw new IllegalStateException("expected ordinal vector for tsid");
        }
        return ordinalVector;
    }

    private LongVector getTimestampVector(Page page) {
        final LongBlock timestampsBlock = page.getBlock(timestampIntervalChannel);
        LongVector timestampsVector = timestampsBlock.asVector();
        if (timestampsVector == null) {
            throw new IllegalStateException("expected long vector for timestamp");
        }
        return timestampsVector;
    }

    @Override
    public void add(Page page, GroupingAggregatorFunction.AddInput addInput) {
        final BytesRefVector tsidDict;
        final IntVector tsidOrdinals;
        {
            final var tsidVector = getTsidVector(page);
            tsidDict = tsidVector.getDictionaryVector();
            tsidOrdinals = tsidVector.getOrdinalsVector();
        }
        try (var ordsBuilder = blockFactory.newIntVectorBuilder(tsidOrdinals.getPositionCount())) {
            final BytesRef spare = new BytesRef();
            final BytesRef lastTsid = new BytesRef();
            final LongVector timestampVector = getTimestampVector(page);
            final Block[] dimensionBlocks = new Block[dimensionFields.size()];
            for (int f = 0; f < dimensionFields.size(); f++) {
                dimensionBlocks[f] = page.getBlock(dimensionFields.get(f).channel());
            }
            int lastOrd = -1;
            for (int pos = 0; pos < tsidOrdinals.getPositionCount(); pos++) {
                final int newOrd = tsidOrdinals.getInt(pos);
                boolean newGroup = false;
                if (lastOrd != newOrd) {
                    final var newTsid = tsidDict.getBytesRef(newOrd, spare);
                    if (positionCount() == 0) {
                        newGroup = true;
                    } else if (lastOrd == -1) {
                        tsidArray.get(lastTsidPosition, lastTsid);
                        newGroup = lastTsid.equals(newTsid) == false;
                    } else {
                        newGroup = true;
                    }
                    if (newGroup) {
                        endTsidGroup();
                        lastTsidPosition = tsidArray.count;
                        tsidArray.append(newTsid);
                        for (int f = 0; f < dimensionCollectors.length; f++) {
                            dimensionCollectors[f].copyFrom(dimensionBlocks[f], pos, pos + 1);
                        }
                    }
                    lastOrd = newOrd;
                }
                final long timestamp = timestampVector.getLong(pos);
                if (newGroup || timestamp != lastTimestamp) {
                    assert newGroup || lastTimestamp >= timestamp : "@timestamp goes backward " + lastTimestamp + " < " + timestamp;
                    timestampArray.append(timestamp);
                    lastTimestamp = timestamp;
                    currentTimestampCount++;
                }
                ordsBuilder.appendInt(timestampArray.count - 1);
            }
            try (var ords = ordsBuilder.build()) {
                addInput.add(0, ords);
            }
        }
    }

    private void endTsidGroup() {
        if (currentTimestampCount > 0) {
            perTsidCountArray.append(currentTimestampCount);
            currentTimestampCount = 0;
        }
    }

    @Override
    public ReleasableIterator<IntBlock> lookup(Page page, ByteSizeValue targetBlockSize) {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public Block[] getKeys() {
        endTsidGroup();
        final Block[] blocks = new Block[2 + dimensionFields.size()];
        try {
            blocks[1] = timestampArray.toBlock();
            if (OrdinalBytesRefBlock.isDense(positionCount(), tsidArray.count)) {
                BytesRefBlock tsid = buildTsidBlockWithOrdinal();
                blocks[0] = tsid;
                OrdinalBytesRefBlock ordinals = tsid.asOrdinals();
                buildDimensionBlocks(blocks, ordinals != null ? ordinals.getOrdinalsBlock() : null);
            } else {
                blocks[0] = buildTsidBlock();
                buildDimensionBlocks(blocks, null);
            }
            return blocks;
        } finally {
            if (blocks[blocks.length - 1] == null) {
                Releasables.close(blocks);
            }
        }
    }

    private BytesRefBlock buildTsidBlockWithOrdinal() {
        try (IntVector.FixedBuilder ordinalBuilder = blockFactory.newIntVectorFixedBuilder(positionCount())) {
            for (int i = 0; i < tsidArray.count; i++) {
                int numTimestamps = perTsidCountArray.array.get(i);
                for (int t = 0; t < numTimestamps; t++) {
                    ordinalBuilder.appendInt(i);
                }
            }
            final IntVector ordinalVector = ordinalBuilder.build();
            BytesRefVector dictionary = null;
            boolean success = false;
            try {
                dictionary = tsidArray.toVector();
                var result = new OrdinalBytesRefVector(ordinalVector, dictionary).asBlock();
                success = true;
                return result;
            } finally {
                if (success == false) {
                    Releasables.close(ordinalVector, dictionary);
                }
            }
        }
    }

    private BytesRefBlock buildTsidBlock() {
        try (BytesRefVector.Builder tsidBuilder = blockFactory.newBytesRefVectorBuilder(positionCount());) {
            final BytesRef tsid = new BytesRef();
            for (int i = 0; i < tsidArray.count; i++) {
                tsidArray.array.get(i, tsid);
                int numTimestamps = perTsidCountArray.array.get(i);
                for (int t = 0; t < numTimestamps; t++) {
                    tsidBuilder.appendBytesRef(tsid);
                }
            }
            return tsidBuilder.build().asBlock();
        }
    }

    private void buildDimensionBlocks(Block[] blocks, IntBlock tsidOrdinals) {
        if (dimensionFields.isEmpty()) {
            return;
        }
        int[] positions = null;
        for (int f = 0; f < dimensionCollectors.length; f++) {
            try (Block block = dimensionCollectors[f].build()) {
                if (tsidOrdinals != null && block.asVector() instanceof BytesRefVector bytes) {
                    tsidOrdinals.incRef();
                    blocks[2 + f] = new OrdinalBytesRefBlock(tsidOrdinals, bytes);
                } else {
                    if (positions == null) {
                        positions = new int[timestampArray.count];
                        int next = 0;
                        for (int i = 0; i < tsidArray.count; i++) {
                            int numTimestamps = perTsidCountArray.array.get(i);
                            for (int p = 0; p < numTimestamps; p++) {
                                positions[next++] = i;
                            }
                        }
                        assert next == positions.length;
                    }
                    blocks[2 + f] = block.filter(positions);
                }
            }
        }
    }

    private int positionCount() {
        return timestampArray.count;
    }

    @Override
    public IntVector nonEmpty() {
        long endExclusive = positionCount();
        return IntVector.range(0, Math.toIntExact(endExclusive), blockFactory);
    }

    @Override
    public BitArray seenGroupIds(BigArrays bigArrays) {
        return new Range(0, positionCount()).seenGroupIds(bigArrays);
    }

    public String toString() {
        return "TimeSeriesBlockHash{keys=[BytesRefKey[channel="
            + tsHashChannel
            + "], LongKey[channel="
            + timestampIntervalChannel
            + "]], entries="
            + positionCount()
            + "b}";
    }

    private static class LongArrayWithSize implements Releasable {
        private final BlockFactory blockFactory;
        private LongArray array;
        private int count = 0;

        LongArrayWithSize(BlockFactory blockFactory) {
            this.blockFactory = blockFactory;
            this.array = blockFactory.bigArrays().newLongArray(1, false);
        }

        void append(long value) {
            this.array = blockFactory.bigArrays().grow(array, count + 1);
            this.array.set(count, value);
            count++;
        }

        LongBlock toBlock() {
            try (var builder = blockFactory.newLongVectorFixedBuilder(count)) {
                for (int i = 0; i < count; i++) {
                    builder.appendLong(array.get(i));
                }
                return builder.build().asBlock();
            }
        }

        @Override
        public void close() {
            Releasables.close(array);
        }
    }

    private static class IntArrayWithSize implements Releasable {
        private final BlockFactory blockFactory;
        private IntArray array;
        private int count = 0;

        IntArrayWithSize(BlockFactory blockFactory) {
            this.blockFactory = blockFactory;
            this.array = blockFactory.bigArrays().newIntArray(1, false);
        }

        void append(int value) {
            this.array = blockFactory.bigArrays().grow(array, count + 1);
            this.array.set(count, value);
            count++;
        }

        @Override
        public void close() {
            Releasables.close(array);
        }
    }

    private static class BytesRefArrayWithSize implements Releasable {
        private final BlockFactory blockFactory;
        private BytesRefArray array;
        private int count = 0;

        BytesRefArrayWithSize(BlockFactory blockFactory) {
            this.blockFactory = blockFactory;
            this.array = new BytesRefArray(1, blockFactory.bigArrays());
        }

        void append(BytesRef value) {
            array.append(value);
            count++;
        }

        void get(int index, BytesRef dest) {
            array.get(index, dest);
        }

        BytesRefVector toVector() {
            BytesRefVector vector = blockFactory.newBytesRefArrayVector(array, count);
            array = null;
            return vector;
        }

        @Override
        public void close() {
            Releasables.close(array);
        }
    }
}
