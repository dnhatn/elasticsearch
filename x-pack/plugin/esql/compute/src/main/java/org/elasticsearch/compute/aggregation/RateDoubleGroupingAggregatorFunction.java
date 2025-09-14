/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.compute.aggregation;

// begin generated imports
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;

import java.util.Arrays;
import java.util.List;
// end generated imports

public final class RateDoubleGroupingAggregatorFunction implements GroupingAggregatorFunction {

    public static final class FunctionSupplier implements AggregatorFunctionSupplier {
        @Override
        public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
            throw new UnsupportedOperationException("non-grouping aggregator is not supported");
        }

        @Override
        public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
            return INTERMEDIATE_STATE_DESC;
        }

        @Override
        public AggregatorFunction aggregator(DriverContext driverContext, List<Integer> channels) {
            throw new UnsupportedOperationException("non-grouping aggregator is not supported");
        }

        @Override
        public RateDoubleGroupingAggregatorFunction groupingAggregator(DriverContext driverContext, List<Integer> channels) {
            return new RateDoubleGroupingAggregatorFunction(channels, driverContext);
        }

        @Override
        public String describe() {
            return "rate of doubles";
        }
    }

    static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
        // LongBlock: 2 * interval values each position
        new IntermediateStateDesc("timestamp", ElementType.LONG),
        // $Type$Block: 2 * interval values each position
        new IntermediateStateDesc("value", ElementType.DOUBLE),
        // LongVector: total samples
        new IntermediateStateDesc("sample", ElementType.LONG),
        // DoubleVector: total increases
        new IntermediateStateDesc("increase", ElementType.DOUBLE)
    );

    private final List<Integer> channels;
    private final DriverContext driverContext;
    private final BigArrays bigArrays;
    // states for raw input
    private int bufferSize;
    private LongArray timestamps;
    private DoubleArray values;
    private ObjectArray<int[]> slices;
    // states for intermediate states
    private ObjectArray<ReducedState> reducedStates;

    public RateDoubleGroupingAggregatorFunction(List<Integer> channels, DriverContext driverContext) {
        this.channels = channels;
        this.driverContext = driverContext;
        this.bigArrays = driverContext.bigArrays();
        boolean success = false;
        try {
            this.timestamps = bigArrays.newLongArray(PageCacheRecycler.LONG_PAGE_SIZE, false);
            this.values = bigArrays.newDoubleArray(PageCacheRecycler.DOUBLE_PAGE_SIZE, false);
            this.slices = bigArrays.newObjectArray(PageCacheRecycler.LONG_PAGE_SIZE);
            this.reducedStates = bigArrays.newObjectArray(PageCacheRecycler.OBJECT_PAGE_SIZE);
            success = true;
        } finally {
            if (success == false) {
                close();
            }
        }
    }

    @Override
    public void selectedMayContainUnseenGroups(SeenGroupIds seenGroupIds) {
        // manage nulls via buffers/reducedStates arrays
    }

    @Override
    public AddInput prepareProcessRawInputPage(SeenGroupIds seenGroupIds, Page page) {
        DoubleBlock valuesBlock = page.getBlock(channels.get(0));
        if (valuesBlock.areAllValuesNull()) {
            return new AddInput() {
                @Override
                public void add(int positionOffset, IntArrayBlock groupIds) {

                }

                @Override
                public void add(int positionOffset, IntBigArrayBlock groupIds) {

                }

                @Override
                public void add(int positionOffset, IntVector groupIds) {

                }

                @Override
                public void close() {

                }
            };
        }
        LongBlock timestampsBlock = page.getBlock(channels.get(1));
        LongVector timestampsVector = timestampsBlock.asVector();
        if (timestampsVector == null) {
            assert false : "expected timestamp vector in time-series aggregation";
            throw new IllegalStateException("expected timestamp vector in time-series aggregation");
        }
        return new AddInput() {
            @Override
            public void add(int positionOffset, IntArrayBlock groupIds) {
                addRawInput(positionOffset, groupIds, valuesBlock, timestampsVector);
            }

            @Override
            public void add(int positionOffset, IntBigArrayBlock groupIds) {
                addRawInput(positionOffset, groupIds, valuesBlock, timestampsVector);
            }

            @Override
            public void add(int positionOffset, IntVector groupIds) {
                var valuesVector = valuesBlock.asVector();
                ensureBufferCapacity(groupIds.getPositionCount());
                if (valuesVector != null) {
                    addRawInput(positionOffset, groupIds, valuesVector, timestampsVector);
                } else {
                    addRawInput(positionOffset, groupIds, valuesBlock, timestampsVector);
                }
            }

            @Override
            public void close() {

            }
        };
    }

    // Note that this path can be executed randomly in tests, not in production
    private void addRawInput(int positionOffset, IntBlock groups, DoubleBlock valueBlock, LongVector timestampVector) {
        ensureBufferCapacity(groups.getTotalValueCount());
        int lastGroup = -1;
        int[] slices = null;
        int positionCount = groups.getPositionCount();
        for (int p = 0; p < positionCount; p++) {
            if (groups.isNull(p)) {
                continue;
            }
            int valuePosition = p + positionOffset;
            if (valueBlock.isNull(valuePosition)) {
                continue;
            }
            assert valueBlock.getValueCount(valuePosition) == 1 : "expected single-valued block " + valueBlock;
            int groupStart = groups.getFirstValueIndex(p);
            int groupEnd = groupStart + groups.getValueCount(p);
            long timestamp = timestampVector.getLong(valuePosition);
            var value = valueBlock.getDouble(valueBlock.getFirstValueIndex(valuePosition));
            for (int g = groupStart; g < groupEnd; g++) {
                final int groupId = groups.getInt(g);
                if (lastGroup != groupId) {
                    slices = getSlicesForRawInput(groupId, timestamp);
                    lastGroup = groupId;
                }
                timestamps.set(bufferSize, timestamp);
                values.set(bufferSize++, value);
                assert slices != null;
                slices[0] = bufferSize;
            }
        }
    }

    private void addRawInput(int positionOffset, IntVector groups, DoubleBlock valueBlock, LongVector timestampVector) {
        if (groups.isConstant()) {
            int groupId = groups.getInt(0);
            int[] slices = getSlicesForRawInput(groupId, timestampVector.getLong(0));
            for (int p = 0; p < groups.getPositionCount(); p++) {
                int valuePosition = positionOffset + p;
                if (valueBlock.isNull(valuePosition)) {
                    continue;
                }
                assert valueBlock.getValueCount(valuePosition) == 1 : "expected single-valued block " + valueBlock;
                timestamps.set(bufferSize, timestampVector.getLong(valuePosition));
                values.set(bufferSize, valueBlock.getDouble(valuePosition));
                ++bufferSize;
            }
            slices[0] = bufferSize;
        } else {
            int lastGroup = -1;
            int[] slices = null;
            for (int p = 0; p < groups.getPositionCount(); p++) {
                int valuePosition = positionOffset + p;
                if (valueBlock.isNull(valuePosition) == false) {
                    continue;
                }
                assert valueBlock.getValueCount(valuePosition) == 1 : "expected single-valued block " + valueBlock;
                long timestamp = timestampVector.getLong(valuePosition);
                timestamps.set(bufferSize, timestamp);
                var value = valueBlock.getDouble(valuePosition);
                values.set(bufferSize, value);
                int groupId = groups.getInt(p);
                if (lastGroup != groupId) {
                    slices = getSlicesForRawInput(groupId, timestamp);
                    lastGroup = groupId;
                }
                assert slices != null;
                slices[0] = ++bufferSize;
            }
        }
    }

    private void addRawInput(int positionOffset, IntVector groups, DoubleVector valueVector, LongVector timestampVector) {
        int positionCount = groups.getPositionCount();
        if (groups.isConstant()) {
            int groupId = groups.getInt(0);
            int[] slices = getSlicesForRawInput(groupId, timestampVector.getLong(0));
            slices[0] = bufferSize + positionCount;
            for (int p = 0; p < positionCount; p++) {
                int valuePosition = positionOffset + p;
                int bufferPosition = bufferSize + p;
                timestamps.set(bufferPosition, timestampVector.getLong(valuePosition));
                values.set(bufferPosition, valueVector.getDouble(valuePosition));
            }
        } else {
            int lastGroup = -1;
            int[] slices = null;
            for (int p = 0; p < positionCount; p++) {
                int valuePosition = positionOffset + p;
                long timestamp = timestampVector.getLong(valuePosition);
                var value = valueVector.getDouble(valuePosition);
                timestamps.set(bufferSize, timestamp);
                values.set(bufferSize, value);
                int groupId = groups.getInt(p);
                if (lastGroup != groupId) {
                    slices = getSlicesForRawInput(groupId, timestamp);
                }
                assert slices != null;
                slices[0] = ++bufferSize;
            }
        }
    }

    @Override
    public int intermediateBlockCount() {
        return INTERMEDIATE_STATE_DESC.size();
    }

    @Override
    public void addIntermediateInput(int positionOffset, IntArrayBlock groups, Page page) {
        addIntermediateInputBlock(positionOffset, groups, page);
    }

    @Override
    public void addIntermediateInput(int positionOffset, IntBigArrayBlock groups, Page page) {
        addIntermediateInputBlock(positionOffset, groups, page);
    }

    @Override
    public void addIntermediateInput(int positionOffset, IntVector groups, Page page) {
        assert channels.size() == intermediateBlockCount();
        LongBlock timestamps = page.getBlock(channels.get(0));
        DoubleBlock values = page.getBlock(channels.get(1));
        assert timestamps.getTotalValueCount() == values.getTotalValueCount() : "timestamps=" + timestamps + "; values=" + values;
        if (values.areAllValuesNull()) {
            return;
        }
        LongVector samples = ((LongBlock) page.getBlock(channels.get(2))).asVector();
        DoubleVector increases = ((DoubleBlock) page.getBlock(channels.get(3))).asVector();
        for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
            int valuePosition = positionOffset + groupPosition;
            long sampleCount = samples.getLong(valuePosition);
            if (sampleCount == 0) {
                continue;
            }
            int groupId = groups.getInt(groupPosition);
            reducedStates = bigArrays.grow(reducedStates, groupId + 1);
            ReducedState merged = reducedStates.get(groupId);
            if (merged == null) {
                merged = new ReducedState();
                reducedStates.set(groupId, merged);
            }
            merged.samples += sampleCount;
            merged.increases += increases.getDouble(valuePosition);
            int tFirstIndex = timestamps.getFirstValueIndex(valuePosition);
            int vFirstIndex = values.getFirstValueIndex(valuePosition);
            int numIntervals = timestamps.getValueCount(valuePosition);
            for (int i = 0; i < numIntervals; i += 2) {
                long t1 = timestamps.getLong(tFirstIndex + i);
                double v1 = values.getDouble(vFirstIndex + i);
                long t2 = timestamps.getLong(tFirstIndex + i + 1);
                double v2 = values.getDouble(vFirstIndex + i + 1);
                merged.appendInterval(new Interval(t1, v1, t2, v2));
            }
        }
    }

    private void addIntermediateInputBlock(int positionOffset, IntBlock groups, Page page) {
        assert channels.size() == intermediateBlockCount();
        LongBlock timestamps = page.getBlock(channels.get(0));
        DoubleBlock values = page.getBlock(channels.get(1));
        assert timestamps.getTotalValueCount() == values.getTotalValueCount() : "timestamps=" + timestamps + "; values=" + values;
        if (values.areAllValuesNull()) {
            return;
        }
        LongVector samples = ((LongBlock) page.getBlock(channels.get(2))).asVector();
        DoubleVector increases = ((DoubleBlock) page.getBlock(channels.get(3))).asVector();
        for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
            int valuePosition = positionOffset + groupPosition;
            long sampleCount = samples.getLong(valuePosition);
            if (sampleCount == 0) {
                continue;
            }
            if (groups.isNull(groupPosition)) {
                continue;
            }
            int firstGroup = groups.getFirstValueIndex(groupPosition);
            int lastGroup = firstGroup + groups.getValueCount(groupPosition);
            for (int g = firstGroup; g < lastGroup; g++) {
                int groupId = groups.getInt(g);
                reducedStates = bigArrays.grow(reducedStates, groupId + 1);
                ReducedState merged = reducedStates.get(groupId);
                if (merged == null) {
                    merged = new ReducedState();
                    reducedStates.set(groupId, merged);
                }
                merged.samples += sampleCount;
                merged.increases += increases.getDouble(valuePosition);
                int tFirstIndex = timestamps.getFirstValueIndex(valuePosition);
                int vFirstIndex = values.getFirstValueIndex(valuePosition);
                int numIntervals = timestamps.getValueCount(valuePosition);
                for (int i = 0; i < numIntervals; i += 2) {
                    long t1 = timestamps.getLong(tFirstIndex + i);
                    double v1 = values.getDouble(vFirstIndex + i);
                    long t2 = timestamps.getLong(tFirstIndex + i + 1);
                    double v2 = values.getDouble(vFirstIndex + i + 1);
                    merged.appendInterval(new Interval(t1, v1, t2, v2));
                }
            }
        }
    }

    static final Interval[] EMPTY_INTERVALS = new Interval[0];
    static class ReducedState {
        long samples;
        double increases;
        Interval[] intervals = EMPTY_INTERVALS;

        void appendInterval(Interval interval) {
            int curr = intervals.length;
            if (curr == 0) {
                intervals = new Interval[]{interval};
            } else {
                var newIntervals = new Interval[curr + 1];
                System.arraycopy(intervals, 0, newIntervals, 0, curr);
                newIntervals[curr] = interval;
                this.intervals = newIntervals;
            }
        }
    }

    record Interval(long t1, double v1, long t2, double v2) implements Comparable<Interval> {
        @Override
        public int compareTo(Interval other) {
            return Long.compare(other.t1, t1); // want most recent first
        }
    }

    @Override
    public void close() {
        Releasables.close(reducedStates, slices, timestamps, values);
    }

    private void ensureBufferCapacity(int newElements) {
        int newSize = bufferSize + newElements;
        timestamps = bigArrays.grow(timestamps, newSize);
        values = bigArrays.grow(values, newSize);
    }

    private int[] getSlicesForRawInput(int groupId, long firstTimestamp) {
        slices = bigArrays.grow(slices, groupId + 1);
        int[] slices = this.slices.get(groupId);
        if (slices == null) {
            slices = new int[] { bufferSize, bufferSize }; // {end, start}
            this.slices.set(groupId, slices);
        } else if (slices[0] != bufferSize || (bufferSize > 0 && timestamps.get(bufferSize - 1) < firstTimestamp)) {
            int[] newSlices = new int[slices.length + 2];
            System.arraycopy(slices, 0, newSlices, 2, slices.length);
            newSlices[0] = bufferSize;
            newSlices[1] = bufferSize;
            slices = newSlices;
            this.slices.set(groupId, slices);
        }
        return slices;
    }

    void flushSlices(int[] slices, ReducedState reduced) {
        var pq = mergeQueue(slices);
        // empty
        if (pq.size() == 0) {
            return;
        }
        // first
        final long lastTimestamp;
        final double lastValue;
        {
            SliceIterator top = pq.top();
            lastTimestamp = top.timestamp;
            int position = top.next();
            lastValue = values.get(position);
            if (top.exhausted()) {
                pq.pop();
            } else {
                pq.updateTop();
            }
            reduced.samples++;
        }
        if (pq.size() == 0) {
            reduced.appendInterval(new Interval(lastTimestamp, lastValue, lastTimestamp, lastValue));
            return;
        }
        var prevValue = lastValue;
        int position = -1;
        while (pq.size() > 0) {
            SliceIterator top = pq.top();
            position = top.next();
            if (top.exhausted()) {
                pq.pop();
            } else {
                pq.updateTop();
            }
            var val = values.get(position);
            // what is the increase between these two
            // reset += dv(val, prevValue) + dv(prevValue, lastValue) - dv(val, lastValue);
            prevValue = val;
            reduced.samples++;
        }
        reduced.appendInterval(new Interval(lastTimestamp, lastValue, timestamps.get(position), prevValue));
    }

    final class SliceIterator {
        int start;
        long timestamp;
        final int end;

        SliceIterator(int start, int end) {
            this.start = start;
            this.end = end;
            this.timestamp = timestamps.get(start);
        }

        boolean exhausted() {
            return start >= end;
        }

        int next() {
            int index = start++;
            if (start < end) {
                timestamp = timestamps.get(start);
            }
            return index;
        }
    }

    PriorityQueue<SliceIterator> mergeQueue(int[] slices) {
        return new PriorityQueue<>(slices.length) {
            {
                for (int i = 0; i < slices.length; i += 2) {
                    int start = slices[i + 1];
                    int end = slices[i];
                    if (end > start) {
                        add(new SliceIterator(start, end));
                    }
                }
            }

            @Override
            protected boolean lessThan(SliceIterator a, SliceIterator b) {
                return a.timestamp > b.timestamp; // want the latest timestamp first
            }
        };
    }

    @Override
    public void evaluateIntermediate(Block[] blocks, int offset, IntVector selected) {
        BlockFactory blockFactory = driverContext.blockFactory();
        int positionCount = selected.getPositionCount();
        try (
            var timestamps = blockFactory.newLongBlockBuilder(positionCount * 2);
            var values = blockFactory.newDoubleBlockBuilder(positionCount * 2);
            var samples = blockFactory.newLongVectorFixedBuilder(positionCount);
            var increase = blockFactory.newDoubleVectorFixedBuilder(positionCount)
        ) {
            for (int p = 0; p < positionCount; p++) {
                int group = selected.getInt(p);
                var state = flushAndCombineState(group);
                if (state != null && state.samples > 0) {
                    timestamps.beginPositionEntry();
                    values.beginPositionEntry();
                    for (Interval interval : state.intervals) {
                        timestamps.appendLong(interval.t1);
                        timestamps.appendLong(interval.t2);
                        values.appendDouble(interval.v1);
                        values.appendDouble(interval.v2);
                    }
                    timestamps.endPositionEntry();
                    values.endPositionEntry();
                } else {
                    timestamps.appendLong(0);
                    values.appendDouble(0);
                    samples.appendLong(0);
                    increase.appendDouble(0);
                }
            }
            blocks[offset] = timestamps.build();
            blocks[offset + 1] = values.build();
            blocks[offset + 2] = samples.build().asBlock();
            blocks[offset + 3] = increase.build().asBlock();
        }
    }

    @Override
    public void evaluateFinal(Block[] blocks, int offset, IntVector selected, GroupingAggregatorEvaluationContext evalContext) {
        BlockFactory blockFactory = driverContext.blockFactory();
        int positionCount = selected.getPositionCount();
        try (var rates = blockFactory.newDoubleBlockBuilder(positionCount)) {
            for (int p = 0; p < positionCount; p++) {
                int group = selected.getInt(p);
                var reduced = flushAndCombineState(group);
                if (reduced == null || reduced.samples < 2) {
                    rates.appendNull();
                    continue;
                }
                var intervals = reduced.intervals;
                ArrayUtil.timSort(intervals);
                double increase = 0.0;
                long t1 = intervals[0].t1;
                long t2 = intervals[intervals.length - 1].t2;
                if (t1 == t2) {
                    rates.appendNull();
                    continue;
                }
                final double rate = increase * 1000.0 / (t1 - t2);
                rates.appendDouble(rate);
            }
            blocks[offset] = rates.build();
        }
    }

    ReducedState flushAndCombineState(int groupId) {
        ReducedState reduced = groupId < reducedStates.size() ? reducedStates.getAndSet(groupId, null) : null;
        int[] slices = this.slices.size() > groupId ? this.slices.get(groupId) : null;
        if (slices != null) {
            if (reduced == null) {
                reduced = new ReducedState();
            }
            flushSlices(slices, reduced);
        }
        return reduced;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName()).append("[");
        sb.append("channels=").append(channels);
        sb.append("]");
        return sb.toString();
    }
}
