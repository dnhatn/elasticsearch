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
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

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
            return "rate of double";
        }
    }

    static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
        new IntermediateStateDesc("timestamps", ElementType.LONG),
        new IntermediateStateDesc("values", ElementType.DOUBLE),
        new IntermediateStateDesc("sampleCounts", ElementType.INT),
        new IntermediateStateDesc("resets", ElementType.DOUBLE)
    );

    private int bufferSize;
    private LongArray timestamps;
    private DoubleArray values;
    private ObjectArray<int[]> rawSlices;
    private final List<Integer> channels;
    private final DriverContext driverContext;
    private final BigArrays bigArrays;
    private ObjectArray<ReducedState> reducedStates;

    public RateDoubleGroupingAggregatorFunction(List<Integer> channels, DriverContext driverContext) {
        this.channels = channels;
        this.driverContext = driverContext;
        this.bigArrays = driverContext.bigArrays();
        this.timestamps = bigArrays.newLongArray(PageCacheRecycler.LONG_PAGE_SIZE, false);
        this.values = bigArrays.newDoubleArray(PageCacheRecycler.DOUBLE_PAGE_SIZE, false);
        ObjectArray<int[]> buffers = bigArrays.newObjectArray(256);
        try {
            this.reducedStates = bigArrays.newObjectArray(256);
            this.rawSlices = buffers;
            buffers = null;
        } finally {
            Releasables.close(buffers);
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
        //                 ensureBufferCapacity(groupIds.getPositionCount());
        throw new AssertionError("should not reach here");
    }

    private void addRawInput(int positionOffset, IntVector groups, DoubleBlock valueBlock, LongVector timestampVector) {
        if (groups.isConstant()) {
            int groupId = groups.getInt(0);
            int[] slices = getRawSlices(groupId, timestampVector.getLong(0));
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
                    slices = getRawSlices(groupId, timestamp);
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
            int[] slices = getRawSlices(groupId, timestampVector.getLong(0));
            for (int p = 0; p < positionCount; p++) {
                int valuePosition = positionOffset + p;
                int bufferPosition = bufferSize + p;
                timestamps.set(bufferPosition, timestampVector.getLong(valuePosition));
                values.set(bufferPosition, valueVector.getDouble(valuePosition));
            }
            slices[0] = bufferSize;
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
                    slices = getRawSlices(groupId, timestamp);
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
        IntVector sampleCounts = ((IntBlock) page.getBlock(channels.get(2))).asVector();
        DoubleVector resets = ((DoubleBlock) page.getBlock(channels.get(3))).asVector();
        for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
            int valuePosition = positionOffset + groupPosition;
            int sampleCount = sampleCounts.getInt(valuePosition);
            if (sampleCount == 0) {
                continue;
            }
            int groupId = groups.getInt(groupPosition);
            reducedStates = bigArrays.grow(reducedStates, groupId + 1);
            ReducedState state = reducedStates.get(groupId);
            if (state == null) {
                state = new ReducedState();
                reducedStates.set(groupId, state);
            }
            state.appendValuesFromBlocks(timestamps, values, valuePosition);
            state.samples += sampleCount;
            state.resets += resets.getDouble(valuePosition);
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
        IntVector sampleCounts = ((IntBlock) page.getBlock(channels.get(2))).asVector();
        DoubleVector resets = ((DoubleBlock) page.getBlock(channels.get(3))).asVector();
        for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++) {
            int valuePosition = positionOffset + groupPosition;
            int sampleCount = sampleCounts.getInt(valuePosition);
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
                ReducedState state = reducedStates.get(groupId);
                if (state == null) {
                    state = new ReducedState();
                    reducedStates.set(groupId, state);
                }
                state.appendValuesFromBlocks(timestamps, values, valuePosition);
                state.samples += sampleCount;
                state.resets += resets.getDouble(groupPosition);
            }
        }
    }

    @Override
    public void evaluateIntermediate(Block[] blocks, int offset, IntVector selected) {
        BlockFactory blockFactory = driverContext.blockFactory();
        int positionCount = selected.getPositionCount();
        try (
            var timestamps = blockFactory.newLongBlockBuilder(positionCount * 2);
            var values = blockFactory.newDoubleBlockBuilder(positionCount * 2);
            var sampleCounts = blockFactory.newIntVectorFixedBuilder(positionCount);
            var resets = blockFactory.newDoubleVectorFixedBuilder(positionCount)
        ) {
            for (int p = 0; p < positionCount; p++) {
                int group = selected.getInt(p);
                var state = flushAndCombineState(group);
                if (state != null && state.timestamps.length > 0) {
                    if (state.samples > 1) {
                        timestamps.beginPositionEntry();
                        values.beginPositionEntry();
                        for (int s = 0; s < state.timestamps.length; s++) {
                            timestamps.appendLong(state.timestamps[s]);
                            values.appendDouble(state.values[s]);
                        }
                        timestamps.endPositionEntry();
                        values.endPositionEntry();
                    } else {
                        timestamps.appendLong(state.timestamps[0]);
                        values.appendDouble(state.values[0]);
                    }
                    sampleCounts.appendInt(state.samples);
                    resets.appendDouble(state.resets);
                } else {
                    timestamps.appendLong(0);
                    values.appendDouble(0);
                    sampleCounts.appendInt(0);
                    resets.appendDouble(0);
                }
            }
            blocks[offset] = timestamps.build();
            blocks[offset + 1] = values.build();
            blocks[offset + 2] = sampleCounts.build().asBlock();
            blocks[offset + 3] = resets.build().asBlock();
        }
    }

    @Override
    public void close() {
        Releasables.close(reducedStates, rawSlices, timestamps, values);
    }

    private void ensureBufferCapacity(int newElements) {
        int requiredCapacity = bufferSize + newElements;
        timestamps = bigArrays.grow(timestamps, requiredCapacity);
        values = bigArrays.resize(values, requiredCapacity);
    }

    private int[] getRawSlices(int groupId, long firstTimestamp) {
        rawSlices = bigArrays.grow(rawSlices, groupId + 1);
        int[] slices = rawSlices.get(groupId);
        if (slices == null) {
            slices = new int[]{bufferSize, bufferSize}; // {end, start}
            rawSlices.set(groupId, slices);
        } else {
            if (slices[0] == bufferSize || (timestamps.get(bufferSize - 1) < firstTimestamp)) {
                int[] newSlices = new int[slices.length + 2];
                System.arraycopy(slices, 0, newSlices, 2, slices.length);
                newSlices[0] = bufferSize;
                newSlices[1] = bufferSize;
                slices = newSlices;
            }
            rawSlices.set(groupId, slices);
        }
        return slices;
    }

    void flush(int[] slices, ReducedState state) {
        PriorityQueue<SliceIterator> pq = new PriorityQueue<>(slices.length) {
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
            state.samples++;
        }
        if (pq.size() == 0) {
            state.appendOneValue(lastTimestamp, lastValue);
            return;
        }
        var prevValue = lastValue;
        double reset = 0;
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
            reset += dv(val, prevValue) + dv(prevValue, lastValue) - dv(val, lastValue);
            prevValue = val;
            state.samples++;
        }
        state.resets += reset;
        state.appendTwoValues(lastTimestamp, lastValue, timestamps.get(position), prevValue);
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

    @Override
    public void evaluateFinal(Block[] blocks, int offset, IntVector selected, GroupingAggregatorEvaluationContext evalContext) {
        BlockFactory blockFactory = driverContext.blockFactory();
        int positionCount = selected.getPositionCount();
        try (var rates = blockFactory.newDoubleBlockBuilder(positionCount)) {
            for (int p = 0; p < positionCount; p++) {
                int group = selected.getInt(p);
                var state = flushAndCombineState(group);
                if (state == null || state.timestamps.length < 2) {
                    rates.appendNull();
                    continue;
                }
                final double rate;
                if (evalContext instanceof TimeSeriesGroupingAggregatorEvaluationContext tsContext) {
                    rate = extrapolateRate(state, tsContext.rangeStartInMillis(group), tsContext.rangeEndInMillis(group));
                } else {
                    rate = computeRateWithoutExtrapolate(state);
                }
                rates.appendDouble(rate);
            }
            blocks[offset] = rates.build();
        }
    }

    ReducedState flushAndCombineState(int groupId) {
        ReducedState state = groupId < reducedStates.size() ? reducedStates.getAndSet(groupId, null) : null;
        if (state != null) {
            return state;
        }
        int[] slices = rawSlices.size() > groupId ? rawSlices.get(groupId) : null;
        if (slices == null) {
            return null;
        }
        ReducedState reduced = new ReducedState();
        flush(slices, reduced);
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

    static final class ReducedState {
        private static final long[] EMPTY_LONGS = new long[0];
        private static final double[] EMPTY_VALUES = new double[0];
        int samples;
        double resets;
        long[] timestamps = EMPTY_LONGS;
        double[] values = EMPTY_VALUES;

        void appendOneValue(long t, double v) {
            int currentSize = timestamps.length;
            this.timestamps = ArrayUtil.growExact(timestamps, currentSize + 1);
            this.values = ArrayUtil.growExact(values, currentSize + 1);
            this.timestamps[currentSize] = t;
            this.values[currentSize] = v;
        }

        void appendTwoValues(long t1, double v1, long t2, double v2) {
            int currentSize = timestamps.length;
            this.timestamps = ArrayUtil.growExact(timestamps, currentSize + 2);
            this.values = ArrayUtil.growExact(values, currentSize + 2);
            this.timestamps[currentSize] = t1;
            this.values[currentSize] = v1;
            currentSize++;
            this.timestamps[currentSize] = t2;
            this.values[currentSize] = v2;
        }

        void appendValuesFromBlocks(LongBlock ts, DoubleBlock vs, int position) {
            int tsFirst = ts.getFirstValueIndex(position);
            int vsFirst = vs.getFirstValueIndex(position);
            int count = ts.getValueCount(position);
            int total = timestamps.length + count;
            long[] mergedTimestamps = new long[total];
            double[] mergedValues = new double[total];
            int i = 0, j = 0, k = 0;
            while (i < timestamps.length && j < count) {
                long t = ts.getLong(tsFirst + j);
                if (timestamps[i] > t) {
                    mergedTimestamps[k] = timestamps[i];
                    mergedValues[k++] = values[i++];
                } else {
                    mergedTimestamps[k] = t;
                    mergedValues[k++] = vs.getDouble(vsFirst + j++);
                }
            }
            while (i < timestamps.length) {
                mergedTimestamps[k] = timestamps[i];
                mergedValues[k++] = values[i++];
            }
            while (j < count) {
                mergedTimestamps[k] = ts.getLong(tsFirst + j);
                mergedValues[k++] = vs.getDouble(vsFirst + j++);
            }
            this.timestamps = mergedTimestamps;
            this.values = mergedValues;
        }
    }

    private static double computeRateWithoutExtrapolate(ReducedState state) {
        final int len = state.timestamps.length;
        assert len >= 2 : "rate requires at least two samples; got " + len;
        final long firstTS = state.timestamps[state.timestamps.length - 1];
        final long lastTS = state.timestamps[0];
        double reset = state.resets;
        for (int i = 1; i < len; i++) {
            if (state.values[i - 1] < state.values[i]) {
                reset += state.values[i];
            }
        }
        final double firstValue = state.values[len - 1];
        final double lastValue = state.values[0] + reset;
        return (lastValue - firstValue) * 1000.0 / (lastTS - firstTS);
    }

    /**
     * Credit to PromQL for this extrapolation algorithm:
     * If samples are close enough to the rangeStart and rangeEnd, we extrapolate the rate all the way to the boundary in question.
     * "Close enough" is defined as "up to 10% more than the average duration between samples within the range".
     * Essentially, we assume a more or less regular spacing between samples. If we don't see a sample where we would expect one,
     * we assume the series does not cover the whole range but starts and/or ends within the range.
     * We still extrapolate the rate in this case, but not all the way to the boundary, only by half of the average duration between
     * samples (which is our guess for where the series actually starts or ends).
     */
    private static double extrapolateRate(ReducedState state, long rangeStart, long rangeEnd) {
        final int len = state.timestamps.length;
        assert len >= 2 : "rate requires at least two samples; got " + len;
        final long firstTS = state.timestamps[state.timestamps.length - 1];
        final long lastTS = state.timestamps[0];
        double reset = state.resets;
        for (int i = 1; i < len; i++) {
            if (state.values[i - 1] < state.values[i]) {
                reset += state.values[i];
            }
        }
        double firstValue = state.values[len - 1];
        double lastValue = state.values[0] + reset;
        final double sampleTS = lastTS - firstTS;
        final double averageSampleInterval = sampleTS / state.samples;
        final double slope = (lastValue - firstValue) / sampleTS;
        double startGap = firstTS - rangeStart;
        if (startGap > 0) {
            if (startGap > averageSampleInterval * 1.1) {
                startGap = averageSampleInterval / 2.0;
            }
            firstValue = Math.max(0.0, firstValue - startGap * slope);
        }
        double endGap = rangeEnd - lastTS;
        if (endGap > 0) {
            if (endGap > averageSampleInterval * 1.1) {
                endGap = averageSampleInterval / 2.0;
            }
            lastValue = lastValue + endGap * slope;
        }
        return (lastValue - firstValue) * 1000.0 / (rangeEnd - rangeStart);
    }

    // TODO: copied from old rate - simplify this or explain why we need it?
    static double dv(double v0, double v1) {
        return v0 > v1 ? v1 : v1 - v0;
    }
}
