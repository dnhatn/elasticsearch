/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.compute.aggregation;

// begin generated imports

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;

import java.util.List;
// end generated imports

public final class DimensionValuesByteRefGroupingAggregatorFunction implements GroupingAggregatorFunction {

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
        public DimensionValuesByteRefGroupingAggregatorFunction groupingAggregator(DriverContext driverContext, List<Integer> channels) {
            return new DimensionValuesByteRefGroupingAggregatorFunction(channels, driverContext);
        }

        @Override
        public String describe() {
            return "rate of double";
        }
    }

    static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(new IntermediateStateDesc("values", ElementType.BYTES_REF));

    private final BytesRefBlock.Builder builder;
    private final int channel;
    private final DriverContext driverContext;
    private int maxGroupId = -1;

    public DimensionValuesByteRefGroupingAggregatorFunction(List<Integer> channels, DriverContext driverContext) {
        this.channel = channels.getFirst();
        this.driverContext = driverContext;
        this.builder = driverContext.blockFactory().newBytesRefBlockBuilder(4096);
    }

    @Override
    public void selectedMayContainUnseenGroups(SeenGroupIds seenGroupIds) {
        // manage nulls
    }

    @Override
    public AddInput prepareProcessRawInputPage(SeenGroupIds seenGroupIds, Page page) {
        BytesRefBlock valuesBlock = page.getBlock(0);
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
        return new AddInput() {
            @Override
            public void add(int positionOffset, IntArrayBlock groupIds) {
                addInputValuesBlock(positionOffset, groupIds, valuesBlock);
            }

            @Override
            public void add(int positionOffset, IntBigArrayBlock groupIds) {
                addInputValuesBlock(positionOffset, groupIds, valuesBlock);
            }

            @Override
            public void add(int positionOffset, IntVector groupIds) {
                var valuesVector = valuesBlock.asVector();
                if (valuesVector != null) {
                    addInputValuesVector(positionOffset, groupIds, valuesVector);
                } else {
                    addInputValuesBlock(positionOffset, groupIds, valuesBlock);
                }
            }

            @Override
            public void close() {

            }
        };
    }

    // Note that this path can be executed randomly in tests, not in production
    private void addInputValuesBlock(int positionOffset, IntBlock groups, BytesRefBlock valueBlock) {
        var scratch = new BytesRef();
        int positionCount = groups.getPositionCount();
        for (int p = 0; p < positionCount; p++) {
            if (groups.isNull(p)) {
                continue;
            }
            int valuePosition = p + positionOffset;
            int groupStart = groups.getFirstValueIndex(p);
            int groupEnd = groupStart + groups.getValueCount(p);
            for (int g = groupStart; g < groupEnd; g++) {
                final int groupId = groups.getInt(g);
                if (maxGroupId < groupId) {
                    for (int i = maxGroupId; i < groupId; i++) {
                        builder.appendNull();
                    }
                    builder.copyFrom(valueBlock, valuePosition, scratch);
                    maxGroupId = groupId;
                }
            }
        }
    }

    private void addInputValuesBlock(int positionOffset, IntVector groups, BytesRefBlock valueBlock) {
        var scratch = new BytesRef();
        int positionCount = groups.getPositionCount();
        if (groups.isConstant()) {
            int groupId = groups.getInt(0);
            if (groupId > maxGroupId) {
                for (int i = maxGroupId; i < groupId; i++) {
                    builder.appendNull();
                }
                builder.copyFrom(valueBlock, positionOffset, scratch);
                maxGroupId = groupId;
            }
        } else {
            for (int p = 0; p < positionCount; p++) {
                int groupId = groups.getInt(p);
                if (groupId > maxGroupId) {
                    for (int i = maxGroupId; i < groupId; i++) {
                        builder.appendNull();
                    }
                    builder.copyFrom(valueBlock, positionOffset + p, scratch);
                    maxGroupId = groupId;
                }
            }
        }
    }

    private void addInputValuesVector(int positionOffset, IntVector groups, BytesRefVector valueVector) {
        var scratch = new BytesRef();
        int positionCount = groups.getPositionCount();
        if (groups.isConstant()) {
            int groupId = groups.getInt(0);
            if (groupId > maxGroupId) {
                for (int i = maxGroupId; i < groupId; i++) {
                    builder.appendNull();
                }
                builder.appendBytesRef(valueVector.getBytesRef(positionOffset, scratch));
                maxGroupId = groupId;
            }
        } else {
            for (int p = 0; p < positionCount; p++) {
                int groupId = groups.getInt(p);
                if (groupId > maxGroupId) {
                    for (int i = maxGroupId; i < groupId; i++) {
                        builder.appendNull();
                    }
                    builder.appendBytesRef(valueVector.getBytesRef(positionOffset + p, scratch));
                    maxGroupId = groupId;
                }
            }
        }
    }

    @Override
    public int intermediateBlockCount() {
        return INTERMEDIATE_STATE_DESC.size();
    }

    @Override
    public void addIntermediateInput(int positionOffset, IntArrayBlock groups, Page page) {
        BytesRefBlock valuesBlock = page.getBlock(channel);
        if (valuesBlock.areAllValuesNull()) {
            return;
        }
        addInputValuesBlock(positionOffset, groups, valuesBlock);
    }

    @Override
    public void addIntermediateInput(int positionOffset, IntBigArrayBlock groups, Page page) {
        BytesRefBlock valuesBlock = page.getBlock(channel);
        if (valuesBlock.areAllValuesNull()) {
            return;
        }
        addInputValuesBlock(positionOffset, groups, valuesBlock);
    }

    @Override
    public void addIntermediateInput(int positionOffset, IntVector groups, Page page) {
        BytesRefBlock valuesBlock = page.getBlock(channel);
        if (valuesBlock.areAllValuesNull()) {
            return;
        }
        var valuesVector = valuesBlock.asVector();
        if (valuesVector != null) {
            addInputValuesVector(positionOffset, groups, valuesVector);
        } else {
            addInputValuesBlock(positionOffset, groups, valuesBlock);
        }
    }

    @Override
    public void evaluateIntermediate(Block[] blocks, int offset, IntVector selected) {
        int positionCount = selected.getPositionCount();
        if (positionCount == 0) {
            blocks[offset] = driverContext.blockFactory().newConstantNullBlock(positionCount);
            return;
        }
        int lastGroup = selected.getInt(positionCount - 1);
        if (selected.getInt(0) == 0 && lastGroup > maxGroupId) {
            for (int i = maxGroupId + 1; i <= lastGroup; i++) {
                builder.appendNull();
            }
            blocks[offset] = builder.build();
            return;
        }
        int estimateBytesSize = Math.toIntExact(Math.min(builder.estimatedBytes(), ByteSizeValue.ofMb(100).getBytes()));
        try (var block = builder.build(); var outputBuilder = driverContext.blockFactory().newBytesRefBlockBuilder(estimateBytesSize)) {
            for (int i = 0; i < positionCount; i++) {
                int groupId = selected.getInt(i);
                if (groupId <= maxGroupId) {
                    builder.copyFrom(block, groupId, new BytesRef());
                } else {
                    builder.appendNull();
                }
            }
            blocks[offset] = outputBuilder.build();
        }
    }

    @Override
    public void close() {
        builder.close();
    }

    @Override
    public void evaluateFinal(Block[] blocks, int offset, IntVector selected, GroupingAggregatorEvaluationContext evalContext) {
        evaluateIntermediate(blocks, offset, selected);
    }
}
