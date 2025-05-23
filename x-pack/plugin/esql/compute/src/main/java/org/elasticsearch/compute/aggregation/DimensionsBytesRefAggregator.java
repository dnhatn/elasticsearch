/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;

@Aggregator({ @IntermediateState(name = "dimensions", type = "BYTES_REF_BLOCK") })
@GroupingAggregator
class DimensionsBytesRefAggregator {
    // ungrouped
    public static SingleState initSingle(DriverContext driverContext, double percentile) {
        throw new UnsupportedOperationException("dimensions aggregation is not supported");
    }

    public static void combine(SingleState current, BytesRef v) {
        throw new UnsupportedOperationException("dimensions aggregation is not supported");
    }

    public static void combineIntermediate(SingleState state, BytesRef value) {
        throw new UnsupportedOperationException("dimensions aggregation is not supported");
    }

    public static void combineIntermediate(SingleState state, BytesRefBlock values) {
        throw new UnsupportedOperationException("dimensions aggregation is not supported");
    }

    public static Block evaluateFinal(SingleState state, DriverContext driverContext) {
        throw new UnsupportedOperationException("dimensions aggregation is not supported");
    }

    public static class SingleState implements AggregatorState {
        @Override
        public void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            throw new UnsupportedOperationException();
        }
    }

    // grouping
    public static GroupingState initGrouping(DriverContext driverContext) {
        return new GroupingState(driverContext);
    }

    public static GroupingAggregatorFunction.AddInput wrapAddInput(
        GroupingAggregatorFunction.AddInput delegate,
        GroupingState state,
        BytesRefBlock values
    ) {
        return DimensionsBytesRefAggregators.wrapAddInput(delegate, state, values);
    }

    public static GroupingAggregatorFunction.AddInput wrapAddInput(
        GroupingAggregatorFunction.AddInput delegate,
        GroupingState state,
        BytesRefVector values
    ) {
        return DimensionsBytesRefAggregators.wrapAddInput(delegate, state, values);
    }

    public static void combine(GroupingState state, int groupId, BytesRef v) {
        throw new UnsupportedOperationException("dimensions aggregation is not supported");
    }

    public static void combineIntermediate(GroupingState state, int groupId, BytesRefBlock values, int position) {
        state.add(groupId, values, position);
    }

    public static void combineStates(GroupingState current, int currentGroupId, GroupingState state, int statePosition) {
        throw new UnsupportedOperationException("dimensions aggregation is not supported");
    }

    public static Block evaluateFinal(GroupingState state, IntVector selected, DriverContext driverContext) {
        return state.evaluate(selected, driverContext);
    }

    public static final class GroupingState implements GroupingAggregatorState {
        final BytesRefBlock.Builder builder;
        int maxGroupId = -1;
        private final BytesRef scratch = new BytesRef();

        GroupingState(DriverContext driverContext) {
            this.builder = driverContext.blockFactory().newBytesRefBlockBuilder(1);
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
            blocks[offset] = evaluate(selected, driverContext);
        }

        BytesRefBlock evaluate(IntVector selected, DriverContext driverContext) {
            // TODO: selected?
            return builder.build();
        }

        @Override
        public void enableGroupIdTracking(SeenGroupIds seenGroupIds) {
            // append nulls
        }

        boolean advanceGroup(int groupId) {
            if (maxGroupId < groupId) {
                maxGroupId = groupId;
                return true;
            } else {
                return false;
            }
        }

        // TODO: support ordinals?
        void add(int groupId, BytesRefBlock values, int position) {
            if (advanceGroup(groupId)) {
                int valueCount = values.getValueCount(position);
                switch (valueCount) {
                    case 0 -> builder.appendNull();
                    case 1 -> builder.appendBytesRef(values.getBytesRef(values.getFirstValueIndex(position), scratch));
                    default -> {
                        builder.beginPositionEntry();
                        int valuesStart = values.getFirstValueIndex(position);
                        int valuesEnd = valuesStart + valueCount;
                        for (int v = valuesStart; v < valuesEnd; v++) {
                            builder.appendBytesRef(values.getBytesRef(v, scratch));
                        }
                        builder.endPositionEntry();
                    }
                }
            }
        }

        @Override
        public void close() {

        }
    }
}
