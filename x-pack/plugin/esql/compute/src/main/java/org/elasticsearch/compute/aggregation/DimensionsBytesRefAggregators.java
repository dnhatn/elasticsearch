/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.core.Releasables;

final class DimensionsBytesRefAggregators {
    static GroupingAggregatorFunction.AddInput wrapAddInput(
        GroupingAggregatorFunction.AddInput delegate,
        DimensionsBytesRefAggregator.GroupingState state,
        BytesRefBlock values
    ) {
        return new GroupingAggregatorFunction.AddInput() {
            @Override
            public void add(int positionOffset, IntArrayBlock groupIds) {
                throw new UnsupportedOperationException("expected vector groupIds");
            }

            @Override
            public void add(int positionOffset, IntBigArrayBlock groupIds) {
                throw new UnsupportedOperationException("expected vector groupIds");
            }

            @Override
            public void add(int positionOffset, IntVector groupIds) {
                for (int groupPosition = 0; groupPosition < groupIds.getPositionCount(); groupPosition++) {
                    final int groupId = groupIds.getInt(groupPosition);
                    state.add(groupId, values, groupPosition + positionOffset);
                }
            }

            @Override
            public void close() {
                Releasables.close(delegate);
            }
        };
    }

    static GroupingAggregatorFunction.AddInput wrapAddInput(
        GroupingAggregatorFunction.AddInput delegate,
        DimensionsBytesRefAggregator.GroupingState state,
        BytesRefVector values
    ) {
        return new GroupingAggregatorFunction.AddInput() {
            @Override
            public void add(int positionOffset, IntArrayBlock groupIds) {
                throw new UnsupportedOperationException("expected vector groupIds");
            }

            @Override
            public void add(int positionOffset, IntBigArrayBlock groupIds) {
                throw new UnsupportedOperationException("expected vector groupIds");
            }

            @Override
            public void add(int positionOffset, IntVector groupIds) {
                BytesRef scratch = new BytesRef();
                BytesRefBlock.Builder builder = state.builder;
                for (int groupPosition = 0; groupPosition < groupIds.getPositionCount(); groupPosition++) {
                    final int groupId = groupIds.getInt(groupPosition);
                    if (state.advanceGroup(groupId)) {
                        builder.appendBytesRef(values.getBytesRef(groupPosition + positionOffset, scratch));
                    }
                }
            }

            @Override
            public void close() {
                Releasables.close(delegate);
            }
        };
    }
}
