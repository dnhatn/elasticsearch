/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.compute.Describable;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.GroupingAggregatorEvaluationContext;
import org.elasticsearch.compute.aggregation.TimeSeriesGroupingAggregatorEvaluationContext;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.aggregation.blockhash.TimeSeriesBlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;

import java.util.List;
import java.util.function.Supplier;

import static java.util.stream.Collectors.joining;

/**
 * A specialized version of {@link HashAggregationOperator} that aggregates time-series aggregations from time-series sources.
 */
public class TimeSeriesAggregationOperator extends HashAggregationOperator {

    public record Factory(
        Rounding.Prepared timeBucket,
        boolean sortedInput,
        List<BlockHash.GroupSpec> groups,
        AggregatorMode aggregatorMode,
        List<GroupingAggregator.Factory> aggregators,
        int maxPageSize
    ) implements OperatorFactory {
        @Override
        public Operator get(DriverContext driverContext) {
            // TODO: use TimeSeriesBlockHash when possible
            return new TimeSeriesAggregationOperator(timeBucket, aggregators, () -> {
                if (sortedInput && groups.size() == 2) {
                    return new TimeSeriesBlockHash(groups.get(0).channel(), groups.get(1).channel(), driverContext.blockFactory());
                } else {
                    return BlockHash.build(
                        groups,
                        driverContext.blockFactory(),
                        maxPageSize,
                        true // we can enable optimizations as the inputs are vectors
                    );
                }
            }, driverContext, maxPageSize);
        }

        @Override
        public String describe() {
            return "TimeSeriesAggregationOperator[mode = "
                + "<not-needed>"
                + ", aggs = "
                + aggregators.stream().map(Describable::describe).collect(joining(", "))
                + "]";
        }
    }

    private final Rounding.Prepared timeBucket;
    private final int maxPageSize;
    private final List<GroupingAggregator.Factory> aggregatorFactories;

    private boolean emitted = false;

    public TimeSeriesAggregationOperator(
        Rounding.Prepared timeBucket,
        List<GroupingAggregator.Factory> aggregatorFactories,
        Supplier<BlockHash> blockHash,
        DriverContext driverContext,
        int maxPageSize
    ) {
        super(aggregatorFactories, blockHash, driverContext);
        this.timeBucket = timeBucket;
        this.maxPageSize = maxPageSize;
        this.aggregatorFactories = aggregatorFactories;
    }

    @Override
    public void addInput(Page page) {
        if (emitted) {
            emitted = false;
            for (int i = 0; i < aggregatorFactories.size(); i++) {
                var prev = aggregators.set(i, aggregatorFactories.get(i).apply(driverContext));
                prev.close();
            }
        }
        super.addInput(page);
    }

    @Override
    protected Page emitOutput() {
        if (emitted) {
            emitted = false;
            return null;
        }
        return super.emitOutput();
    }

    @Override
    public Page getOutput() {
        if (emitted) {
            return null;
        }
        Page output = super.getOutput();
        if (output != null) {
            return output;
        }
        if (blockHash instanceof TimeSeriesBlockHash tsBlockHash) {
            if (tsBlockHash.positionCount() >= 5) {
                Page page = emitOutput();
                emitted = true;
                return page;
            }
        }
        return null;
    }

    @Override
    protected GroupingAggregatorEvaluationContext evaluationContext(Block[] keys) {
        if (keys.length < 2) {
            return super.evaluationContext(keys);
        }
        final LongBlock timestamps = keys[0].elementType() == ElementType.LONG ? (LongBlock) keys[0] : (LongBlock) keys[1];
        return new TimeSeriesGroupingAggregatorEvaluationContext(driverContext) {
            @Override
            public long rangeStartInMillis(int groupId) {
                return timestamps.getLong(groupId);
            }

            @Override
            public long rangeEndInMillis(int groupId) {
                return timeBucket.nextRoundingValue(timestamps.getLong(groupId));
            }
        };
    }
}
