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
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
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
            return new TimeSeriesAggregationOperator(aggregatorMode, timeBucket, aggregators, () -> {
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
            }, driverContext);
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

    private final AggregatorMode aggregatorMode;
    private final Rounding.Prepared timeBucket;
    private final Queue<Page> passThroughPages = new ArrayDeque<>();

    public TimeSeriesAggregationOperator(
        AggregatorMode aggregatorMode,
        Rounding.Prepared timeBucket,
        List<GroupingAggregator.Factory> aggregators,
        Supplier<BlockHash> blockHash,
        DriverContext driverContext
    ) {
        super(aggregators, blockHash, driverContext);
        this.aggregatorMode = aggregatorMode;
        this.timeBucket = timeBucket;
    }

    @Override
    public void addInput(Page page) {
        // accumulate the input page
        if (aggregatorMode.isInputPartial()) {
            passThroughPages.add(page);
        } else {
            super.addInput(page);
        }
    }

    @Override
    public void finish() {
        if (aggregatorMode.isOutputPartial()) {
            forceEmitFinal();
        } else {
            super.finish();
        }
    }

    private void forceEmitFinal() {
        if (finished) {
            return;
        }
        finished = true;
        Block[] blocks = null;
        IntVector selected = null;
        boolean success = false;
        try {
            selected = blockHash.nonEmpty();
            Block[] keys = blockHash.getKeys();
            int[] aggBlockCounts = aggregators.stream().mapToInt(n -> 1).toArray();
            blocks = new Block[keys.length + Arrays.stream(aggBlockCounts).sum()];
            System.arraycopy(keys, 0, blocks, 0, keys.length);
            int offset = keys.length;
            var evaluationContext = evaluationContext(keys);
            for (int i = 0; i < aggregators.size(); i++) {
                var aggregator = aggregators.get(i);
                aggregator.evaluateFinal(blocks, offset, selected, evaluationContext);
                offset += aggBlockCounts[i];
            }
            passThroughPages.add(new Page(blocks));
            success = true;
        } finally {
            // selected should always be closed
            if (selected != null) {
                selected.close();
            }
            if (success == false && blocks != null) {
                Releasables.closeExpectNoException(blocks);
            }
        }
    }

    @Override
    public boolean isFinished() {
        return super.isFinished() && passThroughPages.isEmpty();
    }

    @Override
    public void close() {
        Page p;
        while ((p = passThroughPages.poll()) != null) {
            p.releaseBlocks();
        }
        super.close();
    }

    @Override
    public Page getOutput() {
        Page p = super.getOutput();
        if (p != null) {
            return p;
        }
        return passThroughPages.poll();
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
