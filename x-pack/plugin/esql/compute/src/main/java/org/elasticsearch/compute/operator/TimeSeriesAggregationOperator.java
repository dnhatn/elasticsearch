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
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;

import java.time.Instant;
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
        boolean sortedInput,
        Rounding.Prepared timeBucket,
        Instant timeSeriesStartTime,
        Instant timeSeriesEndTime,
        List<BlockHash.GroupSpec> groups,
        AggregatorMode aggregatorMode,
        List<GroupingAggregator.Factory> aggregators,
        int maxPageSize
    ) implements OperatorFactory {
        @Override
        public Operator get(DriverContext driverContext) {
            return new TimeSeriesAggregationOperator(
                aggregatorMode,
                timeBucket,
                timeSeriesStartTime,
                timeSeriesEndTime,
                aggregators,
                () -> {
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
                },
                driverContext
            );
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
    private final Instant timeSeriesStartTime;
    private final Instant timeSeriesEndTime;
    private final Queue<Page> passThroughPages;

    public TimeSeriesAggregationOperator(
        AggregatorMode aggregatorMode,
        Rounding.Prepared timeBucket,
        Instant timeSeriesStartTime,
        Instant timeSeriesEndTime,
        List<GroupingAggregator.Factory> aggregators,
        Supplier<BlockHash> blockHash,
        DriverContext driverContext
    ) {
        super(aggregators, blockHash, driverContext);
        this.aggregatorMode = aggregatorMode;
        this.timeBucket = timeBucket;
        this.timeSeriesStartTime = timeSeriesStartTime;
        this.timeSeriesEndTime = timeSeriesEndTime;
        this.passThroughPages = new ArrayDeque<>();
    }

    @Override
    public boolean isFinished() {
        return super.isFinished() && passThroughPages.isEmpty();
    }

    @Override
    public void close() {
        Releasables.close(super::close, Releasables.wrap(passThroughPages));
    }

    static final int MULTIPLEXING_FINAL_BLOCK = 0;
    static final int MULTIPLEXING_PARTIAL_BLOCK = 1;

    @Override
    public void addInput(Page page) {
        // raw input, just delegate
        if (aggregatorMode.isInputPartial() == false) {
            super.addInput(page);
            return;
        }
        // for the partial input, a page can be `final` or partial depending on the last the block which is the multiplex block
        final IntBlock multiplexingBlock = page.getBlock(page.getBlockCount() - 1);
        final int multiplexingValue = multiplexingBlock.getInt(0);
        if (multiplexingValue == MULTIPLEXING_FINAL_BLOCK) {
            passThroughPages.add(page);
        } else if (multiplexingValue == MULTIPLEXING_PARTIAL_BLOCK) {
            super.addInput(page);
        } else {
            throw new IllegalStateException("unexpected multiplexing value [" + multiplexingValue + "]");
        }
    }

    @Override
    public Page getOutput() {
        Page p = passThroughPages.poll();
        if (p != null) {
            return p;
        }
        return super.getOutput();
    }

    @Override
    public void finish() {
        if (finished) {
            return;
        }
        // final output, just delegate
        if (aggregatorMode.isOutputPartial() == false) {
            super.finish();
            return;
        }
        finished = true;
        if (timeSeriesStartTime != null && timeSeriesEndTime != null && blockHash instanceof TimeSeriesBlockHash ts) {
            long startNanos = System.nanoTime();
            final long bucketStartTime = timeBucket.round(timeSeriesStartTime.toEpochMilli());
            final long bucketEndTime = timeBucket.round(timeSeriesEndTime.toEpochMilli());
            var finalKeys = ts.finalKeys(bucketStartTime, bucketEndTime);
            if (finalKeys.positionCount() == 0) {
                Releasables.close(finalKeys.tsids(), finalKeys.timeBuckets(), finalKeys.selected());
            } else {
                passThroughPages.add(emitSubResults(finalKeys.selected(), finalKeys.tsids(), finalKeys.timeBuckets(), false));
            }
            if (finalKeys.positionCount() < ts.positionCount()) {
                var partialKeys = ts.partialKeys(bucketStartTime, bucketEndTime);
                assert partialKeys.positionCount() + finalKeys.positionCount() == ts.positionCount()
                    : partialKeys.positionCount() + " + " + finalKeys.positionCount() + " != " + ts.positionCount();
                passThroughPages.add(emitSubResults(partialKeys.selected(), partialKeys.tsids(), partialKeys.timeBuckets(), true));
            }
            outputNanos += System.nanoTime() - startNanos;
        } else {
            Page page = super.emitOutput();
            try {
                var block = driverContext.blockFactory().newConstantIntBlockWith(MULTIPLEXING_PARTIAL_BLOCK, page.getPositionCount());
                var newPage = page.appendBlock(block);
                page = null;
                passThroughPages.add(newPage);
            } finally {
                Releasables.close(page);
            }
        }
    }

    private Page emitSubResults(IntVector selected, BytesRefBlock tsids, LongBlock timeBuckets, boolean partialOutput) {
        Block[] blocks = null;
        boolean success = false;
        try (selected) {
            int[] aggBlockCounts = aggregators.stream().mapToInt(f -> f.evaluateBlockCount(partialOutput)).toArray();
            blocks = new Block[2 + Arrays.stream(aggBlockCounts).sum() + 1];
            blocks[0] = tsids;
            blocks[1] = timeBuckets;
            int offset = 2;
            for (int i = 0; i < aggregators.size(); i++) {
                var aggregator = aggregators.get(i);
                if (partialOutput) {
                    aggregator.evaluateIntermediate(blocks, offset, selected);
                } else {
                    var evaluationContext = evaluationContext(new Block[] { tsids, timeBuckets });
                    aggregator.evaluateFinal(blocks, offset, selected, evaluationContext);
                }
                offset += aggBlockCounts[i];
            }
            blocks[offset] = driverContext.blockFactory()
                .newConstantIntBlockWith(
                    partialOutput ? MULTIPLEXING_PARTIAL_BLOCK : MULTIPLEXING_FINAL_BLOCK,
                    selected.getPositionCount()
                );
            Page page = new Page(blocks);
            success = true;
            return page;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(blocks);
            }
        }
    }

    @Override
    protected GroupingAggregatorEvaluationContext evaluationContext(Block[] keys) {
        return new TimeSeriesGroupingAggregatorEvaluationContext(driverContext) {
            @Override
            public long rangeStartInMillis(long timestamp) {
                return timeBucket.round(timestamp);
            }

            @Override
            public long rangeEndInMillis(long timestamp) {
                return timeBucket.nextRoundingValue(timestamp);
            }
        };
    }

}
