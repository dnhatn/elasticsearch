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
 * A specialized implementation of {@link HashAggregationOperator} for aggregating time-series data.
 * This operator optimizes aggregation by leveraging the non-overlapping nature of time-series indices. It emits:
 * <ul>
 *   <li>Final aggregation results for buckets that do not overlap with other time-series indices.</li>
 *   <li>Partial aggregation results for boundary buckets that may overlap with other time-series indices.</li>
 * </ul>
 * Final results are emitted without the `tsid`, reducing data transfer between nodes and avoiding redundant work
 * on the coordinator by bypassing the final aggregation operator.
 * Note: There is a potential issue where the layout may not perfectly align with page blocks.
 */
public class TimeSeriesAggregationOperator extends HashAggregationOperator {
    static final int MULTIPLEXING_FINAL_OUTPUT = 0;
    static final int MULTIPLEXING_PARTIAL_OUTPUT = 1;

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
        if (multiplexingValue == MULTIPLEXING_FINAL_OUTPUT) {
            passThroughPages.add(page);
        } else if (multiplexingValue == MULTIPLEXING_PARTIAL_OUTPUT) {
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
    protected Page emitOutputPage() {
        // final output, just delegate
        if (aggregatorMode.isOutputPartial() == false) {
            return super.emitOutputPage();
        }
        if (timeBucket != null && timeSeriesStartTime != null && timeSeriesEndTime != null && blockHash instanceof TimeSeriesBlockHash ts) {
            final long bucketStartTime = timeBucket.round(timeSeriesStartTime.toEpochMilli());
            final long bucketEndTime = timeBucket.round(timeSeriesEndTime.toEpochMilli());
            var partitions = ts.partitionBuckets(bucketStartTime, bucketEndTime);
            var finalKeys = partitions.v1();
            var partialKeys = partitions.v2();
            if (finalKeys.positionCount() > 0) {
                passThroughPages.add(emitSubResult(finalKeys.selected(), finalKeys.tsids(), finalKeys.timeBuckets(), false));
            } else {
                Releasables.close(finalKeys);
            }
            if (partialKeys.positionCount() > 0) {
                passThroughPages.add(emitSubResult(partialKeys.selected(), partialKeys.tsids(), partialKeys.timeBuckets(), true));
            } else {
                Releasables.close(partialKeys);
            }
            return null;
        } else {
            Page page = super.emitOutputPage();
            if (page == null) {
                return null;
            }
            try {
                var block = driverContext.blockFactory().newConstantIntBlockWith(MULTIPLEXING_PARTIAL_OUTPUT, page.getPositionCount());
                var newPage = page.appendBlock(block);
                page = null;
                return newPage;
            } finally {
                Releasables.close(page);
            }
        }
    }

    private Page emitSubResult(IntVector selected, BytesRefBlock tsids, LongBlock timeBuckets, boolean partialOutput) {
        Block[] blocks = null;
        boolean success = false;
        try (selected) {
            int[] aggBlockCounts = aggregators.stream().mapToInt(f -> f.evaluateBlockCount(partialOutput)).toArray();
            blocks = new Block[2 + Arrays.stream(aggBlockCounts).sum() + 1];
            blocks[0] = tsids;
            blocks[1] = timeBuckets;
            int offset = 2;
            Block[] keys = { tsids, timeBuckets };
            for (int i = 0; i < aggregators.size(); i++) {
                var aggregator = aggregators.get(i);
                if (partialOutput) {
                    aggregator.evaluateIntermediate(blocks, offset, selected);
                } else {
                    var evaluationContext = evaluationContext(keys);
                    aggregator.evaluateFinal(blocks, offset, selected, evaluationContext);
                }
                offset += aggBlockCounts[i];
            }
            blocks[offset] = driverContext.blockFactory()
                .newConstantIntBlockWith(
                    partialOutput ? MULTIPLEXING_PARTIAL_OUTPUT : MULTIPLEXING_FINAL_OUTPUT,
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
        if (keys.length < 2) {
            return super.evaluationContext(keys);
        }
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
