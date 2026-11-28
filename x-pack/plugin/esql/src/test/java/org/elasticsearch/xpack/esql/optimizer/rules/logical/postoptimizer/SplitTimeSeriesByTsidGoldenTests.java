/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.postoptimizer;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.compute.lucene.query.TsidPrefixRange;
import org.elasticsearch.xpack.esql.optimizer.GoldenTestCase;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.querydsl.query.TsidPrefixRangeQueryBuilder;

import java.util.EnumSet;
import java.util.List;

/**
 * A {@code rate} query split into two {@code _tsid} ranges: the logical plan carries one {@code TimeSeriesSplit}, and the physical
 * plan expands it into a fork whose branches each read one range through the fragment filter, with the outer aggregation once
 * above the fork. The golden indices carry no created version, so the split is applied to the aggregate directly rather than
 * through the histogram.
 */
public class SplitTimeSeriesByTsidGoldenTests extends GoldenTestCase {

    @ParametersFactory(argumentFormatting = "%1$s")
    public static Iterable<Object[]> parameters() {
        return goldenModes();
    }

    public SplitTimeSeriesByTsidGoldenTests(@Name("mode") String mode) {
        super(mode);
    }

    public void testRateSplitByTsidRange() {
        builder("""
            TS k8s
            | STATS rate = SUM(RATE(network.total_bytes_in)) BY cluster, bucket = TBUCKET(1 hour)
            """).optimizer(SplittingOptimizer::new)
            .stages(EnumSet.of(Stage.LOGICAL_OPTIMIZATION, Stage.PHYSICAL_OPTIMIZATION, Stage.LOCAL_PHYSICAL_OPTIMIZATION))
            .since(TsidPrefixRangeQueryBuilder.TSID_PREFIX_RANGE_QUERY)
            .run();
    }

    private static class SplittingOptimizer extends LogicalPlanOptimizer {
        SplittingOptimizer(LogicalOptimizerContext context) {
            super(context);
        }

        @Override
        public LogicalPlan optimize(LogicalPlan verified) {
            LogicalPlan optimized = super.optimize(verified);
            List<TimeSeriesAggregate> aggregates = optimized.collect(TimeSeriesAggregate.class);
            assertEquals(1, aggregates.size());
            List<TsidPrefixRange> ranges = List.of(new TsidPrefixRange(8, 0, 128), new TsidPrefixRange(8, 128, 256));
            LogicalPlan split = SplitTimeSeriesByTsid.split(optimized, aggregates.getFirst(), ranges);
            split.setOptimized();
            return split;
        }
    }
}
