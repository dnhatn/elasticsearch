/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.postoptimizer;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.fieldcaps.TimeSeriesStats;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.lucene.query.TsidPrefixRange;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.SerializationTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
import org.elasticsearch.xpack.esql.index.EsIndexGenerator;
import org.elasticsearch.xpack.esql.io.stream.ExpressionQueryBuilder;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamWrapperQueryBuilder;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.LogicalPostOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.plan.PlanId;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesSplit;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.MergeExec;
import org.elasticsearch.xpack.esql.plan.physical.TimeSeriesSplitExec;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.planner.SubPlan;
import org.elasticsearch.xpack.esql.planner.mapper.Mapper;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.querydsl.query.SingleValueQuery;
import org.elasticsearch.xpack.esql.querydsl.query.TsidPrefixRangeQueryBuilder;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.session.Versioned;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.ConfigurationTestUtils.randomConfiguration;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.optimizer.rules.logical.postoptimizer.SplitTimeSeriesByTsid.ESTIMATED_BYTES_PER_GROUP;
import static org.hamcrest.Matchers.equalTo;

public class SplitTimeSeriesByTsidTests extends ESTestCase {

    public void testNumSplitsFitsGroupsIntoMemoryPerSplit() {
        long memory = 1000 * ESTIMATED_BYTES_PER_GROUP;
        assertThat(SplitTimeSeriesByTsid.numSplits(0, memory, 256), equalTo(0));
        assertThat(SplitTimeSeriesByTsid.numSplits(1000, memory, 256), equalTo(1));
        assertThat(SplitTimeSeriesByTsid.numSplits(1001, memory, 256), equalTo(2));
        assertThat(SplitTimeSeriesByTsid.numSplits(10_000, memory, 4), equalTo(4));
        assertThat(SplitTimeSeriesByTsid.numSplits(Long.MAX_VALUE, memory, 256), equalTo(256));
    }

    public void testEstimatedGroupsFromIndexStats() {
        long span = 1000;
        var stats = new TimeSeriesStats(10 * span, 0, span - 1);
        assertThat(SplitTimeSeriesByTsid.estimatedGroups(List.of(stats, stats), null), equalTo(20L));
        Bucket bucket = new Bucket(
            Source.EMPTY,
            Literal.dateTime(Source.EMPTY, Instant.EPOCH),
            Literal.timeDuration(Source.EMPTY, Duration.ofMillis(100)),
            null,
            null,
            null,
            TEST_CFG
        );
        // 10 series, each covering 10 buckets of 100ms, plus one for the bucket each starts in
        assertThat(SplitTimeSeriesByTsid.estimatedGroups(List.of(stats), bucket), equalTo(110L));
    }

    public void testSplitWrapsTheAggregateOnce() {
        LogicalPlan plan = rateQuery();
        TimeSeriesAggregate aggregate = SplitTimeSeriesByTsid.splittable(plan);
        assertNotNull(aggregate);
        List<TsidPrefixRange> ranges = List.of(new TsidPrefixRange(8, 0, 128), new TsidPrefixRange(8, 128, 256));

        LogicalPlan split = SplitTimeSeriesByTsid.split(plan, aggregate, ranges);

        assertEquals(plan.output(), split.output());
        List<TimeSeriesSplit> splits = split.collect(TimeSeriesSplit.class);
        assertEquals(1, splits.size());
        assertEquals(ranges, splits.getFirst().ranges());
        assertEquals(1, splits.getFirst().collect(TimeSeriesAggregate.class).size());
        assertEquals(1, split.collect(TimeSeriesAggregate.class).size());
    }

    public void testExpandForksTheBranchPerRange() {
        TransportVersion version = TransportVersion.current();
        LogicalPlan plan = rateQuery();
        List<TsidPrefixRange> ranges = List.of(new TsidPrefixRange(8, 0, 128), new TsidPrefixRange(8, 128, 256));
        LogicalPlan split = SplitTimeSeriesByTsid.split(plan, SplitTimeSeriesByTsid.splittable(plan), ranges);
        split.setOptimized();
        var physical = new PhysicalPlanOptimizer(new PhysicalOptimizerContext(TEST_CFG, version)).optimize(
            new Mapper().map(new Versioned<>(split, version))
        );
        var requestFilter = QueryBuilders.termQuery("cluster", "prod");
        physical = PlannerUtils.integrateEsFilterIntoFragment(physical, requestFilter, version);

        assertEquals(plan.output(), physical.output());
        assertTrue(physical.collect(TimeSeriesSplitExec.class).isEmpty());
        List<MergeExec> merges = physical.collect(MergeExec.class);
        assertEquals(1, merges.size());
        MergeExec merge = merges.getFirst();
        assertEquals(MergeExec.Kind.FORK, merge.kind());
        assertEquals(ranges.size(), merge.children().size());
        List<ExchangeExec> exchanges = merge.collect(ExchangeExec.class);
        assertEquals(ranges.size(), exchanges.size());
        assertNotEquals(exchanges.get(0).exchange(), exchanges.get(1).exchange());
        for (int i = 0; i < ranges.size(); i++) {
            FragmentExec fragment = asInstanceOf(FragmentExec.class, exchanges.get(i).child());
            BoolQueryBuilder filter = asInstanceOf(BoolQueryBuilder.class, fragment.esFilter());
            assertEquals(List.of(requestFilter), filter.must());
            assertEquals(List.of(new TsidPrefixRangeQueryBuilder(ranges.get(i))), filter.filter());
        }
        Set<PlanId> first = new HashSet<>();
        merge.children().get(0).forEachDown(p -> first.add(p.planId()));
        merge.children().get(1).forEachDown(p -> assertFalse(first.contains(p.planId())));
        SubPlan.Merge execution = asInstanceOf(SubPlan.Merge.class, PlannerUtils.buildSubPlan(physical));
        assertEquals(ranges.size(), execution.children().size());
    }

    public void testSplittingIsOffByDefault() {
        LogicalPlan plan = rateQuery();
        var context = new LogicalPostOptimizerContext(TEST_CFG, null, new EsqlFlags(true), TransportVersion.current(), null, null);
        var rule = new SplitTimeSeriesByTsid(context);
        rule.apply(plan, ActionListener.wrap(result -> assertSame(plan, result), e -> fail(e.getMessage())));
    }

    public void testHistogramFiltersAreTheQueryFilters() {
        var metric = QueryBuilders.existsQuery("network.total_bytes_in");
        assertEquals(1, histogramFilters(rateQuery(), null).size());
        assertEquals(2, histogramFilters(rateQuery("WHERE cluster == \"prod\""), null).size());
        assertEquals(2, histogramFilters(rateQuery("WHERE cluster LIKE \"*-prod\" AND network.eth0.tx > 0"), null).size());
        assertNull(histogramFilters(rateQuery("WHERE LENGTH(cluster) == 4"), null));

        var term = QueryBuilders.termQuery("cluster", "prod");
        List<QueryBuilder> withRequestFilter = histogramFilters(rateQuery(), term);
        assertEquals(2, withRequestFilter.size());
        assertEquals(term, withRequestFilter.get(1));
        assertEquals(3, histogramFilters(rateQuery("WHERE pod == \"one\""), term).size());
        assertThat(histogramFilters(rateQuery(), null).getFirst().getName(), equalTo(metric.getName()));
    }

    public void testHistogramFiltersHaveNoSingleValueChecks() {
        for (String where : List.of("WHERE cluster == \"prod\"", "WHERE NOT (cluster == \"a\" OR pod == \"b\")")) {
            for (QueryBuilder filter : histogramFilters(rateQuery(where), null)) {
                assertFalse(Strings.toString(filter), Strings.toString(filter).contains(SingleValueQuery.ENTRY.name));
            }
        }
    }

    public void testHistogramFiltersCarryExpressionQueriesInPlanStreams() throws IOException {
        String where = "WHERE cluster LIKE (\"prod*\", \"qa*\")";
        List<QueryBuilder> filters = histogramFilters(rateQuery(where), randomConfiguration(rateQueryText(where), Map.of()), null);
        assertEquals(2, filters.size());
        var filter = asInstanceOf(PlanStreamWrapperQueryBuilder.class, filters.get(1));
        assertEquals("cluster", asInstanceOf(ExpressionQueryBuilder.class, filter.next()).fieldName());
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeNamedWriteable(filter);
            try (
                StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), SerializationTestUtils.writableRegistry())
            ) {
                var read = asInstanceOf(PlanStreamWrapperQueryBuilder.class, in.readNamedWriteable(QueryBuilder.class));
                var expression = asInstanceOf(ExpressionQueryBuilder.class, read.next());
                assertEquals("cluster", expression.fieldName());
                assertEquals("cluster LIKE (\"prod*\", \"qa*\")", expression.getExpression().toString());
            }
        }
    }

    private static List<QueryBuilder> histogramFilters(LogicalPlan plan, QueryBuilder requestFilter) {
        return histogramFilters(plan, TEST_CFG, requestFilter);
    }

    private static List<QueryBuilder> histogramFilters(LogicalPlan plan, Configuration configuration, QueryBuilder requestFilter) {
        return SplitTimeSeriesByTsid.histogramFilters(
            aggregate(plan),
            configuration,
            requestFilter,
            LucenePushdownPredicates.forCanMatch(TransportVersion.current(), new EsqlFlags(true))
        );
    }

    private static TimeSeriesAggregate aggregate(LogicalPlan plan) {
        List<TimeSeriesAggregate> aggregates = plan.collect(TimeSeriesAggregate.class);
        assertEquals(1, aggregates.size());
        return aggregates.getFirst();
    }

    private static LogicalPlan rateQuery() {
        return rateQuery("");
    }

    private static String rateQueryText(String where) {
        return "TS k8s "
            + (where.isEmpty() ? "" : "| " + where + " ")
            + "| STATS rate = SUM(RATE(network.total_bytes_in)) BY cluster, bucket = TBUCKET(1 hour)";
    }

    private static LogicalPlan rateQuery(String where) {
        TransportVersion version = TransportVersion.current();
        var index = EsIndexGenerator.esIndex(
            "k8s",
            EsqlTestUtils.loadMapping("k8s-mappings.json"),
            Map.of("k8s-1", IndexMode.TIME_SERIES, "k8s-2", IndexMode.TIME_SERIES)
        );
        var analyzer = EsqlTestUtils.analyzer().addIndex(index).minimumTransportVersion(version);
        var optimizer = new LogicalPlanOptimizer(EsqlTestUtils.unboundLogicalOptimizerContext(version));
        return optimizer.optimize(analyzer.query(rateQueryText(where)));
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }
}
