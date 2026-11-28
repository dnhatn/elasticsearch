/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.postoptimizer;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.fieldcaps.TimeSeriesStats;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.compute.lucene.query.TsidPrefixRange;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.codec.tsdb.PartitionedDocValues;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.capabilities.TranslationAware;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.aggregate.TimeSeriesAggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamWrapperQueryBuilder;
import org.elasticsearch.xpack.esql.optimizer.LogicalPostOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.local.IgnoreNullMetrics;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MergePlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesSplit;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.querydsl.query.SingleValueQuery;
import org.elasticsearch.xpack.esql.querydsl.query.TsidPrefixRangeQueryBuilder;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.timeseries.TsidPrefixHistogram;
import org.elasticsearch.xpack.esql.timeseries.TsidPrefixHistogramCollector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.esql.planner.TranslatorHandler.TRANSLATOR_HANDLER;

/**
 * Splits a time-series aggregation into {@code _tsid} ranges when its groups would not fit in the coordinator memory of one
 * branch: the field caps {@link TimeSeriesStats} give a cheap upper bound, and only queries above it collect a
 * {@link TsidPrefixHistogram} to size and cut the ranges. The aggregate then runs once per range under a
 * {@link TimeSeriesSplit}, and the outer aggregation runs once over all of them.
 */
public final class SplitTimeSeriesByTsid implements LogicalPlanPostOptimizerRule {
    // TODO: accurate estimate
    static final long ESTIMATED_BYTES_PER_GROUP = 135;
    private static final long NO_BUCKET = 0;

    private final LogicalPostOptimizerContext context;

    public SplitTimeSeriesByTsid(LogicalPostOptimizerContext context) {
        this.context = context;
    }

    @Override
    public void apply(LogicalPlan plan, ActionListener<LogicalPlan> listener) {
        if (context.minimumVersion().supports(TsidPrefixRangeQueryBuilder.TSID_PREFIX_RANGE_QUERY) == false) {
            listener.onResponse(plan);
            return;
        }
        QueryPragmas pragmas = context.configuration().pragmas();
        int maxConcurrentBranches = pragmas.branchParallelDegree();
        int maxSplits = Math.min(
            pragmas.timeSeriesMaxSplits(),
            Math.min(
                pragmas.maxBranchCount(context.flags().maxBranchCount()),
                pragmas.maxBranchCountPerMerge(context.flags().maxBranchCountPerMerge())
            )
        );
        if (canReducePeakMemory(maxSplits, maxConcurrentBranches) == false) {
            listener.onResponse(plan);
            return;
        }
        TimeSeriesAggregate aggregate = splittable(plan);
        if (aggregate == null) {
            listener.onResponse(plan);
            return;
        }
        List<QueryBuilder> histogramFilters = histogramFilters(
            aggregate,
            context.configuration(),
            context.requestFilter(),
            LucenePushdownPredicates.forCanMatch(context.minimumVersion(), context.flags())
        );
        if (histogramFilters == null) {
            listener.onResponse(plan);
            return;
        }
        EsRelation relation = aggregate.collect(EsRelation.class).getFirst();
        List<TimeSeriesStats> stats = stats(relation.indexProperties().values());
        if (stats == null) {
            listener.onResponse(plan);
            return;
        }
        long bytesPerSplit = pragmas.timeSeriesSplitTargetMemory().getBytes();
        long estimatedGroups = estimatedGroups(stats, aggregate.timeBucket());
        if (canReducePeakMemory(numSplits(estimatedGroups, bytesPerSplit, maxSplits), maxConcurrentBranches) == false) {
            listener.onResponse(plan);
            return;
        }
        // field caps clips the stats to the query's time range, so they bound what the histogram has to read
        long minTimestamp = stats.stream().mapToLong(TimeSeriesStats::startMillis).min().orElseThrow();
        long maxTimestamp = stats.stream().mapToLong(TimeSeriesStats::endMillis).max().orElseThrow();
        long bucketMillis = bucketMillis(aggregate.timeBucket(), minTimestamp, maxTimestamp);
        Map<String, String[]> indices = new HashMap<>();
        Map<String, OriginalIndices> originalIndices = new HashMap<>();
        for (Map.Entry<String, List<String>> cluster : relation.concreteIndices().entrySet()) {
            indices.put(cluster.getKey(), cluster.getValue().toArray(String[]::new));
            originalIndices.put(
                cluster.getKey(),
                new OriginalIndices(
                    relation.originalIndices().getOrDefault(cluster.getKey(), List.of()).toArray(String[]::new),
                    SearchRequest.DEFAULT_INDICES_OPTIONS
                )
            );
        }
        TsidPrefixHistogramCollector.collect(
            context.transportService(),
            context.queryTask(),
            indices,
            originalIndices,
            histogramFilter(histogramFilters),
            minTimestamp,
            maxTimestamp,
            bucketMillis,
            listener.map(response -> {
                context.queryTask().ensureNotCancelled();
                // TODO: shard or cluster failures leave a partial histogram, which undercounts the groups
                TsidPrefixHistogram histogram = response.histogram();
                int numSplits = numSplits(histogram.estimatedGroups(bucketMillis), bytesPerSplit, maxSplits);
                if (canReducePeakMemory(numSplits, maxConcurrentBranches) == false) {
                    return plan;
                }
                List<TsidPrefixRange> ranges = histogram.splits(numSplits);
                if (canReducePeakMemory(ranges.size(), maxConcurrentBranches) == false) {
                    return plan;
                }
                return split(plan, aggregate, ranges);
            })
        );
    }

    /**
     * Up to {@code maxConcurrentBranches} branches run together and hold all their groups at once, as an unsplit query
     * does, so only more splits than that lower the peak.
     */
    private static boolean canReducePeakMemory(int numSplits, int maxConcurrentBranches) {
        return numSplits > maxConcurrentBranches;
    }

    static int numSplits(long groups, long memoryPerSplit, int maxSplits) {
        long bytes = groups > Long.MAX_VALUE / ESTIMATED_BYTES_PER_GROUP ? Long.MAX_VALUE : groups * ESTIMATED_BYTES_PER_GROUP;
        return (int) Math.min(maxSplits, Math.ceilDiv(bytes, memoryPerSplit));
    }

    /**
     * The field caps stats of the indices with data in the query's time range, or {@code null} if an index has none.
     */
    @Nullable
    private static List<TimeSeriesStats> stats(Collection<IndexProperties> indices) {
        List<TimeSeriesStats> stats = new ArrayList<>(indices.size());
        for (IndexProperties properties : indices) {
            TimeSeriesStats indexStats = properties.timeSeriesStats();
            if (indexStats == null) {
                return null;
            }
            if (indexStats.startMillis() <= indexStats.endMillis()) {
                stats.add(indexStats);
            }
        }
        return stats.isEmpty() ? null : stats;
    }

    /**
     * Estimates the {@code (_tsid, time bucket)} groups from the field caps {@link TimeSeriesStats}: each series counts the
     * buckets its time span covers, plus one for the bucket it starts in.
     */
    static long estimatedGroups(List<TimeSeriesStats> stats, @Nullable Bucket timeBucket) {
        long groups = 0;
        for (TimeSeriesStats indexStats : stats) {
            long spanMillis = indexStats.endMillis() - indexStats.startMillis() + 1;
            long seriesMillis = (long) Math.ceil(indexStats.seriesMillis());
            if (seriesMillis == 0) {
                continue;
            }
            groups += Math.ceilDiv(seriesMillis, spanMillis);
            long bucketMillis = bucketMillis(timeBucket, indexStats.startMillis(), indexStats.endMillis());
            if (bucketMillis != NO_BUCKET) {
                groups += Math.ceilDiv(seriesMillis, bucketMillis);
            }
        }
        return groups;
    }

    private static long bucketMillis(@Nullable Bucket timeBucket, long minTimestamp, long maxTimestamp) {
        if (timeBucket == null) {
            return NO_BUCKET;
        }
        Rounding.Prepared rounding = timeBucket.getDateRounding(FoldContext.small(), minTimestamp, maxTimestamp);
        long bucketStart = rounding.round(minTimestamp);
        return rounding.nextRoundingValue(bucketStart) - bucketStart;
    }

    /**
     * Wraps the aggregate, and the row-by-row nodes directly above it, in a {@link TimeSeriesSplit} over the ranges; the nodes
     * above it, such as the outer aggregation, run once over all ranges.
     */
    static LogicalPlan split(LogicalPlan plan, TimeSeriesAggregate aggregate, List<TsidPrefixRange> ranges) {
        LogicalPlan root = branchRoot(plan, aggregate);
        return plan.transformUp(LogicalPlan.class, p -> p == root ? new TimeSeriesSplit(root.source(), root, ranges) : p);
    }

    private static LogicalPlan branchRoot(LogicalPlan plan, LogicalPlan node) {
        Holder<LogicalPlan> parent = new Holder<>();
        plan.forEachDown(UnaryPlan.class, unary -> {
            if (unary.child() == node) {
                parent.set(unary);
            }
        });
        return parent.get() instanceof Eval || parent.get() instanceof Project ? branchRoot(plan, parent.get()) : node;
    }

    @Nullable
    static TimeSeriesAggregate splittable(LogicalPlan plan) {
        List<TimeSeriesAggregate> aggregates = plan.collect(TimeSeriesAggregate.class);
        if (aggregates.size() != 1 || plan.anyMatch(p -> p instanceof MergePlan)) {
            return null;
        }
        TimeSeriesAggregate aggregate = aggregates.getFirst();
        if (aggregate.singlePass() || aggregate.aggregates().stream().noneMatch(SplitTimeSeriesByTsid::requiresTimeSeriesSource)) {
            return null;
        }
        List<EsRelation> relations = aggregate.collect(EsRelation.class);
        if (relations.size() != 1) {
            return null;
        }
        EsRelation relation = relations.getFirst();
        if (relation.indexProperties().isEmpty()
            || relation.indexProperties().values().stream().allMatch(SplitTimeSeriesByTsid::hasTsidPrefixes) == false) {
            return null;
        }
        return aggregate;
    }

    /**
     * The filters of the docs the query reads: the request filter, the {@code WHERE} conditions, and the metrics the data
     * nodes require to be non-null (see {@link IgnoreNullMetrics}), so the histogram counts only the series the query reads.
     * {@code null} if the aggregation reads more than metrics or a condition can't be translated to a Lucene query: the
     * histogram would then count series the query skips, possibly by orders of magnitude, so such queries are not split.
     */
    @Nullable
    static List<QueryBuilder> histogramFilters(
        TimeSeriesAggregate aggregate,
        Configuration configuration,
        @Nullable QueryBuilder requestFilter,
        LucenePushdownPredicates predicates
    ) {
        Expression metrics = IgnoreNullMetrics.metricsFilter(aggregate);
        if (metrics == null || TranslationAware.translatable(metrics, predicates) == TranslationAware.Translatable.NO) {
            return null;
        }
        List<QueryBuilder> filters = new ArrayList<>();
        filters.add(toQueryBuilder(metrics, configuration, predicates));
        if (requestFilter != null) {
            filters.add(requestFilter);
        }
        Holder<Boolean> translatable = new Holder<>(true);
        aggregate.child().forEachDown(Filter.class, filter -> {
            if (TranslationAware.translatable(filter.condition(), predicates) == TranslationAware.Translatable.NO) {
                translatable.set(false);
            } else {
                filters.add(toQueryBuilder(filter.condition(), configuration, predicates));
            }
        });
        return translatable.get() ? filters : null;
    }

    private static QueryBuilder toQueryBuilder(Expression condition, Configuration configuration, LucenePushdownPredicates predicates) {
        Query query = TRANSLATOR_HANDLER.asQuery(predicates, condition);
        QueryBuilder builder = withoutSingleValueChecks(query.toQueryBuilder());
        return query.containsPlan() ? new PlanStreamWrapperQueryBuilder(configuration, builder) : builder;
    }

    /**
     * The histogram only needs a superset of the docs the query reads, and the single-value checks need the ES|QL search
     * context of a query's data node, so they are left out. Leaving one out matches more docs, except under a negation, where
     * it would match fewer, so a negated clause that has one is left out entirely.
     */
    static QueryBuilder withoutSingleValueChecks(QueryBuilder query) {
        if (query instanceof SingleValueQuery.AbstractBuilder singleValue) {
            return withoutSingleValueChecks(singleValue.next());
        }
        if (query instanceof BoolQueryBuilder bool) {
            BoolQueryBuilder rewritten = new BoolQueryBuilder();
            bool.must().forEach(clause -> rewritten.must(withoutSingleValueChecks(clause)));
            bool.filter().forEach(clause -> rewritten.filter(withoutSingleValueChecks(clause)));
            bool.should().forEach(clause -> rewritten.should(withoutSingleValueChecks(clause)));
            bool.mustNot().stream().filter(clause -> hasSingleValueCheck(clause) == false).forEach(rewritten::mustNot);
            return rewritten.minimumShouldMatch(bool.minimumShouldMatch());
        }
        return query;
    }

    private static boolean hasSingleValueCheck(QueryBuilder query) {
        if (query instanceof SingleValueQuery.AbstractBuilder) {
            return true;
        }
        if (query instanceof BoolQueryBuilder bool) {
            return Stream.of(bool.must(), bool.filter(), bool.should(), bool.mustNot())
                .flatMap(List::stream)
                .anyMatch(SplitTimeSeriesByTsid::hasSingleValueCheck);
        }
        return false;
    }

    private static QueryBuilder histogramFilter(List<QueryBuilder> filters) {
        if (filters.size() == 1) {
            return filters.getFirst();
        }
        BoolQueryBuilder bool = new BoolQueryBuilder();
        filters.forEach(bool::filter);
        return bool;
    }

    private static boolean hasTsidPrefixes(IndexProperties properties) {
        int createdVersion = properties.createdVersion();
        return properties.indexMode() == IndexMode.TIME_SERIES
            && createdVersion != 0
            && PartitionedDocValues.canPartitionByTsidPrefix(IndexVersion.fromId(createdVersion));
    }

    private static boolean requiresTimeSeriesSource(Expression aggregate) {
        return aggregate.anyMatch(e -> e instanceof TimeSeriesAggregateFunction ts && ts.requiredTimeSeriesSource());
    }

}
