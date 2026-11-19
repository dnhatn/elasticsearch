/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner.premapper;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.fieldcaps.TimeSeriesStats;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.compute.lucene.query.TimeSeriesGroups;
import org.elasticsearch.compute.lucene.query.TsidPrefixHistogram;
import org.elasticsearch.compute.lucene.query.TsidRange;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.codec.tsdb.PartitionedDocValues;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.esql.capabilities.TranslationAware;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.local.IgnoreNullMetrics;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MergePlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.plugin.TransportActionServices;
import org.elasticsearch.xpack.esql.plugin.TsidStats;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.xpack.esql.planner.TranslatorHandler.TRANSLATOR_HANDLER;

public final class TimeSeriesSplitResolver {
    private static final Logger logger = LogManager.getLogger(TimeSeriesSplitResolver.class);

    private TimeSeriesSplitResolver() {}

    static void resolve(
        LogicalPlan plan,
        TransportVersion minimumVersion,
        Configuration configuration,
        @Nullable QueryBuilder requestFilter,
        EsqlFlags flags,
        CancellableTask queryTask,
        TransportActionServices services,
        ActionListener<LogicalPlan> listener
    ) {
        QueryPragmas pragmas = configuration.pragmas();
        TimeSeriesAggregate aggregate = splittable(plan);
        if (aggregate == null || minimumVersion.supports(EsRelation.TSID_SPLITS) == false) {
            listener.onResponse(plan);
            return;
        }
        EsRelation relation = aggregate.collect(EsRelation.class).getFirst();
        String[] indices = relation.concreteIndices().get(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY).toArray(String[]::new);
        QueryBuilder shardFilter = PlannerUtils.canMatchFilter(
            flags,
            configuration,
            minimumVersion,
            new FragmentExec(aggregate.source(), aggregate, requestFilter, 0)
        );
        long[] bounds = TsidStats.timestampBounds(shardFilter, configuration.now().toEpochMilli());
        long indexGroups = estimatedGroups(relation.indexProperties().values(), aggregate.timeBucket(), bounds);
        long memoryPerSplit = pragmas.timeSeriesSplitMemory().getBytes();
        if (indexGroups >= 0 && savesMemory(numSplits(indexGroups, memoryPerSplit, pragmas.timeSeriesMaxSplits()), pragmas) == false) {
            logger.info("no time-series splits for [{}]: estimated groups [{}]", relation.indexPattern(), indexGroups);
            listener.onResponse(plan);
            return;
        }
        QueryBuilder metricFilter = metricFilter(aggregate, LucenePushdownPredicates.forCanMatch(minimumVersion, flags));
        final long probeStartNanos = System.nanoTime();
        TsidStats.probe(
            services.client(),
            services.transportService(),
            queryTask,
            indices,
            shardFilter,
            metricFilter,
            bounds[0],
            bounds[1],
            ActionListener.wrap(histogram -> {
                long groups = estimatedGroups(histogram, aggregate.timeBucket());
                int numSplits = numSplits(groups, memoryPerSplit, pragmas.timeSeriesMaxSplits());
                List<TsidRange> splits = savesMemory(numSplits, pragmas) ? histogram.splits(numSplits) : List.of();
                logger.info(
                    "time-series splits for [{}]: probe took [{}ms], index groups [{}], probed groups [{}], splits [{}], cells [{}]",
                    relation.indexPattern(),
                    (System.nanoTime() - probeStartNanos) / 1_000_000,
                    indexGroups,
                    groups,
                    numSplits,
                    histogram.size()
                );
                logger.debug("time-series splits for [{}]: histogram [{}], splits [{}]", relation.indexPattern(), histogram, splits);
                listener.onResponse(splits.isEmpty() ? plan : fork(plan, aggregate, splits));
            }, e -> {
                if (queryTask.notifyIfCancelled(listener)) {
                    return;
                }
                logger.info(
                    () -> "failed to probe time-series splits after [" + (System.nanoTime() - probeStartNanos) / 1_000_000 + "ms]",
                    e
                );
                listener.onResponse(plan);
            })
        );
    }

    /**
     * Up to {@link QueryPragmas#branchParallelDegree()} forks run together and hold every group at once, as an unsplit
     * query does, so only more forks than that lower the peak.
     */
    static boolean savesMemory(int numSplits, QueryPragmas pragmas) {
        return numSplits > pragmas.branchParallelDegree();
    }

    static int numSplits(long groups, long memoryPerSplit, int maxSplits) {
        return (int) Math.min(maxSplits, Math.ceilDiv(TimeSeriesGroups.bytes(groups), memoryPerSplit));
    }

    /**
     * The estimated number of {@code (_tsid, time bucket)} groups: each series counts the buckets its time span covers,
     * plus one for the bucket it starts in.
     */
    static long estimatedGroups(TsidPrefixHistogram histogram, @Nullable Bucket timeBucket) {
        return estimatedGroups(histogram.totalSeriesMillis(), histogram.minTimestamp(), histogram.maxTimestamp(), timeBucket);
    }

    /**
     * The estimated groups of the indices from their field caps {@link TimeSeriesStats}, each clipped to the query's
     * {@code @timestamp} bounds as if its series spread evenly over its time span, or {@code -1} if an index has no stats.
     */
    static long estimatedGroups(Collection<IndexProperties> indices, @Nullable Bucket timeBucket, long[] bounds) {
        long groups = 0;
        for (IndexProperties properties : indices) {
            TimeSeriesStats stats = properties.timeSeriesStats();
            if (stats == null) {
                return -1;
            }
            long minTimestamp = Math.max(stats.minTimestamp(), bounds[0]);
            long maxTimestamp = Math.min(stats.maxTimestamp(), bounds[1]);
            if (minTimestamp > maxTimestamp) {
                continue;
            }
            double covered = (double) (maxTimestamp - minTimestamp + 1) / (stats.maxTimestamp() - stats.minTimestamp() + 1);
            groups += estimatedGroups((long) Math.ceil(stats.seriesMillis() * covered), minTimestamp, maxTimestamp, timeBucket);
        }
        return groups;
    }

    private static long estimatedGroups(long seriesMillis, long minTimestamp, long maxTimestamp, @Nullable Bucket timeBucket) {
        if (seriesMillis == 0 || minTimestamp > maxTimestamp) {
            return 0;
        }
        Rounding.Prepared rounding = timeBucket == null
            ? null
            : timeBucket.getDateRounding(FoldContext.small(), minTimestamp, maxTimestamp);
        return TimeSeriesGroups.groups(
            seriesMillis,
            maxTimestamp - minTimestamp + 1,
            TimeSeriesGroups.bucketMillis(rounding, minTimestamp)
        );
    }

    static LogicalPlan fork(LogicalPlan plan, TimeSeriesAggregate aggregate, List<TsidRange> splits) {
        LogicalPlan root = branchRoot(plan, aggregate);
        List<LogicalPlan> branches = new ArrayList<>(splits.size());
        for (TsidRange range : splits) {
            branches.add(root.transformUp(EsRelation.class, r -> r.withTsidRange(range)));
        }
        Fork fork = new Fork(root.source(), branches, root.output());
        return plan.transformDown(LogicalPlan.class, p -> p == root ? fork : p);
    }

    /**
     * The top of what each branch runs: the aggregate plus the row-by-row nodes directly above it, such as the division
     * of {@code avg_over_time}. Keeping them in the branches leaves the outer aggregate directly on the fork, so it runs
     * once over all branches.
     */
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
        if (aggregates.size() != 1 || aggregates.getFirst().singlePass() || plan.anyMatch(p -> p instanceof MergePlan)) {
            return null;
        }
        if (aggregates.getFirst().groupings().stream().noneMatch(TimeSeriesSplitResolver::isTsid)) {
            return null;
        }
        List<EsRelation> relations = aggregates.getFirst().collect(EsRelation.class);
        if (relations.size() != 1) {
            return null;
        }
        EsRelation relation = relations.getFirst();
        if (relation.concreteIndices().size() != 1
            || relation.concreteIndices().containsKey(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY) == false
            || relation.indexProperties().size() < 2) {
            return null;
        }
        for (IndexProperties properties : relation.indexProperties().values()) {
            int createdVersion = properties.createdVersion();
            if (properties.indexMode() != IndexMode.TIME_SERIES
                || createdVersion == 0
                || PartitionedDocValues.canPartitionByTsidPrefix(IndexVersion.fromId(createdVersion)) == false) {
                return null;
            }
        }
        return aggregates.getFirst();
    }

    private static boolean isTsid(Expression grouping) {
        return grouping instanceof MetadataAttribute attribute && attribute.name().equals(MetadataAttribute.TSID_FIELD);
    }

    @Nullable
    static QueryBuilder metricFilter(TimeSeriesAggregate aggregate, LucenePushdownPredicates predicates) {
        Expression condition = IgnoreNullMetrics.metricsFilter(aggregate);
        if (condition == null
            || TranslationAware.translatable(condition, predicates).finish() != TranslationAware.FinishedTranslatable.YES) {
            return null;
        }
        return TRANSLATOR_HANDLER.asQuery(predicates, condition).toQueryBuilder();
    }
}
