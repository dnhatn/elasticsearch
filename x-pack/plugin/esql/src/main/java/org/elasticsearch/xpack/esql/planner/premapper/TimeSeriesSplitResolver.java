/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner.premapper;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.fieldcaps.TimeSeriesStats;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.compute.lucene.query.TimeSeriesGroups;
import org.elasticsearch.compute.lucene.query.TsidPrefixRange;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.codec.tsdb.PartitionedDocValues;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Absent;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Avg;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.esql.expression.function.aggregate.PackDimsAgg;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Present;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.local.IgnoreNullMetrics;
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
import org.elasticsearch.xpack.esql.querydsl.query.SingleValueQuery;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.timeseries.TsidPrefixHistogramCollector;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class TimeSeriesSplitResolver {
    private static final Logger logger = LogManager.getLogger(TimeSeriesSplitResolver.class);

    private static final Set<Class<? extends AggregateFunction>> FIXED_WIDTH_AGGREGATIONS = Set.of(
        Sum.class,
        Count.class,
        Avg.class,
        Present.class,
        Absent.class
    );

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
        List<String> metricFields = metricFields(aggregate);
        if (metricFields == null) {
            listener.onResponse(plan);
            return;
        }
        String[] indices = relation.concreteIndices().get(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY).toArray(String[]::new);
        OriginalIndices originalIndices = new OriginalIndices(
            relation.originalIndices().getOrDefault(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, List.of()).toArray(String[]::new),
            SearchRequest.DEFAULT_INDICES_OPTIONS
        );
        QueryBuilder shardFilter = PlannerUtils.canMatchFilter(
            flags,
            configuration,
            minimumVersion,
            new FragmentExec(aggregate.source(), aggregate, requestFilter, 0)
        );
        long[] bounds = timestampBounds(shardFilter, configuration.now().toEpochMilli());
        long indexGroups = estimatedGroups(relation.indexProperties().values(), aggregate.timeBucket(), bounds);
        long bytesPerGroup = bytesPerGroup(aggregate.aggregates());
        long memoryPerSplit = pragmas.timeSeriesSplitMemory().getBytes();
        if (indexGroups >= 0
            && savesMemory(numSplits(indexGroups, bytesPerGroup, memoryPerSplit, pragmas.timeSeriesMaxSplits()), pragmas) == false) {
            logger.info("no time-series splits for [{}]: estimated groups [{}]", relation.indexPattern(), indexGroups);
            listener.onResponse(plan);
            return;
        }
        final long bucketMillis = bucketMillis(
            aggregate.timeBucket(),
            bounds[0] == Long.MIN_VALUE ? configuration.now().toEpochMilli() : bounds[0]
        );
        final long probeStartNanos = System.nanoTime();
        TsidPrefixHistogramCollector.collect(
            services.transportService(),
            queryTask,
            Map.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, indices),
            Map.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, originalIndices),
            metricFields,
            bounds[0],
            bounds[1],
            bucketMillis,
            ActionListener.wrap(response -> {
                var histogram = response.histogram();
                long groups = histogram.estimatedGroups(bucketMillis);
                int numSplits = numSplits(groups, bytesPerGroup, memoryPerSplit, pragmas.timeSeriesMaxSplits());
                List<TsidPrefixRange> splits = savesMemory(numSplits, pragmas) ? histogram.splits(numSplits) : List.of();
                logger.info(
                    "time-series splits for [{}]: probe took [{}ms], index groups [{}], probed groups [{}], splits [{}]",
                    relation.indexPattern(),
                    (System.nanoTime() - probeStartNanos) / 1_000_000,
                    indexGroups,
                    groups,
                    numSplits
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

    static int numSplits(long groups, long bytesPerGroup, long memoryPerSplit, int maxSplits) {
        return (int) Math.min(maxSplits, Math.ceilDiv(TimeSeriesGroups.bytes(groups, bytesPerGroup), memoryPerSplit));
    }

    /**
     * {@link TimeSeriesGroups#ESTIMATED_BYTES_PER_FIXED_WIDTH_GROUP} if every aggregation keeps a few numeric values per group,
     * otherwise {@link TimeSeriesGroups#ESTIMATED_BYTES_PER_GROUP}, plus {@link TimeSeriesGroups#ESTIMATED_BYTES_PER_PACKED_DIMENSION}
     * for each dimension the groups carry.
     */
    public static long bytesPerGroup(List<? extends Expression> aggregates) {
        long stateBytes = TimeSeriesGroups.ESTIMATED_BYTES_PER_FIXED_WIDTH_GROUP;
        long dimensionBytes = 0;
        for (Expression aggregate : aggregates) {
            for (AggregateFunction function : aggregate.collect(AggregateFunction.class)) {
                if (function instanceof PackDimsAgg packDims) {
                    dimensionBytes += packDims.fields().size() * TimeSeriesGroups.ESTIMATED_BYTES_PER_PACKED_DIMENSION;
                } else if (fixedWidth(function) == false) {
                    stateBytes = TimeSeriesGroups.ESTIMATED_BYTES_PER_GROUP;
                }
            }
        }
        return stateBytes + dimensionBytes;
    }

    private static boolean fixedWidth(AggregateFunction function) {
        if (function instanceof Max max) {
            return max.field().dataType().isNumeric();
        }
        if (function instanceof Min min) {
            return min.field().dataType().isNumeric();
        }
        return FIXED_WIDTH_AGGREGATIONS.contains(function.getClass());
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
            long minTimestamp = Math.max(stats.startMillis(), bounds[0]);
            long maxTimestamp = Math.min(stats.endMillis(), bounds[1]);
            if (minTimestamp > maxTimestamp) {
                continue;
            }
            double covered = (double) (maxTimestamp - minTimestamp + 1) / (stats.endMillis() - stats.startMillis() + 1);
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

    static long bucketMillis(@Nullable Bucket timeBucket, long timestamp) {
        Rounding.Prepared rounding = timeBucket == null ? null : timeBucket.getDateRounding(FoldContext.small(), null, null);
        return TimeSeriesGroups.bucketMillis(rounding, timestamp);
    }

    static LogicalPlan fork(LogicalPlan plan, TimeSeriesAggregate aggregate, List<TsidPrefixRange> splits) {
        LogicalPlan root = branchRoot(plan, aggregate);
        List<LogicalPlan> branches = new ArrayList<>(splits.size());
        for (TsidPrefixRange range : splits) {
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

    /**
     * The metric fields the aggregate reads, or {@code null} if it reads a field that isn't a metric.
     */
    @Nullable
    static List<String> metricFields(TimeSeriesAggregate aggregate) {
        Expression condition = IgnoreNullMetrics.metricsFilter(aggregate);
        if (condition == null) {
            return null;
        }
        List<String> fields = new ArrayList<>();
        for (IsNotNull isNotNull : condition.collect(IsNotNull.class)) {
            if (isNotNull.field() instanceof FieldAttribute field) {
                fields.add(field.fieldName().string());
            } else {
                return null;
            }
        }
        return fields;
    }

    static long[] timestampBounds(@Nullable QueryBuilder filter, long nowInMillis) {
        long[] bounds = { Long.MIN_VALUE, Long.MAX_VALUE };
        if (filter != null) {
            collectTimestampBounds(filter, nowInMillis, bounds);
        }
        return bounds;
    }

    private static void collectTimestampBounds(QueryBuilder filter, long nowInMillis, long[] bounds) {
        if (filter instanceof BoolQueryBuilder bool) {
            for (QueryBuilder clause : bool.filter()) {
                collectTimestampBounds(clause, nowInMillis, bounds);
            }
            for (QueryBuilder clause : bool.must()) {
                collectTimestampBounds(clause, nowInMillis, bounds);
            }
        } else if (filter instanceof SingleValueQuery.AbstractBuilder singleValue) {
            collectTimestampBounds(singleValue.next(), nowInMillis, bounds);
        } else if (filter instanceof RangeQueryBuilder range && range.fieldName().equals(DataStream.TIMESTAMP_FIELD_NAME)) {
            try {
                DateFormatter formatter = range.format() == null
                    ? DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER
                    : DateFormatter.forPattern(range.format());
                DateMathParser parser = formatter.toDateMathParser();
                ZoneId zone = range.timeZone() == null ? null : ZoneId.of(range.timeZone());
                if (range.from() != null) {
                    long lower = parser.parse(range.from().toString(), () -> nowInMillis, range.includeLower() == false, zone)
                        .toEpochMilli();
                    bounds[0] = Math.max(bounds[0], lower);
                }
                if (range.to() != null) {
                    long upper = parser.parse(range.to().toString(), () -> nowInMillis, range.includeUpper(), zone).toEpochMilli();
                    bounds[1] = Math.min(bounds[1], upper);
                }
            } catch (RuntimeException e) {
                // a bound that cannot be parsed here only loses the clipping; the shards still apply the filter
            }
        }
    }
}
