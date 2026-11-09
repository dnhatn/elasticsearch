/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.compute.lucene.query.TsidRangePartitioner;
import org.elasticsearch.compute.lucene.query.TsidRangeQuery;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.querydsl.query.TsidRangeQueryBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

/**
 * Cuts the source queries over time-series indices into sub-queries by {@code _tsid} range, of {@code tsid_partition_series}
 * series each; the pragma's {@code 0} turns the cut off. Every {@link EsQueryExec.QueryBuilderAndTags} of such an {@link EsQueryExec}
 * becomes one entry per range, the input query restricted by a {@link TsidRangeQueryBuilder}, with the input's tags: the
 * same multiplication of the shard query into tagged queries as {@link ReplaceRoundToWithQueryAndTags}. The slice queue takes
 * the range back out of each query and cuts the leaves over the range's doc intervals instead, so one weight, the uncut
 * query, serves every range of a shard. A series is whole inside one range, so every time-series function stays
 * exact. The source of a {@code TS} query is in {@link IndexMode#TIME_SERIES} only when a function such as {@code rate}
 * needs it; otherwise, like {@code avg_over_time}, it is a standard source over the same indices, whose first pass still
 * groups by {@code _tsid}, so both are cut.
 * <p>
 *     The number of ranges comes from the shard of this node with the most series in one segment; each shard resolves the
 *     keys of its ranges when the query is built, see {@link TsidRangePartitioner}. With {@code tsid_partition_index}, only
 *     that range is kept, which runs one range per request; an index at or past the number of ranges keeps an empty range,
 *     which has the setup cost of a range and matches nothing.
 * </p>
 * <p>
 *     Experimental: this measures what the ranges cost on the data node. With all ranges in one request nothing bounds
 *     the memory of the aggregation. Sorted sources are left alone, as {@code LuceneTopNSourceOperator} runs one query.
 * </p>
 */
public class PartitionTimeSeriesByTsidRange extends PhysicalOptimizerRules.ParameterizedOptimizerRule<
    EsQueryExec,
    LocalPhysicalOptimizerContext> {

    @Override
    protected PhysicalPlan rule(EsQueryExec queryExec, LocalPhysicalOptimizerContext ctx) {
        QueryPragmas pragmas = ctx.configuration().pragmas();
        int seriesPerRange = pragmas.tsidPartitionSeries();
        if (seriesPerRange <= 0
            || queryExec.canSubstituteRoundToWithQueryBuilderAndTags() == false
            || readsTimeSeriesIndices(queryExec, ctx) == false
            || queryExec.queryBuilderAndTags().stream().anyMatch(q -> q.query() instanceof TsidRangeQueryBuilder)) {
            return queryExec;
        }
        int ranges = TsidRangePartitioner.numRanges(ctx.searchStats().maxSeriesPerSegment(), seriesPerRange);
        int rangeIndex = pragmas.tsidPartitionIndex();
        if (ranges == 1 && rangeIndex <= 0) {
            return queryExec;
        }
        List<Integer> indices = rangeIndex < 0 ? IntStream.range(0, ranges).boxed().toList() : List.of(rangeIndex);
        TsidRangeQuery.Steps steps = new TsidRangeQuery.Steps();
        List<EsQueryExec.QueryBuilderAndTags> expanded = new ArrayList<>(queryExec.queryBuilderAndTags().size() * indices.size());
        for (EsQueryExec.QueryBuilderAndTags query : queryExec.queryBuilderAndTags()) {
            for (int index : indices) {
                expanded.add(query.withQuery(new TsidRangeQueryBuilder(query.query(), seriesPerRange, index, steps)));
            }
        }
        return new EsQueryExec(
            queryExec.source(),
            queryExec.indexPattern(),
            queryExec.indexMode(),
            queryExec.attrs(),
            queryExec.limit(),
            queryExec.sorts(),
            queryExec.estimatedRowSize(),
            expanded
        );
    }

    /**
     * Whether every shard {@code queryExec} reads is a time-series index, where the docs are sorted by {@code _tsid}.
     */
    private static boolean readsTimeSeriesIndices(EsQueryExec queryExec, LocalPhysicalOptimizerContext ctx) {
        if (queryExec.indexMode().isTsdb()) {
            return true;
        }
        Map<ShardId, IndexMetadata> shards = ctx.searchStats().targetShards();
        return shards.isEmpty() == false && shards.values().stream().allMatch(imd -> IndexMode.isTsdb(imd.getIndexMode()));
    }
}
