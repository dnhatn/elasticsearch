/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.lucene.query.TsidRangeQuery;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec.QueryBuilderAndTags;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.querydsl.query.TsidRangeQueryBuilder;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Checks that {@link PartitionTimeSeriesByTsidRange} multiplies the queries of a time-series source by the number of
 * {@code _tsid} ranges of the node, keeps the tags, and leaves other sources alone.
 */
public class PartitionTimeSeriesByTsidRangeTests extends ESTestCase {
    /**
     * Series in the largest segment of the node, as the search stats report it.
     */
    private static final int SERIES = 1000;

    public void testAllRanges() {
        int seriesPerRange = between(1, SERIES - 1);
        int ranges = Math.ceilDiv(SERIES, seriesPerRange);
        List<QueryBuilderAndTags> queries = List.of(
            new QueryBuilderAndTags(QueryBuilders.rangeQuery("@timestamp").gte(0), List.of()),
            new QueryBuilderAndTags(null, List.of("tag"))
        );
        EsQueryExec queryExec = esQueryExec(IndexMode.TIME_SERIES, queries);
        PhysicalPlan result = apply(queryExec, seriesPerRange, -1);
        assertThat(result, instanceOf(EsQueryExec.class));
        EsQueryExec cut = (EsQueryExec) result;
        assertThat(cut.queryBuilderAndTags(), hasSize(queries.size() * ranges));
        TsidRangeQuery.Steps steps = ((TsidRangeQueryBuilder) cut.queryBuilderAndTags().getFirst().query()).steps();
        for (int q = 0; q < queries.size(); q++) {
            for (int i = 0; i < ranges; i++) {
                QueryBuilderAndTags entry = cut.queryBuilderAndTags().get(q * ranges + i);
                assertThat(entry.tags(), equalTo(queries.get(q).tags()));
                assertThat(entry.query(), instanceOf(TsidRangeQueryBuilder.class));
                TsidRangeQueryBuilder range = (TsidRangeQueryBuilder) entry.query();
                assertThat(range.next(), sameInstance(queries.get(q).query()));
                assertThat(range.seriesPerRange(), equalTo(seriesPerRange));
                assertThat(range.rangeIndex(), equalTo(i));
                // One set of counters for the whole source.
                assertThat(range.steps(), sameInstance(steps));
            }
        }
        assertThat(cut.indexPattern(), equalTo(queryExec.indexPattern()));
        assertThat(cut.indexMode(), equalTo(queryExec.indexMode()));
        assertThat(cut.attrs(), equalTo(queryExec.attrs()));
        assertThat(cut.limit(), equalTo(queryExec.limit()));
        assertThat(cut.sorts(), equalTo(queryExec.sorts()));
        assertThat(cut.estimatedRowSize(), equalTo(queryExec.estimatedRowSize()));
        // Applying the rule again changes nothing.
        assertThat(apply(cut, seriesPerRange, -1), sameInstance(cut));
    }

    public void testOneRange() {
        int seriesPerRange = between(1, SERIES - 1);
        int ranges = Math.ceilDiv(SERIES, seriesPerRange);
        // Within the ranges, or at or past the last one for the empty range.
        int rangeIndex = randomBoolean() ? between(0, ranges - 1) : between(ranges, ranges + 10);
        QueryBuilder query = QueryBuilders.rangeQuery("@timestamp").gte(0);
        EsQueryExec queryExec = esQueryExec(IndexMode.TIME_SERIES, List.of(new QueryBuilderAndTags(query, List.of())));
        EsQueryExec cut = (EsQueryExec) apply(queryExec, seriesPerRange, rangeIndex);
        assertThat(cut.queryBuilderAndTags(), hasSize(1));
        TsidRangeQueryBuilder range = (TsidRangeQueryBuilder) cut.queryBuilderAndTags().getFirst().query();
        assertThat(range.next(), sameInstance(query));
        assertThat(range.seriesPerRange(), equalTo(seriesPerRange));
        assertThat(range.rangeIndex(), equalTo(rangeIndex));
    }

    public void testSingleRange() {
        int seriesPerRange = between(SERIES, Integer.MAX_VALUE);
        EsQueryExec queryExec = esQueryExec(IndexMode.TIME_SERIES, List.of(new QueryBuilderAndTags(null, List.of())));
        // One range: the query runs as it is, also when that one range is asked for.
        assertThat(apply(queryExec, seriesPerRange, -1), sameInstance(queryExec));
        assertThat(apply(queryExec, seriesPerRange, 0), sameInstance(queryExec));
        // Past the one range: the empty range.
        EsQueryExec cut = (EsQueryExec) apply(queryExec, seriesPerRange, 1);
        assertThat(cut.queryBuilderAndTags(), hasSize(1));
        assertThat(((TsidRangeQueryBuilder) cut.queryBuilderAndTags().getFirst().query()).rangeIndex(), equalTo(1));
    }

    /**
     * A {@code TS} query without a function that needs the time-series source reads the same indices through a standard
     * source, whose first pass still groups by {@code _tsid}, so it is cut as well; a standard source over other indices is
     * not.
     */
    public void testStandardSourceOverTimeSeriesIndices() {
        List<QueryBuilderAndTags> queries = List.of(new QueryBuilderAndTags(QueryBuilders.rangeQuery("@timestamp").gte(0), List.of()));
        EsQueryExec standard = esQueryExec(IndexMode.STANDARD, queries);
        int seriesPerRange = between(1, SERIES - 1);
        int ranges = Math.ceilDiv(SERIES, seriesPerRange);
        EsQueryExec cut = (EsQueryExec) apply(standard, seriesPerRange, -1, Map.of(shardId(0), indexMetadata(IndexMode.TIME_SERIES)));
        assertThat(cut.indexMode(), equalTo(IndexMode.STANDARD));
        assertThat(cut.queryBuilderAndTags(), hasSize(ranges));
        Map<ShardId, IndexMetadata> mixed = Map.of(
            shardId(0),
            indexMetadata(IndexMode.TIME_SERIES),
            shardId(1),
            indexMetadata(IndexMode.STANDARD)
        );
        assertThat(apply(standard, seriesPerRange, -1, mixed), sameInstance(standard));
    }

    public void testLeavesOtherSourcesAlone() {
        List<QueryBuilderAndTags> queries = List.of(new QueryBuilderAndTags(QueryBuilders.rangeQuery("@timestamp").gte(0), List.of()));
        // Turned off.
        EsQueryExec timeSeries = esQueryExec(IndexMode.TIME_SERIES, queries);
        assertThat(apply(timeSeries, 0, -1), sameInstance(timeSeries));
        // Not a time-series source, and no time-series index behind it.
        EsQueryExec standard = esQueryExec(IndexMode.STANDARD, queries);
        assertThat(apply(standard, 10, -1), sameInstance(standard));
        assertThat(apply(standard, 10, -1, Map.of(shardId(0), indexMetadata(IndexMode.STANDARD))), sameInstance(standard));
        // Sorted: the TopN source operator runs one query.
        EsQueryExec sorted = new EsQueryExec(
            Source.EMPTY,
            "hosts",
            IndexMode.TIME_SERIES,
            List.of(),
            null,
            List.of(new EsQueryExec.ScoreSort(Order.OrderDirection.DESC)),
            null,
            queries
        );
        assertThat(apply(sorted, 10, -1), sameInstance(sorted));
    }

    private static EsQueryExec esQueryExec(IndexMode indexMode, List<QueryBuilderAndTags> queries) {
        return new EsQueryExec(Source.EMPTY, "hosts", indexMode, List.of(), null, null, null, queries);
    }

    private static ShardId shardId(int id) {
        return new ShardId("hosts", "uuid-" + id, id);
    }

    private static IndexMetadata indexMetadata(IndexMode mode) {
        Settings.Builder settings = indexSettings(IndexVersion.current(), 1, 0).put(IndexSettings.MODE.getKey(), mode);
        if (mode == IndexMode.TIME_SERIES) {
            settings.put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "host");
        }
        return IndexMetadata.builder("hosts").settings(settings).build();
    }

    private static PhysicalPlan apply(EsQueryExec queryExec, int seriesPerRange, int rangeIndex) {
        return apply(queryExec, seriesPerRange, rangeIndex, Map.of());
    }

    private static PhysicalPlan apply(EsQueryExec queryExec, int seriesPerRange, int rangeIndex, Map<ShardId, IndexMetadata> targetShards) {
        QueryPragmas pragmas = new QueryPragmas(
            Settings.builder()
                .put(QueryPragmas.TSID_PARTITION_SERIES.getKey(), seriesPerRange)
                .put(QueryPragmas.TSID_PARTITION_INDEX.getKey(), rangeIndex)
                .build()
        );
        SearchStats searchStats = new EsqlTestUtils.TestSearchStats() {
            @Override
            public int maxSeriesPerSegment() {
                return SERIES;
            }

            @Override
            public Map<ShardId, IndexMetadata> targetShards() {
                return targetShards;
            }
        };
        LocalPhysicalOptimizerContext context = new LocalPhysicalOptimizerContext(
            PlannerSettings.DEFAULTS,
            new EsqlFlags(true),
            EsqlTestUtils.configuration(pragmas),
            FoldContext.small(),
            searchStats
        );
        return new PartitionTimeSeriesByTsidRange().rule(queryExec, context);
    }
}
