/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;

/**
 * Verifies that the time-series target-chunk-rows pragma is read independently from the regular aggregation pragmas,
 * keeping the time-series chunking knob fully decoupled.
 */
public class QueryPragmasTests extends ESTestCase {

    public void testTimeSeriesTargetChunkRowsPragmaOverridesDefault() {
        QueryPragmas pragmas = new QueryPragmas(
            Settings.builder().put(PlannerSettings.TIME_SERIES_TARGET_CHUNK_ROWS.getKey(), 4_096).build()
        );
        assertThat(pragmas.timeSeriesTargetChunkRows(10_000), equalTo(4_096));
    }

    public void testTimeSeriesTargetChunkRowsIsValidPragmaName() {
        assertThat(QueryPragmas.VALID_PRAGMA_NAMES, hasItem(PlannerSettings.TIME_SERIES_TARGET_CHUNK_ROWS.getKey()));
    }

    public void testTimeSeriesTargetChunkRowsPragmaFallsBackToProvidedDefault() {
        QueryPragmas pragmas = new QueryPragmas(Settings.EMPTY);
        int clusterDefault = between(1, 1_000_000);
        assertThat(pragmas.timeSeriesTargetChunkRows(clusterDefault), equalTo(clusterDefault));
    }

    public void testMaxBranchCountPerMergeIsValidPragmaName() {
        assertThat(QueryPragmas.VALID_PRAGMA_NAMES, hasItem(QueryPragmas.MAX_BRANCH_COUNT_PER_MERGE.getKey()));
    }

    public void testMaxBranchCountPerMergePragmaOverridesDefault() {
        int clusterDefault = 8;
        int override = 3;
        QueryPragmas pragmas = new QueryPragmas(Settings.builder().put(QueryPragmas.MAX_BRANCH_COUNT_PER_MERGE.getKey(), override).build());
        assertThat(pragmas.maxBranchCountPerMerge(clusterDefault), equalTo(override));
        assertThat(
            pragmas.maxBranchCountPerMergeLimitSource("esql.query.max_branch_count_per_merge"),
            equalTo("[max_branch_count_per_merge] query pragma")
        );
    }

    public void testMaxBranchCountPerMergeFallsBackToClusterDefault() {
        QueryPragmas pragmas = new QueryPragmas(Settings.EMPTY);
        int clusterDefault = between(1, 50);
        assertThat(pragmas.maxBranchCountPerMerge(clusterDefault), equalTo(clusterDefault));
        assertThat(
            pragmas.maxBranchCountPerMergeLimitSource("esql.query.max_branch_count_per_merge"),
            equalTo("[esql.query.max_branch_count_per_merge] cluster setting")
        );
    }

    public void testTsidPartitionPragmas() {
        assertThat(QueryPragmas.VALID_PRAGMA_NAMES, hasItem(QueryPragmas.TSID_PARTITION_SERIES.getKey()));
        assertThat(QueryPragmas.VALID_PRAGMA_NAMES, hasItem(QueryPragmas.TSID_PARTITION_INDEX.getKey()));
        // On by default: a range per 10k series, all ranges in one request.
        assertThat(QueryPragmas.EMPTY.tsidPartitionSeries(), equalTo(10_000));
        assertThat(QueryPragmas.EMPTY.tsidPartitionIndex(), equalTo(-1));
        assertThat(
            new QueryPragmas(Settings.builder().put(QueryPragmas.TSID_PARTITION_SERIES.getKey(), 0).build()).tsidPartitionSeries(),
            equalTo(0)
        );
        QueryPragmas pragmas = new QueryPragmas(
            Settings.builder()
                .put(QueryPragmas.TSID_PARTITION_SERIES.getKey(), 100_000)
                .put(QueryPragmas.TSID_PARTITION_INDEX.getKey(), 3)
                .build()
        );
        assertThat(pragmas.tsidPartitionSeries(), equalTo(100_000));
        assertThat(pragmas.tsidPartitionIndex(), equalTo(3));
        expectThrows(
            IllegalArgumentException.class,
            () -> new QueryPragmas(Settings.builder().put(QueryPragmas.TSID_PARTITION_SERIES.getKey(), -1).build()).tsidPartitionSeries()
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> new QueryPragmas(Settings.builder().put(QueryPragmas.TSID_PARTITION_INDEX.getKey(), -2).build()).tsidPartitionIndex()
        );
    }

    public void testTimeSeriesPragmaIsDecoupledFromRegularAggregationPragmas() {
        // Only the regular pragma is set: it reaches the regular accessor and leaves the time-series one at its default.
        QueryPragmas regularOnly = new QueryPragmas(
            Settings.builder().put(PlannerSettings.PARTIAL_AGGREGATION_EMIT_KEYS_THRESHOLD.getKey(), 333).build()
        );
        assertThat(regularOnly.partialAggregationEmitKeysThreshold(100_000), equalTo(333));
        assertThat(regularOnly.timeSeriesTargetChunkRows(10_000), equalTo(10_000));

        // Only the time-series pragma is set: it reaches the time-series accessor and leaves the regular one at its default.
        QueryPragmas timeSeriesOnly = new QueryPragmas(
            Settings.builder().put(PlannerSettings.TIME_SERIES_TARGET_CHUNK_ROWS.getKey(), 999).build()
        );
        assertThat(timeSeriesOnly.timeSeriesTargetChunkRows(10_000), equalTo(999));
        assertThat(timeSeriesOnly.partialAggregationEmitKeysThreshold(100_000), equalTo(100_000));
    }
}
