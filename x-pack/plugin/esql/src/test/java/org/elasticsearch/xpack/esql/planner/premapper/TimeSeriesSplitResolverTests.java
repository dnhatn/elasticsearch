/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner.premapper;

import org.elasticsearch.action.fieldcaps.TimeSeriesStats;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.lucene.query.TsidPrefixHistogram;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.util.List;

import static org.elasticsearch.compute.lucene.query.TimeSeriesGroups.ESTIMATED_BYTES_PER_GROUP;
import static org.hamcrest.Matchers.equalTo;

public class TimeSeriesSplitResolverTests extends ESTestCase {

    public void testNumSplitsFitsGroupsIntoMemoryPerSplit() {
        long memory = 1000 * ESTIMATED_BYTES_PER_GROUP;
        assertThat(TimeSeriesSplitResolver.numSplits(0, memory, 256), equalTo(0));
        assertThat(TimeSeriesSplitResolver.numSplits(1000, memory, 256), equalTo(1));
        assertThat(TimeSeriesSplitResolver.numSplits(1001, memory, 256), equalTo(2));
        assertThat(TimeSeriesSplitResolver.numSplits(10_000, memory, 256), equalTo(10));
        assertThat(TimeSeriesSplitResolver.numSplits(10_000, memory, 4), equalTo(4));
        assertThat(TimeSeriesSplitResolver.numSplits(Long.MAX_VALUE, memory, 256), equalTo(256));
    }

    public void testSplitsSaveMemoryOnlyBeyondConcurrentForks() {
        int degree = between(1, 8);
        QueryPragmas pragmas = new QueryPragmas(Settings.builder().put(QueryPragmas.BRANCH_PARALLEL_DEGREE.getKey(), degree).build());
        assertFalse(TimeSeriesSplitResolver.savesMemory(degree, pragmas));
        assertTrue(TimeSeriesSplitResolver.savesMemory(degree + 1, pragmas));
    }

    public void testEstimatedGroupsWithoutTimeBucketIsSeries() {
        long span = 1000;
        TsidPrefixHistogram histogram = new TsidPrefixHistogram(8, new int[] { 1, 2 }, new long[] { 10 * span, 30 * span }, 0, span - 1);
        assertThat(TimeSeriesSplitResolver.estimatedGroups(histogram, null), equalTo(40L));
    }

    public void testEstimatedGroupsFromIndexStats() {
        long span = 1000;
        IndexProperties index = new IndexProperties(IndexMode.TIME_SERIES, 1, 0, new TimeSeriesStats(10 * span, 0, span - 1));
        long[] unbounded = { Long.MIN_VALUE, Long.MAX_VALUE };
        assertThat(TimeSeriesSplitResolver.estimatedGroups(List.of(index, index), null, unbounded), equalTo(20L));
        assertThat(TimeSeriesSplitResolver.estimatedGroups(List.of(index), null, new long[] { 0, span / 2 - 1 }), equalTo(10L));
        assertThat(TimeSeriesSplitResolver.estimatedGroups(List.of(index), null, new long[] { span, 2 * span }), equalTo(0L));
        IndexProperties unknown = new IndexProperties(IndexMode.TIME_SERIES, 1, 0, null);
        assertThat(TimeSeriesSplitResolver.estimatedGroups(List.of(index, unknown), null, unbounded), equalTo(-1L));
    }
}
