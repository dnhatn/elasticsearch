/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner.premapper;

import org.elasticsearch.action.fieldcaps.TimeSeriesStats;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.PackDimsAgg;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Values;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.util.List;

import static org.elasticsearch.compute.lucene.query.TimeSeriesGroups.ESTIMATED_BYTES_PER_FIXED_WIDTH_GROUP;
import static org.elasticsearch.compute.lucene.query.TimeSeriesGroups.ESTIMATED_BYTES_PER_GROUP;
import static org.elasticsearch.compute.lucene.query.TimeSeriesGroups.ESTIMATED_BYTES_PER_PACKED_DIMENSION;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.hamcrest.Matchers.equalTo;

public class TimeSeriesSplitResolverTests extends ESTestCase {

    public void testNumSplitsFitsGroupsIntoMemoryPerSplit() {
        long memory = 1000 * ESTIMATED_BYTES_PER_GROUP;
        assertThat(TimeSeriesSplitResolver.numSplits(0, ESTIMATED_BYTES_PER_GROUP, memory, 256), equalTo(0));
        assertThat(TimeSeriesSplitResolver.numSplits(1000, ESTIMATED_BYTES_PER_GROUP, memory, 256), equalTo(1));
        assertThat(TimeSeriesSplitResolver.numSplits(1001, ESTIMATED_BYTES_PER_GROUP, memory, 256), equalTo(2));
        assertThat(TimeSeriesSplitResolver.numSplits(10_000, ESTIMATED_BYTES_PER_GROUP, memory, 256), equalTo(10));
        assertThat(TimeSeriesSplitResolver.numSplits(10_000, ESTIMATED_BYTES_PER_GROUP, memory, 4), equalTo(4));
        assertThat(TimeSeriesSplitResolver.numSplits(Long.MAX_VALUE, ESTIMATED_BYTES_PER_GROUP, memory, 256), equalTo(256));
    }

    public void testBytesPerGroupFromAggregations() {
        FieldAttribute number = getFieldAttribute("cpu", DataType.DOUBLE);
        FieldAttribute keyword = getFieldAttribute("host", DataType.KEYWORD);
        Expression count = new Alias(Source.EMPTY, "c", new Count(Source.EMPTY, number));
        Expression max = new Alias(Source.EMPTY, "m", new Max(Source.EMPTY, number, false));
        assertThat(TimeSeriesSplitResolver.bytesPerGroup(List.of(count, max, keyword)), equalTo(ESTIMATED_BYTES_PER_FIXED_WIDTH_GROUP));
        Expression maxKeyword = new Alias(Source.EMPTY, "k", new Max(Source.EMPTY, keyword, false));
        assertThat(TimeSeriesSplitResolver.bytesPerGroup(List.of(count, maxKeyword)), equalTo(ESTIMATED_BYTES_PER_GROUP));
        Expression values = new Alias(Source.EMPTY, "v", new Values(Source.EMPTY, number, Literal.TRUE, AggregateFunction.NO_WINDOW));
        assertThat(TimeSeriesSplitResolver.bytesPerGroup(List.of(count, values)), equalTo(ESTIMATED_BYTES_PER_GROUP));
        FieldAttribute pod = getFieldAttribute("pod", DataType.KEYWORD);
        Expression oneDimension = new Alias(Source.EMPTY, "d", PackDimsAgg.create(Source.EMPTY, List.of(keyword)));
        Expression twoDimensions = new Alias(Source.EMPTY, "d", PackDimsAgg.create(Source.EMPTY, List.of(keyword, pod)));
        assertThat(
            TimeSeriesSplitResolver.bytesPerGroup(List.of(count, oneDimension)),
            equalTo(ESTIMATED_BYTES_PER_FIXED_WIDTH_GROUP + ESTIMATED_BYTES_PER_PACKED_DIMENSION)
        );
        assertThat(
            TimeSeriesSplitResolver.bytesPerGroup(List.of(values, twoDimensions)),
            equalTo(ESTIMATED_BYTES_PER_GROUP + 2 * ESTIMATED_BYTES_PER_PACKED_DIMENSION)
        );
    }

    public void testSplitsSaveMemoryOnlyBeyondConcurrentForks() {
        int degree = between(1, 8);
        QueryPragmas pragmas = new QueryPragmas(Settings.builder().put(QueryPragmas.BRANCH_PARALLEL_DEGREE.getKey(), degree).build());
        assertFalse(TimeSeriesSplitResolver.savesMemory(degree, pragmas));
        assertTrue(TimeSeriesSplitResolver.savesMemory(degree + 1, pragmas));
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
