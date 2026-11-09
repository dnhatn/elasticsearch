/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.querydsl.query;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.lucene.query.TsidRangePartitioner;
import org.elasticsearch.compute.lucene.query.TsidRangeQuery;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.internal.MaxClauseCountQueryVisitor;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Restricts a time-series source query to one {@code _tsid} range of each shard. The planner only decides how many ranges
 * there are; each shard resolves the keys of its range when the query is built, from its own segments, through
 * {@link TsidRangePartitioner#range}. The {@code next} query, if any, is the source query the range restricts.
 * <p>
 *     The builders of one source share one {@link TsidRangeQuery.Steps}, so the source operator can report how the ranges were
 *     resolved. Like {@link SingleValueQuery.SyntheticSourceDelegateBuilder}, the builder is not serialized: the local
 *     optimizer of a data node creates it, after the plan has crossed the wire.
 * </p>
 */
public final class TsidRangeQueryBuilder extends AbstractQueryBuilder<TsidRangeQueryBuilder> {
    public static final String NAME = "esql_tsid_range";

    @Nullable
    private final QueryBuilder next;
    private final int seriesPerRange;
    private final int rangeIndex;
    private final TsidRangeQuery.Steps steps;

    /**
     * @param next           the query to restrict, or {@code null} to match the whole range
     * @param seriesPerRange series per range, counted in the largest segment of each shard
     * @param rangeIndex     the range to match; at or past a shard's number of ranges it matches nothing there
     * @param steps          counters shared by the builders of one source
     */
    public TsidRangeQueryBuilder(@Nullable QueryBuilder next, int seriesPerRange, int rangeIndex, TsidRangeQuery.Steps steps) {
        if (seriesPerRange <= 0) {
            throw new IllegalArgumentException("series per range must be positive, got [" + seriesPerRange + "]");
        }
        if (rangeIndex < 0) {
            throw new IllegalArgumentException("range index must not be negative, got [" + rangeIndex + "]");
        }
        this.next = next;
        this.seriesPerRange = seriesPerRange;
        this.rangeIndex = rangeIndex;
        this.steps = steps;
    }

    @Override
    protected void doWriteTo(StreamOutput out) {
        throw new UnsupportedOperationException("Not serialized");
    }

    @Nullable
    public QueryBuilder next() {
        return next;
    }

    public int seriesPerRange() {
        return seriesPerRange;
    }

    public int rangeIndex() {
        return rangeIndex;
    }

    public TsidRangeQuery.Steps steps() {
        return steps;
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("Not serialized");
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        throw new UnsupportedOperationException("Not serialized");
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field("series_per_range", seriesPerRange);
        builder.field("range", rangeIndex);
        if (next != null) {
            builder.field("next", next, params);
        }
        builder.endObject();
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        if (next == null) {
            return this;
        }
        QueryBuilder rewritten = next.rewrite(queryRewriteContext);
        if (rewritten instanceof MatchNoneQueryBuilder) {
            return rewritten;
        }
        if (rewritten == next) {
            return this;
        }
        return new TsidRangeQueryBuilder(rewritten, seriesPerRange, rangeIndex, steps);
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context, MaxClauseCountQueryVisitor visitor) throws IOException {
        if (context.getIndexReader() == null) {
            throw new IllegalStateException("[" + NAME + "] needs the shard's index reader to resolve its range");
        }
        TsidRangeQuery range = TsidRangePartitioner.range(context.getIndexReader(), seriesPerRange, rangeIndex, steps);
        range.visit(visitor);
        if (next == null) {
            return range;
        }
        // The source query stays a scoring clause, so what is left once the slice queue strips the range scores as it did uncut.
        return new BooleanQuery.Builder().add(next.toQuery(context, visitor), BooleanClause.Occur.MUST)
            .add(range, BooleanClause.Occur.FILTER)
            .build();
    }

    @Override
    protected boolean doEquals(TsidRangeQueryBuilder other) {
        return Objects.equals(next, other.next) && seriesPerRange == other.seriesPerRange && rangeIndex == other.rangeIndex;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(next, seriesPerRange, rangeIndex);
    }
}
