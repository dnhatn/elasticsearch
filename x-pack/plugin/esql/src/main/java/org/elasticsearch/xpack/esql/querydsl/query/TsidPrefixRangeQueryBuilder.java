/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.querydsl.query;

import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.lucene.query.TsidPrefixRangeQuery;
import org.elasticsearch.compute.lucene.query.TsidRange;
import org.elasticsearch.index.query.LeafQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;

import java.io.IOException;

public final class TsidPrefixRangeQueryBuilder extends LeafQueryBuilder<TsidPrefixRangeQueryBuilder> {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        QueryBuilder.class,
        "esql_tsid_prefix_range",
        TsidPrefixRangeQueryBuilder::new
    );

    private final TsidRange range;

    public TsidPrefixRangeQueryBuilder(TsidRange range) {
        this.range = range;
    }

    private TsidPrefixRangeQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.range = new TsidRange(in);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        range.writeTo(out);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(ENTRY.name);
        builder.field("bits", range.bits());
        builder.field("lower", range.lower());
        builder.field("upper", range.upper());
        builder.endObject();
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) {
        return new TsidPrefixRangeQuery(range);
    }

    @Override
    protected boolean doEquals(TsidPrefixRangeQueryBuilder other) {
        return range.equals(other.range);
    }

    @Override
    protected int doHashCode() {
        return range.hashCode();
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return EsRelation.TSID_SPLITS;
    }
}
