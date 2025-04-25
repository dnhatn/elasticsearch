/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.NodeUtils;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Similar to {@link EsQueryExec}, but this is a physical plan specifically for time series indices.
 * This plan is forked from {@link EsQueryExec} to allow field extractions, leveraging caching
 * and avoiding the cost of sorting and rebuilding blocks.
 */
public class TimeSeriesSourceExec extends LeafExec implements EstimatesRowSize {
    private final List<Attribute> attributesToExtract;
    private final FieldExtractExec.ExtractPreferences extractPreferences;
    private final List<Attribute> attrs;
    private final QueryBuilder query;
    private final Expression limit;
    private final Integer estimatedRowSize;
    private List<Attribute> lazyOutput;

    public TimeSeriesSourceExec(
        Source source,
        List<Attribute> attrs,
        QueryBuilder query,
        Expression limit,
        FieldExtractExec.ExtractPreferences extractPreferences,
        List<Attribute> attributesToExtract,
        Integer estimatedRowSize
    ) {
        super(source);
        this.attrs = attrs;
        this.query = query;
        this.limit = limit;
        this.extractPreferences = extractPreferences;
        this.attributesToExtract = attributesToExtract;
        this.estimatedRowSize = estimatedRowSize;
        if (this.attributesToExtract.isEmpty()) {
            lazyOutput = attrs;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("local plan");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("local plan");
    }

    @Override
    protected NodeInfo<TimeSeriesSourceExec> info() {
        return NodeInfo.create(
            this,
            TimeSeriesSourceExec::new,
            attrs,
            query,
            limit,
            extractPreferences,
            attributesToExtract,
            estimatedRowSize
        );
    }

    public QueryBuilder query() {
        return query;
    }

    public List<Attribute> attrs() {
        return attrs;
    }

    @Override
    public List<Attribute> output() {
        if (lazyOutput == null) {
            lazyOutput = new ArrayList<>(attrs.size() + attributesToExtract.size());
            lazyOutput.addAll(attrs);
            lazyOutput.addAll(attributesToExtract);
        }
        return lazyOutput;
    }

    @Override
    protected AttributeSet computeReferences() {
        return super.computeReferences();
    }

    public Expression limit() {
        return limit;
    }

    public Integer estimatedRowSize() {
        return estimatedRowSize;
    }

    public FieldExtractExec.ExtractPreferences extractPreferences() {
        return extractPreferences;
    }

    public List<Attribute> attributesToExtract() {
        return attributesToExtract;
    }

    @Override
    public PhysicalPlan estimateRowSize(State state) {
        state.add(false, Integer.BYTES * 2);
        state.add(false, 22); // tsid
        state.add(false, 8); // timestamp
        state.add(false, attributesToExtract);
        int size = state.consumeAllFields(false);
        if (Objects.equals(this.estimatedRowSize, size)) {
            return this;
        } else {
            return new TimeSeriesSourceExec(source(), attrs, query, limit, extractPreferences, attributesToExtract, size);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(attrs, query, extractPreferences, attributesToExtract, limit, estimatedRowSize);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        TimeSeriesSourceExec other = (TimeSeriesSourceExec) obj;
        return Objects.equals(attrs, other.attrs)
            && Objects.equals(extractPreferences, other.extractPreferences)
            && Objects.equals(attributesToExtract, other.attributesToExtract)
            && Objects.equals(query, other.query)
            && Objects.equals(limit, other.limit)
            && Objects.equals(estimatedRowSize, other.estimatedRowSize);
    }

    @Override
    public String nodeString() {
        return nodeName()
            + "["
            + "query["
            + (query != null ? Strings.toString(query, false, true) : "")
            + "] attributes: ["
            + NodeUtils.limitedToString(attrs)
            + "], estimatedRowSize["
            + estimatedRowSize
            + "]";
    }
}
