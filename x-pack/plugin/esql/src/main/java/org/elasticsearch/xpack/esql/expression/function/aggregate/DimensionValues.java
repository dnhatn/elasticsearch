/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.io.stream.VersionedExpression;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.emptyList;

/**
 * A specialization of {@link Values} for collecting dimension fields in time-series queries.
 */
public class DimensionValues extends AggregateFunction implements ToAggregator, VersionedExpression {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "DimensionValues",
        DimensionValues::new
    );

    static final TransportVersion DIMENSION_VALUES_VERSION = TransportVersion.fromName("dimension_values");

    public DimensionValues(Source source, Expression field, Expression filter) {
        super(source, field, filter, emptyList());
    }

    private DimensionValues(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public NamedWriteable getVersionedNamedWriteable(TransportVersion transportVersion) {
        if (transportVersion.onOrAfter(DIMENSION_VALUES_VERSION)) {
            return this;
        } else {
            // fallback to VALUES
            return new Values(source(), field(), filter());
        }
    }

    @Override
    protected NodeInfo<DimensionValues> info() {
        return NodeInfo.create(this, DimensionValues::new, field(), filter());
    }

    @Override
    public DimensionValues replaceChildren(List<Expression> newChildren) {
        return new DimensionValues(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    public DimensionValues withFilter(Expression filter) {
        return new DimensionValues(source(), field(), filter);
    }

    @Override
    public DataType dataType() {
        return field().dataType().noText();
    }

    @Override
    protected TypeResolution resolveType() {
        return new Values(source(), field(), filter()).resolveType();
    }

    @Override
    public AggregatorFunctionSupplier supplier() {
        // TODO: link new implementation
        return new Values(source(), field(), filter()).supplier();
    }
}
