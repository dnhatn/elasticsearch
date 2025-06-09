/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;

/**
 * Internal aggregate function that collects values of a user-specified group in time-series aggregation.
 *
 * @see org.elasticsearch.xpack.esql.optimizer.rules.logical.TranslateTimeSeriesAggregate
 */
public class TimeSeriesGroupingValues extends AggregateFunction implements ToAggregator {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "TimeSeriesGroupingValues",
        TimeSeriesGroupingValues::new
    );

    public TimeSeriesGroupingValues(Source source, Expression field) {
        this(source, field, Literal.TRUE);
    }

    public TimeSeriesGroupingValues(Source source, Expression field, Expression filter) {
        super(source, field, filter, emptyList());
    }

    private TimeSeriesGroupingValues(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<TimeSeriesGroupingValues> info() {
        return NodeInfo.create(this, TimeSeriesGroupingValues::new, field(), filter());
    }

    @Override
    public TimeSeriesGroupingValues replaceChildren(List<Expression> newChildren) {
        return new TimeSeriesGroupingValues(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    public TimeSeriesGroupingValues withFilter(Expression filter) {
        return new TimeSeriesGroupingValues(source(), field(), filter);
    }

    @Override
    public DataType dataType() {
        return field().dataType().noText();
    }

    @Override
    protected TypeResolution resolveType() {
        return TypeResolutions.isType(field(), Values.SUPPLIERS::containsKey, sourceText(), DEFAULT, "any type except unsigned_long");
    }

    @Override
    public AggregatorFunctionSupplier supplier() {
        DataType type = field().dataType();
        if (Values.SUPPLIERS.containsKey(type) == false) {
            // If the type checking did its job, this should never happen
            throw EsqlIllegalArgumentException.illegalDataType(type);
        }
        return Values.SUPPLIERS.get(type).get();
    }
}
