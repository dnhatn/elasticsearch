/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.CompositeBlock;
import org.elasticsearch.compute.data.ConstantNullBlock;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class FromAggregateMetricDouble extends AbstractConvertFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "FromAggregateMetricDouble",
        FromAggregateMetricDouble::new
    );

    private final Metric metric;

    public FromAggregateMetricDouble(Source source, Expression field, Metric metric) {
        super(source, field);
        this.metric = metric;
    }

    private FromAggregateMetricDouble(StreamInput in) throws IOException {
        super(in);
        this.metric = Metric.fromBlockIndex(in.readVInt());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(children().get(0));
        out.writeVInt(metric.blockIndex);
    }

    @Override
    protected Map<DataType, BuildFactory> factories() {
        return Map.of(
            DataType.AGGREGATE_METRIC_DOUBLE,
            (field, source) -> driverContext -> new ExtractBlockEval(driverContext, field.get(driverContext), source, metric)
        );
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        return metric.dataType;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new FromAggregateMetricDouble(source(), field, metric);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, FromAggregateMetricDouble::new, field, metric);
    }

    @Override
    public Set<DataType> supportedTypes() {
        return Set.of(DataType.AGGREGATE_METRIC_DOUBLE);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        FromAggregateMetricDouble that = (FromAggregateMetricDouble) o;
        return metric == that.metric;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), metric);
    }

    private static class ExtractBlockEval extends AbstractConvertFunction.AbstractEvaluator {
        private final Metric metric;

        ExtractBlockEval(DriverContext driverContext, EvalOperator.ExpressionEvaluator field, Source source, Metric metric) {
            super(driverContext, field, source);
            this.metric = metric;
        }

        @Override
        protected String name() {
            return "FromAggregateMetricDouble";
        }

        @Override
        protected Block evalBlock(Block b) {
            if (b instanceof ConstantNullBlock nullBlock) {
                nullBlock.incRef();
                return nullBlock;
            }
            CompositeBlock compositeBlock = (CompositeBlock) b;
            Block oneBlock = compositeBlock.getBlock(metric.blockIndex);
            oneBlock.incRef();
            return oneBlock;
        }

        @Override
        protected Block evalVector(Vector v) {
            throw new UnsupportedOperationException("We don't have composite vector");
        }
    }

    public enum Metric {
        MIN(0, DataType.DOUBLE),
        MAX(1, DataType.DOUBLE),
        SUM(2, DataType.DOUBLE),
        VALUE_COUNT(3, DataType.INTEGER);

        final int blockIndex;
        final DataType dataType;

        Metric(int blockIndex, DataType dataType) {
            this.blockIndex = blockIndex;
            this.dataType = dataType;
        }

        static Metric fromBlockIndex(int blockIndex) {
            final Metric metric = switch (blockIndex) {
                case 0 -> MIN;
                case 1 -> MAX;
                case 2 -> SUM;
                case 3 -> VALUE_COUNT;
                default -> throw new IllegalArgumentException("invalid metric [" + blockIndex + "]");
            };
            assert metric.blockIndex == blockIndex : metric.blockIndex + " != " + blockIndex;
            return metric;
        }
    }
}
