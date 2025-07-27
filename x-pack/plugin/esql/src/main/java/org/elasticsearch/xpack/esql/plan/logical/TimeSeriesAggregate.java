/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * An extension of {@link Aggregate} to perform time-series aggregation per time-series, such as rate or _over_time.
 * The grouping must be `_tsid` and `tbucket` or just `_tsid`.
 */
public class TimeSeriesAggregate extends Aggregate {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        LogicalPlan.class,
        "TimeSeriesAggregate",
        TimeSeriesAggregate::new
    );

    private final Bucket timeBucket;
    private final boolean singleStep;

    public TimeSeriesAggregate(
        Source source,
        LogicalPlan child,
        List<Expression> groupings,
        List<? extends NamedExpression> aggregates,
        boolean singleStep,
        Bucket timeBucket
    ) {
        super(source, child, groupings, aggregates);
        this.singleStep = singleStep;
        this.timeBucket = timeBucket;
    }

    public TimeSeriesAggregate(StreamInput in) throws IOException {
        super(in);
        this.singleStep = in.readBoolean();
        this.timeBucket = in.readOptionalWriteable(inp -> (Bucket) Bucket.ENTRY.reader.read(inp));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(singleStep);
        out.writeOptionalWriteable(timeBucket);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<Aggregate> info() {
        return NodeInfo.create(this, TimeSeriesAggregate::new, child(), groupings, aggregates, singleStep, timeBucket);
    }

    @Override
    public TimeSeriesAggregate replaceChild(LogicalPlan newChild) {
        return new TimeSeriesAggregate(source(), newChild, groupings, aggregates, singleStep, timeBucket);
    }

    @Override
    public TimeSeriesAggregate with(LogicalPlan child, List<Expression> newGroupings, List<? extends NamedExpression> newAggregates) {
        return new TimeSeriesAggregate(source(), child, newGroupings, newAggregates, singleStep, timeBucket);
    }

    @Override
    public boolean expressionsResolved() {
        return super.expressionsResolved() && (timeBucket == null || timeBucket.resolved());
    }

    public boolean singleStep() {
        return singleStep;
    }

    @Override
    public boolean shouldBreak() {
        return singleStep == false;
    }

    @Nullable
    public Bucket timeBucket() {
        return timeBucket;
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupings, aggregates, child(), singleStep, timeBucket);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        TimeSeriesAggregate other = (TimeSeriesAggregate) obj;
        return Objects.equals(groupings, other.groupings)
            && Objects.equals(aggregates, other.aggregates)
            && Objects.equals(child(), other.child())
            && singleStep == other.singleStep
            && Objects.equals(timeBucket, other.timeBucket);
    }
}
