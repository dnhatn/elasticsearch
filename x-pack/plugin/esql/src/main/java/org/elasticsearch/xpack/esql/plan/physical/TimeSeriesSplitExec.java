/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.lucene.query.TsidPrefixRange;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesSplit;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * The physical form of {@link TimeSeriesSplit}, expanded into a fork of one branch per range at the end of physical
 * optimization.
 */
public class TimeSeriesSplitExec extends UnaryExec {
    private final List<TsidPrefixRange> ranges;

    public TimeSeriesSplitExec(Source source, PhysicalPlan child, List<TsidPrefixRange> ranges) {
        super(source, child);
        this.ranges = List.copyOf(ranges);
    }

    public List<TsidPrefixRange> ranges() {
        return ranges;
    }

    @Override
    public TimeSeriesSplitExec replaceChild(PhysicalPlan newChild) {
        return new TimeSeriesSplitExec(source(), newChild, ranges);
    }

    @Override
    protected NodeInfo<TimeSeriesSplitExec> info() {
        return NodeInfo.create(this, TimeSeriesSplitExec::new, child(), ranges);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("doesn't escape the coordinator node");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("doesn't escape the coordinator node");
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), ranges);
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj) && ranges.equals(((TimeSeriesSplitExec) obj).ranges);
    }
}
