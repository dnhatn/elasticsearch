/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.lucene.query.TsidPrefixRange;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Runs its child once per {@code _tsid} range, each reading only the series in its range. It stays a single node through
 * mapping and physical optimization, so the child is planned once; the physical plan expands it into a fork of one branch per
 * range just before execution.
 */
public class TimeSeriesSplit extends UnaryPlan {
    private final List<TsidPrefixRange> ranges;

    public TimeSeriesSplit(Source source, LogicalPlan child, List<TsidPrefixRange> ranges) {
        super(source, child);
        this.ranges = List.copyOf(ranges);
    }

    public List<TsidPrefixRange> ranges() {
        return ranges;
    }

    @Override
    public TimeSeriesSplit replaceChild(LogicalPlan newChild) {
        return new TimeSeriesSplit(source(), newChild, ranges);
    }

    @Override
    public boolean expressionsResolved() {
        return true;
    }

    @Override
    protected NodeInfo<TimeSeriesSplit> info() {
        return NodeInfo.create(this, TimeSeriesSplit::new, child(), ranges);
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
        return super.equals(obj) && ranges.equals(((TimeSeriesSplit) obj).ranges);
    }
}
