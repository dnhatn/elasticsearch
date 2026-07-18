/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.capabilities.Resolvables;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Unpacks a single packed dimension column (produced by the {@code PackDimensionValues} aggregation) back into the
 * original {@code unpackedDimensions} columns. Maps to {@code UnpackDimensionValuesExec} / {@code UnpackDimensionValuesOperator}
 * which emits N blocks directly (no intermediate composite block). Local-only, so it is never serialized.
 */
public class UnpackDimensionValues extends UnaryPlan {

    private final Attribute packed;
    private final List<Attribute> unpackedDimensions;
    private List<Attribute> lazyOutput;

    public UnpackDimensionValues(Source source, LogicalPlan child, Attribute packed, List<Attribute> unpackedDimensions) {
        super(source, child);
        this.packed = packed;
        this.unpackedDimensions = unpackedDimensions;
    }

    public Attribute packed() {
        return packed;
    }

    public List<Attribute> unpackedDimensions() {
        return unpackedDimensions;
    }

    @Override
    protected AttributeSet computeReferences() {
        return packed.references();
    }

    @Override
    public List<Attribute> output() {
        if (lazyOutput == null) {
            List<Attribute> out = new ArrayList<>(child().output().size() + unpackedDimensions.size());
            out.addAll(child().output());
            out.addAll(unpackedDimensions);
            lazyOutput = out;
        }
        return lazyOutput;
    }

    @Override
    public boolean expressionsResolved() {
        return packed.resolved() && Resolvables.resolved(unpackedDimensions);
    }

    @Override
    public UnpackDimensionValues replaceChild(LogicalPlan newChild) {
        return new UnpackDimensionValues(source(), newChild, packed, unpackedDimensions);
    }

    @Override
    protected NodeInfo<UnpackDimensionValues> info() {
        return NodeInfo.create(this, UnpackDimensionValues::new, child(), packed, unpackedDimensions);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("UnpackDimensionValues is local only and not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("UnpackDimensionValues is local only and not serialized");
    }

    @Override
    public boolean skipTelemetry() {
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(packed, unpackedDimensions, child());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        UnpackDimensionValues other = (UnpackDimensionValues) obj;
        return Objects.equals(packed, other.packed)
            && Objects.equals(unpackedDimensions, other.unpackedDimensions)
            && Objects.equals(child(), other.child());
    }
}
