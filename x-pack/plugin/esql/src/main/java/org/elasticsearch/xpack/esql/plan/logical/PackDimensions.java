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
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Packs the {@code dimensions} columns into a single {@code packedAttribute} {@code BytesRef}, appended to the output,
 * so a multi-column grouping key can be grouped as one packed key. Maps to {@code PackDimensionsExec} /
 * {@code PackDimensionsOperator}. Local-only, so it is never serialized.
 */
public class PackDimensions extends UnaryPlan {

    private final List<Attribute> dimensions;
    private final Attribute packedAttribute;
    private List<Attribute> lazyOutput;

    public PackDimensions(Source source, LogicalPlan child, List<Attribute> dimensions, Attribute packedAttribute) {
        super(source, child);
        this.dimensions = dimensions;
        this.packedAttribute = packedAttribute;
    }

    public List<Attribute> dimensions() {
        return dimensions;
    }

    public Attribute packedAttribute() {
        return packedAttribute;
    }

    @Override
    protected AttributeSet computeReferences() {
        return AttributeSet.of(dimensions);
    }

    @Override
    public List<Attribute> output() {
        if (lazyOutput == null) {
            lazyOutput = CollectionUtils.combine(child().output(), packedAttribute);
        }
        return lazyOutput;
    }

    @Override
    public boolean expressionsResolved() {
        return Resolvables.resolved(dimensions) && packedAttribute.resolved();
    }

    @Override
    public PackDimensions replaceChild(LogicalPlan newChild) {
        return new PackDimensions(source(), newChild, dimensions, packedAttribute);
    }

    @Override
    protected NodeInfo<PackDimensions> info() {
        return NodeInfo.create(this, PackDimensions::new, child(), dimensions, packedAttribute);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("PackDimensions is local only and not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("PackDimensions is local only and not serialized");
    }

    @Override
    public boolean internalNode() {
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(dimensions, packedAttribute, child());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        PackDimensions other = (PackDimensions) obj;
        return Objects.equals(dimensions, other.dimensions)
            && Objects.equals(packedAttribute, other.packedAttribute)
            && Objects.equals(child(), other.child());
    }
}
