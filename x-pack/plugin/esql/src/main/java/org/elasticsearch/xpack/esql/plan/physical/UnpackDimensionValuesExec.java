/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Counterpart of {@link PackDimensionsExec}: unpacks a single packed dimension column ({@code packedAttribute}) back
 * into the {@code dimensionAttributes} columns with one operator (no intermediate composite block). Local-only
 * (never serialized): inserted by the translator and planned on the same node.
 */
public class UnpackDimensionValuesExec extends UnaryExec {

    private final Attribute packedAttribute;
    private final List<Attribute> dimensionAttributes;
    private List<Attribute> lazyOutput;

    public UnpackDimensionValuesExec(Source source, PhysicalPlan child, Attribute packedAttribute, List<Attribute> dimensionAttributes) {
        super(source, child);
        this.packedAttribute = packedAttribute;
        this.dimensionAttributes = dimensionAttributes;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("UnpackDimensionValuesExec should be used local only");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("UnpackDimensionValuesExec should be used local only");
    }

    public Attribute packedAttribute() {
        return packedAttribute;
    }

    public List<Attribute> dimensionAttributes() {
        return dimensionAttributes;
    }

    @Override
    protected AttributeSet computeReferences() {
        return AttributeSet.of(packedAttribute);
    }

    @Override
    public List<Attribute> output() {
        if (lazyOutput == null) {
            // The packed column is kept in the output (the final ProjectExec/OutputExec selects the user's columns
            // and drops it); the unpacked dimension columns are appended.
            List<Attribute> childOutput = child().output();
            lazyOutput = new ArrayList<>(childOutput.size() + dimensionAttributes.size());
            lazyOutput.addAll(childOutput);
            lazyOutput.addAll(dimensionAttributes);
        }
        return lazyOutput;
    }

    @Override
    public UnaryExec replaceChild(PhysicalPlan newChild) {
        return new UnpackDimensionValuesExec(source(), newChild, packedAttribute, dimensionAttributes);
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, UnpackDimensionValuesExec::new, child(), packedAttribute, dimensionAttributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(packedAttribute, dimensionAttributes, child());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        UnpackDimensionValuesExec other = (UnpackDimensionValuesExec) obj;
        return Objects.equals(packedAttribute, other.packedAttribute)
            && Objects.equals(dimensionAttributes, other.dimensionAttributes)
            && Objects.equals(child(), other.child());
    }
}
