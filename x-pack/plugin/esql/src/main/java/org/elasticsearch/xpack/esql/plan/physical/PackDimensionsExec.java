/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Packs the {@code dimensions} columns into a single {@code packedAttribute} {@code BytesRef} column (appended to the
 * output) so a multi-column grouping key can be replaced by one packed key. Split back with {@link UnpackDimensionValuesExec}.
 */
public class PackDimensionsExec extends UnaryExec {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "PackDimensionsExec",
        PackDimensionsExec::new
    );

    private final List<Attribute> dimensions;
    private final Attribute packedAttribute;
    private List<Attribute> lazyOutput;

    public PackDimensionsExec(Source source, PhysicalPlan child, List<Attribute> dimensions, Attribute packedAttribute) {
        super(source, child);
        this.dimensions = dimensions;
        this.packedAttribute = packedAttribute;
    }

    private PackDimensionsExec(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(PhysicalPlan.class),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            in.readNamedWriteable(Attribute.class)
        );
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
            lazyOutput = CollectionUtils.appendToCopy(child().output(), packedAttribute);
        }
        return lazyOutput;
    }

    @Override
    public UnaryExec replaceChild(PhysicalPlan newChild) {
        return new PackDimensionsExec(source(), newChild, dimensions, packedAttribute);
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, PackDimensionsExec::new, child(), dimensions, packedAttribute);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeNamedWriteableCollection(dimensions);
        out.writeNamedWriteable(packedAttribute);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
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
        PackDimensionsExec other = (PackDimensionsExec) obj;
        return Objects.equals(dimensions, other.dimensions)
            && Objects.equals(packedAttribute, other.packedAttribute)
            && Objects.equals(child(), other.child());
    }
}
