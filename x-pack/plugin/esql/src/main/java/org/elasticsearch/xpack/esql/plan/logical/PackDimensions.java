/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.capabilities.Resolvables;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Packs the {@code dimensions} columns into a single {@code packedAttribute} {@code BytesRef}, appended to the output,
 * so a multi-column grouping key can be grouped as one packed key. Maps to {@code PackDimensionsExec} /
 * {@code PackDimensionsOperator}.
 */
public class PackDimensions extends UnaryPlan {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        LogicalPlan.class,
        "PackDimensions",
        PackDimensions::new
    );

    public static final String PACKED_FIELD_NAME = "_$packed_dims";

    /**
     * Gates moving the pack onto the data nodes (a serialized {@code PackDimensions} in the data-node fragment feeding a
     * single {@code DimensionValues}). Older nodes cannot deserialize the node, so the fusion rule only fires when the
     * whole cluster supports this; otherwise the pack stays coordinator-side (never serialized).
     */
    public static final TransportVersion PACK_DIMENSIONS_ON_DATA_NODE = TransportVersion.fromName("pack_dimensions_on_data_node");

    private final List<Attribute> dimensions;
    private final Attribute packedAttribute;
    private List<Attribute> lazyOutput;

    /**
     * Builds the synthetic output attribute of the pack: a dimension {@link FieldAttribute} (so it flows through the
     * dimension-aware read/aggregation paths) named {@link #PACKED_FIELD_NAME} holding the packed {@code BytesRef}.
     */
    public static FieldAttribute packedAttribute(Source source) {
        EsField field = new EsField(PACKED_FIELD_NAME, DataType.KEYWORD, Map.of(), false, EsField.TimeSeriesFieldType.DIMENSION);
        return new FieldAttribute(source, null, null, PACKED_FIELD_NAME, field, Nullability.TRUE, new NameId(), true);
    }

    public PackDimensions(Source source, LogicalPlan child, List<Attribute> dimensions, Attribute packedAttribute) {
        super(source, child);
        this.dimensions = dimensions;
        this.packedAttribute = packedAttribute;
    }

    private PackDimensions(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(LogicalPlan.class),
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
    public boolean skipTelemetry() {
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
