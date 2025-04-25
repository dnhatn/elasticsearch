/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.NodeUtils;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class FieldExtractExec extends UnaryExec implements EstimatesRowSize {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "FieldExtractExec",
        FieldExtractExec::new
    );

    private final List<Attribute> attributesToExtract;
    private final @Nullable Attribute sourceAttribute;

    /**
     * Holds the extraction preferences for fields.
     *
     * @param defaultPreference   the default for if the plan doesn't require a preference.
     * @param docValuesAttributes attributes that may be extracted as doc values even if that makes them less accurate.
     *                            This is mostly used for geo fields which lose a lot of precision in their doc values,
     *                            but in some cases doc values provides <strong>enough</strong> precision to do the job.
     * @param boundsAttributes    Attributes of a shape whose extent can be extracted directly from the doc-values encoded geometry.
     */
    public record ExtractPreferences(
        MappedFieldType.FieldExtractPreference defaultPreference,
        Set<Attribute> docValuesAttributes,
        Set<Attribute> boundsAttributes
    ) {

        ExtractPreferences withDocValuesAttributes(Set<Attribute> docValuesAttributes) {
            return new ExtractPreferences(defaultPreference, docValuesAttributes, boundsAttributes);
        }

        ExtractPreferences withBoundsAttributes(Set<Attribute> boundsAttributes) {
            return new ExtractPreferences(defaultPreference, docValuesAttributes, boundsAttributes);
        }

        public MappedFieldType.FieldExtractPreference fieldExtractPreference(Attribute attr) {
            if (boundsAttributes.contains(attr)) {
                return MappedFieldType.FieldExtractPreference.EXTRACT_SPATIAL_BOUNDS;
            }
            if (docValuesAttributes.contains(attr)) {
                return MappedFieldType.FieldExtractPreference.DOC_VALUES;
            }
            return defaultPreference;
        }
    }

    private List<Attribute> lazyOutput;
    private final ExtractPreferences extractPreferences;

    public FieldExtractExec(
        Source source,
        PhysicalPlan child,
        List<Attribute> attributesToExtract,
        MappedFieldType.FieldExtractPreference defaultPreference
    ) {
        this(source, child, attributesToExtract, new ExtractPreferences(defaultPreference, Set.of(), Set.of()));
    }

    private FieldExtractExec(
        Source source,
        PhysicalPlan child,
        List<Attribute> attributesToExtract,
        ExtractPreferences extractPreferences
    ) {
        super(source, child);
        this.attributesToExtract = attributesToExtract;
        this.sourceAttribute = extractSourceAttributesFrom(child);
        this.extractPreferences = extractPreferences;
    }

    private FieldExtractExec(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(PhysicalPlan.class),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            MappedFieldType.FieldExtractPreference.NONE
        );
        // defaultPreference is only used on the data node and never serialized.
        // docValueAttributes and boundsAttributes are only used on the data node and never serialized.
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeNamedWriteableCollection(attributesToExtract());
        // defaultPreference is only used on the data node and never serialized.
        // docValueAttributes and boundsAttributes are only used on the data node and never serialized.
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public static @Nullable Attribute extractSourceAttributesFrom(PhysicalPlan plan) {
        for (Attribute attribute : plan.outputSet()) {
            if (EsQueryExec.isSourceAttribute(attribute)) {
                return attribute;
            }
        }
        return null;
    }

    @Override
    protected AttributeSet computeReferences() {
        return sourceAttribute != null ? AttributeSet.of(sourceAttribute) : AttributeSet.EMPTY;
    }

    @Override
    protected NodeInfo<FieldExtractExec> info() {
        return NodeInfo.create(this, FieldExtractExec::new, child(), attributesToExtract, extractPreferences);
    }

    @Override
    public UnaryExec replaceChild(PhysicalPlan newChild) {
        return new FieldExtractExec(source(), newChild, attributesToExtract, extractPreferences);
    }

    public FieldExtractExec withDocValuesAttributes(Set<Attribute> docValuesAttributes) {
        return new FieldExtractExec(
            source(),
            child(),
            attributesToExtract,
            extractPreferences.withDocValuesAttributes(docValuesAttributes)
        );
    }

    public FieldExtractExec withBoundsAttributes(Set<Attribute> boundsAttributes) {
        return new FieldExtractExec(source(), child(), attributesToExtract, extractPreferences.withBoundsAttributes(boundsAttributes));
    }

    public List<Attribute> attributesToExtract() {
        return attributesToExtract;
    }

    public @Nullable Attribute sourceAttribute() {
        return sourceAttribute;
    }

    public ExtractPreferences extractPreferences() {
        return extractPreferences;
    }

    public Set<Attribute> docValuesAttributes() {
        return extractPreferences.docValuesAttributes;
    }

    public Set<Attribute> boundsAttributes() {
        return extractPreferences.boundsAttributes;
    }

    @Override
    public List<Attribute> output() {
        if (lazyOutput == null) {
            List<Attribute> childOutput = child().output();
            lazyOutput = new ArrayList<>(childOutput.size() + attributesToExtract.size());
            lazyOutput.addAll(childOutput);
            lazyOutput.addAll(attributesToExtract);
        }

        return lazyOutput;
    }

    @Override
    public PhysicalPlan estimateRowSize(State state) {
        state.add(true, attributesToExtract);
        return this;
    }

    @Override
    public int hashCode() {
        return Objects.hash(attributesToExtract, extractPreferences, child());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        FieldExtractExec other = (FieldExtractExec) obj;
        return Objects.equals(attributesToExtract, other.attributesToExtract)
            && Objects.equals(extractPreferences, other.extractPreferences)
            && Objects.equals(child(), other.child());
    }

    @Override
    public String nodeString() {
        return Strings.format(
            "%s<%s,%s>",
            nodeName() + NodeUtils.limitedToString(attributesToExtract),
            extractPreferences.docValuesAttributes,
            extractPreferences.docValuesAttributes
        );
    }

    public MappedFieldType.FieldExtractPreference fieldExtractPreference(Attribute attr) {
        return extractPreferences.fieldExtractPreference(attr);
    }
}
