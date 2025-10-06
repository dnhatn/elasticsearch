/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;
import org.elasticsearch.xpack.esql.expression.function.ReferenceAttributeTests;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class DimensionValuesSerializationTests extends AbstractExpressionSerializationTests<DimensionValues> {
    @Override
    protected DimensionValues createTestInstance() {
        return new DimensionValues(randomSource(), randomChild(), randomChild());
    }

    @Override
    protected DimensionValues mutateInstance(DimensionValues instance) throws IOException {
        return new DimensionValues(
            instance.source(),
            randomValueOtherThan(instance.field(), AbstractExpressionSerializationTests::randomChild),
            randomValueOtherThan(instance.filter(), AbstractExpressionSerializationTests::randomChild)
        );
    }

    public void testBWC() throws Exception {
        DimensionValues dimensionValues = new DimensionValues(
            randomSource(),
            randomAttributeWithoutQualifier(),
            randomAttributeWithoutQualifier()
        );
        TransportVersion version = TransportVersionUtils.randomVersionBetween(
            random(),
            TransportVersion.minimumCompatible(),
            TransportVersionUtils.getPreviousVersion(DimensionValues.DIMENSION_VALUES_VERSION)
        );
        NamedWriteableRegistry namedWriteableRegistry = getNamedWriteableRegistry();
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.setTransportVersion(version);
            PlanStreamOutput planOut = new PlanStreamOutput(output, configuration());
            planOut.writeNamedWriteable(dimensionValues);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), namedWriteableRegistry)) {
                PlanStreamInput planIn = new PlanStreamInput(in, namedWriteableRegistry, configuration());
                planIn.setTransportVersion(version);
                Expression oldValues = (Expression) planIn.readNamedWriteable(categoryClass());
                Values expected = new Values(dimensionValues.source(), dimensionValues.field(), dimensionValues.filter());
                assertThat(oldValues, equalTo(expected));
            }
        }
    }

    // can't serialize qualified attribute with older versions
    private Expression randomAttributeWithoutQualifier() {
        ReferenceAttribute e = ReferenceAttributeTests.randomReferenceAttribute(ESTestCase.randomBoolean());
        return new ReferenceAttribute(e.source(), null, e.name(), e.dataType(), e.nullable(), e.id(), e.synthetic());
    }
}
