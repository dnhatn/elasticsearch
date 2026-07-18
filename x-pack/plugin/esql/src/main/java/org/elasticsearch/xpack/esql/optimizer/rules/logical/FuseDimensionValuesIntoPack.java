/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.expression.function.aggregate.DimensionValues;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.PackDimensions;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Moves the dimension pack onto the data nodes.
 */
public final class FuseDimensionValuesIntoPack extends OptimizerRules.ParameterizedOptimizerRule<PackDimensions, LogicalOptimizerContext> {

    public FuseDimensionValuesIntoPack() {
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    protected LogicalPlan rule(PackDimensions pack, LogicalOptimizerContext context) {
        if (context.minimumVersion().supports(PackDimensions.PACK_DIMENSIONS_ON_DATA_NODE) == false) {
            return pack;
        }
        if (pack.child() instanceof TimeSeriesAggregate tsAgg) {
            Set<NameId> packedDimIds = new HashSet<>();
            for (Attribute d : pack.dimensions()) {
                packedDimIds.add(d.id());
            }
            var source = pack.source();
            Attribute packedRaw = PackDimensions.packedAttribute(source);
            Alias packedValues = new Alias(
                source,
                pack.packedAttribute().name(),
                new DimensionValues(source, packedRaw),
                pack.packedAttribute().id()
            );
            List<Attribute> rawFields = new ArrayList<>(pack.dimensions().size());
            List<NamedExpression> newAggs = new ArrayList<>(tsAgg.aggregates().size());
            boolean inserted = false;
            for (NamedExpression agg : tsAgg.aggregates()) {
                if (agg instanceof Alias a
                    && packedDimIds.contains(a.id())
                    && Alias.unwrap(a) instanceof DimensionValues dv
                    && dv.field() instanceof Attribute field) {
                    rawFields.add(field);
                    if (inserted == false) {
                        newAggs.add(packedValues);
                        inserted = true;
                    }
                } else {
                    newAggs.add(agg);
                }
            }
            if (rawFields.size() == pack.dimensions().size()) {
                PackDimensions preAggPack = new PackDimensions(source, tsAgg.child(), rawFields, packedRaw);
                return tsAgg.with(preAggPack, tsAgg.groupings(), newAggs);
            }
        }
        return pack;
    }
}
