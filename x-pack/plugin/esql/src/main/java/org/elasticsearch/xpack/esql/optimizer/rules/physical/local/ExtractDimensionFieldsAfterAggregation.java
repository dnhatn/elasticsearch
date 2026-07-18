/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.TimeSeriesMetadataAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.FunctionEsField;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.DimensionValues;
import org.elasticsearch.xpack.esql.expression.function.aggregate.FirstDocId;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.PackDimensionsExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.TimeSeriesAggregateExec;
import org.elasticsearch.xpack.esql.planner.AggregateMapper;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A rule that moves `VALUES(dimension-field)` aggregations in time-series aggregations
 * to execute after the aggregation, reading the dimension fields once each group.
 * This is possible because dimension field values for `_tsid` are identical across all
 * documents in the same time-series.
 * For example:
 * `TS .. | STATS sum(rate(r1)), sum(rate(r2)) BY cluster, host, tbucket(1m)`
 * without this rule
 * `TS ..
 * | EXTRACT_FIELDS(r1,r2,cluster, host)
 * | STATS rate(r1), rate(r2), VALUES(cluster), VALUES(host) BY _tsid, tbucket(1m)`
 * with this rule
 * `TS ..
 * | EXTRACT_FIELDS(r1,r2)
 * | STATS rate(r1), rate(r2), FIRST_DOC_ID(_doc) BY _tsid, tbucket(1m)
 * | EXTRACT_FIELDS(cluster, host)
 * | ...
 */
public final class ExtractDimensionFieldsAfterAggregation extends PhysicalOptimizerRules.ParameterizedOptimizerRule<
    PhysicalPlan,
    LocalPhysicalOptimizerContext> {

    @Override
    public PhysicalPlan rule(PhysicalPlan plan, LocalPhysicalOptimizerContext context) {
        if (plan instanceof TimeSeriesAggregateExec oldAgg && oldAgg.getMode() == AggregatorMode.INITIAL) {
            return rule(oldAgg, context);
        }
        return plan;
    }

    private PhysicalPlan rule(TimeSeriesAggregateExec oldAgg, LocalPhysicalOptimizerContext context) {
        AttributeSet inputAttributes = oldAgg.inputSet();
        var sourceAttr = inputAttributes.stream().filter(EsQueryExec::isDocAttribute).findFirst().orElse(null);
        if (sourceAttr == null) {
            return oldAgg;
        }
        PackDimensionsExec preAggPack = oldAgg.child() instanceof PackDimensionsExec p ? p : null;
        Attribute packedAttr = preAggPack != null ? preAggPack.packedAttribute() : null;

        List<NamedExpression> newAggregates = new ArrayList<>();
        List<Attribute> dimensionFields = new ArrayList<>();
        List<Alias> aliases = new ArrayList<>();
        // For the packed path: the raw fields to extract after the aggregation, and the alias that re-packs them into the
        // DimensionValues intermediate.
        List<Attribute> packRawFields = null;
        Alias packAlias = null;
        Set<AggregateFunction> seen = new HashSet<>();
        List<Attribute> oldIntermediates = oldAgg.intermediateAttributes();
        List<Attribute> newIntermediates = new ArrayList<>(oldIntermediates.subList(0, oldAgg.groupings().size()));
        int intermediateOffset = oldAgg.groupings().size();
        for (var agg : oldAgg.aggregates()) {
            Attribute dimensionField = null;
            boolean packedDim = false;
            if (Alias.unwrap(agg) instanceof AggregateFunction af) {
                if (packedAttr != null
                    && af instanceof DimensionValues dv
                    && dv.hasFilter() == false
                    && dv.field() instanceof Attribute fieldAttr
                    && packedAttr.semanticEquals(fieldAttr)) {
                    packedDim = true;
                } else {
                    dimensionField = valuesOfDimensionField(af, inputAttributes);
                }
                if (seen.add(af)) {
                    int size = intermediateStateSize(af);
                    if (packedDim) {
                        if (size != 1) {
                            throw new IllegalStateException("expected one intermediate attribute for [" + af + "] but got [" + size + "]");
                        }
                        Attribute oldAttr = oldIntermediates.get(intermediateOffset);
                        packRawFields = new ArrayList<>(preAggPack.dimensions());
                        // Re-packing produces the same packed attribute, aliased to the DimensionValues intermediate.
                        packAlias = new Alias(agg.source(), agg.name(), packedAttr, oldAttr.id());
                    } else if (dimensionField != null) {
                        if (size != 1) {
                            throw new IllegalStateException("expected one intermediate attribute for [" + af + "] but got [" + size + "]");
                        }
                        Attribute oldAttr = oldIntermediates.get(intermediateOffset);
                        if (dimensionField instanceof TimeSeriesMetadataAttribute timeSeriesMetadataAttribute) {
                            var withoutFields = timeSeriesMetadataAttribute.excludedFields();
                            var sourceField = new TimeSeriesMetadataAttribute(
                                dimensionField.source(),
                                null,
                                dimensionField.qualifier(),
                                dimensionField.name(),
                                new FunctionEsField(
                                    new EsField(
                                        SourceFieldMapper.NAME,
                                        DataType.KEYWORD,
                                        Map.of(),
                                        false,
                                        EsField.TimeSeriesFieldType.DIMENSION
                                    ),
                                    DataType.KEYWORD,
                                    new BlockLoaderFunctionConfig.TimeSeriesMetadata(false, withoutFields)
                                ),
                                dimensionField.nullable(),
                                null,
                                true,
                                withoutFields
                            );
                            aliases.add(new Alias(agg.source(), agg.name(), sourceField, oldAttr.id()));
                        } else {
                            aliases.add(new Alias(agg.source(), agg.name(), dimensionField, oldAttr.id()));
                            dimensionFields.add(dimensionField);
                        }
                    } else {
                        for (int i = 0; i < size; i++) {
                            newIntermediates.add(oldIntermediates.get(intermediateOffset + i));
                        }
                    }
                    intermediateOffset += size;
                }
            }
            if (dimensionField == null && packedDim == false) {
                newAggregates.add(agg);
            }
        }
        if (aliases.isEmpty() && packAlias == null) {
            return oldAgg;
        }
        // Drop the pre-agg pack (its child feeds the aggregation directly) when we re-pack after the aggregation.
        PhysicalPlan aggChild = packAlias != null ? preAggPack.child() : oldAgg.child();
        newIntermediates.add(new ReferenceAttribute(oldAgg.source(), sourceAttr.qualifier(), sourceAttr.name(), sourceAttr.dataType()));
        newAggregates.add(new Alias(oldAgg.source(), sourceAttr.name(), new FirstDocId(oldAgg.source(), sourceAttr)));
        TimeSeriesAggregateExec newStats = new TimeSeriesAggregateExec(
            oldAgg.source(),
            aggChild,
            oldAgg.groupings(),
            newAggregates,
            oldAgg.getMode(),
            newIntermediates,
            oldAgg.estimatedRowSize(),
            oldAgg.timeBucket(),
            oldAgg.outputTimeBucket()
        );
        List<Attribute> extract = new ArrayList<>(dimensionFields);
        if (packRawFields != null) {
            extract.addAll(packRawFields);
        }
        PhysicalPlan afterStats = newStats;
        if (extract.isEmpty() == false) {
            afterStats = new FieldExtractExec(
                oldAgg.source(),
                afterStats,
                extract,
                context.configuration().pragmas().fieldExtractPreference()
            );
        }
        if (packRawFields != null) {
            // Re-pack the first-doc raw fields into the packed dimension column.
            afterStats = new PackDimensionsExec(oldAgg.source(), afterStats, packRawFields, packedAttr);
        }
        List<Alias> allAliases = new ArrayList<>(aliases);
        if (packAlias != null) {
            allAliases.add(packAlias);
        }
        EvalExec evalExec = new EvalExec(oldAgg.source(), afterStats, allAliases);
        return new ProjectExec(oldAgg.source(), evalExec, oldIntermediates);
    }

    private static Attribute valuesOfDimensionField(AggregateFunction af, AttributeSet inputAttributes) {
        if (af instanceof DimensionValues values && values.hasFilter() == false && values.field() instanceof Attribute attr) {
            if (inputAttributes.contains(attr) == false || attr instanceof TimeSeriesMetadataAttribute) {
                return attr;
            }
        }
        return null;
    }

    private static int intermediateStateSize(AggregateFunction af) {
        return AggregateMapper.intermediateStateDesc(af, true).size();
    }

}
