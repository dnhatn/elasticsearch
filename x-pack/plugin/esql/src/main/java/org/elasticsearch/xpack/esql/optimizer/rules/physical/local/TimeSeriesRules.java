/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.OptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.TimeSeriesSourceExec;

import java.util.List;
import java.util.Set;

public class TimeSeriesRules {
    /**
     * Convert a {@link EsQueryExec} with time_series mode to a {@link TimeSeriesSourceExec}
     */
    public static class ConvertToTimeSeriesSource extends PhysicalOptimizerRules.ParameterizedOptimizerRule<
        EsQueryExec,
        LocalPhysicalOptimizerContext> {

        public ConvertToTimeSeriesSource() {
            super(OptimizerRules.TransformDirection.UP);
        }

        @Override
        public PhysicalPlan rule(EsQueryExec plan, LocalPhysicalOptimizerContext context) {
            if (plan.indexMode() == IndexMode.TIME_SERIES) {
                return new TimeSeriesSourceExec(
                    plan.source(),
                    plan.output(),
                    plan.query(),
                    plan.limit(),
                    new FieldExtractExec.ExtractPreferences(context.configuration().pragmas().fieldExtractPreference(), Set.of(), Set.of()),
                    List.of(),
                    plan.estimatedRowSize()
                );
            } else {
                return plan;
            }
        }
    }

    /**
     * Push down {@link FieldExtractExec} to time-series source to minimize the overhead.
     */
    public static class PushTimeFieldExtractionToSource extends PhysicalOptimizerRules.ParameterizedOptimizerRule<
        FieldExtractExec,
        LocalPhysicalOptimizerContext> {

        @Override
        public PhysicalPlan rule(FieldExtractExec plan, LocalPhysicalOptimizerContext context) {
            Holder<Boolean> added = new Holder<>(Boolean.FALSE);
            PhysicalPlan newChild = plan.child().transformDown(TimeSeriesSourceExec.class, ts -> {
                added.set(Boolean.TRUE);
                return addFieldExtract(ts, plan);
            });
            if (added.get()) {
                return newChild;
            } else {
                return plan;
            }
        }

        private TimeSeriesSourceExec addFieldExtract(TimeSeriesSourceExec ts, FieldExtractExec extract) {
            var docValuesAttributes = Sets.union(ts.extractPreferences().docValuesAttributes(), extract.docValuesAttributes());
            var boundsAttributes = Sets.union(ts.extractPreferences().boundsAttributes(), extract.boundsAttributes());
            var attributesToExtract = CollectionUtils.combine(ts.attributesToExtract(), extract.attributesToExtract());
            return new TimeSeriesSourceExec(
                ts.source(),
                ts.attrs(),
                ts.query(),
                ts.limit(),
                new FieldExtractExec.ExtractPreferences(ts.extractPreferences().defaultPreference(), docValuesAttributes, boundsAttributes),
                attributesToExtract,
                ts.estimatedRowSize()
            );
        }
    }

    /**
     * After field-extraction are moved to the {@link TimeSeriesSourceExec}, we then can drop the doc-attributes from the source.
     */
    public static class DropDocAttributeFromSource extends PhysicalOptimizerRules.ParameterizedOptimizerRule<
        TimeSeriesSourceExec,
        LocalPhysicalOptimizerContext> {

        public DropDocAttributeFromSource() {
            super(OptimizerRules.TransformDirection.UP);
        }

        @Override
        public PhysicalPlan rule(TimeSeriesSourceExec ts, LocalPhysicalOptimizerContext context) {
            var attributes = ts.attrs().stream().filter(a -> EsQueryExec.isSourceAttribute(a) == false).toList();
            return new TimeSeriesSourceExec(
                ts.source(),
                attributes,
                ts.query(),
                ts.limit(),
                ts.extractPreferences(),
                ts.attributesToExtract(),
                ts.estimatedRowSize()
            );
        }
    }
}
