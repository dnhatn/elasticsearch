/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical;

import org.elasticsearch.compute.lucene.query.TsidPrefixRange;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.plan.ExchangeRef;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.MergeExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.TimeSeriesSplitExec;
import org.elasticsearch.xpack.esql.querydsl.query.TsidPrefixRangeQueryBuilder;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.ArrayList;
import java.util.List;

/**
 * Expands each {@link TimeSeriesSplitExec} into a fork with one copy of its already planned child per range. Each copy reads
 * its range through the fragment filter and gets its own exchange. Copying after physical optimization keeps the planning
 * cost independent of the number of ranges.
 */
public class ExpandTimeSeriesSplits extends Rule<PhysicalPlan, PhysicalPlan> {
    @Override
    public PhysicalPlan apply(PhysicalPlan plan) {
        return plan.transformUp(TimeSeriesSplitExec.class, ExpandTimeSeriesSplits::expand);
    }

    static PhysicalPlan expand(TimeSeriesSplitExec split) {
        // without exactly one fragment to restrict, every branch would read all series and the outer aggregation would count them
        // once per branch
        if (split.child().collect(FragmentExec.class).size() != 1) {
            return split.child();
        }
        List<PhysicalPlan> branches = new ArrayList<>(split.ranges().size());
        for (TsidPrefixRange range : split.ranges()) {
            PhysicalPlan branch = split.child()
                .transformUp(
                    FragmentExec.class,
                    fragment -> new FragmentExec(
                        fragment.source(),
                        fragment.fragment(),
                        rangeFilter(range, fragment.esFilter()),
                        fragment.estimatedRowSize(),
                        fragment.isFromViewBranch()
                    ).withOrigin(fragment.planId())
                );
            branch.forEachDown(PhysicalPlan.class, PhysicalPlan::renewPlanId);
            branch.forEachDown(ExchangeExec.class, exchange -> exchange.withExchange(ExchangeRef.newExchange()));
            branches.add(branch);
        }
        return new MergeExec(split.source(), branches, split.output(), MergeExec.Kind.FORK);
    }

    private static QueryBuilder rangeFilter(TsidPrefixRange range, QueryBuilder fragmentFilter) {
        QueryBuilder filter = new TsidPrefixRangeQueryBuilder(range);
        return fragmentFilter == null ? filter : new BoolQueryBuilder().filter(filter).must(fragmentFilter);
    }
}
