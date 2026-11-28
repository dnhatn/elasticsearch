/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.postoptimizer.LogicalPlanPostOptimizerRule;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.postoptimizer.SplitTimeSeriesByTsid;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.List;

/**
 * Applies the rules that need the optimized logical plan and may do async work, such as cost-based rewrites that read
 * statistics from the data nodes.
 */
public class LogicalPlanPostOptimizer {
    private final List<LogicalPlanPostOptimizerRule> rules;

    public LogicalPlanPostOptimizer(LogicalPostOptimizerContext context) {
        this(List.of(new SplitTimeSeriesByTsid(context)));
    }

    LogicalPlanPostOptimizer(List<LogicalPlanPostOptimizerRule> rules) {
        this.rules = rules;
    }

    public void postOptimize(LogicalPlan plan, ActionListener<LogicalPlan> listener) {
        if (plan.optimized() == false) {
            listener.onFailure(new IllegalStateException("Expected optimized plan"));
            return;
        }
        SubscribableListener<LogicalPlan> chain = SubscribableListener.newSucceeded(plan);
        for (LogicalPlanPostOptimizerRule rule : rules) {
            chain = chain.andThen((l, p) -> rule.apply(p, l));
        }
        chain.addListener(listener.delegateFailureAndWrap((l, p) -> {
            p.setOptimized();
            l.onResponse(p);
        }));
    }
}
