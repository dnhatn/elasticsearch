/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.postoptimizer;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

/**
 * A rule applied to the optimized logical plan, before it is mapped. Unlike optimizer rules it may do async work, such as
 * collecting statistics from data nodes; no optimizer rule runs after it.
 */
public interface LogicalPlanPostOptimizerRule {
    void apply(LogicalPlan plan, ActionListener<LogicalPlan> listener);
}
