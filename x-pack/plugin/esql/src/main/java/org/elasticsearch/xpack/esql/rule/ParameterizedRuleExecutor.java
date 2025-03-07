/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.rule;

import org.elasticsearch.compute.ExecutionTime;
import org.elasticsearch.xpack.esql.core.tree.Node;

import java.util.function.Function;

public abstract class ParameterizedRuleExecutor<TreeType extends Node<TreeType>, Context> extends RuleExecutor<TreeType> {

    private final Context context;

    protected ParameterizedRuleExecutor(Context context) {
        this.context = context;
    }

    protected Context context() {
        return context;
    }

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    protected Function<TreeType, TreeType> transform(Rule<?, TreeType> rule) {
        if (rule instanceof ParameterizedRule pr) {
            return t -> {
                long startTime = System.nanoTime();
                TreeType r = (TreeType) pr.apply(t, context);
                ExecutionTime.INSTANCE.trackRule(pr.getClass().getSimpleName(), System.nanoTime() - startTime);
                return r;
            };
        } else {
            return t -> {
                long startTime = System.nanoTime();
                TreeType r = rule.apply(t);
                ExecutionTime.INSTANCE.trackRule(rule.getClass().getSimpleName(), System.nanoTime() - startTime);
                return r;
            };
        }
    }
}
