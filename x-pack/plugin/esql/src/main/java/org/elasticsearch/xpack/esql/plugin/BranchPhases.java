/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.Releasable;

/**
 * Lets a merge bound how many of its branches run their coordinator plan apart from how many read data, so the next branch can
 * read data while earlier branches finish on the coordinator.
 */
public interface BranchPhases {
    BranchPhases NONE = new BranchPhases() {
        @Override
        public void startCoordinator(ActionListener<Releasable> start) {
            start.onResponse(() -> {});
        }

        @Override
        public void dataCompleted() {}
    };

    /**
     * Completes {@code start} with a slot once the branch may start its coordinator plan. The branch releases the slot when that
     * plan completes.
     */
    void startCoordinator(ActionListener<Releasable> start);

    /**
     * Called at most once, when every data-node and remote compute of the branch has completed.
     */
    void dataCompleted();
}
