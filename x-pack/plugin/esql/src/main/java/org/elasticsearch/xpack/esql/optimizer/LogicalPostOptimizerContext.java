/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.session.Configuration;

/**
 * What {@link LogicalPlanPostOptimizer} rules need to collect statistics for a query: its settings and request filter, the
 * minimum transport version of every cluster it reaches, and the task their requests run under.
 */
public record LogicalPostOptimizerContext(
    Configuration configuration,
    @Nullable QueryBuilder requestFilter,
    EsqlFlags flags,
    TransportVersion minimumVersion,
    TransportService transportService,
    CancellableTask queryTask
) {}
