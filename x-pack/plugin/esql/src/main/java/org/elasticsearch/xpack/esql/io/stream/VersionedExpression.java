/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.io.stream;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteable;

/**
 * An interface indicating to {@link PlanStreamOutput} that this expression can have multiple versions and may need to fallback.
 * TODO: Should we move this to VersionedNamedWriteable?
 */
public interface VersionedExpression {
    /**
     * Returns the writeable expression for the specified transport version.
     * This allows introducing a new expression when nodes are ready to handle it; otherwise, it falls back to the previous expression.
     */
    NamedWriteable getVersionedNamedWriteable(TransportVersion transportVersion);
}
