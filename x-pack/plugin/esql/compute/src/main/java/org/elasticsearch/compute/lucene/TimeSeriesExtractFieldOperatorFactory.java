/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;

import java.util.List;

public record TimeSeriesExtractFieldOperatorFactory(
    List<? extends ShardContext> contexts,
    boolean emitDocs,
    List<ValuesSourceReaderOperator.FieldInfo> fieldToExtracts
) implements Operator.OperatorFactory {

    @Override
    public TimeSeriesExtractFieldsOperator get(DriverContext driverContext) {
        return new TimeSeriesExtractFieldsOperator(driverContext, contexts, emitDocs, fieldToExtracts);
    }

    @Override
    public String describe() {
        return "TimeSeriesExtractFieldsOperator[maxPageSize = " + fieldToExtracts + "]";
    }
}
