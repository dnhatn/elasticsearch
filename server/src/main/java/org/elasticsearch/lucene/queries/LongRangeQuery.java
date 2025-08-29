/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.queries;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;

import java.io.IOException;
import java.util.Objects;

public class LongRangeQuery extends Query {
    public final boolean withDocValues;
    public final long lowerValue;
    public final long upperValue;
    public final String name;

    public LongRangeQuery(String name, boolean withDocValues, long lowerValue, long upperValue) {
        this.name = name;
        this.withDocValues = withDocValues;
        this.lowerValue = lowerValue;
        this.upperValue = upperValue;
    }

    @Override
    public String toString(String field) {
        return "LongRangeQuery" + (withDocValues ? " with doc values" : "") + " [" + lowerValue + " to " + upperValue + "]";
    }

    @Override
    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
        Query point = LongPoint.newRangeQuery(name, lowerValue, upperValue);
        if (withDocValues) {
            var dv = SortedNumericDocValuesField.newSlowRangeQuery(name, lowerValue, upperValue);
            return new IndexOrDocValuesQuery(point, dv);
        } else {
            return point;
        }
    }

    @Override
    public void visit(QueryVisitor visitor) {
        visitor.visitLeaf(this);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        LongRangeQuery that = (LongRangeQuery) o;
        return withDocValues == that.withDocValues && lowerValue == that.lowerValue && upperValue == that.upperValue;
    }

    @Override
    public int hashCode() {
        return Objects.hash(withDocValues, lowerValue, upperValue);
    }
}
