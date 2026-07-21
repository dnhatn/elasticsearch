/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
import org.elasticsearch.xpack.esql.expression.function.grouping.BucketSerializationTests;

import java.io.IOException;

public class WindowFilterSerializationTests extends AbstractExpressionSerializationTests<WindowFilter> {
    @Override
    protected WindowFilter createTestInstance() {
        Source source = randomSource();
        Expression window = randomChild();
        Bucket bucket = BucketSerializationTests.createRandomBucket(configuration());
        Expression timestamp = randomChild();
        return new WindowFilter(source, window, bucket, timestamp);
    }

    @Override
    protected WindowFilter mutateInstance(WindowFilter instance) throws IOException {
        Source source = randomSource();
        Expression window = instance.window();
        Bucket bucket = instance.bucket();
        Expression timestamp = instance.timestamp();
        switch (between(0, 2)) {
            case 0 -> window = randomValueOtherThan(window, AbstractExpressionSerializationTests::randomChild);
            case 1 -> bucket = randomValueOtherThan(bucket, () -> BucketSerializationTests.createRandomBucket(configuration()));
            case 2 -> timestamp = randomValueOtherThan(timestamp, AbstractExpressionSerializationTests::randomChild);
        }
        return new WindowFilter(source, window, bucket, timestamp);
    }
}
