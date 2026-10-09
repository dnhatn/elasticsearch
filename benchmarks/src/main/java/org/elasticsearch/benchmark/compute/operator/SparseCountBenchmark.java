/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.compute.operator;

import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.CountAggregatorFunction;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.HashAggregationOperator;
import org.elasticsearch.compute.operator.Operator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Fork(1)
public class SparseCountBenchmark {
    static final int BLOCK_LENGTH = 8 * 1024;
    static final int PAGES = 256;

    static {
        BenchmarkLogging.configure();
    }

    private static final BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("bench"))
        .build();

    @Param({ "unique", "dup2", "small" })
    public String keys;

    @Param({ "true", "false" })
    public boolean topN;

    private List<Page> pages;

    @Setup
    public void setup() {
        pages = new ArrayList<>();
        for (int p = 0; p < PAGES; p++) {
            long[] k1 = new long[BLOCK_LENGTH];
            long[] k2 = new long[BLOCK_LENGTH];
            for (int i = 0; i < BLOCK_LENGTH; i++) {
                long row = (long) p * BLOCK_LENGTH + i;
                long key = switch (keys) {
                    case "unique" -> row;
                    case "dup2" -> row / 2;
                    case "small" -> row % 5;
                    default -> throw new IllegalArgumentException(keys);
                };
                k1[i] = key;
                k2[i] = key * 31;
            }
            pages.add(
                new Page(blockFactory.newLongArrayVector(k1, BLOCK_LENGTH).asBlock(), blockFactory.newLongArrayVector(k2, BLOCK_LENGTH).asBlock())
            );
        }
    }

    @Benchmark
    public long run() {
        DriverContext ctx = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory, null);
        var builder = new HashAggregationOperator.Builder().mode(AggregatorMode.SINGLE)
            .aggregators(List.of(CountAggregatorFunction.supplier().groupingAggregatorFactory(AggregatorMode.SINGLE, List.of())))
            .groups(List.of(new BlockHash.GroupSpec(0, ElementType.LONG), new BlockHash.GroupSpec(1, ElementType.LONG)));
        if (topN) {
            builder.topAggregation(new HashAggregationOperator.TopAggregation(0, false, 10));
        }
        long rows = 0;
        try (Operator op = builder.build().get(ctx)) {
            for (Page page : pages) {
                op.addInput(page.shallowCopy());
            }
            op.finish();
            Page out;
            while ((out = op.getOutput()) != null) {
                rows += out.getPositionCount();
                out.releaseBlocks();
            }
        }
        return rows;
    }
}
