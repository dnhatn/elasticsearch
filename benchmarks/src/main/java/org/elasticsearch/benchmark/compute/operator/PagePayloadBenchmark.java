/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.OrdinalBytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.PagePayload;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

/**
 * The cost of taking a page's value volume, against the per-document cost of the column read that produces the
 * page: a column read reports what it decoded, and this is what reporting it costs.
 */
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Fork(1)
public class PagePayloadBenchmark {

    static {
        BenchmarkLogging.configure();
    }

    /** One page of a column read. */
    private static final int POSITIONS = 8 * 1024;

    private static final BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(NoopCircuitBreaker.INSTANCE)
        .build();

    /**
     * The shapes a column read hands on: a column of short keywords, one of whole text values, one holding several
     * values a document, one read as ordinals against a dictionary, one of a single repeated value, and a numeric
     * column, whose volume is its width.
     */
    @Param({ "keyword_8", "keyword_30", "text_1000", "keyword_mv_3", "ordinals_1000", "constant_30", "long" })
    public String shape;

    private Page page;

    @Setup
    public void buildPage() {
        page = new Page(switch (shape) {
            case "keyword_8" -> bytes(8, 1);
            case "keyword_30" -> bytes(30, 1);
            case "text_1000" -> bytes(1000, 1);
            case "keyword_mv_3" -> bytes(30, 3);
            case "ordinals_1000" -> ordinals(30, 1000);
            case "constant_30" -> blockFactory.newConstantBytesRefBlockWith(value(30, new Random(0)), POSITIONS);
            case "long" -> blockFactory.newLongArrayVector(new long[POSITIONS], POSITIONS).asBlock();
            default -> throw new IllegalArgumentException("unknown shape [" + shape + "]");
        });
    }

    /** A column of {@code length} byte values, {@code valuesPerPosition} of them a document. */
    private static Block bytes(int length, int valuesPerPosition) {
        final Random random = new Random(0);
        final int values = POSITIONS * valuesPerPosition;
        final BytesRefArray array = new BytesRefArray(values, BigArrays.NON_RECYCLING_INSTANCE);
        for (int value = 0; value < values; value++) {
            array.append(value(length, random));
        }
        if (valuesPerPosition == 1) {
            return blockFactory.newBytesRefArrayVector(array, POSITIONS).asBlock();
        }
        return blockFactory.newBytesRefArrayBlock(
            array,
            POSITIONS,
            IntStream.rangeClosed(0, POSITIONS).map(position -> position * valuesPerPosition).toArray(),
            null,
            Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING
        );
    }

    /** A column read against a dictionary of {@code terms} values, which is how a low cardinality column arrives. */
    private static Block ordinals(int length, int terms) {
        final Random random = new Random(0);
        final BytesRefArray dictionary = new BytesRefArray(terms, BigArrays.NON_RECYCLING_INSTANCE);
        for (int term = 0; term < terms; term++) {
            dictionary.append(value(length, random));
        }
        final BytesRefVector bytes = blockFactory.newBytesRefArrayVector(dictionary, terms);
        final int[] ordinals = new int[POSITIONS];
        for (int position = 0; position < POSITIONS; position++) {
            ordinals[position] = random.nextInt(terms);
        }
        final IntVector ordinalVector = blockFactory.newIntArrayVector(ordinals, POSITIONS);
        return new OrdinalBytesRefVector(ordinalVector, bytes).asBlock();
    }

    private static BytesRef value(int length, Random random) {
        final byte[] bytes = new byte[length];
        random.nextBytes(bytes);
        return new BytesRef(bytes);
    }

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public long payload() {
        return PagePayload.of(page);
    }
}
