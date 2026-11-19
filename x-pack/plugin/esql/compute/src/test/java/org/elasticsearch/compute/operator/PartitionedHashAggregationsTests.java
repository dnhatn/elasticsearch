/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.PartitionedHashTable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.SumIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Each range of partitions folds its pending generations when they repeat keys. Disjoint generations are detected from one
 * partition's keys and no range folds. Either way the combined result must match a plain sum per key.
 */
public class PartitionedHashAggregationsTests extends ComputeTestCase {
    private static final int KEYS_PER_GENERATION = 2_000;

    public void testFoldsOverlappingGenerations() {
        int distinctKeys = 10_000;
        int[] keys = new int[100_000];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = randomIntBetween(0, distinctKeys - 1);
        }
        runFold(keys, (partitions, gens) -> {
            assertThat(gens, greaterThanOrEqualTo(PartitionedHashAggregations.FOLD_MIN_GENERATIONS));
            assertThat(partitions.folds(), equalTo(PartitionedHashAggregations.FOLD_RANGES));
            assertThat(partitions.lowOverlapFolds(), equalTo(0));
            assertThat(partitions.skippedFolds(), equalTo(0));
            assertThat(partitions.liveGenerations(), equalTo(0));
        });
    }

    public void testSkipsDisjointGenerations() {
        int[] keys = new int[20 * KEYS_PER_GENERATION];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = i;
        }
        runFold(keys, (partitions, gens) -> {
            assertThat(partitions.folds(), equalTo(0));
            assertThat(partitions.skippedFolds(), equalTo(1));
            assertThat(partitions.liveGenerations(), equalTo(gens));
        });
    }

    interface FoldCheck {
        void check(PartitionedHashAggregations partitions, int generationsBeforeFold);
    }

    /**
     * Workers split and fold at the same time, as in the parallel operator, so folds of different ranges overlap with each other
     * and with new generations being added.
     */
    public void testConcurrentSplitsAndFolds() throws Exception {
        BlockFactory blockFactory = blockFactory();
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        boolean bytesKeys = randomBoolean();
        HashAggregationOperator op = newOperator(driverContext, bytesKeys);
        Map<Integer, Long> expected = new HashMap<>();
        Map<Integer, Long> actual = new HashMap<>();
        List<List<Page>> pagesPerWorker = new ArrayList<>();
        int numWorkers = between(2, 8);
        for (int w = 0; w < numWorkers; w++) {
            int[] keys = new int[between(20_000, 40_000)];
            for (int i = 0; i < keys.length; i++) {
                keys[i] = randomIntBetween(0, 4_999);
            }
            List<Page> pages = new ArrayList<>();
            for (int start = 0; start < keys.length;) {
                int end = Math.min(keys.length, start + between(1, 1024));
                pages.add(page(blockFactory, keys, start, end, bytesKeys, expected));
                start = end;
            }
            pagesPerWorker.add(pages);
        }
        try (PartitionedHashAggregations partitions = new PartitionedHashAggregations(driverContext.breaker(), op::spawnWorker)) {
            HashAggregationOperator[] workers = new HashAggregationOperator[numWorkers];
            try {
                for (int w = 0; w < numWorkers; w++) {
                    workers[w] = op.spawnWorker();
                }
                CountDownLatch start = new CountDownLatch(1);
                AtomicReference<Exception> failure = new AtomicReference<>();
                Thread[] threads = new Thread[numWorkers];
                for (int w = 0; w < numWorkers; w++) {
                    HashAggregationOperator worker = workers[w];
                    List<Page> pages = pagesPerWorker.get(w);
                    threads[w] = new Thread(() -> {
                        safeAwait(start);
                        try {
                            for (int i = 0; i < pages.size(); i++) {
                                worker.addInput(pages.set(i, null));
                                if (worker.partitioningThresholdReached()) {
                                    partitions.split(worker.driverContext.breaker(), worker);
                                }
                                partitions.maybeFold();
                            }
                            partitions.split(worker.driverContext.breaker(), worker);
                        } catch (Exception e) {
                            failure.compareAndSet(null, e);
                        }
                    });
                    threads[w].start();
                }
                start.countDown();
                for (Thread thread : threads) {
                    thread.join();
                }
                if (failure.get() != null) {
                    throw failure.get();
                }
            } finally {
                Releasables.close(workers);
                for (List<Page> pages : pagesPerWorker) {
                    for (Page page : pages) {
                        if (page != null) {
                            page.releaseBlocks();
                        }
                    }
                }
            }
            assertThat(partitions.folds(), greaterThan(0));
            emitAll(partitions, op, bytesKeys, actual);
        } finally {
            op.close();
        }
        assertThat(actual, equalTo(expected));
    }

    private HashAggregationOperator newOperator(DriverContext driverContext, boolean bytesKeys) {
        List<BlockHash.GroupSpec> groupSpecs = List.of(
            new BlockHash.GroupSpec(0, ElementType.LONG),
            new BlockHash.GroupSpec(1, bytesKeys ? ElementType.BYTES_REF : ElementType.INT)
        );
        return new HashAggregationOperator(
            AggregatorMode.SINGLE,
            List.of(new SumIntAggregatorFunctionSupplier().groupingAggregatorFactory(AggregatorMode.SINGLE, List.of(2))),
            dc -> BlockHash.build(groupSpecs, dc.blockFactory(), 1024, false),
            Integer.MAX_VALUE,
            1.0,
            between(128, 4096),
            null,
            driverContext,
            new HashAggregationOperator.ParallelConfig(EsExecutors.DIRECT_EXECUTOR_SERVICE, 1, 1, KEYS_PER_GENERATION),
            false
        );
    }

    private static void emitAll(
        PartitionedHashAggregations partitions,
        HashAggregationOperator op,
        boolean bytesKeys,
        Map<Integer, Long> actual
    ) {
        try (PartitionedHashAggregations.Combiner combiner = partitions.newCombiner(op)) {
            for (int p = 0; p < PartitionedHashTable.NUM_PARTITIONS; p++) {
                combiner.combine(p);
                op.emit();
                Page out;
                while ((out = op.getOutput()) != null) {
                    try {
                        collect(out, bytesKeys, actual);
                    } finally {
                        out.releaseBlocks();
                    }
                }
            }
        }
    }

    private void runFold(int[] keys, FoldCheck check) {
        BlockFactory blockFactory = blockFactory();
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        boolean bytesKeys = randomBoolean();
        HashAggregationOperator op = newOperator(driverContext, bytesKeys);
        Map<Integer, Long> expected = new HashMap<>();
        Map<Integer, Long> actual = new HashMap<>();
        CircuitBreaker breaker = driverContext.breaker();
        try (PartitionedHashAggregations partitions = new PartitionedHashAggregations(breaker, op::spawnWorker)) {
            for (int start = 0; start < keys.length;) {
                int end = Math.min(keys.length, start + between(1, 1024));
                op.addInput(page(blockFactory, keys, start, end, bytesKeys, expected));
                if (op.partitioningThresholdReached()) {
                    partitions.split(breaker, op);
                }
                start = end;
            }
            partitions.split(breaker, op);
            int gens = partitions.numGenerations();
            while (partitions.maybeFold()) {
                assertThat(partitions.folds(), lessThanOrEqualTo(PartitionedHashAggregations.FOLD_RANGES));
            }
            check.check(partitions, gens);
            emitAll(partitions, op, bytesKeys, actual);
        } finally {
            op.close();
        }
        assertThat(actual, equalTo(expected));
    }

    private static Page page(BlockFactory blockFactory, int[] keys, int start, int end, boolean bytesKeys, Map<Integer, Long> expected) {
        Block[] blocks = new Block[3];
        try (
            LongBlock.Builder longs = blockFactory.newLongBlockBuilder(end - start);
            Block.Builder second = bytesKeys
                ? blockFactory.newBytesRefBlockBuilder(end - start)
                : blockFactory.newIntBlockBuilder(end - start);
            IntBlock.Builder values = blockFactory.newIntBlockBuilder(end - start)
        ) {
            for (int i = start; i < end; i++) {
                int key = keys[i];
                longs.appendLong(key / 100);
                if (bytesKeys) {
                    ((BytesRefBlock.Builder) second).appendBytesRef(new BytesRef("host-" + (key % 100)));
                } else {
                    ((IntBlock.Builder) second).appendInt(key % 100);
                }
                int value = between(0, 1000);
                values.appendInt(value);
                expected.merge(key, (long) value, Long::sum);
            }
            blocks[0] = longs.build();
            blocks[1] = second.build();
            blocks[2] = values.build();
            Page page = new Page(blocks);
            blocks = null;
            return page;
        } finally {
            if (blocks != null) {
                Releasables.close(blocks);
            }
        }
    }

    private static void collect(Page page, boolean bytesKeys, Map<Integer, Long> actual) {
        LongBlock longs = page.getBlock(0);
        LongBlock sums = page.getBlock(2);
        BytesRef scratch = new BytesRef();
        for (int i = 0; i < page.getPositionCount(); i++) {
            int low = bytesKeys
                ? Integer.parseInt(((BytesRefBlock) page.getBlock(1)).getBytesRef(i, scratch).utf8ToString().substring("host-".length()))
                : ((IntBlock) page.getBlock(1)).getInt(i);
            int key = Math.toIntExact(longs.getLong(i) * 100 + low);
            assertNull(actual.put(key, sums.getLong(i)));
        }
    }
}
