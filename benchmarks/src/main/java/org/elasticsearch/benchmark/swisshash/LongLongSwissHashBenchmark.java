/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.swisshash;

import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.aggregation.CountGroupingAggregatorFunction;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.node.Node;
import org.elasticsearch.swisshash.LongLongSwissHash;
import org.elasticsearch.swisshash.SwissHashFactory;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.threadpool.DefaultBuiltInExecutorBuilders;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 3, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector", "-Xms10g", "-Xmx10g" })
@State(Scope.Thread)
public class LongLongSwissHashBenchmark {
    static {
        Utils.configureBenchmarkLogging();
    }

    // "1000", "10000", "100000", "1000000",
    @Param({ "1000000", "10000000", "100000000" })
    int cardinality;

    //  "duplicates", "collision"
    @Param({ "uniform" })
    String distribution;

    long[] keys;

    BigArrays bigArrays;
    PageCacheRecycler recycler;
    NoopCircuitBreaker breaker;

    TestThreadPool threadPool;

    static final int NUM_WORKERS = 8; // EsExecutors.allocatedProcessors(Settings.EMPTY);

    private static final BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("bench"))
        .build();

    @Setup(Level.Trial)
    public void setup() {
        keys = generate(distribution, cardinality);
        bigArrays = BigArrays.NON_RECYCLING_INSTANCE;
        recycler = PageCacheRecycler.NON_RECYCLING_INSTANCE;
        breaker = new NoopCircuitBreaker("dummy");
        threadPool = new TestThreadPool("test", Settings.EMPTY);
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        Releasables.close(threadPool);
    }

    static final int CHUNK_SIZE = 1024;

    static DriverContext driverContext() {
        return new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory, null);
    }

    public static class TestThreadPool extends ThreadPool implements Releasable {
        public TestThreadPool(String name, Settings settings, ExecutorBuilder<?>... customBuilders) {
            super(
                Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), name).put(settings).build(),
                MeterRegistry.NOOP,
                new DefaultBuiltInExecutorBuilders(),
                customBuilders
            );
        }

        @Override
        public void close() {
            ThreadPool.terminate(this, 10, TimeUnit.SECONDS);
        }
    }

    @Benchmark
    public long testOnePassHashOnly() {
        try (var swiss =  SwissHashFactory.getInstance().newLongLongSwissHash(recycler, breaker)) {
            long acc = 0;
            int offset = 0;
            long[] batch = new long[CHUNK_SIZE];
            int[] ids = new int[CHUNK_SIZE];
            while (offset < keys.length) {
                int len = Math.min(keys.length - offset, CHUNK_SIZE);
                System.arraycopy(keys, offset, batch, 0, len);
                if (swiss.supportBulkAdd()) {
                    swiss.bulkAdd(batch, batch, ids, len);
                } else {
                    for (int i = 0; i < len; i++) {
                        var v= batch[i];
                        long id = swiss.add(v, v);
                        if (id < 0) {
                            id = -1 - id;
                        }
                        ids[i] = (int) id;
                    }
                }
                offset += len;
            }
            acc = offset;
            return acc;
        }
    }


    @Benchmark
    public long testPartitionOnly() throws Exception {
        LongLongSwissHash[] hashes = new LongLongSwissHash[NUM_WORKERS];
        int keysPerWorker = keys.length / NUM_WORKERS;
        CountDownLatch collectLatch = new CountDownLatch(NUM_WORKERS);
        List<LongLongSwissHash.PartitionHash> allGens = new ArrayList<>();
        for (int w = 0; w < NUM_WORKERS; w++) {
            final int start = keysPerWorker * w;
            final int end = start + keysPerWorker;
            var hash = hashes[w] = SwissHashFactory.getInstance().newLongLongSwissHash(recycler, breaker);
            threadPool.executor(ThreadPool.Names.SEARCH).execute(() -> {
                int offset = start;
                long[] batchKeys = new long[CHUNK_SIZE];
                int[] ids = new int[CHUNK_SIZE];
                int[][] partitionedIds = null;
                List<LongLongSwissHash.PartitionHash> gens = new ArrayList<>();
                while (offset < end) {
                    int len = Math.min(end - offset, CHUNK_SIZE);
                    if (hash.size() + len > 392_000) {
                        var partition = hash.partition(partitionedIds);
                        gens.add(partition);
                        partitionedIds = partition.ids;
                        partition.ids = null;
                    }
                    if (hash.supportBulkAdd()) {
                        System.arraycopy(keys, offset, batchKeys, 0, len);
                        hash.bulkAdd(batchKeys, batchKeys, ids, len);
                    } else {
                        for (int i = 0; i < len; i++) {
                            var v = keys[offset + i];
                            var id = (int) hash.add(v, v);
                            if (id < 0) {
                                id = -1 - id;
                            }
                            ids[i] = id;
                        }
                    }
                    offset += len;
                }
                if (hash.size() > 0) {
                    var partition = hash.partition(partitionedIds);
                    gens.add(partition);
                    partitionedIds = partition.ids;
                    partition.ids = null;
                }
                partitionedIds = null;
                synchronized (allGens) {
                    allGens.addAll(gens);
                }
                gens.clear();
                collectLatch.countDown();
            });
        }
        collectLatch.await();
        long acc = 0;
        CountDownLatch mergeLatch = new CountDownLatch(NUM_WORKERS);
        AtomicInteger nextPartition = new AtomicInteger(-1);
        for (int w = 0; w < NUM_WORKERS; w++) {
            LongLongSwissHash partition = hashes[w];
            threadPool.executor(ThreadPool.Names.SEARCH).execute(() -> {
                LongLongSwissHash.MergeKeys mergeKeys = null;
                for (; ; ) {
                    int p = nextPartition.incrementAndGet();
                    if (p >= LongLongSwissHash.NUM_PARTITIONS) {
                        break;
                    }
                    int totalSize = 0;
                    for (var gen : allGens) {
                        totalSize += gen.partitionSize(p);
                    }
                    partition.clear();
                    for (var gen : allGens) {
                        mergeKeys = partition.mergeKeys(gen, p, totalSize, mergeKeys);
                        gen.keys[p] = null;
                    }
                }
                mergeLatch.countDown();
            });
        }
        mergeLatch.await();
        for (var worker : hashes) {
            acc += worker.size();
        }
        Releasables.close(hashes);
        return acc;
    }

    public record PartitionHashAndAggs(LongLongSwissHash.PartitionHash hash, CountGroupingAggregatorFunction.PartitionAggs aggs) {

    }

    public record PartitionHashAndNCounts(LongLongSwissHash.PartitionHash hash, PartitionNCounts nCounts) {

    }


    @Benchmark
    public long testPartitionOneAgg() throws Exception {
        LongLongSwissHash[] hashes = new LongLongSwissHash[NUM_WORKERS];
        CountGroupingAggregatorFunction[] aggs = new CountGroupingAggregatorFunction[NUM_WORKERS];
        int keysPerWorker = keys.length / NUM_WORKERS;
        CountDownLatch collectLatch = new CountDownLatch(NUM_WORKERS);
        List<PartitionHashAndAggs> allGens = new ArrayList<>();
        for (int w = 0; w < NUM_WORKERS; w++) {
            final int start = keysPerWorker * w;
            final int end = start + keysPerWorker;
            var hash = hashes[w] = SwissHashFactory.getInstance().newLongLongSwissHash(recycler, breaker);
            var agg = aggs[w] = new CountGroupingAggregatorFunction(List.of(0), driverContext());
            threadPool.executor(ThreadPool.Names.SEARCH).execute(() -> {
                int offset = start;
                long[] batchKeys = new long[CHUNK_SIZE];
                int[] ids = new int[CHUNK_SIZE];
                int[] fills = new int[256];
                int[][] partitionedIds = null;
                List<PartitionHashAndAggs> gens = new ArrayList<>();
                while (offset < end) {
                    int len = Math.min(end - offset, CHUNK_SIZE);
                    if (hash.size() + len > 392_000) {
                        int hashSize = Math.toIntExact(hash.size());
                        var partitionedHash = hash.partition(partitionedIds);
                        partitionedIds = partitionedHash.ids;
                        partitionedHash.ids = null;
                        var partitionAggs = agg.partitionAggs(partitionedIds, partitionedHash.lengths, hashSize, fills);
                        agg.clear();
                        gens.add(new PartitionHashAndAggs(partitionedHash, partitionAggs));
                    }
                    if (hash.supportBulkAdd()) {
                        System.arraycopy(keys, offset, batchKeys, 0, len);
                        hash.bulkAdd(batchKeys, batchKeys, ids, len);
                    } else {
                        for (int i = 0; i < len; i++) {
                            var v = keys[offset + i];
                            var id = (int) hash.add(v, v);
                            if (id < 0) {
                                id = -1 - id;
                            }
                            ids[i] = id;
                        }
                    }
                    for (int i = 0; i < len; i++) {
                        agg.accumulateCount(ids[i], 1);
                    }
                    offset += len;
                }
                if (hash.size() > 0) {
                    int hashSize = Math.toIntExact(hash.size());
                    var partitionedHash = hash.partition(partitionedIds);
                    partitionedIds = partitionedHash.ids;
                    partitionedHash.ids = null;
                    var partitionAggs = agg.partitionAggs(partitionedIds, partitionedHash.lengths, hashSize, fills);
                    agg.clear();
                    gens.add(new PartitionHashAndAggs(partitionedHash, partitionAggs));
                }
                partitionedIds = null;
                synchronized (allGens) {
                    allGens.addAll(gens);
                }
                gens.clear();
                collectLatch.countDown();
            });
        }
        collectLatch.await();
        long acc = 0;
        CountDownLatch mergeLatch = new CountDownLatch(NUM_WORKERS);
        AtomicInteger nextPartition = new AtomicInteger(-1);
        for (int w = 0; w < NUM_WORKERS; w++) {
            LongLongSwissHash partition = hashes[w];
            threadPool.executor(ThreadPool.Names.SEARCH).execute(() -> {
                LongLongSwissHash.MergeKeys[] mergedKeys = new LongLongSwissHash.MergeKeys[allGens.size()];
                for (; ; ) {
                    int p = nextPartition.incrementAndGet();
                    if (p >= LongLongSwissHash.NUM_PARTITIONS) {
                        break;
                    }
                    int totalSize = 0;
                    for (var gen : allGens) {
                        totalSize += gen.hash.partitionSize(p);
                    }
                    partition.clear();
                    for (int g = 0; g < allGens.size(); g++) {
                        var gen = allGens.get(g);
                        LongLongSwissHash.PartitionHash hash = gen.hash;
                        mergedKeys[g] = partition.mergeKeys(hash, p, totalSize, mergedKeys[g]);
                        hash.keys[p] = null; // release the keys
                    }
                    var newAgg = new CountGroupingAggregatorFunction(List.of(0), driverContext());
                    for (int g = 0; g < mergedKeys.length; g++) {
                        var gen = allGens.get(g);
                        LongLongSwissHash.MergeKeys merged = mergedKeys[g];
                        newAgg.combinePartition(
                            gen.aggs.values[p],
                            merged.ids,
                            merged.length,
                            totalSize
                        );
                        gen.aggs.values[p] = null; // release the agg
                    }
                }
                mergeLatch.countDown();
            });
        }
        mergeLatch.await();
        for (var worker : hashes) {
            acc += worker.size();
        }
        Releasables.close(hashes);
        return acc;
    }


    @Benchmark
    public long testPartitionNAgg() throws Exception {
        LongLongSwissHash[] hashes = new LongLongSwissHash[NUM_WORKERS];
        NCount[] aggs = new NCount[NUM_WORKERS];
        int keysPerWorker = keys.length / NUM_WORKERS;
        CountDownLatch collectLatch = new CountDownLatch(NUM_WORKERS);
        List<PartitionHashAndNCounts> allGens = new ArrayList<>();
        for (int w = 0; w < NUM_WORKERS; w++) {
            final int start = keysPerWorker * w;
            final int end = start + keysPerWorker;
            var hash = hashes[w] = SwissHashFactory.getInstance().newLongLongSwissHash(recycler, breaker);
            var agg = aggs[w] = new NCount();
            threadPool.executor(ThreadPool.Names.SEARCH).execute(() -> {
                int offset = start;
                long[] batchKeys = new long[CHUNK_SIZE];
                int[] ids = new int[CHUNK_SIZE];
                int[] fills = new int[256];
                int[][] partitionedIds = null;
                List<PartitionHashAndNCounts> gens = new ArrayList<>();
                while (offset < end) {
                    int len = Math.min(end - offset, CHUNK_SIZE);
                    if (hash.size() + len > 392_000) {
                        int hashSize = Math.toIntExact(hash.size());
                        var partitionedHash = hash.partition(partitionedIds);
                        partitionedIds = partitionedHash.ids;
                        partitionedHash.ids = null;
                        var partitionAggs = agg.partitionAggs(partitionedIds, partitionedHash.lengths, hashSize, fills);
                        agg.clear();
                        gens.add(new PartitionHashAndNCounts(partitionedHash, partitionAggs));
                    }
                    if (hash.supportBulkAdd()) {
                        System.arraycopy(keys, offset, batchKeys, 0, len);
                        hash.bulkAdd(batchKeys, batchKeys, ids, len);
                    } else {
                        for (int i = 0; i < len; i++) {
                            var v = keys[offset + i];
                            var id = (int) hash.add(v, v);
                            if (id < 0) {
                                id = -1 - id;
                            }
                            ids[i] = id;
                        }
                    }
                    for (CountGroupingAggregatorFunction oneCount : agg.nCounts) {
                        for (int i = 0; i < len; i++) {
                            oneCount.accumulateCount(ids[i], 1);
                        }
                    }
                    offset += len;
                }
                if (hash.size() > 0) {
                    int hashSize = Math.toIntExact(hash.size());
                    var partitionedHash = hash.partition(partitionedIds);
                    partitionedIds = partitionedHash.ids;
                    partitionedHash.ids = null;
                    var partitionAggs = agg.partitionAggs(partitionedIds, partitionedHash.lengths, hashSize, fills);
                    agg.clear();
                    gens.add(new PartitionHashAndNCounts(partitionedHash, partitionAggs));
                }
                partitionedIds = null;
                synchronized (allGens) {
                    allGens.addAll(gens);
                }
                gens.clear();
                collectLatch.countDown();
            });
        }
        collectLatch.await();
        long acc = 0;
        CountDownLatch mergeLatch = new CountDownLatch(NUM_WORKERS);
        AtomicInteger nextPartition = new AtomicInteger(-1);
        for (int w = 0; w < NUM_WORKERS; w++) {
            LongLongSwissHash partition = hashes[w];
            threadPool.executor(ThreadPool.Names.SEARCH).execute(() -> {
                LongLongSwissHash.MergeKeys[] mergedKeys = new LongLongSwissHash.MergeKeys[allGens.size()];
                for (; ; ) {
                    int p = nextPartition.incrementAndGet();
                    if (p >= LongLongSwissHash.NUM_PARTITIONS) {
                        break;
                    }
                    int totalSize = 0;
                    for (var gen : allGens) {
                        totalSize += gen.hash.partitionSize(p);
                    }
                    partition.clear();
                    for (int g = 0; g < allGens.size(); g++) {
                        var gen = allGens.get(g);
                        LongLongSwissHash.PartitionHash hash = gen.hash;
                        mergedKeys[g] = partition.mergeKeys(hash, p, totalSize, mergedKeys[g]);
                        hash.keys[p] = null; // release the keys
                    }
                    for (int c = 0; c < N_COUNTS; c++) {
                        var newAgg = new CountGroupingAggregatorFunction(List.of(0), driverContext());
                        for (int g = 0; g < mergedKeys.length; g++) {
                            var gen = allGens.get(g);
                            LongLongSwissHash.MergeKeys merged = mergedKeys[g];
                            CountGroupingAggregatorFunction.PartitionAggs oneCount = gen.nCounts.subs[c];
                            newAgg.combinePartition(
                                oneCount.values[p],
                                merged.ids,
                                merged.length,
                                totalSize
                            );
                            oneCount.values[p] = null; // release the agg
                        }
                    }
                }
                mergeLatch.countDown();
            });
        }
        mergeLatch.await();
        for (var worker : hashes) {
            acc += worker.size();
        }
        Releasables.close(hashes);
        return acc;
    }

    private long[] generate(String dist, int size) {
        ThreadLocalRandom r = ThreadLocalRandom.current();
        long[] out = new long[size];

        switch (dist) {
            case "uniform":
                for (int i = 0; i < size; i++) {
                    out[i] = r.nextLong();
                }
                break;
            case "duplicates":
                // 80% of keys come from a small "hot" set
                int hotSet = Math.max(32, Math.min(1000, size / 50)); // ~2% of cardinality
                long[] hot = new long[hotSet];
                for (int i = 0; i < hotSet; i++) {
                    hot[i] = r.nextLong();
                }
                for (int i = 0; i < size; i++) {
                    if (r.nextInt(10) < 8) {        // 80% duplicates
                        out[i] = hot[r.nextInt(hotSet)];
                    } else {                               // 20% random noise
                        out[i] = r.nextLong();
                    }
                }
                break;
            case "collision":
                // Force collisions by clamping top bits so BitMixer mixes poorly
                final long seed = 0xABCDEFL;
                for (int i = 0; i < size; i++) {
                    out[i] = seed | ((long) i & 0xFFFF); // all share same high bits
                }
                break;
            default:
                throw new IllegalArgumentException("unknown distribution: " + dist);
        }
        return out;
    }

    public static final int N_COUNTS = 5;

    public record PartitionNCounts(CountGroupingAggregatorFunction.PartitionAggs[] subs) {

    }

    public static class NCount {
        CountGroupingAggregatorFunction[] nCounts;

        public NCount() {
            this.nCounts = new CountGroupingAggregatorFunction[N_COUNTS];
            for (int c = 0; c < N_COUNTS; c++) {
                nCounts[c] = new CountGroupingAggregatorFunction(List.of(), driverContext());
            }
        }

        public PartitionNCounts partitionAggs(int[][] ids, int[] lengths, int totalLength, int[] fills) {
            CountGroupingAggregatorFunction.PartitionAggs[] subs = new CountGroupingAggregatorFunction.PartitionAggs[N_COUNTS];
            for (int i = 0; i < N_COUNTS; i++) {
                subs[i] = nCounts[i].partitionAggs(ids, lengths, totalLength, fills);
            }
            return new PartitionNCounts(subs);
        }

        public void clear() {
            for (CountGroupingAggregatorFunction c : nCounts) {
                c.clear();
            }
        }
    }
}
