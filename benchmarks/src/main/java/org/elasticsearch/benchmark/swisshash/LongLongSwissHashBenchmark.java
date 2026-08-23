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
import org.elasticsearch.common.util.PartitionedHashTable;
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
@Fork(value = 1, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector", "-Xms10g", "-Xmx10g" })
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

    static final PartitionedHashTable.AggSplitter NOOP_SPLITTER = new PartitionedHashTable.AggSplitter() {
        @Override
        public void split(PartitionedHashTable.ScratchBuffer scratch, int idOffset, int batchLen, short[] positions, int[] fills) {
        }

        @Override
        public PartitionedHashTable.PartitionedAgg finish() {
            return null;
        }

        @Override
        public void close() {
        }
    };

    protected PartitionedHashTable.PartitionedKeys partition(LongLongSwissHash swiss) {
        return swiss.partition(bigArrays, breaker, NOOP_SPLITTER);
    }

    protected PartitionedHashTable.PartitionedKeysAndAggs partition(LongLongSwissHash hashTable, CountGroupingAggregatorFunction agg) {
        PartitionedHashTable.AggSplitter splitter = agg.newSplitter();
        PartitionedHashTable.PartitionedKeys partitionedkeys = hashTable.partition(bigArrays, breaker, splitter);
        PartitionedHashTable.PartitionedAgg partitionedAggs = splitter.finish();
        splitter.close();
        hashTable.clear();
        agg.clear();
        return new PartitionedHashTable.PartitionedKeysAndAggs(partitionedkeys, partitionedAggs);
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
    public long testOnePassWithOneAgg() {
        CountGroupingAggregatorFunction agg = new CountGroupingAggregatorFunction(List.of(0), driverContext());
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
                for (int i = 0; i < len; i++) {
                    agg.accumulateCount(ids[i], offset + i);
                }
                offset += len;
            }
            acc = offset;
            return acc;
        }
    }

    @Benchmark
    public long testOnePassWithNAgg() {
        CountGroupingAggregatorFunction[] aggs = new CountGroupingAggregatorFunction[5];
        for (int i = 0; i < aggs.length; i++) {
            aggs[i] = new CountGroupingAggregatorFunction(List.of(0), driverContext());
        }
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
                // chunk to 64
                {
                    int writeOffset = 0;
                    while (writeOffset < len) {
                        int batchWrite = Math.min(len - writeOffset, 64); // 64 vs 128
                        int end = writeOffset + batchWrite;
                        for (CountGroupingAggregatorFunction agg : aggs) {
                            for (int i = writeOffset; i < end; i++) {
                                agg.accumulateCount(ids[i], offset + i);
                            }
                        }
                        writeOffset += batchWrite;
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
        LongLongSwissHash[] workers = new LongLongSwissHash[NUM_WORKERS];
        int keysPerWorker = keys.length / NUM_WORKERS;
        CountDownLatch collectLatch = new CountDownLatch(NUM_WORKERS);
        List<PartitionedHashTable.PartitionedKeys> allGens = new ArrayList<>();
        for (int w = 0; w < NUM_WORKERS; w++) {
            final int start = keysPerWorker * w;
            final int end = start + keysPerWorker;
            var worker = workers[w] = SwissHashFactory.getInstance().newLongLongSwissHash(recycler, breaker);
            threadPool.executor(ThreadPool.Names.SEARCH).execute(() -> {
                int offset = start;
                long[] batchKeys = new long[CHUNK_SIZE];
                int[] ids = new int[CHUNK_SIZE];
                List<PartitionedHashTable.PartitionedKeys> gens = new ArrayList<>();
                while (offset < end) {
                    int len = Math.min(end - offset, CHUNK_SIZE);
                    if (worker.size() + len > 392_000) {
                        gens.add(partition(worker));
                        worker.clear();
                    }
                    if (worker.supportBulkAdd()) {
                        System.arraycopy(keys, offset, batchKeys, 0, len);
                        worker.bulkAdd(batchKeys, batchKeys, ids, len);
                    } else {
                        for (int i = 0; i < len; i++) {
                            var v = keys[offset + i];
                            var id = (int) worker.add(v, v);
                            if (id < 0) {
                                id = -1 - id;
                            }
                            ids[i] = id;
                        }
                    }
                    offset += len;
                }
                if (worker.size() > 0) {
                    gens.add(partition(worker));
                    worker.clear();
                }
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
            LongLongSwissHash partition = workers[w];
            threadPool.executor(ThreadPool.Names.SEARCH).execute(() -> {
                PartitionedHashTable.MergedKeys mergedKeys = null;
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
                        mergedKeys = partition.mergeKeys(gen, p, totalSize, mergedKeys);
                        gen.releasePartition(p);
                    }
                }
                mergeLatch.countDown();
            });
        }
        mergeLatch.await();
        for (var worker : workers) {
            acc += worker.size();
        }
        Releasables.close(workers);
        Releasables.close(allGens);
        return acc;
    }

    @Benchmark
    public long testPartitionWithOneAgg() throws Exception {
        LongLongSwissHash[] workers = new LongLongSwissHash[NUM_WORKERS];
        CountGroupingAggregatorFunction[] aggs = new  CountGroupingAggregatorFunction[NUM_WORKERS];
        int keysPerWorker = keys.length / NUM_WORKERS;
        CountDownLatch collectLatch = new CountDownLatch(NUM_WORKERS);
        List<PartitionedHashTable.PartitionedKeysAndAggs> allGens = new ArrayList<>();
        for (int w = 0; w < NUM_WORKERS; w++) {
            final int start = keysPerWorker * w;
            final int end = start + keysPerWorker;
            var worker = workers[w] = SwissHashFactory.getInstance().newLongLongSwissHash(recycler, breaker);
            var agg = aggs[w] = new CountGroupingAggregatorFunction(List.of(0), driverContext());
            threadPool.executor(ThreadPool.Names.SEARCH).execute(() -> {
                int offset = start;
                long[] batchKeys = new long[CHUNK_SIZE];
                int[] ids = new int[CHUNK_SIZE];
                List<PartitionedHashTable.PartitionedKeysAndAggs> gens = new ArrayList<>();
                while (offset < end) {
                    int len = Math.min(end - offset, CHUNK_SIZE);
                    if (worker.size() + len > 392_000) {
                        gens.add(partition(worker, agg));
                    }
                    if (worker.supportBulkAdd()) {
                        System.arraycopy(keys, offset, batchKeys, 0, len);
                        worker.bulkAdd(batchKeys, batchKeys, ids, len);
                    } else {
                        for (int i = 0; i < len; i++) {
                            var v = keys[offset + i];
                            var id = (int) worker.add(v, v);
                            if (id < 0) {
                                id = -1 - id;
                            }
                            ids[i] = id;
                        }
                    }
                    for (int i = 0; i < len; i++) {
                        agg.accumulateCount(ids[i], offset + i);
                    }
                    offset += len;
                }
                if (worker.size() > 0) {
                    gens.add(partition(worker, agg));
                }
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
            final LongLongSwissHash partition = workers[w];
            final var agg = aggs[w];
            threadPool.executor(ThreadPool.Names.SEARCH).execute(() -> {
                PartitionedHashTable.MergedKeys[] mergedKeys = new PartitionedHashTable.MergedKeys[allGens.size()];
                for (; ; ) {
                    int p = nextPartition.incrementAndGet();
                    if (p >= LongLongSwissHash.NUM_PARTITIONS) {
                        break;
                    }
                    int totalSize = 0;
                    for (var gen : allGens) {
                        totalSize += gen.keys().partitionSize(p);
                    }
                    partition.clear();
                    int totalLen = 0;
                    for (int i = 0; i < allGens.size(); i++) {
                        var gen = allGens.get(i);
                        mergedKeys[i] = partition.mergeKeys(gen.keys(), p, totalSize, mergedKeys[i]);
                        totalLen += mergedKeys[i].length;
                        gen.keys().releasePartition(p);
                    }
                    agg.clear();
                    for (int i = 0; i < allGens.size(); i++) {
                        var gen = allGens.get(i);
                        agg.combinePartition(gen.aggs(), p, mergedKeys[i].ids, 0, mergedKeys[i].length, totalLen);
                        gen.aggs().releasePartition(p);
                    }
                }
                mergeLatch.countDown();
            });
        }
        mergeLatch.await();
        for (var worker : workers) {
            acc += worker.size();
        }
        Releasables.close(workers);
        Releasables.close(allGens);
        Releasables.close(aggs);
        return acc;
    }

    record NAggs(PartitionedHashTable.PartitionedAgg[] subs) implements PartitionedHashTable.PartitionedAgg {
        @Override
        public void releasePartition(int partition) {
            for (var sub : subs) {
                sub.releasePartition(partition);
            }
        }

        @Override
        public void close() {
            Releasables.close(subs);
        }
    }

    static class NCount {
        static int N = 5;
        final CountGroupingAggregatorFunction[] counts = new CountGroupingAggregatorFunction[N];

        NCount() {
            for (int i = 0; i < N; i++) {
                counts[i] = new CountGroupingAggregatorFunction(List.of(0), driverContext());
            }
        }

        public PartitionedHashTable.AggSplitter newSplitter() {
            PartitionedHashTable.AggSplitter[] splitters = new PartitionedHashTable.AggSplitter[N];
            for (int i = 0; i < N; i++) {
                splitters[i] = counts[i].newSplitter();
            }
            return new PartitionedHashTable.AggSplitter() {
                @Override
                public void split(PartitionedHashTable.ScratchBuffer scratch, int idOffset, int batchLen, short[] positions, int[] fills) {
                    for (var s : splitters) {
                        s.split(scratch, idOffset, batchLen, positions, fills);
                    }
                }

                @Override
                public PartitionedHashTable.PartitionedAgg finish() {
                    PartitionedHashTable.PartitionedAgg[] partitioned = new PartitionedHashTable.PartitionedAgg[N];
                    for (int i = 0; i < N; i++) {
                        partitioned[i] = splitters[i].finish();
                    }
                    return new NAggs(partitioned);
                }

                @Override
                public void close() {
                    Releasables.close(splitters);
                }
            };
        }

        public void clear() {
            for (var count : counts) {
                count.clear();
            }
        }

        public void close() {
            Releasables.close(counts);
        }
    }

    protected PartitionedHashTable.PartitionedKeysAndAggs partition(LongLongSwissHash hashTable, NCount nagg) {
        PartitionedHashTable.AggSplitter splitter = nagg.newSplitter();
        PartitionedHashTable.PartitionedKeys partitionedKeys = hashTable.partition(bigArrays, breaker, splitter);
        PartitionedHashTable.PartitionedAgg partitionedAggs = splitter.finish();
        splitter.close();
        hashTable.clear();
        nagg.clear();
        return new PartitionedHashTable.PartitionedKeysAndAggs(partitionedKeys, partitionedAggs);
    }

    @Benchmark
    public long testPartitionWithNAggs() throws Exception {
        LongLongSwissHash[] workers = new LongLongSwissHash[NUM_WORKERS];
        NCount[] aggs = new NCount[NUM_WORKERS];
        int keysPerWorker = keys.length / NUM_WORKERS;
        CountDownLatch collectLatch = new CountDownLatch(NUM_WORKERS);
        List<PartitionedHashTable.PartitionedKeysAndAggs> allGens = new ArrayList<>();
        for (int w = 0; w < NUM_WORKERS; w++) {
            final int start = keysPerWorker * w;
            final int end = start + keysPerWorker;
            var worker = workers[w] = SwissHashFactory.getInstance().newLongLongSwissHash(recycler, breaker);
            var agg = aggs[w] = new NCount();
            threadPool.executor(ThreadPool.Names.SEARCH).execute(() -> {
                int offset = start;
                long[] batchKeys = new long[CHUNK_SIZE];
                int[] ids = new int[CHUNK_SIZE];
                List<PartitionedHashTable.PartitionedKeysAndAggs> gens = new ArrayList<>();
                while (offset < end) {
                    int len = Math.min(end - offset, CHUNK_SIZE);
                    if (worker.size() + len > 392_000) { //
                        gens.add(partition(worker, agg));
                    }
                    if (worker.supportBulkAdd()) {
                        System.arraycopy(keys, offset, batchKeys, 0, len);
                        worker.bulkAdd(batchKeys, batchKeys, ids, len);
                    } else {
                        for (int i = 0; i < len; i++) {
                            var v = keys[offset + i];
                            var id = (int) worker.add(v, v);
                            if (id < 0) {
                                id = -1 - id;
                            }
                            ids[i] = id;
                        }
                    }
                    // chunk to 64
                    {
                        int writeOffset = 0;
                        while (writeOffset < len) {
                            int batchWrite = Math.min(len - writeOffset, 64);
                            int endWriteBatch = writeOffset + batchWrite;
                            for (CountGroupingAggregatorFunction c : agg.counts) {
                                for (int i = writeOffset; i < endWriteBatch; i++) {
                                    c.accumulateCount(ids[i], offset + i);
                                }
                            }
                            writeOffset += batchWrite;
                        }
                    }
                    offset += len;
                }
                if (worker.size() > 0) {
                    gens.add(partition(worker, agg));
                }
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
            final LongLongSwissHash partition = workers[w];
            final var agg = aggs[w];
            threadPool.executor(ThreadPool.Names.SEARCH).execute(() -> {
                PartitionedHashTable.MergedKeys[] mergedKeys = new PartitionedHashTable.MergedKeys[allGens.size()];
                for (;;) {
                    int p = nextPartition.incrementAndGet();
                    if (p >= LongLongSwissHash.NUM_PARTITIONS) {
                        break;
                    }
                    int totalSize = 0;
                    for (var gen : allGens) {
                        totalSize += gen.keys().partitionSize(p);
                    }
                    partition.clear();
                    int totalLen = 0;
                    for (int i = 0; i < allGens.size(); i++) {
                        var gen = allGens.get(i);
                        mergedKeys[i] = partition.mergeKeys(gen.keys(), p, totalSize, mergedKeys[i]);
                        totalLen += mergedKeys[i].length;
                        gen.keys().releasePartition(p);
                    }
                    agg.clear();
                    for (int si = 0; si < NCount.N; si++) {
                        CountGroupingAggregatorFunction dst = agg.counts[si];
                        for (int gi = 0; gi < allGens.size(); gi++) {
                            NAggs naggs = (NAggs) allGens.get(gi).aggs();
                            dst.combinePartition(naggs.subs[si], p, mergedKeys[gi].ids, 0, mergedKeys[gi].length, totalLen);
                            naggs.subs[si].releasePartition(p);
                        }
                    }
                }
                mergeLatch.countDown();
            });
        }
        mergeLatch.await();
        for (var worker : workers) {
            acc += worker.size();
        }
        Releasables.close(workers);
        Releasables.close(allGens);
        for (var agg : aggs) {
            agg.close();
        }
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
}
