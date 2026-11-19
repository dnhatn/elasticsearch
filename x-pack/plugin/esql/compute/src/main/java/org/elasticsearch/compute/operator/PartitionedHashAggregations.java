/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.PartitionedHashTable;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.SeenGroupIds;
import org.elasticsearch.compute.aggregation.blockhash.PartitionedBlockHash;
import org.elasticsearch.compute.data.PartitionedAggregationBlock;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * Helper class for {@link ParallelHashAggregationOperator} to hold partitioned aggregation states.
 */
final class PartitionedHashAggregations extends AbstractRefCounted implements Releasable {
    private static final Logger logger = LogManager.getLogger(PartitionedHashAggregations.class);

    static final int FOLD_RANGES = 16;
    static final int PARTITIONS_PER_RANGE = PartitionedHashTable.NUM_PARTITIONS / FOLD_RANGES;
    static final int FOLD_MIN_GENERATIONS = 8;
    static final int FOLD_MIN_OVERLAP = 2;

    private final CircuitBreaker globalBreaker;
    /** Append-only, so a range's unfolded generations are a suffix; a slot is cleared once every range has folded it. */
    private final List<Generation> generations = new ArrayList<>();

    private final AtomicInteger nextPartition = new AtomicInteger();
    private final AtomicInteger completedPartitions = new AtomicInteger();

    private final FoldRange[] foldRanges = new FoldRange[FOLD_RANGES];
    private final AtomicInteger nextFoldRange = new AtomicInteger();
    private final AtomicBoolean probing = new AtomicBoolean();
    private volatile boolean foldsPay;
    private volatile int foldThreshold = FOLD_MIN_GENERATIONS;
    private final AtomicInteger folds = new AtomicInteger();
    private final AtomicInteger lowOverlapFolds = new AtomicInteger();
    private final AtomicInteger skippedFolds = new AtomicInteger();
    private final AtomicLong sampleNanos = new AtomicLong();
    private final AtomicLong foldNanos = new AtomicLong();
    private final AtomicLong foldedEntries = new AtomicLong();
    private final AtomicLong foldedKeys = new AtomicLong();

    /** Returned by {@link #claimPartition()} when every partition has been claimed. */
    static final int NO_MORE_PARTITION = Integer.MAX_VALUE;

    /**
     * @param folders creates the operator each range folds into
     */
    PartitionedHashAggregations(CircuitBreaker globalBreaker, Supplier<HashAggregationOperator> folders) {
        this.globalBreaker = globalBreaker;
        boolean success = false;
        try {
            for (int r = 0; r < FOLD_RANGES; r++) {
                foldRanges[r] = new FoldRange(folders.get());
            }
            success = true;
        } finally {
            if (success == false) {
                closeFolders();
            }
        }
    }

    /**
     * Claims the next partition to emit, or returns {@link #NO_MORE_PARTITION} if every partition has been claimed.
     */
    int claimPartition() {
        int p = nextPartition.getAndIncrement();
        return p < PartitionedHashTable.NUM_PARTITIONS ? p : NO_MORE_PARTITION;
    }

    boolean hasUncombinedPartitions() {
        return nextPartition.get() < PartitionedHashTable.NUM_PARTITIONS;
    }

    /**
     * Records that one partition has been fully emitted; returns true when this was the last one.
     */
    boolean completePartition() {
        return completedPartitions.incrementAndGet() == PartitionedHashTable.NUM_PARTITIONS;
    }

    void split(CircuitBreaker breaker, HashAggregationOperator op) {
        if (op.blockHash.numKeys() == 0) {
            return;
        }
        PartitionedKeyAndAggs partitioned = splitKeysAndAggs(breaker, op);
        synchronized (generations) {
            generations.add(new Generation(partitioned));
        }
        op.clearCurrentBatch();
    }

    @Override
    public void close() {
        decRef();
    }

    @Override
    protected void closeInternal() {
        if (folds.get() > 0 || skippedFolds.get() > 0) {
            logger.info(
                "folds [{}] low overlap [{}] in [{}ms]: entries [{}] keys [{}]; skipped [{}] after sampling for [{}ms]",
                folds.get(),
                lowOverlapFolds.get(),
                foldNanos.get() / 1_000_000,
                foldedEntries.get(),
                foldedKeys.get(),
                skippedFolds.get(),
                sampleNanos.get() / 1_000_000
            );
        }
        Releasables.close(() -> {
            for (Generation gen : generations) {
                if (gen != null) {
                    gen.data.releaseAll(globalBreaker);
                }
            }
            for (FoldRange range : foldRanges) {
                if (range.folded != null) {
                    range.folded.releaseAll(globalBreaker);
                }
            }
        }, this::closeFolders);
    }

    private void closeFolders() {
        for (FoldRange range : foldRanges) {
            if (range != null) {
                Releasables.close(range.folder);
            }
        }
    }

    int folds() {
        return folds.get();
    }

    int lowOverlapFolds() {
        return lowOverlapFolds.get();
    }

    int skippedFolds() {
        return skippedFolds.get();
    }

    int numGenerations() {
        synchronized (generations) {
            return generations.size();
        }
    }

    int liveGenerations() {
        synchronized (generations) {
            int live = 0;
            for (Generation gen : generations) {
                if (gen != null) {
                    live++;
                }
            }
            return live;
        }
    }

    /**
     * Folds the unfolded generations of one range of partitions into that range's previous fold, so a range holds about its
     * distinct keys rather than one entry per key per generation. Ranges fold independently, so workers can fold different
     * ranges at once, each into a hash of one range's keys. Until folds pay off, only one range folds at a time, and only
     * after the keys of one of its partitions show that generations repeat keys. A skipped fold, or a fold that merges little,
     * backs off. Callers must not fold once emitting has started.
     *
     * @return whether a range was folded
     */
    boolean maybeFold() {
        final boolean probe = foldsPay == false;
        if (probe && probing.compareAndSet(false, true) == false) {
            return false;
        }
        try {
            final int end = numGenerations();
            final int threshold = foldThreshold;
            final int start = nextFoldRange.getAndIncrement();
            for (int i = 0; i < FOLD_RANGES; i++) {
                final int r = Math.floorMod(start + i, FOLD_RANGES);
                final FoldRange range = foldRanges[r];
                if (end - range.foldedUpTo < threshold || range.lock.tryLock() == false) {
                    continue;
                }
                try {
                    final int pending = end - range.foldedUpTo;
                    if (pending >= threshold) {
                        if (probe && sampleOverlaps(r, range, end) == false) {
                            skippedFolds.incrementAndGet();
                            foldThreshold = 2 * pending;
                            return false;
                        }
                        foldRange(r, range, end);
                        return true;
                    }
                } finally {
                    range.lock.unlock();
                }
            }
            return false;
        } finally {
            if (probe) {
                probing.set(false);
            }
        }
    }

    /**
     * Merges only the keys of one partition of the range, without releasing them, to tell whether folding would merge much.
     */
    private boolean sampleOverlaps(int r, FoldRange range, int end) {
        final long startNanos = System.nanoTime();
        final int p = r * PARTITIONS_PER_RANGE;
        final PartitionedKeyAndAggs previous = range.folded;
        final PartitionedBlockHash hash = (PartitionedBlockHash) range.folder.blockHash;
        long entries = 0;
        int[] ids = new int[0];
        try {
            for (PartitionedKeyAndAggs source : sources(range, end)) {
                final int numKeys = source.keys.keysInPartition(p);
                if (numKeys > 0) {
                    entries += numKeys;
                    if (ids.length < numKeys) {
                        ids = new int[ArrayUtil.oversize(numKeys, Integer.BYTES)];
                    }
                    hash.combinePartition(source.keys, p, ids);
                }
            }
            final long previousKeys = previous == null ? 0 : previous.keys.keysInPartition(p);
            return overlaps(entries - previousKeys, hash.numKeys() - previousKeys);
        } finally {
            range.folder.clearCurrentBatch();
            sampleNanos.addAndGet(System.nanoTime() - startNanos);
        }
    }

    private static boolean overlaps(long newEntries, long newKeys) {
        return newEntries >= FOLD_MIN_OVERLAP * newKeys;
    }

    /** The range's previous fold, if any, followed by the generations it hasn't folded. */
    private List<PartitionedKeyAndAggs> sources(FoldRange range, int end) {
        final int from = range.foldedUpTo;
        final List<PartitionedKeyAndAggs> sources = new ArrayList<>(end - from + 1);
        if (range.folded != null) {
            sources.add(range.folded);
        }
        synchronized (generations) {
            for (int g = from; g < end; g++) {
                sources.add(generations.get(g).data);
            }
        }
        return sources;
    }

    private void foldRange(int r, FoldRange range, int end) {
        final long startNanos = System.nanoTime();
        final HashAggregationOperator folder = range.folder;
        final int from = range.foldedUpTo;
        final PartitionedKeyAndAggs previous = range.folded;
        final List<PartitionedKeyAndAggs> sources = sources(range, end);
        final int firstPartition = r * PARTITIONS_PER_RANGE;
        long previousKeys = 0;
        long entries = 0;
        for (int p = firstPartition; p < firstPartition + PARTITIONS_PER_RANGE; p++) {
            for (PartitionedKeyAndAggs source : sources) {
                entries += source.keys.keysInPartition(p);
            }
            if (previous != null) {
                previousKeys += previous.keys.keysInPartition(p);
            }
        }
        final List<PartitionedKeyAndAggs> foldedGenerations = new ArrayList<>();
        try (Combiner combiner = new Combiner(folder)) {
            for (int p = firstPartition; p < firstPartition + PARTITIONS_PER_RANGE; p++) {
                combiner.combine(sources, p);
            }
            final int keys = folder.blockHash.numKeys();
            range.folded = keys == 0 ? null : splitKeysAndAggs(folder.driverContext.breaker(), folder);
            if (range.folded != null) {
                // splitters size every partition up front, but a range's fold only ever reads its own partitions
                for (int p = 0; p < PartitionedHashTable.NUM_PARTITIONS; p++) {
                    if (p < firstPartition || p >= firstPartition + PARTITIONS_PER_RANGE) {
                        range.folded.releasePartition(globalBreaker, p);
                    }
                }
            }
            range.foldedUpTo = end;
            if (previous != null) {
                previous.releaseAll(globalBreaker);
            }
            synchronized (generations) {
                for (int g = from; g < end; g++) {
                    final Generation gen = generations.get(g);
                    if (--gen.unfoldedRanges == 0) {
                        generations.set(g, null);
                        foldedGenerations.add(gen.data);
                    }
                }
            }
            if (overlaps(entries - previousKeys, keys - previousKeys) == false) {
                lowOverlapFolds.incrementAndGet();
                foldsPay = false;
                foldThreshold = 2 * (end - from);
            } else {
                foldsPay = true;
                foldThreshold = FOLD_MIN_GENERATIONS;
            }
            folds.incrementAndGet();
            foldedEntries.addAndGet(entries);
            foldedKeys.addAndGet(keys);
            foldNanos.addAndGet(System.nanoTime() - startNanos);
        } finally {
            folder.clearCurrentBatch();
            for (PartitionedKeyAndAggs gen : foldedGenerations) {
                gen.releaseAll(globalBreaker);
            }
        }
    }

    Combiner newCombiner(HashAggregationOperator op) {
        return new Combiner(op);
    }

    static PartitionedAggregationBlock splitToPartitionedBlock(CircuitBreaker breaker, HashAggregationOperator op) {
        int numKeys = op.blockHash.numKeys();
        PartitionedKeyAndAggs keysAndAggs = splitKeysAndAggs(breaker, op);
        try {
            var block = new PartitionedAggregationBlock(
                op.driverContext.blockFactory(),
                numKeys,
                keysAndAggs.keys,
                keysAndAggs.aggs.states
            );
            keysAndAggs = null;
            return block;
        } finally {
            if (keysAndAggs != null) {
                keysAndAggs.releaseAll(breaker);
            }
        }
    }

    private static PartitionedKeyAndAggs splitKeysAndAggs(CircuitBreaker breaker, HashAggregationOperator op) {
        PartitionedHashTable.PartitionedHashKeys partitionedKeys = null;
        MultiAggsPartitionSplitter aggSplitter = new MultiAggsPartitionSplitter(breaker, op.aggregators);
        try {
            partitionedKeys = ((PartitionedHashTable) op.blockHash).splitPartition(breaker, aggSplitter);
            PartitionedKeyAndAggs result = new PartitionedKeyAndAggs(partitionedKeys, aggSplitter.finishAll(breaker));
            partitionedKeys = null;
            return result;
        } finally {
            aggSplitter.release(breaker);
            if (partitionedKeys != null) {
                partitionedKeys.releaseAll(breaker);
            }
        }
    }

    /**
     * Receive partitioned output blocks from initial operators
     */
    void addPartitionedBlocks(List<PartitionedAggregationBlock> blocks) {
        // blocking but should be fast
        synchronized (generations) {
            for (PartitionedAggregationBlock block : blocks) {
                var keys = block.takeKeys();
                generations.add(new Generation(new PartitionedKeyAndAggs(keys, new MultiAggsPartitionedState(block.takeAggs()))));
            }
        }
    }

    /**
     * Combines one partition from every generation
     */
    final class Combiner implements Releasable {
        private final HashAggregationOperator op;
        private final CircuitBreaker breaker;
        private int[][] allGenIds = new int[0][];
        private boolean[] appendOnly = new boolean[0];

        Combiner(HashAggregationOperator op) {
            this.op = op;
            this.breaker = op.driverContext.breaker();
        }

        /**
         * Combines partition {@code p} of the range's fold and of every generation the range hasn't folded into a cleared operator.
         */
        boolean combine(int p) {
            final List<PartitionedKeyAndAggs> sources = sources(foldRanges[p / PARTITIONS_PER_RANGE], numGenerations());
            op.clearCurrentBatch();
            combine(sources, p);
            return true;
        }

        void combine(List<PartitionedKeyAndAggs> sources, int p) {
            final int numGens = sources.size();
            PartitionedBlockHash blockHash = (PartitionedBlockHash) op.blockHash;
            if (allGenIds.length < numGens) {
                allGenIds = Arrays.copyOf(allGenIds, numGens);
                appendOnly = new boolean[numGens];
            }
            // Combine keys from every generation first, then combine each aggregation across all generations.
            // This keeps accesses to the hash table and aggregation state cache-friendly.
            for (int g = 0; g < numGens; g++) {
                PartitionedKeyAndAggs partitioned = sources.get(g);
                var partitionedKeys = partitioned.keys;
                int numKeys = partitionedKeys.keysInPartition(p);
                ensureGenIds(g, numKeys);
                appendOnly[g] = blockHash.combinePartition(partitionedKeys, p, allGenIds[g]);
                op.rowsAddedInCurrentBatch += numKeys;
                partitionedKeys.releasePartition(breaker, p);
            }
            // now combine aggregations
            List<GroupingAggregator> aggregators = op.aggregators;
            for (int i = 0; i < aggregators.size(); i++) {
                final var aggregator = aggregators.get(i).aggregatorFunction();
                // If some group in any generation is missing a value we need to track groupIds
                for (int g = 0; g < numGens; g++) {
                    if (sources.get(g).aggs.states[i].hasAllValues(p) == false) {
                        aggregator.selectedMayContainUnseenGroups(new SeenGroupIds.Empty());
                        break;
                    }
                }
                aggregator.maybeEnsureCapacity(blockHash.numKeys() + 1);
                for (int g = 0; g < numGens; g++) {
                    PartitionedKeyAndAggs keysAndAggs = sources.get(g);
                    GroupingAggregatorFunction.PartitionedState agg = keysAndAggs.aggs.states[i];
                    aggregator.combinePartition(agg, p, appendOnly[g], allGenIds[g], keysAndAggs.keys.keysInPartition(p));
                    agg.releasePartition(breaker, p);
                }
            }
        }

        private void ensureGenIds(int g, int numKeys) {
            int[] genIds = allGenIds[g];
            final int oldLength = genIds == null ? 0 : genIds.length;
            if (genIds == null || oldLength < numKeys) {
                final int newLength = ArrayUtil.oversize(numKeys, Integer.BYTES);
                breaker.addEstimateBytesAndMaybeBreak((long) (newLength - oldLength) * Integer.BYTES, "PartitionedHashAggregations");
                allGenIds[g] = new int[newLength];
            }
        }

        @Override
        public void close() {
            long bytes = 0;
            for (int[] genIds : allGenIds) {
                if (genIds != null) {
                    bytes += (long) genIds.length * Integer.BYTES;
                }
            }
            breaker.addWithoutBreaking(-bytes, "PartitionedHashAggregations");
        }
    }

    private static final class Generation {
        final PartitionedKeyAndAggs data;
        /** Guarded by the generations lock. */
        int unfoldedRanges = FOLD_RANGES;

        Generation(PartitionedKeyAndAggs data) {
            this.data = data;
        }
    }

    private static final class FoldRange {
        final ReentrantLock lock = new ReentrantLock();
        final HashAggregationOperator folder;
        volatile PartitionedKeyAndAggs folded;
        volatile int foldedUpTo;

        FoldRange(HashAggregationOperator folder) {
            this.folder = folder;
        }
    }

    private record PartitionedKeyAndAggs(PartitionedHashTable.PartitionedHashKeys keys, MultiAggsPartitionedState aggs) {
        void releasePartition(CircuitBreaker breaker, int partition) {
            keys.releasePartition(breaker, partition);
            for (var state : aggs.states) {
                state.releasePartition(breaker, partition);
            }
        }

        void releaseAll(CircuitBreaker breaker) {
            keys.releaseAll(breaker);
            aggs.releaseAll(breaker);
        }
    }

    private record MultiAggsPartitionedState(GroupingAggregatorFunction.PartitionedState[] states) {
        void releaseAll(CircuitBreaker breaker) {
            for (var state : states) {
                state.releaseAll(breaker);
            }
        }
    }

    private static class MultiAggsPartitionSplitter implements PartitionedHashTable.PartitionSplitter {
        final GroupingAggregatorFunction.PartitionSplitter[] splitters;

        MultiAggsPartitionSplitter(CircuitBreaker breaker, List<GroupingAggregator> aggregators) {
            this.splitters = new GroupingAggregatorFunction.PartitionSplitter[aggregators.size()];
            boolean success = false;
            try {
                for (int i = 0; i < splitters.length; i++) {
                    splitters[i] = aggregators.get(i).aggregatorFunction().createPartitioningSplitter(breaker);
                }
                success = true;
            } finally {
                if (success == false) {
                    release(breaker);
                }
            }
        }

        public MultiAggsPartitionedState finishAll(CircuitBreaker breaker) {
            GroupingAggregatorFunction.PartitionedState[] states = new GroupingAggregatorFunction.PartitionedState[splitters.length];
            boolean success = false;
            try {
                for (int i = 0; i < splitters.length; i++) {
                    states[i] = splitters[i].finish();
                }
                success = true;
            } finally {
                if (success == false) {
                    for (var state : states) {
                        if (state != null) {
                            state.releaseAll(breaker);
                        }
                    }
                }
            }
            return new MultiAggsPartitionedState(states);
        }

        @Override
        public void split(int firstId, short[] shiftedIds, int batchSize, int[] batchPartitionCounts, int[] partitionOffsets) {
            for (var splitter : splitters) {
                splitter.split(firstId, shiftedIds, batchSize, batchPartitionCounts, partitionOffsets);
            }
        }

        @Override
        public void release(CircuitBreaker breaker) {
            for (int i = 0; i < splitters.length; i++) {
                var splitter = splitters[i];
                if (splitter != null) {
                    splitters[i] = null;
                    splitter.release(breaker);
                }
            }
        }
    }
}
