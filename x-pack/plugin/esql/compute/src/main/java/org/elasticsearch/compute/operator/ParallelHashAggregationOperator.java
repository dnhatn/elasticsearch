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
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.exchange.ExchangeBuffer;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public final class ParallelHashAggregationOperator implements Operator {
    public static final int PARTITION_THRESHOLD = 256 * 1500;
    final Function<DriverContext, HashAggregationOperator> fork;
    final Executor executor;
    final ExchangeBuffer in = new ExchangeBuffer(10 * 1024);
    final ExchangeBuffer out = new ExchangeBuffer(10 * 1024);
    final AtomicLong pendingRows = new AtomicLong(0L);
    final Worker[] workers;
    long lastPendingRows = 0L;
    final PendingTasks pendingTasks;
    private final List<PartitionedKeyAndAggs> globalGens = new ArrayList<>();
    boolean finished = false;

    public ParallelHashAggregationOperator(HashAggregationOperator operator, Function<DriverContext, HashAggregationOperator> fork) {
        this.fork = fork;
        this.executor = operator.driverContext.executor;
        this.workers = new Worker[8];
        for (int i = 0; i < workers.length; i++) {
            if (i == 1) {
                workers[i] = new Worker(operator, operator.driverContext.globalBreaker());
            } else {
                workers[i] = new Worker(
                    fork.apply(operator.driverContext.forkDriverContext()),
                    operator.driverContext.globalBreaker()
                );
            }
        }
        pendingTasks = new PendingTasks(this::combinePartitions);
    }

    record MultiPartitionedState(GroupingAggregatorFunction.PartitionedGroupingState[] subs)
        implements
            GroupingAggregatorFunction.PartitionedGroupingState {

        @Override
        public void releasePartition(int p) {
            for (GroupingAggregatorFunction.PartitionedGroupingState sub : subs) {
                if (sub != null) {
                    sub.releasePartition(p);
                }
            }
        }

        @Override
        public void close() {
            Releasables.close(subs);
            Arrays.fill(subs, null);
        }
    }

    final AtomicInteger nextPartition = new AtomicInteger(-1);
    final AtomicInteger completedPartitions = new AtomicInteger(0);

    record PartitionedKeyAndAggs(PartitionedHashTable.PartitionedHashKeys keys, MultiPartitionedState aggs) implements Releasable {
        @Override
        public void close() {
            Releasables.close(keys, aggs);
        }
    }

    class Worker implements Releasable {
        private final List<PartitionedKeyAndAggs> localGens = new ArrayList<>();
        private final HashAggregationOperator operator;
        private final CircuitBreaker globalBreaker;
        int[][] allGenIds = null;
        AtomicBoolean running = new AtomicBoolean(false);

        Worker(HashAggregationOperator operator, CircuitBreaker globalBreaker) {
            this.operator = operator;
            this.globalBreaker = globalBreaker;
        }

        void splitPartition() {
            int partitionSize = Math.ceilDiv(operator.blockHash.numKeys(), PartitionedHashTable.NUM_PARTITIONS);
            List<GroupingAggregatorFunction.GroupingStatePartitioner> splitters = new ArrayList<>(operator.aggregators.size());
            boolean succes = false;
            try {
                for (GroupingAggregator aggregator : operator.aggregators) {
                    var splitter = aggregator.aggregatorFunction().splitPartition(globalBreaker, partitionSize);
                    splitters.add(splitter);
                }
                succes = true;
            } finally {
                if (succes == false) {
                    Releasables.close(splitters);
                }
            }

            GroupingAggregatorFunction.GroupingStatePartitioner splitter = new GroupingAggregatorFunction.GroupingStatePartitioner() {
                @Override
                public GroupingAggregatorFunction.PartitionedGroupingState finish() {
                    int n = splitters.size();
                    GroupingAggregatorFunction.PartitionedGroupingState[] subs = new GroupingAggregatorFunction.PartitionedGroupingState[n];
                    boolean success = false;
                    try {
                        for (int i = 0; i < n; i++) {
                            subs[i] = splitters.get(i).finish();
                        }
                        success = true;
                        return new MultiPartitionedState(subs);
                    } finally {
                        if (success == false) {
                            Releasables.close(subs);
                        }
                    }
                }

                @Override
                public void split(int firstId, short[] shiftedIds, int batchSize, int[] partitionCounts, int[] partitionOffsets) {
                    for (GroupingAggregatorFunction.GroupingStatePartitioner splitter : splitters) {
                        splitter.split(firstId, shiftedIds, batchSize, partitionCounts, partitionOffsets);
                    }
                }

                @Override
                public void close() {
                    Releasables.close(splitters);
                }
            };

            PartitionedHashTable.PartitionedHashKeys partitionKeys = null;
            try {
                partitionKeys = operator.blockHash.splitPartition(operator.driverContext.breaker(), splitter);
                localGens.add(new PartitionedKeyAndAggs(partitionKeys, (MultiPartitionedState) splitter.finish()));
                partitionKeys = null;
            } finally {
                Releasables.close(partitionKeys, splitter);
            }
            operator.blockHash.clear();
            for (int i = 0; i < operator.aggregators.size(); i++) {
                Releasables.close(operator.aggregators.set(i, operator.aggregatorFactories.get(i).apply(operator.driverContext)));
            }
        }

        void addPage(Page page) {
            operator.addInput(page);
            if (operator.blockHash.numKeys() >= PARTITION_THRESHOLD) {
                splitPartition();
            }
        }

        void combinePartitions() {
            int p;
            while ((p = nextPartition.incrementAndGet()) < PartitionedHashTable.NUM_PARTITIONS) {
                combinePartition(p);
                if (completedPartitions.incrementAndGet() >= PartitionedHashTable.NUM_PARTITIONS) {
                    out.finish(false);
                }
            }
        }

        void combinePartition(final int p) {
            final int numGens = globalGens.size();
            int totalKeys = 0;
            for (PartitionedKeyAndAggs partitioned : globalGens) {
                totalKeys += partitioned.keys.keysInPartition(p);
            }
            if (totalKeys == 0) {
                return;
            }
            BlockHash blockHash = operator.blockHash;
            blockHash.clear();
            if (allGenIds == null) {
                allGenIds = new int[globalGens.size()][];
            }
            for (int g = 0; g < numGens; g++) {
                PartitionedKeyAndAggs partitioned = globalGens.get(g);
                var partitionedKeys = partitioned.keys;
                int numKeys = partitionedKeys.keysInPartition(p);
                int[] genIds = this.allGenIds[g];
                if (genIds == null || genIds.length < numKeys) {
                    this.allGenIds[g] = genIds = new int[ArrayUtil.oversize(numKeys, Integer.BYTES)];
                }
                // TODO: use append-only
                blockHash.combinePartition(partitionedKeys, p, totalKeys, genIds);
                operator.rowsAddedInCurrentBatch += numKeys;
                partitionedKeys.releasePartition(p);
            }
            for (int i = 0; i < operator.aggregators.size(); i++) {
                Releasables.close(operator.aggregators.set(i, operator.aggregatorFactories.get(i).apply(operator.driverContext)));
            }
            for (int i = 0; i < operator.aggregators.size(); i++) {
                GroupingAggregatorFunction af = operator.aggregators.get(i).aggregatorFunction();
                for (int g = 0; g < numGens; g++) {
                    PartitionedKeyAndAggs gen = globalGens.get(g);
                    MultiPartitionedState partitioned = gen.aggs;
                    af.combinePartition(partitioned.subs[i], p, allGenIds[g], gen.keys.keysInPartition(p), blockHash.numKeys());
                    partitioned.subs[i].releasePartition(p);
                }
            }
            operator.emit();
            Page page;
            while ((page = operator.getOutput()) != null) {
                page.allowPassingToDifferentDriver();
                out.addPage(page);
            }
        }

        @Override
        public void close() {
            operator.close();
            Releasables.close(localGens);
        }
    }

    @Override
    public boolean needsInput() {
        return true;
    }

    @Override
    public void addInput(Page page) {
        long addInputStart = System.nanoTime();
        page.allowPassingToDifferentDriver();
        in.addPage(page);
        final long pendingRows = this.pendingRows.addAndGet(page.getPositionCount());
        final long newRows = pendingRows - lastPendingRows;
        // better to have something like 1/4 and 3/4
        if (newRows >= PARTITION_THRESHOLD / 4) {
            lastPendingRows = pendingRows;
            startWorkers();
        }
        if (pendingRows >= PARTITION_THRESHOLD * 5) {
            Page p;
            while (this.pendingRows.get() > PARTITION_THRESHOLD * 5 && (p = in.pollPage()) != null) {
                this.pendingRows.addAndGet(-p.getPositionCount());
                workers[0].addPage(p);
            }
        }
        long addInputEnd = System.nanoTime();
        addInputNanos += (addInputEnd - addInputStart);
    }

    int triggers = 0;

    void startWorkers() {
        triggers++;
        Worker selected = null;
        for (int i = 1; i < workers.length; i++) {
            var w = workers[i];
            if (w.running.compareAndSet(false, true)) {
                selected = w;
                break;
            }
        }
        if (selected == null) {
            return;
        }
        Worker worker = selected;
        executor.execute(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {

            }
            @Override
            public void onAfter() {
                worker.running.set(false);
            }

            @Override
            protected void doRun() throws Exception {
                pendingTasks.newTask();
                try {
                    Page page;
                    while ((page = in.pollPage()) != null) {
                        pendingRows.addAndGet(-page.getPositionCount());
                        worker.addPage(page);
                    }
                } finally {
                    worker.running.set(false);
                    pendingTasks.finishTask();
                }
            }
        });
    }

    void combinePartitions() {
        for (Worker w : workers) {
            globalGens.addAll(w.localGens);
            w.localGens.clear();
        }
        for (Worker w : workers) {
            executor.execute(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {

                }

                @Override
                protected void doRun() throws Exception {
                    w.combinePartitions();
                }
            });
        }
    }

    @Override
    public void finish() {
        long finishStart = System.nanoTime();
        finished = true;
        in.finish(false);
        if (pendingRows.get() > PARTITION_THRESHOLD / 4) {
            startWorkers();
        }
        Page page;
        Worker worker = workers[0];
        while ((page = in.pollPage()) != null) {
            pendingRows.addAndGet(-page.getPositionCount());
            worker.addPage(page);
        }
        pendingTasks.finishTask();
        long finishEnd = System.nanoTime();
        System.err.println("--> finish took " + (finishEnd - finishStart));
    }

    @Override
    public Status status() {
        return workers[1].operator.status();
    }

    long addInputNanos = 0L;


    @Override
    public Page getOutput() {
        return out.pollPage();
    }

    @Override
    public IsBlockedResult isBlocked() {
        if (finished) {
            return out.waitForReading();
        } else {
            return NOT_BLOCKED;
        }
    }

    @Override
    public boolean isFinished() {
        return out.isFinished();
    }

    @Override
    public boolean canProduceMoreDataWithoutExtraInput() {
        return false;
    }

    @Override
    public void close() {
        System.err.println("--> triggered workers " + triggers + " addInput=" + addInputNanos);
        for (Worker w : workers) {
            w.close();
        }
    }

    private static class PendingTasks {
        final AtomicInteger instances = new AtomicInteger(1);
        final AtomicBoolean completed = new AtomicBoolean();
        final Runnable completion;

        PendingTasks(Runnable completion) {
            this.completion = completion;
        }

        void newTask() {
            int refs = instances.incrementAndGet();
            assert refs > 0;
        }

        boolean finishTask() {
            int refs = instances.decrementAndGet();
            assert refs >= 0;
            if (refs == 0 && completed.compareAndSet(false, true)) {
                completion.run();
                return true;
            }
            return false;
        }
    }
}
