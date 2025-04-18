/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.OrdinalBytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.function.Function;

/**
 * Creates a source operator that takes advantage of the natural sorting of segments in a tsdb index.
 * <p>
 * This source operator loads the _tsid and @timestamp fields, which is used for emitting documents in the correct order. These field values
 * are included in the page as seperate blocks and downstream operators can make use of these loaded time series ids and timestamps.
 * <p>
 * The source operator includes all documents of a time serie with the same page. So the same time series never exists in multiple pages.
 * Downstream operators can make use of this implementation detail.
 * <p>
 * This operator currently only supports shard level concurrency. A new concurrency mechanism should be introduced at the time serie level
 * in order to read tsdb indices in parallel.
 */
public class TimeSeriesSortedSourceOperatorFactory extends LuceneOperator.Factory {

    private final int maxPageSize;

    private TimeSeriesSortedSourceOperatorFactory(
        List<? extends ShardContext> contexts,
        Function<ShardContext, Query> queryFunction,
        int taskConcurrency,
        int maxPageSize,
        int limit
    ) {
        super(
            contexts,
            queryFunction,
            DataPartitioning.SHARD,
            query -> { throw new UnsupportedOperationException("locked to SHARD partitioning"); },
            taskConcurrency,
            limit,
            false,
            ScoreMode.COMPLETE_NO_SCORES
        );
        this.maxPageSize = maxPageSize;
    }

    @Override
    public SourceOperator get(DriverContext driverContext) {
        return new Impl(driverContext.blockFactory(), sliceQueue, maxPageSize, limit);
    }

    @Override
    public String describe() {
        return "TimeSeriesSortedSourceOperator[maxPageSize = " + maxPageSize + ", limit = " + limit + "]";
    }

    public static TimeSeriesSortedSourceOperatorFactory create(
        int limit,
        int maxPageSize,
        int taskConcurrency,
        List<? extends ShardContext> searchContexts,
        Function<ShardContext, Query> queryFunction
    ) {
        return new TimeSeriesSortedSourceOperatorFactory(searchContexts, queryFunction, taskConcurrency, maxPageSize, limit);
    }

    static final class Impl extends SourceOperator {

        private final int maxPageSize;
        private final BlockFactory blockFactory;
        private final LuceneSliceQueue sliceQueue;
        private int currentPagePos = 0;
        private int remainingDocs;
        private boolean doneCollecting;
        private IntVector.Builder docsBuilder;
        private IntVector.Builder segmentsBuilder;
        private LongVector.Builder timestampsBuilder;
        private TsidBuilder tsHashesBuilder;
        private SegmentsIterator iterator;

        Impl(BlockFactory blockFactory, LuceneSliceQueue sliceQueue, int maxPageSize, int limit) {
            this.maxPageSize = maxPageSize;
            this.blockFactory = blockFactory;
            this.remainingDocs = limit;
            this.docsBuilder = blockFactory.newIntVectorBuilder(Math.min(limit, maxPageSize));
            this.segmentsBuilder = null;
            this.timestampsBuilder = blockFactory.newLongVectorBuilder(Math.min(limit, maxPageSize));
            this.tsHashesBuilder = new TsidBuilder(blockFactory, Math.min(limit, maxPageSize));
            this.sliceQueue = sliceQueue;
        }

        @Override
        public void finish() {
            this.doneCollecting = true;
        }

        @Override
        public boolean isFinished() {
            return doneCollecting;
        }

        @Override
        public Page getOutput() {
            if (isFinished()) {
                return null;
            }

            if (remainingDocs <= 0) {
                doneCollecting = true;
                return null;
            }

            Page page = null;
            IntBlock shards = null;
            IntVector segments = null;
            IntVector docs = null;
            LongVector timestamps = null;
            BytesRefVector tsids = null;
            try {
                while (iterator == null) {
                    var slice = sliceQueue.nextSlice();
                    if (slice == null) {
                        doneCollecting = true;
                        return null;
                    }
                    Weight weight = slice.weight();
                    PriorityQueue<Leaf> queue = new PriorityQueue<>(slice.numLeaves()) {
                        @Override
                        protected boolean lessThan(Leaf a, Leaf b) {
                            return a.timeSeriesHash.compareTo(b.timeSeriesHash) < 0;
                        }
                    };
                    for (var leafReaderContext : slice.leaves()) {
                        Leaf leaf = new Leaf(weight, leafReaderContext.leafReaderContext());
                        leaf.nextDoc();
                        if (leaf.docID != DocIdSetIterator.NO_MORE_DOCS) {
                            queue.add(leaf);
                        }
                    }
                    if (queue.size() > 0) {
                        iterator = new SegmentsIterator(queue);
                    }
                }
                iterator.readDocsForNextPage();
                if (currentPagePos > 0) {
                    shards = blockFactory.newConstantIntBlockWith(0, currentPagePos);
                    if (segmentsBuilder != null) {
                        segments = segmentsBuilder.build();
                        segmentsBuilder = null;
                    } else {
                        segments = blockFactory.newConstantIntVector(iterator.lastVisitedSegment, currentPagePos);
                    }
                    docs = docsBuilder.build();
                    docsBuilder = blockFactory.newIntVectorBuilder(Math.min(remainingDocs, maxPageSize));
                    timestamps = timestampsBuilder.build();
                    timestampsBuilder = blockFactory.newLongVectorBuilder(Math.min(remainingDocs, maxPageSize));
                    tsids = tsHashesBuilder.build();
                    tsHashesBuilder = new TsidBuilder(blockFactory, Math.min(remainingDocs, maxPageSize));
                    page = new Page(
                        currentPagePos,
                        new DocVector(shards.asVector(), segments, docs, segments.isConstant()).asBlock(),
                        tsids.asBlock(),
                        timestamps.asBlock()
                    );
                    currentPagePos = 0;
                }
                if (iterator.completed()) {
                    iterator = null;
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } finally {
                if (page == null) {
                    Releasables.closeExpectNoException(shards, segments, docs, timestamps, tsids);
                }
            }
            return page;
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(docsBuilder, segmentsBuilder, timestampsBuilder, tsHashesBuilder);
        }

        class SegmentsIterator {
            private int lastVisitedSegment = -1;
            private final PriorityQueue<Leaf> mainQueue;
            private final PriorityQueue<Leaf> oneTsidQueue;

            SegmentsIterator(PriorityQueue<Leaf> mainQueue) throws IOException {
                this.mainQueue = mainQueue;
                this.oneTsidQueue = new PriorityQueue<>(mainQueue.size()) {
                    @Override
                    protected boolean lessThan(Leaf a, Leaf b) {
                        return a.timestamp > b.timestamp;
                    }
                };
            }

            void readDocsForNextPage() throws IOException {
                if (mainQueue.size() == 1) {
                    Leaf top = mainQueue.top();
                    readDocsFromSingleLeaf(top);
                    if (top.docID == DocIdSetIterator.NO_MORE_DOCS) {
                        mainQueue.pop();
                    }
                    return;
                }
                if (segmentsBuilder == null) {
                    segmentsBuilder = blockFactory.newIntVectorBuilder(Math.min(remainingDocs, maxPageSize));
                }
                Thread executingThread = Thread.currentThread();
                for (Leaf leaf : mainQueue) {
                    leaf.reinitializeIfNeeded(executingThread);
                }
                for (Leaf leaf : oneTsidQueue) {
                    leaf.reinitializeIfNeeded(executingThread);
                }
                do {
                    PriorityQueue<Leaf> sub = subQueueForNextTsid();
                    if (sub.size() == 0) {
                        break;
                    }
                    tsHashesBuilder.appendNewTsid(sub.top().timeSeriesHash);
                    if (readValuesForOneTsid(sub)) {
                        break;
                    }
                } while (mainQueue.size() > 0);
            }

            private boolean readValuesForOneTsid(PriorityQueue<Leaf> sub) throws IOException {
                do {
                    Leaf top = sub.top();
                    currentPagePos++;
                    remainingDocs--;
                    segmentsBuilder.appendInt(top.segmentOrd);
                    docsBuilder.appendInt(top.docID);
                    tsHashesBuilder.appendOrdinal();
                    timestampsBuilder.appendLong(top.timestamp);
                    if (top.nextDoc()) {
                        sub.updateTop();
                    } else if (top.docID == DocIdSetIterator.NO_MORE_DOCS) {
                        sub.pop();
                    } else {
                        mainQueue.add(sub.pop());
                    }
                    if (remainingDocs <= 0 || currentPagePos >= maxPageSize) {
                        return true;
                    }
                } while (sub.size() > 0);
                return false;
            }

            private void readDocsFromSingleLeaf(Leaf leaf) throws IOException {
                lastVisitedSegment = leaf.segmentOrd;
                for (;;) {
                    currentPagePos++;
                    remainingDocs--;
                    if (segmentsBuilder != null) {
                        segmentsBuilder.appendInt(leaf.segmentOrd);
                    }
                    docsBuilder.appendInt(leaf.docID);
                    tsHashesBuilder.appendOrdinal();
                    timestampsBuilder.appendLong(leaf.timestamp);
                    leaf.nextDoc();
                    if (leaf.docID == DocIdSetIterator.NO_MORE_DOCS) {
                        break;
                    }
                    if (remainingDocs <= 0 || currentPagePos >= maxPageSize) {
                        break;
                    }
                }
            }

            private PriorityQueue<Leaf> subQueueForNextTsid() {
                if (oneTsidQueue.size() == 0 && mainQueue.size() > 0) {
                    Leaf last = mainQueue.pop();
                    oneTsidQueue.add(last);
                    while (mainQueue.size() > 0) {
                        var top = mainQueue.top();
                        if (top.timeSeriesHash.equals(last.timeSeriesHash)) {
                            oneTsidQueue.add(mainQueue.pop());
                        } else {
                            break;
                        }
                    }
                }
                return oneTsidQueue;
            }

            boolean completed() {
                return oneTsidQueue.size() == 0 && mainQueue.size() == 0;
            }
        }

        static class Leaf {
            private final int segmentOrd;
            private final Weight weight;
            private final LeafReaderContext leaf;
            private SortedDocValues tsids;
            private NumericDocValues timestamps;
            private DocIdSetIterator disi;
            private Thread createdThread;

            private long timestamp;
            private int lastTsidOrd = -1;
            private BytesRef timeSeriesHash;
            private int docID = -1;

            Leaf(Weight weight, LeafReaderContext leaf) throws IOException {
                this.segmentOrd = leaf.ord;
                this.weight = weight;
                this.leaf = leaf;
                this.createdThread = Thread.currentThread();
                tsids = leaf.reader().getSortedDocValues("_tsid");
                timestamps = DocValues.unwrapSingleton(leaf.reader().getSortedNumericDocValues("@timestamp"));
                final Scorer scorer = weight.scorer(leaf);
                disi = scorer != null ? scorer.iterator() : DocIdSetIterator.empty();
            }

            boolean nextDoc() throws IOException {
                docID = disi.nextDoc();
                if (docID == DocIdSetIterator.NO_MORE_DOCS) {
                    return false;
                }
                boolean advanced = timestamps.advanceExact(docID);
                assert advanced;
                timestamp = timestamps.longValue();
                advanced = tsids.advanceExact(docID);
                assert advanced;

                int ord = tsids.ordValue();
                if (ord != lastTsidOrd) {
                    timeSeriesHash = tsids.lookupOrd(ord);
                    lastTsidOrd = ord;
                    return false;
                } else {
                    return true;
                }
            }

            void reinitializeIfNeeded(Thread executingThread) throws IOException {
                if (executingThread != createdThread) {
                    tsids = leaf.reader().getSortedDocValues("_tsid");
                    timestamps = DocValues.unwrapSingleton(leaf.reader().getSortedNumericDocValues("@timestamp"));
                    final Scorer scorer = weight.scorer(leaf);
                    disi = scorer != null ? scorer.iterator() : DocIdSetIterator.empty();
                    if (docID != -1) {
                        disi.advance(docID);
                    }
                    createdThread = executingThread;
                }
            }
        }

        @Override
        public String toString() {
            return this.getClass().getSimpleName() + "[" + "maxPageSize=" + maxPageSize + ", remainingDocs=" + remainingDocs + "]";
        }

    }

    /**
     * Collect tsids then build a {@link OrdinalBytesRefVector}
     */
    static final class TsidBuilder implements Releasable {
        private int currentOrd = -1;
        private final BytesRefVector.Builder dictBuilder;
        private final IntVector.Builder ordinalsBuilder;

        TsidBuilder(BlockFactory blockFactory, int estimatedSize) {
            final var dictBuilder = blockFactory.newBytesRefVectorBuilder(estimatedSize);
            boolean success = false;
            try {
                this.dictBuilder = dictBuilder;
                this.ordinalsBuilder = blockFactory.newIntVectorBuilder(estimatedSize);
                success = true;
            } finally {
                if (success == false) {
                    dictBuilder.close();
                }
            }
        }

        void appendNewTsid(BytesRef tsid) {
            currentOrd++;
            dictBuilder.appendBytesRef(tsid);
        }

        void appendOrdinal() {
            ordinalsBuilder.appendInt(currentOrd);
        }

        @Override
        public void close() {
            Releasables.close(dictBuilder, ordinalsBuilder);
        }

        BytesRefVector build() throws IOException {
            BytesRefVector dict = null;
            BytesRefVector result = null;
            IntVector ordinals = null;
            try {
                dict = dictBuilder.build();
                ordinals = ordinalsBuilder.build();
                result = new OrdinalBytesRefVector(ordinals, dict);
            } finally {
                if (result == null) {
                    Releasables.close(dict, ordinalsBuilder);
                }
            }
            return result;
        }
    }
}
