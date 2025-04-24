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
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.OrdinalBytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.BlockLoader;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
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
    private final List<String> counterFields;


    private TimeSeriesSortedSourceOperatorFactory(
        List<? extends ShardContext> contexts,
        Function<ShardContext, Query> queryFunction,
        List<String> counterFields,
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
        this.counterFields = counterFields;
    }

    @Override
    public SourceOperator get(DriverContext driverContext) {
        return new Impl(driverContext.blockFactory(), counterFields, sliceQueue, maxPageSize, limit);
    }

    @Override
    public String describe() {
        return "TimeSeriesSortedSourceOperator[maxPageSize = " + maxPageSize + ", limit = " + limit + "]";
    }

    public static TimeSeriesSortedSourceOperatorFactory create(
        int limit,
        int maxPageSize,
        int taskConcurrency,
        List<String> counterFields,
        List<? extends ShardContext> searchContexts,
        Function<ShardContext, Query> queryFunction
    ) {
        return new TimeSeriesSortedSourceOperatorFactory(searchContexts, queryFunction, counterFields, taskConcurrency, maxPageSize, limit);
    }

    static final class Impl extends SourceOperator {

        private final int maxPageSize;
        private final BlockFactory blockFactory;
        private final LuceneSliceQueue sliceQueue;
        private int currentPagePos = 0;
        private int remainingDocs;
        private boolean doneCollecting;
        private LongVector.Builder timestampsBuilder;
        private TsidBuilder tsHashesBuilder;
        private SegmentsIterator iterator;
        private final List<String> counterFields;
        private final LongBlock.Builder[] counterBuilders;

        Impl(BlockFactory blockFactory, List<String> counterFields, LuceneSliceQueue sliceQueue, int maxPageSize, int limit) {
            this.maxPageSize = maxPageSize;
            this.blockFactory = blockFactory;
            this.remainingDocs = limit;
            this.counterFields = counterFields;
            this.timestampsBuilder = blockFactory.newLongVectorBuilder(Math.min(limit, maxPageSize));
            this.tsHashesBuilder = new TsidBuilder(blockFactory, Math.min(limit, maxPageSize));
            this.sliceQueue = sliceQueue;
            this.counterBuilders = new LongBlock.Builder[counterFields.size()];
            for (int i = 0; i < counterBuilders.length; i++) {
                counterBuilders[i] = blockFactory.newLongBlockBuilder(Math.min(limit, maxPageSize));
            }
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
            IntVector shards = null;
            IntVector segments = null;
            IntVector docs = null;
            LongVector timestamps = null;
            BytesRefVector tsids = null;
            try {
                if (iterator == null) {
                    var slice = sliceQueue.nextSlice();
                    if (slice == null) {
                        doneCollecting = true;
                        return null;
                    }
                    iterator = new SegmentsIterator(slice);
                }
                iterator.readDocsForNextPage();
                if (currentPagePos > 0) {
                    shards = blockFactory.newConstantIntVector(iterator.luceneSlice.shardContext().index(), currentPagePos);
                    segments = blockFactory.newConstantIntVector(0, currentPagePos);
                    docs = blockFactory.newConstantIntVector(-1, currentPagePos);
                    timestamps = timestampsBuilder.build();
                    timestampsBuilder = blockFactory.newLongVectorBuilder(Math.min(remainingDocs, maxPageSize));
                    tsids = tsHashesBuilder.build();
                    tsHashesBuilder = new TsidBuilder(blockFactory, Math.min(remainingDocs, maxPageSize));

                    Block[] results = new Block[counterFields.size() + 3];
                    results[0] = new DocVector(shards, segments, docs, true).asBlock();
                    results[1] = tsids.asBlock();
                    results[2] = timestamps.asBlock();
                    for (int i = 0; i < counterBuilders.length; i++) {
                        results[2 + i] = counterBuilders[i].build();
                    }
                    page = new Page(currentPagePos, results);
                    for (int i = 0; i < counterBuilders.length; i++) {
                        counterBuilders[i] = blockFactory.newLongBlockBuilder(Math.min(remainingDocs, maxPageSize));
                    }
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
            Releasables.closeExpectNoException(timestampsBuilder, tsHashesBuilder);
            Releasables.closeExpectNoException(counterBuilders);
        }

        class SegmentsIterator {
            private final PriorityQueue<LeafIterator> mainQueue;
            private final PriorityQueue<LeafIterator> oneTsidQueue;
            final LuceneSlice luceneSlice;

            SegmentsIterator(LuceneSlice luceneSlice) throws IOException {
                this.luceneSlice = luceneSlice;
                this.mainQueue = new PriorityQueue<>(luceneSlice.numLeaves()) {
                    @Override
                    protected boolean lessThan(LeafIterator a, LeafIterator b) {
                        return a.timeSeriesHash.compareTo(b.timeSeriesHash) < 0;
                    }
                };
                Weight weight = luceneSlice.weight();
                int maxSegmentOrd = 0;
                for (var leafReaderContext : luceneSlice.leaves()) {
                    LeafIterator leafIterator = new LeafIterator(weight, leafReaderContext.leafReaderContext(), counterFields);
                    leafIterator.nextDoc();
                    if (leafIterator.docID != DocIdSetIterator.NO_MORE_DOCS) {
                        mainQueue.add(leafIterator);
                        maxSegmentOrd = Math.max(maxSegmentOrd, leafIterator.segmentOrd);
                    }
                }
                this.oneTsidQueue = new PriorityQueue<>(mainQueue.size()) {
                    @Override
                    protected boolean lessThan(LeafIterator a, LeafIterator b) {
                        return a.timestamp > b.timestamp;
                    }
                };
            }

            // TODO: add optimize for one leaf?
            void readDocsForNextPage() throws IOException {
                Thread executingThread = Thread.currentThread();
                for (LeafIterator leaf : mainQueue) {
                    leaf.reinitializeIfNeeded(executingThread);
                }
                for (LeafIterator leaf : oneTsidQueue) {
                    leaf.reinitializeIfNeeded(executingThread);
                }
                do {
                    PriorityQueue<LeafIterator> sub = subQueueForNextTsid();
                    if (sub.size() == 0) {
                        break;
                    }
                    tsHashesBuilder.appendNewTsid(sub.top().timeSeriesHash);
                    if (readValuesForOneTsid(sub)) {
                        break;
                    }
                } while (mainQueue.size() > 0);
            }

            private boolean readValuesForOneTsid(PriorityQueue<LeafIterator> sub) throws IOException {
                do {
                    LeafIterator top = sub.top();
                    currentPagePos++;
                    remainingDocs--;
                    tsHashesBuilder.appendOrdinal();
                    timestampsBuilder.appendLong(top.timestamp);
                    for (int i = 0; i < top.counterValues.length; i++) {
                        SortedNumericDocValues counterValue = top.counterValues[i];
                        if (counterValue != null && counterValue.advanceExact(top.docID)) {
                            for (int v = 0; v < counterValue.docValueCount(); v++) {
                                counterBuilders[i].appendLong(counterValue.nextValue());
                            }
                        } else {
                            counterBuilders[i].appendNull();
                        }
                    }
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

            private PriorityQueue<LeafIterator> subQueueForNextTsid() {
                if (oneTsidQueue.size() == 0 && mainQueue.size() > 0) {
                    LeafIterator last = mainQueue.pop();
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
                return mainQueue.size() == 0 && oneTsidQueue.size() == 0;
            }
        }

        static class LeafIterator {
            private final int segmentOrd;
            private final Weight weight;
            private final LeafReaderContext leafContext;
            private SortedDocValues tsids;
            private NumericDocValues timestamps;
            private DocIdSetIterator disi;
            private Thread createdThread;

            private long timestamp;
            private int lastTsidOrd = -1;
            private BytesRef timeSeriesHash;
            private int docID = -1;
            private final List<String> counterFields;
            final SortedNumericDocValues[] counterValues;

            LeafIterator(Weight weight, LeafReaderContext leafContext, List<String> counterFields) throws IOException {
                this.segmentOrd = leafContext.ord;
                this.weight = weight;
                this.leafContext = leafContext;
                this.createdThread = Thread.currentThread();
                tsids = leafContext.reader().getSortedDocValues("_tsid");
                timestamps = DocValues.unwrapSingleton(leafContext.reader().getSortedNumericDocValues("@timestamp"));
                final Scorer scorer = weight.scorer(leafContext);
                disi = scorer != null ? scorer.iterator() : DocIdSetIterator.empty();
                this.counterFields = counterFields;
                this.counterValues = new SortedNumericDocValues[counterFields.size()];
                for (int i = 0; i < counterValues.length; i++) {
                    counterValues[i] = leafContext.reader().getSortedNumericDocValues(counterFields.get(i));
                }
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
                    tsids = leafContext.reader().getSortedDocValues("_tsid");
                    timestamps = DocValues.unwrapSingleton(leafContext.reader().getSortedNumericDocValues("@timestamp"));
                    final Scorer scorer = weight.scorer(leafContext);
                    disi = scorer != null ? scorer.iterator() : DocIdSetIterator.empty();
                    if (docID != -1) {
                        disi.advance(docID);
                    }
                    createdThread = executingThread;
                    for (int i = 0; i < counterValues.length; i++) {
                        counterValues[i] = leafContext.reader().getSortedNumericDocValues(counterFields.get(i));
                    }
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
            assert currentOrd >= 0;
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
                    Releasables.close(dict, ordinals);
                }
            }
            return result;
        }
    }
}
