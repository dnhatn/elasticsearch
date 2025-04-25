/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
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
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.OrdinalBytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.BlockLoaderStoredFieldsFromLeafLoader;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.search.fetch.StoredFieldsSpec;

import java.io.IOException;
import java.io.UncheckedIOException;
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
    private final ValuesSourceReaderOperator.FieldInfo[] fieldsToExact;

    private TimeSeriesSortedSourceOperatorFactory(
        List<? extends ShardContext> contexts,
        ValuesSourceReaderOperator.FieldInfo[] fieldsToExact,
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
        this.fieldsToExact = fieldsToExact;
    }

    @Override
    public SourceOperator get(DriverContext driverContext) {
        return new Impl(driverContext.blockFactory(), fieldsToExact, sliceQueue, maxPageSize, limit);
    }

    @Override
    public String describe() {
        return "TimeSeriesSortedSourceOperator[maxPageSize = " + maxPageSize + ", limit = " + limit + "]";
    }

    public static TimeSeriesSortedSourceOperatorFactory create(
        int limit,
        int maxPageSize,
        int taskConcurrency,
        List<? extends ShardContext> contexts,
        ValuesSourceReaderOperator.FieldInfo[] fieldsToExact,
        Function<ShardContext, Query> queryFunction
    ) {
        return new TimeSeriesSortedSourceOperatorFactory(contexts, fieldsToExact, queryFunction, taskConcurrency, maxPageSize, limit);
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
        private final ValuesSourceReaderOperator.FieldInfo[] fieldsToExact;
        private ShardLevelFieldsReader fieldsReader;

        Impl(
            BlockFactory blockFactory,
            ValuesSourceReaderOperator.FieldInfo[] fieldsToExtract,
            LuceneSliceQueue sliceQueue,
            int maxPageSize,
            int limit
        ) {
            this.maxPageSize = maxPageSize;
            this.blockFactory = blockFactory;
            this.fieldsToExact = fieldsToExtract;
            this.remainingDocs = limit;
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
            Block[] blocks = new Block[2 + fieldsToExact.length];
            try {
                if (iterator == null) {
                    var slice = sliceQueue.nextSlice();
                    if (slice == null) {
                        doneCollecting = true;
                        return null;
                    }
                    Releasables.close(fieldsReader);
                    fieldsReader = new ShardLevelFieldsReader(blockFactory, slice.shardContext(), fieldsToExact);
                    iterator = new SegmentsIterator(slice);
                }
                iterator.readDocsForNextPage();
                if (currentPagePos > 0) {
                    blocks[0] = tsHashesBuilder.build().asBlock();
                    tsHashesBuilder = new TsidBuilder(blockFactory, Math.min(remainingDocs, maxPageSize));
                    blocks[1] = timestampsBuilder.build().asBlock();
                    timestampsBuilder = blockFactory.newLongVectorBuilder(Math.min(remainingDocs, maxPageSize));
                    System.arraycopy(fieldsReader.buildBlocks(), 0, blocks, 2, fieldsToExact.length);
                    page = new Page(currentPagePos, blocks);
                    currentPagePos = 0;
                }
                if (iterator.completed()) {
                    iterator = null;
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } finally {
                if (page == null) {
                    Releasables.closeExpectNoException(blocks);
                }
            }
            return page;
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(timestampsBuilder, tsHashesBuilder, fieldsReader);
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
                    LeafIterator leafIterator = new LeafIterator(weight, leafReaderContext.leafReaderContext());
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
                fieldsReader.prepareForReading(maxPageSize);
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
                    fieldsReader.readValues(top.segmentOrd, top.docID);
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

            LeafIterator(Weight weight, LeafReaderContext leafContext) throws IOException {
                this.segmentOrd = leafContext.ord;
                this.weight = weight;
                this.leafContext = leafContext;
                this.createdThread = Thread.currentThread();
                tsids = leafContext.reader().getSortedDocValues("_tsid");
                timestamps = DocValues.unwrapSingleton(leafContext.reader().getSortedNumericDocValues("@timestamp"));
                final Scorer scorer = weight.scorer(leafContext);
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
                    tsids = leafContext.reader().getSortedDocValues("_tsid");
                    timestamps = DocValues.unwrapSingleton(leafContext.reader().getSortedNumericDocValues("@timestamp"));
                    final Scorer scorer = weight.scorer(leafContext);
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

    static class BlockLoaderFactory extends ValuesSourceReaderOperator.DelegatingBlockLoaderFactory {
        BlockLoaderFactory(BlockFactory factory) {
            super(factory);
        }

        @Override
        public BlockLoader.Block constantNulls() {
            throw new UnsupportedOperationException("must not be used by column readers");
        }

        @Override
        public BlockLoader.Block constantBytes(BytesRef value) {
            throw new UnsupportedOperationException("must not be used by column readers");
        }

        @Override
        public BlockLoader.SingletonOrdinalsBuilder singletonOrdinalsBuilder(SortedDocValues ordinals, int count) {
            throw new UnsupportedOperationException("must not be used by column readers");
        }
    }

    static final class ShardLevelFieldsReader implements Releasable {
        private final BlockLoaderFactory blockFactory;
        private final SegmentLevelFieldsReader[] segments;
        private final BlockLoader[] loaders;
        private final Block.Builder[] builders;
        private final StoredFieldsSpec storedFieldsSpec;
        private final SourceLoader sourceLoader;

        ShardLevelFieldsReader(BlockFactory blockFactory, ShardContext shardContext, ValuesSourceReaderOperator.FieldInfo[] fields) {
            this.blockFactory = new BlockLoaderFactory(blockFactory);
            final IndexReader indexReader = shardContext.searcher().getIndexReader();
            this.segments = new SegmentLevelFieldsReader[indexReader.leaves().size()];
            this.loaders = new BlockLoader[fields.length];
            this.builders = new Block.Builder[loaders.length];
            StoredFieldsSpec storedFieldsSpec = StoredFieldsSpec.NO_REQUIREMENTS;
            for (int i = 0; i < fields.length; i++) {
                BlockLoader loader = fields[i].blockLoader().apply(0);
                storedFieldsSpec = storedFieldsSpec.merge(loader.rowStrideStoredFieldSpec());
                loaders[i] = loader;
            }
            for (int i = 0; i < indexReader.leaves().size(); i++) {
                LeafReaderContext leafReaderContext = indexReader.leaves().get(i);
                segments[i] = new SegmentLevelFieldsReader(leafReaderContext, loaders);
            }
            if (storedFieldsSpec.requiresSource()) {
                sourceLoader = shardContext.newSourceLoader();
            } else {
                sourceLoader = null;
            }
            this.storedFieldsSpec = storedFieldsSpec;
        }

        void readValues(int segment, int docID) throws IOException {
            segments[segment].read(docID, builders);
        }

        void prepareForReading(int estimateSize) throws IOException {
            if (this.builders[0] == null) {
                for (int f = 0; f < builders.length; f++) {
                    builders[f] = (Block.Builder) loaders[f].builder(blockFactory, estimateSize);
                }
            }
            for (SegmentLevelFieldsReader segment : segments) {
                if (segment != null) {
                    segment.reinitializeIfNeeded(sourceLoader, storedFieldsSpec);
                }
            }
        }

        Block[] buildBlocks() {
            Block[] blocks = Block.Builder.buildAll(builders);
            Arrays.fill(builders, null);
            return blocks;
        }

        @Override
        public void close() {
            Releasables.close(builders);
        }
    }

    static final class SegmentLevelFieldsReader {
        private final BlockLoader.RowStrideReader[] rowStride;
        private final BlockLoader[] loaders;
        private final LeafReaderContext leafContext;
        private BlockLoaderStoredFieldsFromLeafLoader storedFields;
        private Thread loadedThread = null;

        public SegmentLevelFieldsReader(LeafReaderContext leafContext, BlockLoader[] loaders) {
            this.leafContext = leafContext;
            this.loaders = loaders;
            this.rowStride = new BlockLoader.RowStrideReader[loaders.length];
        }

        private void reinitializeIfNeeded(SourceLoader sourceLoader, StoredFieldsSpec storedFieldsSpec) throws IOException {
            final Thread currentThread = Thread.currentThread();
            if (loadedThread != currentThread) {
                loadedThread = currentThread;
                for (int f = 0; f < loaders.length; f++) {
                    rowStride[f] = loaders[f].rowStrideReader(leafContext);
                }
                storedFields = new BlockLoaderStoredFieldsFromLeafLoader(
                    StoredFieldLoader.fromSpec(storedFieldsSpec).getLoader(leafContext, null),
                    sourceLoader != null ? sourceLoader.leaf(leafContext.reader(), null) : null
                );
            }
        }

        void read(int docId, Block.Builder[] builder) throws IOException {
            storedFields.advanceTo(docId);
            for (int i = 0; i < rowStride.length; i++) {
                rowStride[i].read(docId, storedFields, builder[i]);
            }
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
