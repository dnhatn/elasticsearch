/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.OrdinalBytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AbstractPageMappingOperator;
import org.elasticsearch.compute.operator.DriverContext;
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

public final class TimeSeriesExtractFieldsOperator extends AbstractPageMappingOperator {
    private final List<? extends ShardContext> shardContexts;
    private final List<ValuesSourceReaderOperator.FieldInfo> fieldsToExtract;
    private final boolean emitDocIds;
    private ShardLevelFieldsReader fieldsReader;
    private final DriverContext driverContext;

    public TimeSeriesExtractFieldsOperator(
        DriverContext driverContext,
        List<? extends ShardContext> shardContexts,
        boolean emitDocIds,
        List<ValuesSourceReaderOperator.FieldInfo> fieldsToExtract
    ) {
        this.fieldsToExtract = fieldsToExtract;
        this.emitDocIds = emitDocIds;
        this.shardContexts = shardContexts;
        this.driverContext = driverContext;
    }

    @Override
    protected Page process(Page page) {
        try {
            Block[] blocks;
            try {
                blocks = readFields(page);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            // TODO: handle error
            Block[] newBlocks = new Block[blocks.length + 2];
            Block tsid = page.getBlock(1);
            tsid.incRef();
            Block timestamps = page.getBlock(2);
            timestamps.incRef();
            newBlocks[0] = tsid; // tsid
            newBlocks[1] = timestamps; // timestamps
            System.arraycopy(blocks, 0, newBlocks, 2, blocks.length);
            return new Page(newBlocks);
        } finally {
            page.releaseBlocks();
        }
    }

    @Override
    public void close() {
        Releasables.close(fieldsReader, super::close);
    }

    private Block[] readFields(Page page) throws IOException {
        if (fieldsReader == null) {
            fieldsReader = new ShardLevelFieldsReader(driverContext.blockFactory(), shardContexts.get(0), fieldsToExtract);
        }
        fieldsReader.prepareForReading(page.getPositionCount());
        DocBlock docBlock = page.getBlock(0);
        DocVector docVector = docBlock.asVector();
        IntVector docs = docVector.docs();
        IntVector segments = docVector.segments();
        BytesRefBlock tsid = page.getBlock(1);
        IntVector tsidOrdinals = tsid.asOrdinals().asVector().getOrdinalsVector();
        int lastOrd = -1;
        for (int i = 0; i < page.getPositionCount(); i++) {
            final boolean loadNonDimensionFieldsOnly;
            int tsidOrd = tsidOrdinals.getInt(i);
            if (tsidOrd != lastOrd) {
                loadNonDimensionFieldsOnly = false;
                lastOrd = tsidOrd;
            } else {
                loadNonDimensionFieldsOnly = true;
            }
            fieldsReader.readValues(segments.getInt(i), docs.getInt(i), loadNonDimensionFieldsOnly);
        }
        return fieldsReader.buildBlocks(tsidOrdinals);
    }

    @Override
    public String toString() {
        return "TimeSeriesExtractFieldsOperator";
    }

    static final class ShardLevelFieldsReader implements Releasable {
        private final BlockLoaderFactory blockFactory;
        private final SegmentLevelFieldsReader[] segments;
        private final BlockLoader[] loaders;
        private final boolean[] dimensions;
        private final Block.Builder[] builders;
        private final StoredFieldsSpec storedFieldsSpec;
        private final SourceLoader sourceLoader;

        ShardLevelFieldsReader(BlockFactory blockFactory, ShardContext shardContext, List<ValuesSourceReaderOperator.FieldInfo> fields) {
            this.blockFactory = new BlockLoaderFactory(blockFactory);
            final IndexReader indexReader = shardContext.searcher().getIndexReader();
            this.segments = new SegmentLevelFieldsReader[indexReader.leaves().size()];
            this.loaders = new BlockLoader[fields.size()];
            this.builders = new Block.Builder[loaders.length];
            StoredFieldsSpec storedFieldsSpec = StoredFieldsSpec.NO_REQUIREMENTS;
            for (int i = 0; i < fields.size(); i++) {
                BlockLoader loader = fields.get(i).blockLoader().apply(shardContext.index());
                storedFieldsSpec = storedFieldsSpec.merge(loader.rowStrideStoredFieldSpec());
                loaders[i] = loader;
            }
            for (int i = 0; i < indexReader.leaves().size(); i++) {
                LeafReaderContext leafReaderContext = indexReader.leaves().get(i);
                segments[i] = new SegmentLevelFieldsReader(leafReaderContext, loaders);
            }
            if (storedFieldsSpec.requiresSource()) {
                sourceLoader = shardContext.newSourceLoader();
                storedFieldsSpec = storedFieldsSpec.merge(new StoredFieldsSpec(false, false, sourceLoader.requiredStoredFields()));
            } else {
                sourceLoader = null;
            }
            this.storedFieldsSpec = storedFieldsSpec;
            this.dimensions = new boolean[fields.size()];
            for (int i = 0; i < fields.size(); i++) {
                dimensions[i] = shardContext.fieldType(fields.get(i).name()).isDimension();
            }
        }

        /**
         * For dimension fields, skips reading them when {@code nonDimensionFieldsOnly} is true,
         * since they only need to be read once per tsid.
         */
        void readValues(int segment, int docID, boolean nonDimensionFieldsOnly) throws IOException {
            segments[segment].read(docID, builders, nonDimensionFieldsOnly, dimensions);
        }

        void prepareForReading(int estimatedSize) throws IOException {
            if (this.builders.length > 0 && this.builders[0] == null) {
                for (int f = 0; f < builders.length; f++) {
                    builders[f] = (Block.Builder) loaders[f].builder(blockFactory, estimatedSize);
                }
            }
            for (SegmentLevelFieldsReader segment : segments) {
                if (segment != null) {
                    segment.reinitializeIfNeeded(sourceLoader, storedFieldsSpec);
                }
            }
        }

        Block[] buildBlocks(IntVector tsidOrdinals) {
            final Block[] blocks = new Block[loaders.length];
            try {
                for (int i = 0; i < builders.length; i++) {
                    if (dimensions[i]) {
                        blocks[i] = buildBlockForDimensionField(builders[i], tsidOrdinals);
                    } else {
                        blocks[i] = builders[i].build();
                    }
                }
                Arrays.fill(builders, null);
            } finally {
                if (blocks.length > 0 && blocks[blocks.length - 1] == null) {
                    Releasables.close(blocks);
                }
            }
            return blocks;
        }

        private Block buildBlockForDimensionField(Block.Builder builder, IntVector tsidOrdinals) {
            try (var values = builder.build()) {
                if (values.asVector() instanceof BytesRefVector bytes) {
                    tsidOrdinals.incRef();
                    values.incRef();
                    return new OrdinalBytesRefVector(tsidOrdinals, bytes).asBlock();
                } else if (values.areAllValuesNull()) {
                    return blockFactory.factory.newConstantNullBlock(tsidOrdinals.getPositionCount());
                } else {
                    final int positionCount = tsidOrdinals.getPositionCount();
                    try (var newBuilder = values.elementType().newBlockBuilder(positionCount, blockFactory.factory)) {
                        for (int p = 0; p < positionCount; p++) {
                            int pos = tsidOrdinals.getInt(p);
                            newBuilder.copyFrom(values, pos, pos + 1);
                        }
                        return newBuilder.build();
                    }
                }
            }
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

        SegmentLevelFieldsReader(LeafReaderContext leafContext, BlockLoader[] loaders) {
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

        void read(int docId, Block.Builder[] builder, boolean nonDimensionFieldsOnly, boolean[] dimensions) throws IOException {
            storedFields.advanceTo(docId);
            if (nonDimensionFieldsOnly) {
                for (int i = 0; i < rowStride.length; i++) {
                    if (dimensions[i] == false) {
                        rowStride[i].read(docId, storedFields, builder[i]);
                    }
                }
            } else {
                for (int i = 0; i < rowStride.length; i++) {
                    rowStride[i].read(docId, storedFields, builder[i]);
                }
            }
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
}
