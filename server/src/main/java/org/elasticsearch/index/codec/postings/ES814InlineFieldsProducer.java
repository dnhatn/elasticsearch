/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.codec.postings;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.index.BaseTermsEnum;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SlowImpactsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.IOUtils;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.index.codec.postings.ES814InlinePostingsFormat.*;

final class ES814InlineFieldsProducer extends FieldsProducer {

    private record Meta(
        IndexOptions options,
        boolean hasPayloads,
        long indexOffset,
        long proxOffset,
        long size,
        long numBlocks,
        int docCount,
        long sumDocFreq,
        long sumTotalTermFreq,
        int maxTermLength
    ) {}

    private final IndexInput index, prox;
    private final SegmentReadState state;
    private final Map<String, Meta> meta;

    ES814InlineFieldsProducer(SegmentReadState state) throws IOException {
        String metaFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, META_EXTENSION);
        String indexFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, INVERTED_INDEX_EXTENSION);
        String proxFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, PROXIMITY_EXTENSION);
        boolean success = false;
        try (ChecksumIndexInput meta = state.directory.openChecksumInput(metaFileName, IOContext.READONCE)) {
            index = state.directory.openInput(indexFileName, state.context);
            CodecUtil.checkIndexHeader(
                index,
                INVERTED_INDEX_CODEC,
                VERSION_START,
                VERSION_CURRENT,
                state.segmentInfo.getId(),
                state.segmentSuffix
            );
            CodecUtil.retrieveChecksum(index);
            if (state.fieldInfos.hasProx()) {
                prox = state.directory.openInput(proxFileName, state.context);
                CodecUtil.checkIndexHeader(
                    prox,
                    PROXIMITY_CODEC,
                    VERSION_START,
                    VERSION_CURRENT,
                    state.segmentInfo.getId(),
                    state.segmentSuffix
                );
                CodecUtil.retrieveChecksum(prox);
            } else {
                prox = null;
            }

            Throwable priorE = null;
            try {
                CodecUtil.checkIndexHeader(
                    meta,
                    META_CODEC,
                    VERSION_START,
                    VERSION_CURRENT,
                    state.segmentInfo.getId(),
                    state.segmentSuffix
                );
                this.meta = new LinkedHashMap<>();
                for (byte flag = meta.readByte(); flag != -1; flag = meta.readByte()) {
                    IndexOptions opts = switch (flag) {
                        case 0 -> IndexOptions.DOCS;
                        case 1 -> IndexOptions.DOCS_AND_FREQS;
                        case 2 -> IndexOptions.DOCS_AND_FREQS_AND_POSITIONS;
                        case 3 -> IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS;
                        default -> throw new CorruptIndexException("Unexpected flag: " + flag, meta);
                    };
                    String fieldName = meta.readString();
                    long indexOffset = meta.readLong();
                    long proxOffset = -1L;
                    if (opts.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0) {
                        proxOffset = meta.readLong();
                    }
                    long size = meta.readLong();
                    long numBlocks = meta.readLong();
                    assert size >= numBlocks : size + " < " + numBlocks;
                    int docCount = meta.readInt();
                    long sumDocFreq = meta.readLong();
                    long sumTotalTermFreq = sumDocFreq;
                    if (opts.compareTo(IndexOptions.DOCS_AND_FREQS) >= 0) {
                        sumTotalTermFreq = meta.readLong();
                    }
                    int maxTermLength = meta.readInt();
                    int hasPayloads = meta.readByte();
                    if (hasPayloads < 0 || hasPayloads > 1) {
                        throw new CorruptIndexException("Unexpected boolean " + hasPayloads, meta);
                    }
                    this.meta.put(
                        fieldName,
                        new Meta(
                            opts,
                            hasPayloads != 0,
                            indexOffset,
                            proxOffset,
                            size,
                            numBlocks,
                            docCount,
                            sumDocFreq,
                            sumTotalTermFreq,
                            maxTermLength
                        )
                    );
                }
            } catch (Throwable e) {
                priorE = e;
                throw e;
            } finally {
                CodecUtil.checkFooter(meta, priorE);
            }
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(this);
            }
        }
        this.state = state;
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(index, prox);
    }

    @Override
    public void checkIntegrity() throws IOException {
        CodecUtil.checksumEntireFile(index);
        if (prox != null) {
            CodecUtil.checksumEntireFile(prox);
        }
    }

    @Override
    public Iterator<String> iterator() {
        return meta.keySet().iterator();
    }

    @Override
    public int size() {
        return meta.size();
    }

    @Override
    public Terms terms(String field) throws IOException {
        Meta meta = this.meta.get(field);
        if (meta == null) {
            return null;
        }
        return new InlineTerms(meta);
    }

    private class InlineTerms extends Terms {

        private final Meta meta;

        InlineTerms(Meta meta) {
            this.meta = meta;
        }

        @Override
        public TermsEnum iterator() throws IOException {
            return new InlineTermsEnum(meta);
        }

        @Override
        public long size() throws IOException {
            return meta.size;
        }

        @Override
        public long getSumTotalTermFreq() throws IOException {
            return meta.sumTotalTermFreq;
        }

        @Override
        public long getSumDocFreq() throws IOException {
            return meta.sumDocFreq;
        }

        @Override
        public int getDocCount() throws IOException {
            return meta.docCount;
        }

        @Override
        public boolean hasFreqs() {
            return meta.options.compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;
        }

        @Override
        public boolean hasOffsets() {
            return meta.options.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
        }

        @Override
        public boolean hasPositions() {
            return meta.options.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
        }

        @Override
        public boolean hasPayloads() {
            return meta.hasPayloads;
        }
    }

    private class InlineTermsEnum extends BaseTermsEnum {

        private final IndexInput index = ES814InlineFieldsProducer.this.index.clone();
        private final Meta meta;
        private int docFreq;
        private long totalTermFreq;
        private final BytesRef term;
        private long blockIndex;
        private int numTermsInBlock;
        private int termIndexInBlock;
        private long postingsPointer;
        private long postingsSize;
        private long docOffset;
        private long proxOffset;

        InlineTermsEnum(Meta meta) throws IOException {
            this.meta = meta;
            term = new BytesRef(meta.maxTermLength);
            reset();
        }

        private void reset() throws IOException {
            index.seek(meta.indexOffset);
            blockIndex = -1;
            termIndexInBlock = -1;
            numTermsInBlock = -1;
            postingsPointer = 0;
            postingsSize = 0;
        }

        @Override
        public SeekStatus seekCeil(BytesRef text) throws IOException {
            reset();
            BytesRef term;
            do {
                term = next();
            } while (term != null && term.compareTo(text) < 0);
            if (term == null) {
                return SeekStatus.END;
            } else if (term.equals(text)) {
                return SeekStatus.FOUND;
            } else {
                return SeekStatus.NOT_FOUND;
            }
        }

        @Override
        public void seekExact(long ord) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public BytesRef term() throws IOException {
            return term;
        }

        @Override
        public long ord() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int docFreq() throws IOException {
            return docFreq;
        }

        @Override
        public long totalTermFreq() throws IOException {
            return totalTermFreq;
        }

        @Override
        public ImpactsEnum impacts(int flags) throws IOException {
            return new SlowImpactsEnum(postings(null, flags));
        }

        @Override
        public BytesRef next() throws IOException {
            if (++termIndexInBlock >= numTermsInBlock) {
                if (++blockIndex >= meta.numBlocks) {
                    return null; // exhausted
                }
                if (blockIndex > 0) {
                    index.seek(postingsPointer + postingsSize);
                }
                termIndexInBlock = 0;
                numTermsInBlock = index.readVInt();
                postingsSize = index.readVLong();
                postingsPointer = index.readLong();
            }
            term.length = index.readVInt();
            index.readBytes(term.bytes, 0, term.length);
            docFreq = index.readInt();
            if (meta.options.compareTo(IndexOptions.DOCS_AND_FREQS) >= 0) {
                totalTermFreq = index.readLong();
            } else {
                totalTermFreq = docFreq;
            }
            if (meta.options.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0) {
                proxOffset = index.readLong();
            }
            docOffset = postingsPointer + index.readLong();
            return term;
        }

        @Override
        public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
            InlinePostingsEnum postings;
            if (reuse != null
                && reuse instanceof InlinePostingsEnum inlinePostingsEnum
                && inlinePostingsEnum.canReuse(ES814InlineFieldsProducer.this, flags)) {
                postings = inlinePostingsEnum;
            } else {
                IndexInput prox = null;
                if (ES814InlineFieldsProducer.this.prox != null) {
                    prox = ES814InlineFieldsProducer.this.prox.clone();
                }
                postings = new InlinePostingsEnum(ES814InlineFieldsProducer.this, flags, index.clone(), prox);
            }
            postings.reset(meta.options, docFreq, docOffset, proxOffset);
            return postings;
        }
    }

    private static class InlinePostingsEnum extends PostingsEnum {

        private final ES814InlineFieldsProducer producer;
        private final int flags;
        private IndexOptions options;
        private int docFreq;
        private final IndexInput index, prox;
        private int docIndex;
        private int posIndex;
        private int doc = -1;
        private int freq;
        private int pos;
        private int startOffset, endOffset;
        private BytesRef payload;

        InlinePostingsEnum(ES814InlineFieldsProducer producer, int flags, IndexInput index, IndexInput prox) {
            this.producer = Objects.requireNonNull(producer);
            this.flags = flags;
            this.index = Objects.requireNonNull(index);
            this.prox = prox;
        }

        boolean canReuse(ES814InlineFieldsProducer producer, int flags) {
            // NOTE: tests assume it's illegal to reuse an enum that was constructed with different flags
            return this.producer == producer && this.flags == flags;
        }

        void reset(IndexOptions options, int docFreq, long docOffset, long proxOffset) throws IOException {
            this.options = Objects.requireNonNull(options);
            this.docFreq = docFreq;
            docIndex = -1;
            doc = -1;
            freq = 1;
            posIndex = 1;
            index.seek(docOffset);
            if (prox != null) {
                prox.seek(proxOffset);
            }
            startOffset = -1;
            endOffset = -1;
            payload = null;
        }

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public int nextDoc() throws IOException {
            while (options.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0 && posIndex < freq - 1) {
                // read unused positions
                nextPosition();
            }

            if (++docIndex >= docFreq) {
                return doc = DocIdSetIterator.NO_MORE_DOCS;
            }
            doc = index.readInt();
            if (options.compareTo(IndexOptions.DOCS_AND_FREQS) >= 0) {
                freq = index.readInt();
            } else {
                freq = 1;
            }
            posIndex = -1;
            return doc;
        }

        @Override
        public int freq() throws IOException {
            return freq;
        }

        @Override
        public int nextPosition() throws IOException {
            if (options.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) < 0) {
                return -1;
            }
            if (++posIndex >= freq) {
                throw new IllegalStateException();
            }
            pos = prox.readInt();
            if (options.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0) {
                startOffset = prox.readInt();
                endOffset = prox.readInt();
            } else {
                startOffset = endOffset = -1;
            }
            int payloadLength = prox.readVInt() - 1;
            if (payloadLength == -1) {
                payload = null;
            } else {
                payload = new BytesRef(payloadLength);
                payload.length = payloadLength;
                prox.readBytes(payload.bytes, 0, payloadLength);
            }
            return pos;
        }

        @Override
        public int startOffset() throws IOException {
            return startOffset;
        }

        @Override
        public int endOffset() throws IOException {
            return endOffset;
        }

        @Override
        public BytesRef getPayload() throws IOException {
            return payload;
        }

        @Override
        public int advance(int target) throws IOException {
            return slowAdvance(target);
        }

        @Override
        public long cost() {
            return docFreq;
        }
    }
}
