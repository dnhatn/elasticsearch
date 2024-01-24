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
import org.apache.lucene.util.packed.MonotonicBlockPackedReader;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.lucene.util.BinaryInterpolativeCoding;
import org.elasticsearch.lucene.util.BitStreamInput;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.index.codec.postings.ES814InlinePostingsFormat.INVERTED_INDEX_CODEC;
import static org.elasticsearch.index.codec.postings.ES814InlinePostingsFormat.INVERTED_INDEX_EXTENSION;
import static org.elasticsearch.index.codec.postings.ES814InlinePostingsFormat.META_CODEC;
import static org.elasticsearch.index.codec.postings.ES814InlinePostingsFormat.META_EXTENSION;
import static org.elasticsearch.index.codec.postings.ES814InlinePostingsFormat.POSTINGS_BLOCK_SIZE;
import static org.elasticsearch.index.codec.postings.ES814InlinePostingsFormat.PROXIMITY_CODEC;
import static org.elasticsearch.index.codec.postings.ES814InlinePostingsFormat.PROXIMITY_EXTENSION;
import static org.elasticsearch.index.codec.postings.ES814InlinePostingsFormat.TERM_INDEX_BLOCK_SIZE;
import static org.elasticsearch.index.codec.postings.ES814InlinePostingsFormat.TERM_INDEX_CODEC;
import static org.elasticsearch.index.codec.postings.ES814InlinePostingsFormat.TERM_INDEX_EXTENSION;
import static org.elasticsearch.index.codec.postings.ES814InlinePostingsFormat.VERSION_CURRENT;
import static org.elasticsearch.index.codec.postings.ES814InlinePostingsFormat.VERSION_START;

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
        int maxTermLength,
        int packedIntVersion,
        TermIndexMeta termIndexMeta
    ) {}

    private record TermIndexMeta(long entries, long termIndexOffset, long blockAddress) {}

    private final IndexInput index, termIndex, prox;
    private final SegmentReadState state;
    private final Map<String, Meta> meta;

    ES814InlineFieldsProducer(SegmentReadState state) throws IOException {
        String metaFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, META_EXTENSION);
        String indexFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, INVERTED_INDEX_EXTENSION);
        String termIndexFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, TERM_INDEX_EXTENSION);
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

            termIndex = state.directory.openInput(termIndexFileName, state.context);
            CodecUtil.checkIndexHeader(
                termIndex,
                TERM_INDEX_CODEC,
                VERSION_START,
                VERSION_CURRENT,
                state.segmentInfo.getId(),
                state.segmentSuffix
            );
            CodecUtil.retrieveChecksum(termIndex);

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
                    int packedIntVersion = meta.readVInt();
                    var termIndexMeta = new TermIndexMeta(meta.readLong(), meta.readLong(), meta.readLong());
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
                            maxTermLength,
                            packedIntVersion,
                            termIndexMeta
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
        IOUtils.close(index, termIndex, prox);
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
        private final long termIndexOffsetStart;
        private final MonotonicBlockPackedReader termIndexOffsets;

        private final long blockStartAddress;
        private final MonotonicBlockPackedReader blockAddresses;

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
            this.term = new BytesRef(meta.maxTermLength);

            IndexInput offsetInput = termIndex.clone();
            offsetInput.seek(meta.termIndexMeta.termIndexOffset);
            termIndexOffsetStart = offsetInput.readLong();
            termIndexOffsets = MonotonicBlockPackedReader.of(
                offsetInput,
                meta.packedIntVersion,
                TERM_INDEX_BLOCK_SIZE,
                meta.termIndexMeta.entries
            );

            IndexInput addressInput = termIndex.clone();
            addressInput.seek(meta.termIndexMeta.blockAddress);
            blockStartAddress = addressInput.readLong();
            blockAddresses = MonotonicBlockPackedReader.of(
                addressInput,
                meta.packedIntVersion,
                TERM_INDEX_BLOCK_SIZE,
                meta.termIndexMeta.entries
            );
            reset();
        }

        private void reset() throws IOException {
            index.seek(meta.indexOffset);
            blockIndex = -1;
            termIndexInBlock = 0;
            numTermsInBlock = -1;
            postingsPointer = 0;
            postingsSize = 0;
        }

        private long findBlockIndex(BytesRef target) throws IOException {
            long lo = 0;
            long hi = termIndexOffsets.size() - 2;
            while (hi >= lo) {
                long mid = (lo + hi) >>> 1;
                long offset = termIndexOffsets.get(mid);
                term.length = Math.toIntExact(termIndexOffsets.get(mid + 1) - offset);
                termIndex.seek(termIndexOffsetStart + offset);
                termIndex.readBytes(term.bytes, 0, term.length);
                int cmp = target.compareTo(term);
                if (cmp == 0) {
                    return mid;
                } else if (cmp < 0) {
                    hi = mid - 1;
                } else {
                    lo = mid + 1;
                }
            }
            return hi + 1;
        }

        @Override
        public SeekStatus seekCeil(BytesRef target) throws IOException {
            blockIndex = findBlockIndex(target);
            if (blockIndex == meta.numBlocks) {
                return SeekStatus.END;
            }
            index.seek(blockStartAddress + blockAddresses.get(blockIndex));
            loadFrame();
            // TODO: should we support binary search here?
            int cmp;
            do {
                scanNextTermInCurrentFrame();
            } while ((cmp = term.compareTo(target)) < 0 && termIndexInBlock < numTermsInBlock);

            if (cmp == 0) {
                return SeekStatus.FOUND;
            } else if (cmp > 0) {
                return SeekStatus.NOT_FOUND;
            } else {
                return SeekStatus.END;
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
            if (termIndexInBlock >= numTermsInBlock) {
                if (++blockIndex >= meta.numBlocks) {
                    return null; // exhausted
                }
                if (blockIndex > 0) {
                    index.seek(postingsPointer + postingsSize);
                }
                loadFrame();
            }
            scanNextTermInCurrentFrame();
            return term;
        }

        private void loadFrame() throws IOException {
            termIndexInBlock = 0;
            numTermsInBlock = index.readVInt();
            postingsSize = index.readVLong();
            postingsPointer = index.readLong();
        }

        private void scanNextTermInCurrentFrame() throws IOException {
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
            termIndexInBlock++;
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
        private final int maxDoc;
        private final int flags;
        private IndexOptions options;
        private int docFreq;
        private final IndexInput index, prox;
        private int docIndex;
        private int posIndex;
        private int doc;
        private int freq;
        private int pos;
        private int startOffset, endOffset;
        private BytesRef payload;
        private final long[] docBuffer = new long[POSTINGS_BLOCK_SIZE];
        private final long[] freqBuffer = new long[POSTINGS_BLOCK_SIZE];
        private int docBufferIndex;
        private int skipDoc; // last doc in the current block
        private long nextBlockProxOffset; // start offset of proximity data for the next block
        private final PForUtil2 pforUtil = new PForUtil2(new ForUtil());

        InlinePostingsEnum(ES814InlineFieldsProducer producer, int flags, IndexInput index, IndexInput prox) {
            this.producer = Objects.requireNonNull(producer);
            this.maxDoc = producer.state.segmentInfo.maxDoc();
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
            docBufferIndex = POSTINGS_BLOCK_SIZE - 1; // Trigger a refill on the next read
            doc = -1;
            skipDoc = -1;
            freq = 1;
            posIndex = 1;
            index.seek(docOffset);
            if (options.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0) {
                prox.seek(proxOffset);
                nextBlockProxOffset = proxOffset;
            }
            pos = -1;
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
            if (++docBufferIndex == POSTINGS_BLOCK_SIZE) {
                refillDocs(0);
                docBufferIndex = 0;
            }
            posIndex = -1;
            freq = (int) freqBuffer[docBufferIndex];
            resetProxData();
            return doc = (int) docBuffer[docBufferIndex];
        }

        @Override
        public int advance(int target) throws IOException {
            if (target > skipDoc) { // Target is not in the current block
                // Move to the boundary between this block and the next one
                docIndex = docIndex - docBufferIndex + POSTINGS_BLOCK_SIZE;
                docBufferIndex = 0;
                while (true) {
                    long proxOffset = nextBlockProxOffset;
                    refillDocs(target);
                    if (target <= skipDoc) { // Just found the target block, or last block
                        if (options.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0) {
                            prox.seek(proxOffset);
                        }
                        break;
                    } else {
                        docIndex += POSTINGS_BLOCK_SIZE;
                    }
                }
                posIndex = -1;
                freq = (int) freqBuffer[docBufferIndex];
                doc = (int) docBuffer[docBufferIndex];
                resetProxData();
                if (doc >= target) {
                    return doc;
                }
            }
            return slowAdvance(target);
        }

        private void resetProxData() {
            if (options.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0) {
                pos = 0;
                if (options.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0) {
                    endOffset = 0;
                }
            }
        }

        /**
         * Refill blocks of documents, or skip data if none of the documents in the next block may be greater than or equal
         * to {@code target}.
         */
        private void refillDocs(int target) throws IOException {
            final int remaining = docFreq - docIndex;
            if (remaining >= POSTINGS_BLOCK_SIZE) {
                // Full block
                final int lastDocInPrevBlock = skipDoc;
                skipDoc += index.readVInt() + POSTINGS_BLOCK_SIZE;
                if (options.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0) {
                    nextBlockProxOffset += index.readVLong();
                }
                if (skipDoc >= target) {
                    pforUtil.decodeAndPrefixSum(index, lastDocInPrevBlock, docBuffer);
                    docBuffer[ForUtil.BLOCK_SIZE] = skipDoc;
                    if (options.compareTo(IndexOptions.DOCS_AND_FREQS) >= 0) {
                        pforUtil.decode(index, freqBuffer);
                        freqBuffer[ForUtil.BLOCK_SIZE] = 1L + index.readVLong();
                    } else {
                        Arrays.fill(freqBuffer, 1L);
                    }
                } else {
                    // Skip block
                    pforUtil.skip(index);
                    if (options.compareTo(IndexOptions.DOCS_AND_FREQS) >= 0) {
                        pforUtil.skip(index);
                        index.readVLong();
                    }
                }
            } else {
                // Tail postings
                BitStreamInput bitIn = new BitStreamInput(index);
                BinaryInterpolativeCoding.decodeIncreasing(bitIn, docBuffer, 0, remaining - 1, skipDoc + 1, maxDoc - 1);
                if (options.compareTo(IndexOptions.DOCS_AND_FREQS) >= 0) {
                    for (int i = 0; i < remaining; ++i) {
                        freqBuffer[i] = bitIn.readDeltaCode();
                    }
                } else {
                    Arrays.fill(freqBuffer, 0, remaining, 1L);
                }
                bitIn.done();
                skipDoc = NO_MORE_DOCS;
                nextBlockProxOffset = -1L;
            }
        }

        @Override
        public int freq() throws IOException {
            return freq;
        }

        @Override
        public int nextPosition() throws IOException {
            if (options.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) < 0) {
                assert pos == -1;
                return -1;
            }
            if (++posIndex >= freq) {
                throw new IllegalStateException();
            }
            pos += prox.readVInt();
            if (options.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0) {
                startOffset = endOffset + prox.readVInt();
                endOffset = startOffset + prox.readVInt();
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
        public long cost() {
            return docFreq;
        }
    }
}
