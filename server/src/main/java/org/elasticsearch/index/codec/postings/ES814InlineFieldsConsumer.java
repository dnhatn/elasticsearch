/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.codec.postings;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.core.IOUtils;

import java.io.IOException;

import static org.elasticsearch.index.codec.postings.ES814InlinePostingsFormat.*;

final class ES814InlineFieldsConsumer extends FieldsConsumer {

    private final IndexOutput meta, index, prox;
    private final SegmentWriteState state;

    ES814InlineFieldsConsumer(SegmentWriteState state) throws IOException {
        String metaFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, META_EXTENSION);
        String indexFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, INVERTED_INDEX_EXTENSION);
        String proxFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, PROXIMITY_EXTENSION);
        boolean success = false;
        try {
            meta = state.directory.createOutput(metaFileName, state.context);
            CodecUtil.writeIndexHeader(meta, META_CODEC, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
            index = state.directory.createOutput(indexFileName, state.context);
            CodecUtil.writeIndexHeader(index, INVERTED_INDEX_CODEC, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
            if (state.fieldInfos.hasProx()) {
                prox = state.directory.createOutput(proxFileName, state.context);
                CodecUtil.writeIndexHeader(prox, PROXIMITY_CODEC, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
            } else {
                prox = null;
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
    public void write(Fields fields, NormsProducer norms) throws IOException {
        for (String field : fields) {
            Terms terms = fields.terms(field);
            final boolean hasFreqs = terms.hasFreqs();
            final boolean hasPositions = terms.hasPositions();
            final boolean hasOffsets = terms.hasOffsets();

            if (hasOffsets) {
                meta.writeByte((byte) 3);
            } else if (hasPositions) {
                meta.writeByte((byte) 2);
            } else if (hasFreqs) {
                meta.writeByte((byte) 1);
            } else {
                meta.writeByte((byte) 0);
            }
            meta.writeString(field);
            meta.writeLong(index.getFilePointer());
            if (hasPositions) {
                meta.writeLong(prox.getFilePointer());
            }

            int flags = PostingsEnum.NONE;
            if (hasFreqs) {
                flags |= PostingsEnum.FREQS;
            }
            if (hasPositions) {
                flags |= PostingsEnum.POSITIONS;
            }
            if (hasOffsets) {
                flags |= PostingsEnum.OFFSETS;
            }

            ByteBuffersDataOutput tempIndex = new ByteBuffersDataOutput();
            PostingsWriter writer = new PostingsWriter(hasFreqs, hasPositions, hasOffsets);
            TermsEnum te = terms.iterator();
            PostingsEnum pe = null;
            long numTerms = 0;
            int maxTermLength = 0;
            for (BytesRef term = te.next(); term != null; term = te.next()) {
                pe = te.postings(pe, flags);
                int doc = pe.nextDoc();
                if (doc != DocIdSetIterator.NO_MORE_DOCS) {
                    long proxOffset = hasPositions ? prox.getFilePointer() : -1L;
                    writer.write(pe, tempIndex);
                    index.writeVInt(term.length);
                    index.writeBytes(term.bytes, term.offset, term.length);
                    index.writeInt(writer.docFreq);
                    if (hasFreqs) {
                        index.writeLong(writer.totalTermFreq);
                    }
                    index.writeLong(tempIndex.size());
                    if (hasPositions) {
                        index.writeLong(proxOffset);
                    }
                    ++numTerms;
                    maxTermLength = Math.max(maxTermLength, term.length);

                    tempIndex.copyTo(this.index);
                    tempIndex.reset();
                }
            }

            meta.writeLong(numTerms);
            meta.writeInt(writer.docsWithField.cardinality()); // docCount
            meta.writeLong(writer.sumDocFreq);
            if (hasFreqs) {
                meta.writeLong(writer.sumTotalTermFreq);
            }
            meta.writeInt(maxTermLength);
            meta.writeByte((byte) (writer.hasPayloads ? 1 : 0));
        }
        meta.writeByte((byte) -1); // no more fields
        CodecUtil.writeFooter(meta);
        CodecUtil.writeFooter(index);
        if (prox != null) {
            CodecUtil.writeFooter(prox);
        }
    }

    private class PostingsWriter {

        private final boolean hasFreqs, hasPositions, hasOffsets;
        final FixedBitSet docsWithField = new FixedBitSet(state.segmentInfo.maxDoc());
        int docFreq;
        long sumDocFreq, totalTermFreq, sumTotalTermFreq;
        boolean hasPayloads;

        PostingsWriter(boolean hasFreqs, boolean hasPositions, boolean hasOffsets) {
            this.hasFreqs = hasFreqs;
            this.hasPositions = hasPositions;
            this.hasOffsets = hasOffsets;
        }

        void write(PostingsEnum pe, ByteBuffersDataOutput index) throws IOException {
            docFreq = 0;
            totalTermFreq = 0;
            for (int doc = pe.docID(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = pe.nextDoc()) {
                docsWithField.set(doc);
                ++docFreq;
                index.writeInt(doc);
                int freq;
                if (hasFreqs) {
                    freq = pe.freq();
                    index.writeInt(freq);
                } else {
                    freq = 1;
                }
                totalTermFreq += freq;
                if (hasPositions) {
                    for (int i = 0; i < freq; ++i) {
                        prox.writeInt(pe.nextPosition());
                        if (hasOffsets) {
                            prox.writeInt(pe.startOffset());
                            prox.writeInt(pe.endOffset());
                        }
                        BytesRef payload = pe.getPayload();
                        if (payload == null) {
                            prox.writeVInt(0);
                        } else {
                            hasPayloads = true;
                            prox.writeVInt(1 + payload.length);
                            prox.writeBytes(payload.bytes, payload.offset, payload.length);
                        }
                    }
                }
            }
            sumDocFreq += docFreq;
            sumTotalTermFreq += totalTermFreq;
        }
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(meta, index, prox);
    }

}
