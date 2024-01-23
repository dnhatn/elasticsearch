/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.codec.postings;

import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

import java.io.IOException;

/**
 * A postings format that inlines docs and freqs in the terms dictionary in order to improve access patterns for slow storage mediums such
 * as S3 and other object stores. This way, reading a single block is likely enough to download the term, metadata about the term and its
 * associated postings. Positions, offsets and payloads are still stored in a separate file.
 */
public final class ES814InlinePostingsFormat extends PostingsFormat {

    public static final String META_CODEC = "ES814InlineMeta";
    public static final String INVERTED_INDEX_CODEC = "ES814InlineInvertedIndex";
    public static final String PROXIMITY_CODEC = "ES814InlineProximity";
    public static final String META_EXTENSION = "tpm";
    public static final String INVERTED_INDEX_EXTENSION = "inv";
    public static final String PROXIMITY_EXTENSION = "prx";
    static final int VERSION_START = 0;
    static final int VERSION_CURRENT = VERSION_START;
    // One for the last posting in a block, which is used as skip data. Plus 128 for the block of docs that gets compressed together.
    static final int POSTINGS_BLOCK_SIZE = ForUtil.BLOCK_SIZE + 1;

    /**
     * Sole constructor.
     */
    public ES814InlinePostingsFormat() {
        super("ES814Inline");
    }

    @Override
    public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        return new ES814InlineFieldsConsumer(state);
    }

    @Override
    public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
        return new ES814InlineFieldsProducer(state);
    }
}
