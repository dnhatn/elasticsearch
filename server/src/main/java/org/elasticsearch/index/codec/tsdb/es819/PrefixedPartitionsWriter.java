/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es819;

import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * Writes prefix-based partition metadata for the primary sort field during segment flush. Terms are grouped by their first
 * {@link #PARTITION_PREFIX_BITS} bits, and the starting document for each prefix group is recorded. This enables the query engine
 * to partition work by prefix without scanning all doc values.
 * <p>
 * Usage is two-pass: first call {@link #trackTerm} for each term during the terms dict write, then call {@link #prepareForTrackingDocs}
 * to compact the prefix-to-ordinal mapping, and finally call {@link #trackDoc} for each ordinal during the numeric field write.
 *
 * <pre>{@code
 * // Pass 1: track terms during terms dict write
 * PrefixedPartitionsWriter writer = new PrefixedPartitionsWriter();
 * for (BytesRef term = termsEnum.next(); term != null; term = termsEnum.next()) {
 *     writer.trackTerm(term, termsEnum.ord());
 * }
 *
 * // Transition: compact prefix-to-ordinal mapping
 * writer.prepareForTrackingDocs();
 *
 * // Pass 2: track start docs
 * for (int doc = values.nextDoc(); doc != NO_MORE_DOCS; doc = values.nextDoc()) {
 *     writer.trackDoc(doc, values.ordValue());
 * }
 *
 * writer.flush(data, meta);
 * }</pre>
 *
 * @see PrefixedPartitionsReader
 */
final class PrefixedPartitionsWriter {
    static final int PARTITION_PREFIX_BITS = 16;
    static final VarHandle BE_SHORT = MethodHandles.byteArrayViewVarHandle(short[].class, ByteOrder.BIG_ENDIAN);

    private int nextOrd = -1;

    private final int[] ords = new int[1 << PARTITION_PREFIX_BITS];

    private int numPrefixes;
    private int idx = 0;
    private int[] prefixes;
    private int[] startDocs;

    PrefixedPartitionsWriter() {
        Arrays.fill(ords, -1);
    }

    private static int prefix(BytesRef term) {
        if (term.length < 2) {
            return 0;
        }
        return ((short) BE_SHORT.get(term.bytes, term.offset) & 0xFFFF) >>> (Short.SIZE - PARTITION_PREFIX_BITS);
    }

    void trackTerm(BytesRef term, long ord) {
        final int prefix = prefix(term);
        if (ords[prefix] < 0) {
            ords[prefix] = Math.toIntExact(ord);
            numPrefixes++;
        }
    }

    void prepareForTrackingDocs() {
        prefixes = new int[numPrefixes];
        int dst = 0;
        for (int i = 0; i < ords.length; i++) {
            final int ord = ords[i];
            if (ord >= 0) {
                prefixes[dst] = i;
                ords[dst++] = ords[i];
            }
        }
        assert dst == numPrefixes : dst + " != " + numPrefixes;
        startDocs = new int[numPrefixes];
        if (idx < numPrefixes) {
            nextOrd = ords[idx];
        }
    }

    void trackDoc(int docId, long ord) {
        if (nextOrd == ord) {
            startDocs[idx] = docId;
            if (++idx < numPrefixes) {
                nextOrd = ords[idx];
            } else {
                nextOrd = Integer.MAX_VALUE;
            }
        }
    }

    void flush(IndexOutput data, IndexOutput meta) throws IOException {
        final long startPointer = data.getFilePointer();
        data.writeVInt(numPrefixes);
        int last = 0;
        for (int prefix : prefixes) {
            data.writeVInt(prefix - last);
            last = prefix;
        }
        last = 0;
        for (int startDoc : startDocs) {
            data.writeVInt(startDoc - last);
            last = startDoc;
        }
        final long length = data.getFilePointer() - startPointer;
        meta.writeLong(startPointer);
        meta.writeVLong(length);
    }
}
