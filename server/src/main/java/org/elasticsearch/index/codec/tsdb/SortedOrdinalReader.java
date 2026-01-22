/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.util.packed.DirectMonotonicReader;

public final class SortedOrdinalReader {
    final long maxOrd;
    final DirectMonotonicReader startDocs;
    private long currentIndex = -1;
    private long rangeEndExclusive = -1;

    public SortedOrdinalReader(long maxOrd, DirectMonotonicReader startDocs) {
        this.maxOrd = maxOrd;
        this.startDocs = startDocs;
    }

    public long readValueAndAdvance(int doc) {
        if (doc < rangeEndExclusive) {
            return currentIndex;
        }
        // move to the next range
        if (doc == rangeEndExclusive) {
            currentIndex++;
        } else {
            currentIndex = searchRange(doc);
        }
        rangeEndExclusive = startDocs.get(currentIndex + 1);
        return currentIndex;
    }

    public long getRangeEndExclusive(int doc) {
        if (doc < rangeEndExclusive) {
            return rangeEndExclusive;
        }
        // move to the next range
        if (doc == rangeEndExclusive) {
            currentIndex++;
        } else {
            currentIndex = searchRange(doc);
        }
        rangeEndExclusive = startDocs.get(currentIndex + 1);
        return rangeEndExclusive;
    }

    public long getRangeEndExclusive() {
        return rangeEndExclusive;
    }

    private long searchRange(int doc) {
        long index = startDocs.binarySearch(currentIndex + 1, maxOrd, doc);
        if (index < 0) {
            index = -2 - index;
        }
        assert index < maxOrd : "invalid range " + index + " for doc " + doc + " in maxOrd " + maxOrd;
        return index;
    }

    public long lookAheadValue(int targetDoc) {
        if (targetDoc < rangeEndExclusive) {
            return currentIndex;
        } else {
            return searchRange(targetDoc);
        }
    }

    public interface Accessor {
        default SortedOrdinalReader getOrdinalReader() {
            return null;
        }
    }
}
