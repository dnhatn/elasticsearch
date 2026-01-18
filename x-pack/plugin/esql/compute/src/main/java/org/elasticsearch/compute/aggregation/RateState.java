/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

final class RateState {
    /**
     * Holds a list of raw slices for each group
     */
    record GroupedSlices(int minGroupId, int maxGroupId, ObjectArray<int[]> slices) implements Releasable {
        int[] getSlicesForGroup(int groupId) {
            if (groupId < minGroupId || groupId > maxGroupId) {
                return null;
            }
            return slices.get(groupId - minGroupId);
        }

        @Override
        public void close() {
            Releasables.close(slices);
        }
    }

    /**
     * One slice of data points to be merged
     */
    static final class Slice {
        final LongArray timestamps;
        int start;
        int end;
        long lastTimestamp;
        long timestamp;

        public Slice(LongArray timestamps, int start, int end) {
            this.timestamps = timestamps;
            this.start = start;
            this.end = end;
            this.lastTimestamp = timestamps.get(end - 1);
            this.timestamp = timestamps.get(start);
        }

        boolean exhausted() {
            return start >= end;
        }

        int next() {
            int index = start++;
            if (index < end) {
                timestamp = timestamps.get(index);
            }
            return index;
        }
    }

    static final class SliceQueue extends PriorityQueue<Slice> {
        SliceQueue(int maxSize) {
            super(maxSize);
        }

        @Override
        protected boolean lessThan(Slice a, Slice b) {
            return a.timestamp > b.timestamp; // want the latest timestamp first
        }

        public long getNextTimestamp() {
            if (size() <= 1) {
                return Long.MIN_VALUE;
            }
            Slice slice = (Slice) getHeapArray()[1];
            return slice.timestamp;
        }
    }
}
