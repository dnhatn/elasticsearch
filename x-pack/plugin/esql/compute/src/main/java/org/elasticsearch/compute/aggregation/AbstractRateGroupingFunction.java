/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.util.Arrays;

import static org.elasticsearch.common.util.BigArrays.overSize;

class AbstractRateGroupingFunction {
    /**
     * Buffers data points in two arrays: one for timestamps and one for values, partitioned into multiple slices.
     * Each slice is sorted in descending order of timestamp. A new slice is created when a data point has a
     * timestamp greater than the last point of the current slice or it belongs to a different group (=timeseries).
     * Since each page is sorted by descending timestamp,
     * we only need to compare the first point of the new page with the last point of the current slice to decide
     * if a new slice is needed. During merging, a priority queue is used to iterate through the slices, selecting
     * the slice with the greatest timestamp.
     */
    static final int INITIAL_SLICE_CAPACITY = 512;

    abstract static class RawBuffer implements Releasable {
        private final CircuitBreaker breaker;
        private long acquiredBytes;
        LongBuffer timestamps;
        int valueCount;

        int[] sliceStarts;
        int[] sliceGroupIds;
        int sliceCount;
        int lastGroupId = -1;
        int minGroupId = Integer.MAX_VALUE;
        int maxGroupId = Integer.MIN_VALUE;

        RawBuffer(CircuitBreaker breaker) {
            this.breaker = breaker;
            this.acquiredBytes = (long) INITIAL_SLICE_CAPACITY * Integer.BYTES * 2;
            breaker.addEstimateBytesAndMaybeBreak(acquiredBytes, "rate-slices");
            this.sliceStarts = new int[INITIAL_SLICE_CAPACITY];
            this.sliceGroupIds = new int[INITIAL_SLICE_CAPACITY];
            this.timestamps = new LongBuffer(breaker, PAGE_SIZE);
        }

        final void prepareSlicesOnly(int groupId, long firstTimestamp) {
            if (lastGroupId == groupId && valueCount > 0) {
                if (timestamps.get(valueCount - 1) > firstTimestamp) {
                    return; // continue with the current slice
                }
            }
            if (sliceCount >= sliceStarts.length) {
                int newLen = ArrayUtil.oversize(sliceCount + 1, Integer.BYTES);
                long delta = (long) (newLen - sliceStarts.length) * Integer.BYTES * 2;
                breaker.addEstimateBytesAndMaybeBreak(delta, "rate-slices");
                acquiredBytes += delta;
                sliceStarts = Arrays.copyOf(sliceStarts, newLen);
                sliceGroupIds = Arrays.copyOf(sliceGroupIds, newLen);
            }
            if (groupId < minGroupId) {
                minGroupId = groupId;
            }
            if (groupId > maxGroupId) {
                maxGroupId = groupId;
            }
            sliceStarts[sliceCount] = valueCount;
            sliceGroupIds[sliceCount] = groupId;
            lastGroupId = groupId;
            sliceCount++;
        }

        final FlushQueues prepareForFlush() {
            if (minGroupId > maxGroupId) {
                return new FlushQueues(this, minGroupId, maxGroupId, null, null);
            }
            final int numGroups = maxGroupId - minGroupId + 1;
            int[] runningOffsets = new int[numGroups];
            for (int i = 0; i < sliceCount; i++) {
                runningOffsets[sliceGroupIds[i] - minGroupId]++;
            }
            int runningOffset = 0;
            for (int i = 0; i < numGroups; i++) {
                int count = runningOffsets[i];
                runningOffsets[i] = runningOffset;
                runningOffset += count;
            }
            int[] sliceOffsets = new int[sliceCount * 2];
            for (int i = 0; i < sliceCount; i++) {
                int groupIndex = sliceGroupIds[i] - minGroupId;
                int startOffset = sliceStarts[i];
                int endOffset = (i + 1) < sliceCount ? sliceStarts[i + 1] : valueCount;
                int dstIndex = runningOffsets[groupIndex]++;
                sliceOffsets[dstIndex * 2] = startOffset;
                sliceOffsets[dstIndex * 2 + 1] = endOffset;
            }
            valueCount = 0;
            sliceCount = 0;
            lastGroupId = -1;
            return new FlushQueues(this, minGroupId, maxGroupId, runningOffsets, sliceOffsets);
        }

        @Override
        public void close() {
            Releasables.close(timestamps);
            breaker.addWithoutBreaking(-acquiredBytes);
            acquiredBytes = 0;
        }
    }

    record FlushQueues(RawBuffer buffer, int minGroupId, int maxGroupId, int[] runningOffsets, int[] sliceOffsets) implements Releasable {
        FlushQueue getFlushQueue(int groupId) {
            if (groupId < minGroupId || groupId > maxGroupId) {
                return null;
            }
            int groupIndex = groupId - minGroupId;
            int endIndex = runningOffsets[groupIndex];
            int startIndex = groupIndex == 0 ? 0 : runningOffsets[groupIndex - 1];
            int numSlices = endIndex - startIndex;
            if (numSlices == 0) {
                return null;
            }
            FlushQueue queue = new FlushQueue(numSlices);
            for (int i = startIndex; i < endIndex; i++) {
                int start = sliceOffsets[i * 2];
                int end = sliceOffsets[i * 2 + 1];
                if (start < end) {
                    queue.valueCount += (end - start);
                    queue.add(new Slice(buffer.timestamps, start, end));
                }
            }
            if (queue.valueCount == 0) {
                return null;
            }
            return queue;
        }

        @Override
        public void close() {

        }
    }

    static final class Slice {
        int start;
        int end;
        long nextTimestamp;
        private long lastTimestamp = Long.MAX_VALUE;
        final LongBuffer timestamps;

        Slice(LongBuffer timestamps, int start, int end) {
            this.timestamps = timestamps;
            this.start = start;
            this.end = end;
            this.nextTimestamp = timestamps.get(start);
        }

        boolean exhausted() {
            return start >= end;
        }

        int next() {
            int currentIndex = start;
            start++;
            if (start < end) {
                nextTimestamp = timestamps.get(start); // next timestamp
            }
            return currentIndex;
        }

        long lastTimestamp() {
            if (lastTimestamp == Long.MAX_VALUE) {
                lastTimestamp = timestamps.get(end - 1);
            }
            return lastTimestamp;
        }
    }

    static final class FlushQueue extends PriorityQueue<Slice> {
        int valueCount;

        FlushQueue(int maxSize) {
            super(maxSize);
        }

        /**
         * Returns the timestamp of the slice that would be next in line after the best slice.
         */
        long secondNextTimestamp() {
            final Object[] heap = getHeapArray();
            final int size = size();
            if (size == 2) {
                return ((Slice) heap[2]).nextTimestamp;
            } else if (size >= 3) {
                return Math.max(((Slice) heap[2]).nextTimestamp, ((Slice) heap[3]).nextTimestamp);
            } else {
                return Long.MIN_VALUE;
            }
        }

        @Override
        protected boolean lessThan(Slice a, Slice b) {
            return a.nextTimestamp > b.nextTimestamp; // want the latest timestamp first
        }
    }

    static final int PAGE_SHIFT = 12;
    static final int PAGE_SIZE = 1 << PAGE_SHIFT;
    static final int PAGE_MASK = PAGE_SIZE - 1;

    abstract static class BufferedArray implements Releasable {
        protected final CircuitBreaker breaker;
        private long acquiredBytes;

        BufferedArray(CircuitBreaker breaker) {
            this.breaker = breaker;
        }

        void acquireMemory(long bytes) {
            breaker.addEstimateBytesAndMaybeBreak(bytes, "buffered-array");
            acquiredBytes += bytes;
        }

        @Override
        public void close() {
            if (acquiredBytes > 0) {
                breaker.addWithoutBreaking(-acquiredBytes);
                acquiredBytes = 0;
            }
        }
    }

    /**
     * Paged long array backed by {@code long[][]}. Each page holds {@link #PAGE_SIZE} elements.
     * Avoids the VarHandle indirection of BigArrays' byte[]-backed LongArray.
     */
    static final class LongBuffer extends BufferedArray {
        long[][] pages;
        long capacity;

        LongBuffer(CircuitBreaker breaker, long initialCapacity) {
            super(breaker);
            int numPages = Math.max(1, Math.toIntExact((initialCapacity + PAGE_SIZE - 1) >>> PAGE_SHIFT));
            acquireMemory((long) numPages * PAGE_SIZE * Long.BYTES);
            pages = new long[numPages][];
            for (int i = 0; i < numPages; i++) {
                pages[i] = new long[PAGE_SIZE];
            }
            capacity = (long) numPages << PAGE_SHIFT;
        }

        long get(long index) {
            return pages[(int) (index >>> PAGE_SHIFT)][(int) (index & PAGE_MASK)];
        }

        void set(long index, long value) {
            pages[(int) (index >>> PAGE_SHIFT)][(int) (index & PAGE_MASK)] = value;
        }

        long scanResets(int from, int to, long prevValue, double[] resets) {
            int pos = from;
            while (pos < to) {
                long[] page = pages[pos >>> PAGE_SHIFT];
                int pageOffset = pos & PAGE_MASK;
                int chunk = Math.min(to - pos, PAGE_SIZE - pageOffset);
                int end = pageOffset + chunk;
                for (int i = pageOffset; i < end; i++) {
                    long val = page[i];
                    if (val > prevValue) {
                        resets[0] += val;
                    }
                    prevValue = val;
                }
                pos += chunk;
            }
            return prevValue;
        }

        void appendRange(long dst, LongVector src, int from, int count) {
            int srcPos = from;
            while (count > 0) {
                long[] page = pages[(int) (dst >>> PAGE_SHIFT)];
                int pageOffset = (int) (dst & PAGE_MASK);
                int chunk = Math.min(count, PAGE_SIZE - pageOffset);
                for (int i = 0; i < chunk; i++) {
                    page[pageOffset + i] = src.getLong(srcPos + i);
                }
                srcPos += chunk;
                dst += chunk;
                count -= chunk;
            }
        }

        void ensureCapacity(long minCapacity) {
            if (minCapacity <= capacity) {
                return;
            }
            long newSize = overSize(minCapacity, PageCacheRecycler.BYTE_PAGE_SIZE, Long.BYTES);
            int numPages = Math.toIntExact((newSize + PAGE_SIZE - 1) >>> PAGE_SHIFT);
            int oldLength = pages.length;
            acquireMemory((long) (numPages - oldLength) * PAGE_SIZE * Long.BYTES);
            pages = Arrays.copyOf(pages, numPages);
            for (int i = oldLength; i < numPages; i++) {
                pages[i] = new long[PAGE_SIZE];
            }
            capacity = (long) numPages << PAGE_SHIFT;
        }
    }

    /**
     * Paged double array backed by {@code double[][]}. Each page holds {@link #PAGE_SIZE} elements.
     * Avoids the VarHandle indirection of BigArrays' byte[]-backed DoubleArray.
     */
    static final class DoubleBuffer extends BufferedArray {
        double[][] pages;
        long capacity;

        DoubleBuffer(CircuitBreaker breaker, long initialCapacity) {
            super(breaker);
            int numPages = Math.max(1, Math.toIntExact((initialCapacity + PAGE_SIZE - 1) >>> PAGE_SHIFT));
            acquireMemory((long) numPages * PAGE_SIZE * Double.BYTES);
            pages = new double[numPages][];
            for (int i = 0; i < numPages; i++) {
                pages[i] = new double[PAGE_SIZE];
            }
            capacity = (long) numPages << PAGE_SHIFT;
        }

        double get(long index) {
            return pages[(int) (index >>> PAGE_SHIFT)][(int) (index & PAGE_MASK)];
        }

        void set(long index, double value) {
            pages[(int) (index >>> PAGE_SHIFT)][(int) (index & PAGE_MASK)] = value;
        }

        void appendRange(long dst, DoubleVector src, int from, int count) {
            int srcPos = from;
            while (count > 0) {
                double[] page = pages[(int) (dst >>> PAGE_SHIFT)];
                int pageOffset = (int) (dst & PAGE_MASK);
                int chunk = Math.min(count, PAGE_SIZE - pageOffset);
                for (int i = 0; i < chunk; i++) {
                    page[pageOffset + i] = src.getDouble(srcPos + i);
                }
                srcPos += chunk;
                dst += chunk;
                count -= chunk;
            }
        }

        double scanResets(int from, int to, double prevValue, double[] resets) {
            int pos = from;
            while (pos < to) {
                double[] page = pages[pos >>> PAGE_SHIFT];
                int pageOffset = pos & PAGE_MASK;
                int chunk = Math.min(to - pos, PAGE_SIZE - pageOffset);
                int end = pageOffset + chunk;
                for (int i = pageOffset; i < end; i++) {
                    double val = page[i];
                    if (val > prevValue) {
                        resets[0] += val;
                    }
                    prevValue = val;
                }
                pos += chunk;
            }
            return prevValue;
        }

        void ensureCapacity(long minCapacity) {
            if (minCapacity <= capacity) {
                return;
            }
            long newSize = overSize(minCapacity, PageCacheRecycler.BYTE_PAGE_SIZE, Double.BYTES);
            int numPages = Math.toIntExact((newSize + PAGE_SIZE - 1) >>> PAGE_SHIFT);
            int oldLength = pages.length;
            acquireMemory((long) (numPages - oldLength) * PAGE_SIZE * Double.BYTES);
            pages = Arrays.copyOf(pages, numPages);
            for (int i = oldLength; i < numPages; i++) {
                pages[i] = new double[PAGE_SIZE];
            }
            capacity = (long) numPages << PAGE_SHIFT;
        }
    }

    /**
     * Paged int array backed by {@code int[][]}. Each page holds {@link #PAGE_SIZE} elements.
     * Avoids the VarHandle indirection of BigArrays' byte[]-backed IntArray.
     */
    static final class IntBuffer extends BufferedArray {
        int[][] pages;
        long capacity;

        IntBuffer(CircuitBreaker breaker, long initialCapacity) {
            super(breaker);
            int numPages = Math.max(1, Math.toIntExact((initialCapacity + PAGE_SIZE - 1) >>> PAGE_SHIFT));
            acquireMemory((long) numPages * PAGE_SIZE * Integer.BYTES);
            pages = new int[numPages][];
            for (int i = 0; i < numPages; i++) {
                pages[i] = new int[PAGE_SIZE];
            }
            capacity = (long) numPages << PAGE_SHIFT;
        }

        int get(long index) {
            return pages[(int) (index >>> PAGE_SHIFT)][(int) (index & PAGE_MASK)];
        }

        void set(long index, int value) {
            pages[(int) (index >>> PAGE_SHIFT)][(int) (index & PAGE_MASK)] = value;
        }

        void appendRange(long dst, IntVector src, int from, int count) {
            int srcPos = from;
            while (count > 0) {
                int[] page = pages[(int) (dst >>> PAGE_SHIFT)];
                int pageOffset = (int) (dst & PAGE_MASK);
                int chunk = Math.min(count, PAGE_SIZE - pageOffset);
                for (int i = 0; i < chunk; i++) {
                    page[pageOffset + i] = src.getInt(srcPos + i);
                }
                srcPos += chunk;
                dst += chunk;
                count -= chunk;
            }
        }

        int scanResets(int from, int to, int prevValue, double[] resets) {
            int pos = from;
            while (pos < to) {
                int[] page = pages[pos >>> PAGE_SHIFT];
                int pageOffset = pos & PAGE_MASK;
                int chunk = Math.min(to - pos, PAGE_SIZE - pageOffset);
                int end = pageOffset + chunk;
                for (int i = pageOffset; i < end; i++) {
                    int val = page[i];
                    if (val > prevValue) {
                        resets[0] += val;
                    }
                    prevValue = val;
                }
                pos += chunk;
            }
            return prevValue;
        }

        void ensureCapacity(long minCapacity) {
            if (minCapacity <= capacity) {
                return;
            }
            long newSize = overSize(minCapacity, PageCacheRecycler.BYTE_PAGE_SIZE, Integer.BYTES);
            int numPages = Math.toIntExact((newSize + PAGE_SIZE - 1) >>> PAGE_SHIFT);
            int oldLength = pages.length;
            acquireMemory((long) (numPages - oldLength) * PAGE_SIZE * Integer.BYTES);
            pages = Arrays.copyOf(pages, numPages);
            for (int i = oldLength; i < numPages; i++) {
                pages[i] = new int[PAGE_SIZE];
            }
            capacity = (long) numPages << PAGE_SHIFT;
        }
    }
}
