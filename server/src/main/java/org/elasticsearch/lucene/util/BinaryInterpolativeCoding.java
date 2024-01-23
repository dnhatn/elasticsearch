/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.lucene.util;

import java.io.IOException;

/**
 * Implements binary interpolative coding, as described in Moffat, A., Stuiver, L. Binary Interpolative Coding for Effective Index Compression. Information Retrieval 3, 25–47 (2000). https://doi.org/10.1023/A:1013002601898
 */
public final class BinaryInterpolativeCoding {

    private static final int split(int length) {
        // idealLength is the greatest value that is less than or equal to currentLength and can be
        // expressed as 2^r-1
        int idealLength = Integer.highestOneBit(length + 1) - 1;
        return length - idealLength - 1;
    }

    private BinaryInterpolativeCoding() {}

    public static void encodeIncreasing(
        long[] values,
        int fromInclusive,
        int toInclusive,
        long minInclusive,
        long maxInclusive,
        BitStreamOutput out
    ) throws IOException {
        final int size = toInclusive - fromInclusive + 1;
        for (int i = 1; i < size; ++i) {
            values[fromInclusive + i] -= i;
        }
        try {
            encodeNonDecreasing(values, fromInclusive, toInclusive, minInclusive, maxInclusive - size + 1, out);
        } finally {
            // Restore the array back to its original values
            for (int i = 1; i < size; ++i) {
                values[fromInclusive + i] += i;
            }
        }
    }

    public static void encodeNonDecreasing(
        long[] values,
        int fromInclusive,
        int toInclusive,
        long minInclusive,
        long maxInclusive,
        BitStreamOutput out
    ) throws IOException {

        if (minInclusive < Integer.MIN_VALUE || maxInclusive > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Binary interpolative coding can only handle integers");
        }

        for (int i = fromInclusive; i < toInclusive; ++i) {
            if (values[i] > values[i + 1]) {
                throw new IllegalArgumentException("Sequence must be non decreasing");
            }
        }

        for (int i = fromInclusive; i <= toInclusive; ++i) {
            if (values[i] < minInclusive || values[i] > maxInclusive) {
                throw new IllegalArgumentException(
                    "Value at index " + i + " out of range: " + values[i] + " not in [" + minInclusive + ", " + maxInclusive + "]"
                );
            }
        }

        if (fromInclusive > toInclusive) {
            return;
        }

        // Interpolative coding works best on a perfectly balanced tree, which happens when there are
        // 2^r-1 values to encode. So we split the recursion tree into as many trees as necessary so
        // that each tree has exactly a number of leaves that can be expressed as 2^r-1.
        final int split = split(toInclusive - fromInclusive + 1);
        if (split == -1) {
            encodeInternal(values, fromInclusive, toInclusive, minInclusive, maxInclusive, out);
        } else {
            long splitValue = values[fromInclusive + split];
            out.writeLeftmostMinimalCode((int) (splitValue - minInclusive), (int) (maxInclusive - minInclusive));

            encodeNonDecreasing(values, fromInclusive, fromInclusive + split - 1, minInclusive, splitValue, out);
            encodeInternal(values, fromInclusive + split + 1, toInclusive, splitValue, maxInclusive, out);
        }
    }

    private static void encodeInternal(
        long[] values,
        int fromInclusive,
        int toInclusive,
        long minInclusive,
        long maxInclusive,
        BitStreamOutput out
    ) throws IOException {

        // the length is a power of 2
        assert Integer.highestOneBit(toInclusive - fromInclusive + 2) == toInclusive - fromInclusive + 2;
        assert toInclusive >= fromInclusive;

        if (fromInclusive == toInclusive) {
            // TODO: The paper recommends using a code that uses fewer bits for outermost values in that case
            out.writeLeftmostMinimalCode((int) (values[fromInclusive] - minInclusive), (int) (maxInclusive - minInclusive));
            return;
        }

        // unsigned shift to protect from overflows
        final int mid = (fromInclusive + toInclusive) >>> 1;
        final long midValue = values[mid];
        assert midValue >= minInclusive;
        assert midValue <= maxInclusive;
        out.writeCenteredMinimalCode((int) (midValue - minInclusive), (int) (maxInclusive - minInclusive));

        encodeInternal(values, fromInclusive, mid - 1, minInclusive, midValue, out);
        encodeInternal(values, mid + 1, toInclusive, midValue, maxInclusive, out);
    }

    public static void decodeIncreasing(
        BitStreamInput in,
        long[] values,
        int fromInclusive,
        int toInclusive,
        long minInclusive,
        long maxInclusive
    ) throws IOException {
        final int size = toInclusive - fromInclusive + 1;
        decodeNonDecreasing(in, values, fromInclusive, toInclusive, minInclusive, maxInclusive - size + 1);
        for (int i = 1; i < size; ++i) {
            values[fromInclusive + i] += i;
        }
    }

    public static void decodeNonDecreasing(
        BitStreamInput in,
        long[] values,
        int fromInclusive,
        int toInclusive,
        long minInclusive,
        long maxInclusive
    ) throws IOException {

        if (minInclusive < Integer.MIN_VALUE || maxInclusive > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Binary interpolative coding can only handle integers");
        }

        if (fromInclusive > toInclusive) {
            return;
        }

        // Mirrors similar logic on the encoding side.
        final int split = split(toInclusive - fromInclusive + 1);
        if (split == -1) {
            decodeInternal(in, values, fromInclusive, toInclusive, minInclusive, maxInclusive);
        } else {
            long splitValue = minInclusive + in.readLeftmostMinimalCode((int) (maxInclusive - minInclusive));
            values[fromInclusive + split] = splitValue;

            decodeNonDecreasing(in, values, fromInclusive, fromInclusive + split - 1, minInclusive, splitValue);
            decodeInternal(in, values, fromInclusive + split + 1, toInclusive, splitValue, maxInclusive);
        }
    }

    public static void decodeInternal(
        BitStreamInput in,
        long[] values,
        int fromInclusive,
        int toInclusive,
        long minInclusive,
        long maxInclusive
    ) throws IOException {

        // the length is a power of 2
        assert Integer.highestOneBit(toInclusive - fromInclusive + 2) == toInclusive - fromInclusive + 2;
        assert toInclusive >= fromInclusive;

        if (fromInclusive == toInclusive) {
            values[fromInclusive] = minInclusive + in.readLeftmostMinimalCode((int) (maxInclusive - minInclusive));
            return;
        }

        final int mid = (fromInclusive + toInclusive) >>> 1;
        final long midValue = minInclusive + in.readCenteredMinimalCode((int) (maxInclusive - minInclusive));
        values[mid] = midValue;

        decodeInternal(in, values, fromInclusive, mid - 1, minInclusive, midValue);
        decodeInternal(in, values, mid + 1, toInclusive, midValue, maxInclusive);
    }
}
