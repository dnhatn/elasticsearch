/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.lucene.util;

import org.apache.lucene.store.DataOutput;

import java.io.IOException;

/**
 * Utility class to write integers of arbitrary size to a stream.
 *
 * @see BitStreamInput
 */
public final class BitStreamOutput {

    private final DataOutput out;
    private long buffer;
    int numBufferedBits; // pkg-private for testing

    /** Sole constructor. */
    public BitStreamOutput(DataOutput out) {
        this.out = out;
    }

    /**
     * Write more bits to the stream. The value that is being written must have its upper bits unset.
     *
     * @param v the value
     * @param bits a number of bits in 0..31
     */
    public void write(int v, int bits) throws IOException {
        assert bits >= 0 && bits < 32;
        assert (v >> bits) == 0;

        buffer |= (((long) v) << numBufferedBits);
        numBufferedBits += bits;

        if (numBufferedBits >= Integer.SIZE) {
            out.writeInt((int) buffer);
            buffer >>= Integer.SIZE;
            numBufferedBits -= Integer.SIZE;
        }
    }

    /** Write an Elias Gamma code for the given positive integer. */
    public void writeGammaCode(int v) throws IOException {
        assert v >= 1;
        final int numBitsMinus1 = 31 - Integer.numberOfLeadingZeros(v);
        final int mask = (1 << numBitsMinus1) - 1;
        write(mask, numBitsMinus1 + 1);
        write(v & mask, numBitsMinus1);
    }

    /** Write an Elias Delta code for the given positive integer. */
    public void writeDeltaCode(int v) throws IOException {
        assert v >= 1;
        final int numBitsMinus1 = 31 - Integer.numberOfLeadingZeros(v);
        final int mask = (1 << numBitsMinus1) - 1;
        writeGammaCode(numBitsMinus1 + 1);
        write(v & mask, numBitsMinus1);
    }

    /**
     * Write a leftmost minimal code of the given non-negative value {@code v}. Assuming {@code b =
     * ceil(log2(max))}, this will encode values that are close to 0 on {@code b - 1} bits and other
     * values on {@code b} bits.
     */
    public void writeLeftmostMinimalCode(int v, int maxInclusive) throws IOException {
        assert v >= 0;
        assert v <= maxInclusive;

        final int maxNumberOfBitsPerValue = Math.max(1, 32 - Integer.numberOfLeadingZeros(maxInclusive));
        final int numberOfFreeValues = (1 << maxNumberOfBitsPerValue) - 1 - maxInclusive;

        if (v < numberOfFreeValues) {
            write(v, maxNumberOfBitsPerValue - 1);
        } else {
            int encoded = numberOfFreeValues + v;
            write(encoded >> 1, maxNumberOfBitsPerValue - 1);
            write(encoded & 1, 1);
        }
    }

    /**
     * Like {@link #writeLeftmostMinimalCode(int, int)} but tries to allocate fewer bits to values
     * that are in the center of the range.
     */
    public void writeCenteredMinimalCode(int v, int maxInclusive) throws IOException {
        assert v >= 0;
        assert v <= maxInclusive;

        final int maxNumberOfBitsPerValue = Math.max(1, 32 - Integer.numberOfLeadingZeros(maxInclusive));
        final int numberOfFreeValues = (1 << maxNumberOfBitsPerValue) - 1 - maxInclusive;
        final int centerMax = 1 << (maxNumberOfBitsPerValue - 1);
        final int centerMin = centerMax - numberOfFreeValues;

        if (v >= centerMin && v < centerMax) {
            write(v, maxNumberOfBitsPerValue - 1);
        } else {
            write(v, maxNumberOfBitsPerValue);
        }
    }

    /** Flush buffered bits. */
    public void flush() throws IOException {
        while (numBufferedBits > 0) {
            out.writeByte((byte) buffer);
            buffer >>= Byte.SIZE;
            numBufferedBits = Math.max(0, numBufferedBits - Byte.SIZE);
        }
    }
}
