/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.lucene.util;

import org.apache.lucene.store.IndexInput;

import java.io.IOException;

/**
 * Utility class to read a stream of bits.
 *
 * @see BitStreamOutput
 */
public final class BitStreamInput {

    public static final int PADDING_BYTES = 3;

    private final IndexInput in;
    private long buffer;
    private int numBufferedBits;

    /** Sole constructor. */
    public BitStreamInput(IndexInput in) {
        this.in = in;
    }

    /** Read {@code bits} bits from the stream. */
    public int read(int bits) throws IOException {
        assert bits >= 0 && bits < 32;

        if (numBufferedBits < bits) {
            buffer |= Integer.toUnsignedLong(in.readInt()) << numBufferedBits;
            numBufferedBits += Integer.SIZE;
        }
        final long mask = (1L << bits) - 1;
        final long result = buffer & mask;
        buffer >>= bits;
        numBufferedBits -= bits;
        return (int) result;
    }

    private int readUnaryCode() throws IOException {
        if (numBufferedBits == 0) {
            buffer = Integer.toUnsignedLong(in.readInt());
            numBufferedBits = Integer.SIZE;
        }

        int bits = 0;
        while (true) {
            final int numberOfTrailingOnes = Math.min(numBufferedBits, Long.numberOfTrailingZeros(~buffer));
            bits += numberOfTrailingOnes;
            numBufferedBits -= (numberOfTrailingOnes + 1);
            if (numBufferedBits == -1) {
                buffer = Integer.toUnsignedLong(in.readInt());
                numBufferedBits = Integer.SIZE;
            } else {
                buffer >>= (numberOfTrailingOnes + 1);
                break;
            }
        }
        return bits;
    }

    public void done() throws IOException {
        in.seek(in.getFilePointer() - numBufferedBits / Byte.SIZE);
    }

    public int readGammaCode() throws IOException {
        int bitsMinus1 = readUnaryCode();
        return (1 << bitsMinus1) | read(bitsMinus1);
    }

    public int readDeltaCode() throws IOException {
        int bitsMinus1 = readGammaCode() - 1;
        return (1 << bitsMinus1) | read(bitsMinus1);
    }

    /** Read a value written with {@link BitStreamOutput#writeLeftmostMinimalCode}. */
    public int readLeftmostMinimalCode(int maxInclusive) throws IOException {
        final int maxNumberOfBitsPerValue = Math.max(1, 32 - Integer.numberOfLeadingZeros(maxInclusive));
        final int numberOfFreeValues = (1 << maxNumberOfBitsPerValue) - 1 - maxInclusive;
        int v = read(maxNumberOfBitsPerValue - 1);
        if (v >= numberOfFreeValues) {
            v = ((v << 1) | read(1)) - numberOfFreeValues;
        }
        return v;
    }

    /** Read a value written with {@link BitStreamOutput#writeCenteredMinimalCode}. */
    public int readCenteredMinimalCode(int maxInclusive) throws IOException {
        final int maxNumberOfBitsPerValue = Math.max(1, 32 - Integer.numberOfLeadingZeros(maxInclusive));
        final int numberOfFreeValues = (1 << maxNumberOfBitsPerValue) - 1 - maxInclusive;
        final int centerMax = 1 << (maxNumberOfBitsPerValue - 1); // exclusive
        final int centerMin = centerMax - numberOfFreeValues; // inclusive
        int v = read(maxNumberOfBitsPerValue - 1);
        if (v < centerMin) {
            v |= read(1) << (maxNumberOfBitsPerValue - 1);
        }
        return v;
    }
}
