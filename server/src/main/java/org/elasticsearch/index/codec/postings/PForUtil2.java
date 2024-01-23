/*
 * @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modifications copyright (C) 2022 Elasticsearch B.V.
 */
package org.elasticsearch.index.codec.postings;

import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.LongHeap;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.lucene.util.BinaryInterpolativeCoding;
import org.elasticsearch.lucene.util.BitStreamInput;
import org.elasticsearch.lucene.util.BitStreamOutput;

import java.io.IOException;
import java.util.Arrays;

/** Like {@link PForUtil}, but trades more CPU for better space efficiency by allowing more exceptions and encoding them in a more compact way. */
final class PForUtil2 {

    private static final int NUM_EXCEPTIONS_BITS = 4;
    private static final int MAX_EXCEPTIONS = (1 << NUM_EXCEPTIONS_BITS) - 1;
    private static final int HALF_BLOCK_SIZE = ForUtil.BLOCK_SIZE / 2;

    private final ForUtil forUtil;
    // buffers for reading exception data
    private final long[] exceptionIndexes = new long[MAX_EXCEPTIONS];
    private final long[] exceptionValues = new long[MAX_EXCEPTIONS];

    PForUtil2(ForUtil forUtil) {
        assert ForUtil.BLOCK_SIZE <= 256 : "blocksize must fit in one byte. got " + ForUtil.BLOCK_SIZE;
        this.forUtil = forUtil;
    }

    /** Encode 128 integers from {@code longs} into {@code out}. */
    void encodeValuesMinus1(long[] longs, DataOutput out, ByteBuffersDataOutput spare) throws IOException {
        // Determine the top MAX_EXCEPTIONS + 1 values
        final LongHeap top = new LongHeap(MAX_EXCEPTIONS + 1);
        for (int i = 0; i <= MAX_EXCEPTIONS; ++i) {
            top.push(longs[i]);
        }
        long topValue = top.top();
        for (int i = MAX_EXCEPTIONS + 1; i < ForUtil.BLOCK_SIZE; ++i) {
            if (longs[i] > topValue) {
                topValue = top.updateTop(longs[i]);
            }
        }

        long max = 0L;
        for (int i = 1; i <= top.size(); ++i) {
            max = Math.max(max, top.get(i));
        }

        final int patchedBitsRequired = topValue == 0 ? 0 : PackedInts.bitsRequired(topValue);
        int numExceptions = 0;
        final long maxUnpatchedValue = (1L << patchedBitsRequired) - 1;
        for (int i = 0; i < ForUtil.BLOCK_SIZE; ++i) {
            if (longs[i] > maxUnpatchedValue) {
                exceptionIndexes[numExceptions] = i;
                exceptionValues[numExceptions] = Math.toIntExact(longs[i] >> patchedBitsRequired);
                longs[i] &= maxUnpatchedValue;
                numExceptions++;
            }
        }
        assert numExceptions <= MAX_EXCEPTIONS;

        spare.reset();
        BitStreamOutput bitOut = new BitStreamOutput(spare);
        bitOut.write(numExceptions, NUM_EXCEPTIONS_BITS);
        BinaryInterpolativeCoding.encodeIncreasing(exceptionIndexes, 0, numExceptions - 1, 0, ForUtil.BLOCK_SIZE - 1, bitOut);
        for (int i = 0; i < numExceptions; ++i) {
            bitOut.writeGammaCode(Math.toIntExact(exceptionValues[i]));
        }
        bitOut.flush();
        out.writeVLong((spare.size() << 5) | patchedBitsRequired);
        spare.copyTo(out);
        if (patchedBitsRequired > 0) {
            forUtil.encode(longs, patchedBitsRequired, out);
        }
    }

    /** Decode 128 integers into {@code longs}. */
    void decode(IndexInput in, long[] longs) throws IOException {
        final long token = in.readVLong();
        final int patchBitsRequired = (int) (token & 0x1F);
        BitStreamInput bitIn = new BitStreamInput(in);
        final int numExceptions = bitIn.read(NUM_EXCEPTIONS_BITS);
        BinaryInterpolativeCoding.decodeIncreasing(bitIn, exceptionIndexes, 0, numExceptions - 1, 0, ForUtil.BLOCK_SIZE - 1);
        for (int i = 0; i < numExceptions; ++i) {
            exceptionValues[i] = bitIn.readGammaCode();
        }
        bitIn.done();

        if (patchBitsRequired == 0) {
            Arrays.fill(longs, 0L);
        } else {
            forUtil.decode(patchBitsRequired, in, longs);
        }
        for (int i = 0; i < numExceptions; ++i) {
            final int index = (int) exceptionIndexes[i];
            longs[index] |= exceptionValues[i] << patchBitsRequired;
        }
        for (int i = 0; i < ForUtil.BLOCK_SIZE; ++i) {
            longs[i] += 1;
        }
    }

    /** Decode deltas minus 1, add one, compute the prefix sum and add {@code base} to all decoded longs. */
    void decodeAndPrefixSum(IndexInput in, long base, long[] longs) throws IOException {
        final long token = in.readVLong();
        final int patchBitsRequired = (int) (token & 0x1F);
        BitStreamInput bitIn = new BitStreamInput(in);
        final int numExceptions = bitIn.read(NUM_EXCEPTIONS_BITS);
        BinaryInterpolativeCoding.decodeIncreasing(bitIn, exceptionIndexes, 0, numExceptions - 1, 0, ForUtil.BLOCK_SIZE - 1);
        for (int i = 0; i < numExceptions; ++i) {
            exceptionValues[i] = bitIn.readGammaCode();
        }
        bitIn.done();

        if (patchBitsRequired == 0) {
            Arrays.fill(longs, 0L);
        } else {
            forUtil.decodeTo32(patchBitsRequired, in, longs);
        }
        for (int i = 0; i < numExceptions; ++i) {
            final int index = (int) exceptionIndexes[i];
            // note that we pack two values per long, so the index is [0..63] for 128 values
            final int index32 = index & 0x3F;
            // we need to shift by 1) the bpv, and 2) 32 for positions [0..63] (and no 32 shift for
            // [64..127])
            final int shift32 = patchBitsRequired + ((1 ^ (index >> 6)) << 5);
            ;
            longs[index32] |= exceptionValues[i] << shift32;
        }
        for (int i = 0; i < HALF_BLOCK_SIZE; ++i) {
            longs[i] += (1L << 32) | 1L;
        }
        prefixSum32(longs, base);
    }

    /** Skip 128 integers. */
    void skip(DataInput in) throws IOException {
        final long token = in.readVLong();
        final int patchBitsRequired = (int) (token & 0x1F);
        final long exceptionsLength = token >> 5;
        final long forLength;
        if (patchBitsRequired == 0) {
            forLength = 0;
        } else {
            forLength = forUtil.numBytes(patchBitsRequired);
        }
        in.skipBytes(exceptionsLength + forLength);
    }

    /** Apply prefix sum logic where the values are packed two-per-long in {@code longs}. */
    private static void prefixSum32(long[] longs, long base) {
        longs[0] += base << 32;
        innerPrefixSum32(longs);
        expand32(longs);
        final long l = longs[HALF_BLOCK_SIZE - 1];
        for (int i = HALF_BLOCK_SIZE; i < ForUtil.BLOCK_SIZE; ++i) {
            longs[i] += l;
        }
    }

    /**
     * Expand the values packed two-per-long in {@code longs} into 128 individual long values stored
     * back into {@code longs}.
     */
    private static void expand32(long[] longs) {
        for (int i = 0; i < 64; ++i) {
            final long l = longs[i];
            longs[i] = l >>> 32;
            longs[64 + i] = l & 0xFFFFFFFFL;
        }
    }

    /**
     * Unrolled "inner" prefix sum logic where the values are packed two-per-long in {@code longs}.
     * After this method, the final values will be correct for all high-order bits (values [0..63])
     * but a final prefix loop will still need to run to "correct" the values of [64..127] in the
     * low-order bits, which need the 64th value added to all of them.
     */
    private static void innerPrefixSum32(long[] longs) {
        longs[1] += longs[0];
        longs[2] += longs[1];
        longs[3] += longs[2];
        longs[4] += longs[3];
        longs[5] += longs[4];
        longs[6] += longs[5];
        longs[7] += longs[6];
        longs[8] += longs[7];
        longs[9] += longs[8];
        longs[10] += longs[9];
        longs[11] += longs[10];
        longs[12] += longs[11];
        longs[13] += longs[12];
        longs[14] += longs[13];
        longs[15] += longs[14];
        longs[16] += longs[15];
        longs[17] += longs[16];
        longs[18] += longs[17];
        longs[19] += longs[18];
        longs[20] += longs[19];
        longs[21] += longs[20];
        longs[22] += longs[21];
        longs[23] += longs[22];
        longs[24] += longs[23];
        longs[25] += longs[24];
        longs[26] += longs[25];
        longs[27] += longs[26];
        longs[28] += longs[27];
        longs[29] += longs[28];
        longs[30] += longs[29];
        longs[31] += longs[30];
        longs[32] += longs[31];
        longs[33] += longs[32];
        longs[34] += longs[33];
        longs[35] += longs[34];
        longs[36] += longs[35];
        longs[37] += longs[36];
        longs[38] += longs[37];
        longs[39] += longs[38];
        longs[40] += longs[39];
        longs[41] += longs[40];
        longs[42] += longs[41];
        longs[43] += longs[42];
        longs[44] += longs[43];
        longs[45] += longs[44];
        longs[46] += longs[45];
        longs[47] += longs[46];
        longs[48] += longs[47];
        longs[49] += longs[48];
        longs[50] += longs[49];
        longs[51] += longs[50];
        longs[52] += longs[51];
        longs[53] += longs[52];
        longs[54] += longs[53];
        longs[55] += longs[54];
        longs[56] += longs[55];
        longs[57] += longs[56];
        longs[58] += longs[57];
        longs[59] += longs[58];
        longs[60] += longs[59];
        longs[61] += longs[60];
        longs[62] += longs[61];
        longs[63] += longs[62];
    }
}
