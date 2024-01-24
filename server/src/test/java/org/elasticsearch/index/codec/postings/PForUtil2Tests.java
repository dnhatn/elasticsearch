/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.codec.postings;

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;

import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;

public class PForUtil2Tests extends ESTestCase {

    public void testEncodeDecode() throws IOException {
        final int iterations = RandomNumbers.randomIntBetween(random(), 50, 1000);
        final int[] values = createTestData(iterations, 31);

        final Directory d = new ByteBuffersDirectory();
        final long endPointer = encodeTestData(iterations, values, d);

        IndexInput in = d.openInput("test.bin", IOContext.READONCE);
        final PForUtil2 pforUtil = new PForUtil2(new ForUtil());
        for (int i = 0; i < iterations; ++i) {
            if (random().nextInt(5) == 0) {
                pforUtil.skip(in);
                continue;
            }
            final long[] restored = new long[ForUtil.BLOCK_SIZE];
            pforUtil.decode(in, restored);
            int[] ints = new int[ForUtil.BLOCK_SIZE];
            for (int j = 0; j < ForUtil.BLOCK_SIZE; ++j) {
                ints[j] = Math.toIntExact(restored[j]);
            }
            assertArrayEquals(
                Arrays.toString(ints),
                ArrayUtil.copyOfSubArray(values, i * ForUtil.BLOCK_SIZE, (i + 1) * ForUtil.BLOCK_SIZE),
                ints
            );
        }
        assertEquals(endPointer, in.getFilePointer());
        in.close();

        d.close();
    }

    public void testDeltaEncodeDecode() throws IOException {
        final int iterations = RandomNumbers.randomIntBetween(random(), 50, 1000);
        // cap at 31 - 7 bpv to ensure we don't overflow when working with deltas (i.e., 128 24 bit
        // values treated as deltas will result in a final value that can fit in 31 bits)
        final int[] values = createTestData(iterations, 7);// 31 - 7);

        final Directory d = new ByteBuffersDirectory();
        final long endPointer = encodeTestData(iterations, values, d);

        IndexInput in = d.openInput("test.bin", IOContext.READONCE);
        final PForUtil2 pForUtil = new PForUtil2(new ForUtil());
        for (int i = 0; i < iterations; ++i) {
            if (random().nextInt(5) == 0) {
                pForUtil.skip(in);
                continue;
            }
            long base = 0;
            final long[] restored = new long[ForUtil.BLOCK_SIZE];
            pForUtil.decodeAndPrefixSum(in, base, restored);
            final long[] expected = new long[ForUtil.BLOCK_SIZE];
            for (int j = 0; j < ForUtil.BLOCK_SIZE; ++j) {
                expected[j] = values[i * ForUtil.BLOCK_SIZE + j];
                if (j > 0) {
                    expected[j] += expected[j - 1];
                } else {
                    expected[j] += base;
                }
            }
            assertArrayEquals(Arrays.toString(restored), expected, restored);
        }
        assertEquals(endPointer, in.getFilePointer());
        in.close();

        d.close();
    }

    private int[] createTestData(int iterations, int maxBpv) {
        final int[] values = new int[iterations * ForUtil.BLOCK_SIZE];

        for (int i = 0; i < iterations; ++i) {
            final int bpv = TestUtil.nextInt(random(), 1, maxBpv);
            for (int j = 0; j < ForUtil.BLOCK_SIZE; ++j) {
                values[i * ForUtil.BLOCK_SIZE + j] = RandomNumbers.randomIntBetween(random(), 1, (int) PackedInts.maxValue(bpv));
                if (random().nextInt(100) == 0) {
                    final int exceptionBpv;
                    if (random().nextInt(10) == 0) {
                        exceptionBpv = Math.min(bpv + TestUtil.nextInt(random(), 9, 16), maxBpv);
                    } else {
                        exceptionBpv = Math.min(bpv + TestUtil.nextInt(random(), 1, 8), maxBpv);
                    }
                    values[i * ForUtil.BLOCK_SIZE + j] |= random().nextInt(1 << (exceptionBpv - bpv)) << bpv;
                }
            }
        }

        return values;
    }

    private long encodeTestData(int iterations, int[] values, Directory d) throws IOException {
        IndexOutput out = d.createOutput("test.bin", IOContext.DEFAULT);
        final PForUtil2 pforUtil = new PForUtil2(new ForUtil());

        for (int i = 0; i < iterations; ++i) {
            long[] source = new long[ForUtil.BLOCK_SIZE];
            for (int j = 0; j < ForUtil.BLOCK_SIZE; ++j) {
                source[j] = values[i * ForUtil.BLOCK_SIZE + j] - 1;
            }
            pforUtil.encode(source, out, new ByteBuffersDataOutput());
        }
        final long endPointer = out.getFilePointer();
        out.close();

        return endPointer;
    }

}
