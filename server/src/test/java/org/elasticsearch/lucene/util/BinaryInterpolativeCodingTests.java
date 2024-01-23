/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.lucene.util;

import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexInput;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;

public class BinaryInterpolativeCodingTests extends ESTestCase {

    public void testRandomNonDecreasing() throws Exception {
        final int iters = atLeast(10);
        for (int iter = 0; iter < iters; ++iter) {
            final int numValues = TestUtil.nextInt(random(), 1, 200);
            long[] values = new long[numValues];
            final int numBits = TestUtil.nextInt(random(), 4, 20);
            for (int i = 0; i < numValues; ++i) {
                values[i] = random().nextInt(1 << numBits);
            }
            Arrays.sort(values);

            ByteBuffersDataOutput outBytes = new ByteBuffersDataOutput();
            BitStreamOutput out = new BitStreamOutput(outBytes);
            BinaryInterpolativeCoding.encodeNonDecreasing(values, 0, numValues - 1, 0, (1 << numBits) - 1, out);
            out.flush();
            outBytes.writeBytes(new byte[BitStreamInput.PADDING_BYTES]);

            BitStreamInput in = new BitStreamInput(new ByteBuffersIndexInput(outBytes.toDataInput(), "test input"));
            long[] decoded = new long[numValues];
            BinaryInterpolativeCoding.decodeNonDecreasing(in, decoded, 0, numValues - 1, 0, (1 << numBits) - 1);

            assertArrayEquals(values, decoded);
        }
    }

    public void testRandomIncreasing() throws Exception {
        final int iters = atLeast(10);
        for (int iter = 0; iter < iters; ++iter) {
            final int numValues = TestUtil.nextInt(random(), 1, 200);
            long[] values = new long[numValues];
            for (int i = 0; i < numValues; ++i) {
                final int numBits = TestUtil.nextInt(random(), 2, 10);
                values[i] = randomIntBetween(1, (1 << numBits) - 1);
                if (i != 0) {
                    values[i] += values[i - 1];
                }
            }
            final long maxInclusive = values[numValues - 1] + randomInt(10);
            Arrays.sort(values);

            ByteBuffersDataOutput outBytes = new ByteBuffersDataOutput();
            BitStreamOutput out = new BitStreamOutput(outBytes);
            BinaryInterpolativeCoding.encodeNonDecreasing(values, 0, numValues - 1, 0, maxInclusive, out);
            out.flush();
            outBytes.writeBytes(new byte[BitStreamInput.PADDING_BYTES]);

            BitStreamInput in = new BitStreamInput(new ByteBuffersIndexInput(outBytes.toDataInput(), "test input"));
            long[] decoded = new long[numValues];
            BinaryInterpolativeCoding.decodeNonDecreasing(in, decoded, 0, numValues - 1, 0, maxInclusive);

            assertArrayEquals(values, decoded);
        }
    }
}
