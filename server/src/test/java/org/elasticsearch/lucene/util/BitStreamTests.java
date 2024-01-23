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
import org.elasticsearch.test.ESTestCase;

import java.util.Random;

public class BitStreamTests extends ESTestCase {

    private static long numBitsWritten(ByteBuffersDataOutput outBytes, BitStreamOutput out) {
        return outBytes.size() * Byte.SIZE + out.numBufferedBits;
    }

    public void testSimpleRead() throws Exception {
        ByteBuffersDataOutput outBytes = new ByteBuffersDataOutput();
        BitStreamOutput out = new BitStreamOutput(outBytes);
        out.write(42, 16);
        out.write(65535, 16);
        out.write(0, 0);
        out.write(1, 7);
        out.write(2, 7);
        out.write(3, 7);
        out.write(4, 7);
        out.write(5, 7);
        out.write(6, 7);
        out.flush();
        outBytes.writeBytes(new byte[BitStreamInput.PADDING_BYTES]);

        BitStreamInput in = new BitStreamInput(new ByteBuffersIndexInput(outBytes.toDataInput(), "test input"));
        assertEquals(42, in.read(16));
        assertEquals(65535, in.read(16));
        assertEquals(0, in.read(0));
        assertEquals(1, in.read(7));
        assertEquals(2, in.read(7));
        assertEquals(3, in.read(7));
        assertEquals(4, in.read(7));
        assertEquals(5, in.read(7));
        assertEquals(6, in.read(7));
    }

    public void testSimpleReadLeftmostMinimalCode() throws Exception {
        ByteBuffersDataOutput outBytes = new ByteBuffersDataOutput();
        BitStreamOutput out = new BitStreamOutput(outBytes);
        out.writeLeftmostMinimalCode(0, 0);
        assertEquals(0, numBitsWritten(outBytes, out));
        // 13 is 2^4-3 so 2 values (0 and 1) can be encoded on 3 bits, and other values require 4 bits.
        out.writeLeftmostMinimalCode(0, 13);
        assertEquals(3, numBitsWritten(outBytes, out));
        out.writeLeftmostMinimalCode(1, 13);
        assertEquals(6, numBitsWritten(outBytes, out));
        out.writeLeftmostMinimalCode(2, 13);
        assertEquals(10, numBitsWritten(outBytes, out));
        out.writeLeftmostMinimalCode(12, 13);
        assertEquals(14, numBitsWritten(outBytes, out));
        out.writeLeftmostMinimalCode(13, 13);
        assertEquals(18, numBitsWritten(outBytes, out));
        out.flush();
        outBytes.writeBytes(new byte[BitStreamInput.PADDING_BYTES]);

        BitStreamInput in = new BitStreamInput(new ByteBuffersIndexInput(outBytes.toDataInput(), "test input"));
        assertEquals(0, in.readLeftmostMinimalCode(0));
        assertEquals(0, in.readLeftmostMinimalCode(13));
        assertEquals(1, in.readLeftmostMinimalCode(13));
        assertEquals(2, in.readLeftmostMinimalCode(13));
        assertEquals(12, in.readLeftmostMinimalCode(13));
        assertEquals(13, in.readLeftmostMinimalCode(13));
    }

    public void testSimpleReadCenteredMinimalCode() throws Exception {
        ByteBuffersDataOutput outBytes = new ByteBuffersDataOutput();
        BitStreamOutput out = new BitStreamOutput(outBytes);
        out.writeCenteredMinimalCode(0, 0);
        assertEquals(0, numBitsWritten(outBytes, out));
        // 13 is 2^4-3 so 2 values (6 and 3) can be encoded on 3 bits, and other values require 4 bits.
        out.writeCenteredMinimalCode(5, 13);
        assertEquals(4, numBitsWritten(outBytes, out));
        out.writeCenteredMinimalCode(6, 13);
        assertEquals(7, numBitsWritten(outBytes, out));
        out.writeCenteredMinimalCode(7, 13);
        assertEquals(10, numBitsWritten(outBytes, out));
        out.writeCenteredMinimalCode(8, 13);
        assertEquals(14, numBitsWritten(outBytes, out));
        out.flush();
        outBytes.writeBytes(new byte[BitStreamInput.PADDING_BYTES]);

        BitStreamInput in = new BitStreamInput(new ByteBuffersIndexInput(outBytes.toDataInput(), "test input"));
        assertEquals(0, in.readCenteredMinimalCode(0));
        assertEquals(5, in.readCenteredMinimalCode(13));
        assertEquals(6, in.readCenteredMinimalCode(13));
        assertEquals(7, in.readCenteredMinimalCode(13));
        assertEquals(8, in.readCenteredMinimalCode(13));
    }

    public void testSimpleReadGammaCode() throws Exception {
        ByteBuffersDataOutput outBytes = new ByteBuffersDataOutput();
        BitStreamOutput out = new BitStreamOutput(outBytes);
        out.writeGammaCode(1);
        out.writeGammaCode(Integer.MAX_VALUE);
        out.writeGammaCode(42);
        out.flush();
        outBytes.writeBytes(new byte[BitStreamInput.PADDING_BYTES]);

        BitStreamInput in = new BitStreamInput(new ByteBuffersIndexInput(outBytes.toDataInput(), "test input"));
        assertEquals(1, in.readGammaCode());
        assertEquals(Integer.MAX_VALUE, in.readGammaCode());
        assertEquals(42, in.readGammaCode());
    }

    public void testSimpleReadDeltaCode() throws Exception {
        ByteBuffersDataOutput outBytes = new ByteBuffersDataOutput();
        BitStreamOutput out = new BitStreamOutput(outBytes);
        out.writeDeltaCode(1);
        out.writeDeltaCode(Integer.MAX_VALUE);
        out.writeDeltaCode(42);
        out.flush();
        outBytes.writeBytes(new byte[BitStreamInput.PADDING_BYTES]);

        BitStreamInput in = new BitStreamInput(new ByteBuffersIndexInput(outBytes.toDataInput(), "test input"));
        assertEquals(1, in.readDeltaCode());
        assertEquals(Integer.MAX_VALUE, in.readDeltaCode());
        assertEquals(42, in.readDeltaCode());
    }

    public void testRandomRead() throws Exception {
        final int numValues = atLeast(1_000);
        final long seed = random().nextLong();

        Random r = new Random(seed);
        ByteBuffersDataOutput outBytes = new ByteBuffersDataOutput();
        BitStreamOutput out = new BitStreamOutput(outBytes);
        for (int i = 0; i < numValues; ++i) {
            final int numBits = r.nextInt(Integer.SIZE);
            final int v = randomValue(r, numBits);
            out.write(v, numBits);
        }
        out.flush();
        outBytes.writeBytes(new byte[BitStreamInput.PADDING_BYTES]);

        BitStreamInput in = new BitStreamInput(new ByteBuffersIndexInput(outBytes.toDataInput(), "test input"));
        r = new Random(seed);
        for (int i = 0; i < numValues; ++i) {
            final int numBits = r.nextInt(Integer.SIZE);
            final int expected = randomValue(r, numBits);
            assertEquals(expected, in.read(numBits));
        }
    }

    private static int randomValue(Random r, int numBits) {
        final int mask = (1 << numBits) - 1;
        return switch (r.nextInt(10)) {
            case 0 -> 0;
            case 1 -> mask;
            default -> r.nextInt() & mask;
        };
    }

    public void testRandomReadLeftmostMinimalCode() throws Exception {
        final int numValues = atLeast(1_000);
        final long seed = random().nextLong();

        Random r = new Random(seed);
        ByteBuffersDataOutput outBytes = new ByteBuffersDataOutput();
        BitStreamOutput out = new BitStreamOutput(outBytes);
        for (int i = 0; i < numValues; ++i) {
            final int max = r.nextInt(1000);
            final int v = r.nextInt(max + 1);
            out.writeLeftmostMinimalCode(v, max);
        }
        out.flush();
        outBytes.writeBytes(new byte[BitStreamInput.PADDING_BYTES]);

        BitStreamInput in = new BitStreamInput(new ByteBuffersIndexInput(outBytes.toDataInput(), "test input"));
        r = new Random(seed);
        for (int i = 0; i < numValues; ++i) {
            final int max = r.nextInt(1000);
            final int expected = r.nextInt(max + 1);
            assertEquals(expected, in.readLeftmostMinimalCode(max));
        }
    }

    public void testRandomReadCenteredMinimalCode() throws Exception {
        final int numValues = atLeast(1_000);
        final long seed = random().nextLong();

        Random r = new Random(seed);
        ByteBuffersDataOutput outBytes = new ByteBuffersDataOutput();
        BitStreamOutput out = new BitStreamOutput(outBytes);
        for (int i = 0; i < numValues; ++i) {
            final int max = r.nextInt(1000);
            final int v = r.nextInt(max + 1);
            out.writeCenteredMinimalCode(v, max);
        }
        out.flush();
        outBytes.writeBytes(new byte[BitStreamInput.PADDING_BYTES]);

        BitStreamInput in = new BitStreamInput(new ByteBuffersIndexInput(outBytes.toDataInput(), "test input"));
        r = new Random(seed);
        for (int i = 0; i < numValues; ++i) {
            final int max = r.nextInt(1000);
            final int expected = r.nextInt(max + 1);
            assertEquals(expected, in.readCenteredMinimalCode(max));
        }
    }

    public void testRandomReadGammaCode() throws Exception {
        final int numValues = atLeast(1_000);
        final long seed = random().nextLong();

        Random r = new Random(seed);
        ByteBuffersDataOutput outBytes = new ByteBuffersDataOutput();
        BitStreamOutput out = new BitStreamOutput(outBytes);
        for (int i = 0; i < numValues; ++i) {
            final int numBits = r.nextInt(20);
            final int v = 1 + randomValue(r, numBits);
            out.writeGammaCode(v);
        }
        out.flush();
        outBytes.writeBytes(new byte[BitStreamInput.PADDING_BYTES]);

        BitStreamInput in = new BitStreamInput(new ByteBuffersIndexInput(outBytes.toDataInput(), "test input"));
        r = new Random(seed);
        for (int i = 0; i < numValues; ++i) {
            final int numBits = r.nextInt(20);
            final int expected = 1 + randomValue(r, numBits);
            assertEquals(expected, in.readGammaCode());
        }
    }

    public void testRandomReadDeltaCode() throws Exception {
        final int numValues = atLeast(1_000);
        final long seed = random().nextLong();

        Random r = new Random(seed);
        ByteBuffersDataOutput outBytes = new ByteBuffersDataOutput();
        BitStreamOutput out = new BitStreamOutput(outBytes);
        for (int i = 0; i < numValues; ++i) {
            final int numBits = r.nextInt(20);
            final int v = 1 + randomValue(r, numBits);
            out.writeDeltaCode(v);
        }
        out.flush();
        outBytes.writeBytes(new byte[BitStreamInput.PADDING_BYTES]);

        BitStreamInput in = new BitStreamInput(new ByteBuffersIndexInput(outBytes.toDataInput(), "test input"));
        r = new Random(seed);
        for (int i = 0; i < numValues; ++i) {
            final int numBits = r.nextInt(20);
            final int expected = 1 + randomValue(r, numBits);
            assertEquals(expected, in.readDeltaCode());
        }
    }
}
