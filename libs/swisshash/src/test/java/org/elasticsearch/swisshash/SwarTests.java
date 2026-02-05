/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.swisshash;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorSpecies;

import org.elasticsearch.test.ESTestCase;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

public class SwarTests extends ESTestCase {

    static final VarHandle LONG_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.nativeOrder());
    static final VectorSpecies<Byte> BS = ByteVector.SPECIES_64;
    static byte[] buffer= new byte[BS.vectorByteSize()];

    public void testOneMillionCombinations() {
        int iterations = 100_000_000;
        for (int i = 0; i < iterations; i++) {
            // 1. Generate a random control tag
            byte control = (byte) randomInt(256);

            // 2. Construct a block with a mix of:
            //    - Exact matches
            //    - "Dangerous" neighbors (control + 1)
            //    - Empty markers (0x80)
            //    - Random noise
            long block = 0;
            for (int lane = 0; lane < 8; lane++) {
                long laneValue;
                int dice = randomInt(9);

                if (dice < 2) laneValue = control & 0xFF;        // 20% True Match
                else if (dice < 4) laneValue = (control + 1) & 0xFF; // 20% Potential Borrow Trap
                else if (dice < 6) laneValue = 0x80;            // 20% Empty Marker
                else laneValue = randomInt(256);           // 40% Random Noise

                block |= (laneValue << (lane * 8));
            }

            // 3. Run the Truth (Vector) vs the Challenger (SWAR)
            assertSwarMatchesVector(block, control);
        }
        System.out.println("Passed 1,000,000 combinations!");
    }

    private void assertSwarMatchesVector(long block, byte control) {
        // SWAR Result
        long cleanSar = Swar.matchClean(block, control);
        // Vector Result (The Gold Standard)
        // Assuming BS is your ByteVector species
        LONG_HANDLE.set(buffer, 0, block);
        long fromVector = ByteVector.fromArray(BS, buffer, 0).eq(control).toLong();

        // Convert SAR Heavy Mask (0x80) to Light Mask (1-bit)
        long sarLightMask = 0;
        long temp = cleanSar;
        while (temp != 0) {
            int bitPos = Long.numberOfTrailingZeros(temp);
            // Correcting for your find: bit 24/31 -> lane 3
            int lane = bitPos >>> 3;
            sarLightMask |= (1L << lane);
            temp &= (temp - 1);
        }

        if (sarLightMask != fromVector) {
            throw new AssertionError(String.format(
                "\nFAILED COMBINATION" +
                    "\nBlock:   0x%016X" +
                    "\nControl: 0x%02X" +
                    "\nVector:  %s" +
                    "\nSWAR:    %s",
                block, control,
                Long.toBinaryString(fromVector),
                Long.toBinaryString(sarLightMask)
            ));
        }
    }

}
