/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.swisshash;

final class Swar {

    /**
     * Finds the first empty byte (0x80) in the block
     */
    static long findEmpty(final long block) {
        return block & 0x8080808080808080L;
    }

    public static long findMatches(final long block, final byte control) {
        final long pattern = (control & 0xFFL) * 0x0101010101010101L;
        final long xor = block ^ pattern;

        // Using the 0x7F trick to stop borrows from crossing lanes
        // This is the "Clean" signal you need for the ADD path
        final long v = (xor & 0x7F7F7F7F7F7F7F7FL) + 0x7F7F7F7F7F7F7F7FL;
        return ~(xor | v) & 0x8080808080808080L;
    }

    /*
    public static long match(final long block, final byte control) {
    // 1. Broadcast pattern
    final long pattern = (control & 0xFFL) * 0x0101010101010101L;

    // 2. XOR to find zero-diff lanes
    final long xor = block ^ pattern;

    // 3. The 4-Op Zero-Detector (High-Bit Safe)
    // - Guaranteed no false negatives (will never miss a 0x00)
    // - Possible ghost matches (borrows from 0x01 neighbors)
    return (xor - 0x0101010101010101L) & (~xor & 0x8080808080808080L);
}
     */
}
