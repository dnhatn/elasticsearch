/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.swisshash;

import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.PartitionedHashTable;
import org.elasticsearch.core.Releasable;

import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * Partitions a {@link BytesRefSwissHash} dictionary together with a {@link LongLongSwissHash} whose key2 carries a dictionary id in
 * its low 32 bits. Ids are private to one table, so pairs cannot be partitioned by hashing their keys; instead the dictionary is split
 * first and each pair follows its id. Routing by the bytes alone would put every pair of one dominant value into one partition, so the
 * partition index mixes both keys: the high {@link #DICT_BITS} come from the dictionary partition, the low bits from the long. The
 * dictionary is therefore split into only {@link #DICT_PARTITIONS} slices, one shared by the {@code 2^SUB_BITS} pair partitions that
 * reference it, and a value is sealed exactly once. A bitset per pair partition records which entries of its slice it references, so
 * the combine merges only those, and a reference count releases the slice when its last pair partition is done.
 * <p>
 * Key2 layout: the id widened with {@link #WIDEN}, plus {@link #LONG_NULL_MASK} when the long key is missing and
 * {@link #INT_NULL_MASK} when the bytes are missing. Keys with {@link #INT_NULL_MASK} carry no id and use dictionary partition 0;
 * bits above the id are preserved. Pairs store the id's position within its slice in place of the id.
 */
public final class BytesLongPartitionedHash implements PartitionedHashTable, Releasable {
    /*
     * longValue, intValue  -> longValue, intValue & WIDEN
     * null, intValue       -> 0, intValue & WIDEN | LONG_NULL_MASK
     * longValue, null      -> longValue, INT_NULL_MASK
     * null, null           -> 0, LONG_NULL_MASK | INT_NULL_MASK
     */
    public static final long LONG_NULL_MASK = 0x00F0_0000_0000_0000L;
    public static final long INT_NULL_MASK = 0x000F_0000_0000_0000L;
    public static final long WIDEN = 0xFFFF_FFFFL;

    /** How the 8 partition bits are shared: a dominant bytes value spreads over {@code 2^SUB_BITS} partitions. */
    static final int DICT_BITS = 4;
    static final int DICT_PARTITIONS = 1 << DICT_BITS;
    static final int SUB_BITS = Integer.numberOfTrailingZeros(NUM_PARTITIONS) - DICT_BITS;
    static final int SUB_MASK = (1 << SUB_BITS) - 1;

    static {
        if (DICT_PARTITIONS << SUB_BITS != NUM_PARTITIONS) {
            throw new AssertionError("partition bits must split between dictionary and long");
        }
    }

    private static final PartitionSplitter NO_SPLITTER = new PartitionSplitter() {
        @Override
        public void split(int firstId, short[] shiftedIds, int batchSize, int[] batchPartitionCounts, int[] partitionOffsets) {}

        @Override
        public void release(CircuitBreaker breaker) {}
    };

    final BytesRefSwissHash bytesHash;
    final LongLongSwissHash longlongHash;
    private int[] bytesIds = new int[0];

    public BytesLongPartitionedHash(BytesRefSwissHash bytesHash, LongLongSwissHash longlongHash) {
        this.bytesHash = bytesHash;
        this.longlongHash = longlongHash;
    }

    static int dictionaryPartitionOf(int pairPartition) {
        return pairPartition >>> SUB_BITS;
    }

    @Override
    public PartitionedHashKeys splitPartition(CircuitBreaker breaker, PartitionSplitter partitionSplitter) {
        final int numIds = bytesHash.size;
        final long routesBytes = (long) numIds * (Byte.BYTES + Integer.BYTES);
        breaker.addEstimateBytesAndMaybeBreak(routesBytes, "BytesLongPartitionedHash#split");
        PartitionedHashKeys bytesKeys = null;
        LongLongSwissHash.LongLongPartitionedHashKeys longKeys = null;
        try {
            final byte[] dictionaryPartitions = new byte[numIds];
            bytesKeys = bytesHash.splitPartition(breaker, DICT_PARTITIONS, dictionaryPartitions, NO_SPLITTER);
            longKeys = longlongHash.splitPartition(breaker, new Routing(dictionaryPartitions), partitionSplitter);
            final long[][] referenced = mapToLocalOrds(breaker, bytesKeys, longKeys, dictionaryPartitions);
            final BytesAndLongKeys combinedKeys = new BytesAndLongKeys(bytesKeys, longKeys, referenced);
            bytesKeys = null;
            longKeys = null;
            return combinedKeys;
        } finally {
            breaker.addWithoutBreaking(-routesBytes);
            if (longKeys != null) {
                longKeys.releaseAll(breaker);
            }
            if (bytesKeys != null) {
                bytesKeys.releaseAll(breaker);
            }
        }
    }

    /** Dictionary partition in the high bits so the pair partitions sharing a slice are claimed consecutively and release it early. */
    private record Routing(byte[] dictionaryPartitions) implements LongLongSwissHash.Partitioner {
        @Override
        public int partition(long key1, long key2) {
            final int dictionaryPartition = (key2 & INT_NULL_MASK) != 0 ? 0 : dictionaryPartitions[(int) key2] & 0xFF;
            return dictionaryPartition << SUB_BITS | BitMixer.mix(key1) & SUB_MASK;
        }
    }

    /**
     * Rewrites every pair's dictionary id into the id's position within its dictionary slice, which is the number of lower ids in the
     * same slice since the dictionary split appends ids in order, and records per pair partition which slice positions it references.
     */
    private static long[][] mapToLocalOrds(
        CircuitBreaker breaker,
        PartitionedHashKeys bytesKeys,
        LongLongSwissHash.LongLongPartitionedHashKeys longKeys,
        byte[] dictionaryPartitions
    ) {
        final int[] positions = new int[dictionaryPartitions.length];
        final int[] nextPosition = new int[DICT_PARTITIONS];
        for (int id = 0; id < positions.length; id++) {
            positions[id] = nextPosition[dictionaryPartitions[id] & 0xFF]++;
        }
        long bitsetBytes = 0;
        for (int p = 0; p < NUM_PARTITIONS; p++) {
            if (longKeys.keysInPartition(p) > 0) {
                bitsetBytes += (long) bitsetWords(bytesKeys.keysInPartition(dictionaryPartitionOf(p))) * Long.BYTES;
            }
        }
        breaker.addEstimateBytesAndMaybeBreak(bitsetBytes, "BytesLongPartitionedHash#referenced");
        final long[][] referenced = new long[NUM_PARTITIONS][];
        for (int p = 0; p < NUM_PARTITIONS; p++) {
            final int numKeys = longKeys.keysInPartition(p);
            if (numKeys > 0) {
                referenced[p] = new long[bitsetWords(bytesKeys.keysInPartition(dictionaryPartitionOf(p)))];
                mapOnePartitionKeys(longKeys.partitionKeys[p], numKeys, positions, referenced[p]);
            }
        }
        return referenced;
    }

    private static int bitsetWords(int bits) {
        return (bits + Long.SIZE - 1) >>> 6;
    }

    /** Replaces the id bits of every key2 with {@code newOrds[id]}; when {@code referenced} is given, marks each new value in it. */
    private static void mapOnePartitionKeys(long[] keys, int numKeys, int[] newOrds, long[] referenced) {
        final int end = numKeys * 2;
        for (int idx = 1; idx < end; idx += 2) {
            final long key2 = keys[idx];
            if ((key2 & INT_NULL_MASK) == 0) {
                final int newOrd = newOrds[(int) key2];
                keys[idx] = (key2 & ~WIDEN) | newOrd;
                if (referenced != null) {
                    referenced[newOrd >>> 6] |= 1L << newOrd;
                }
            }
        }
    }

    /**
     * The sealed keys: {@link #DICT_PARTITIONS} dictionary slices, {@link #NUM_PARTITIONS} pair slices, and per pair slice the bitset of
     * dictionary positions it references. A dictionary slice is released once every pair partition sharing it has been released.
     */
    static final class BytesAndLongKeys implements PartitionedHashKeys {
        final PartitionedHashKeys bytesKeys;
        final LongLongSwissHash.LongLongPartitionedHashKeys longKeys;
        final long[][] referenced;
        private final AtomicIntegerArray dictionaryRefs = new AtomicIntegerArray(DICT_PARTITIONS);

        BytesAndLongKeys(PartitionedHashKeys bytesKeys, LongLongSwissHash.LongLongPartitionedHashKeys longKeys, long[][] referenced) {
            this.bytesKeys = bytesKeys;
            this.longKeys = longKeys;
            this.referenced = referenced;
            for (int d = 0; d < DICT_PARTITIONS; d++) {
                dictionaryRefs.set(d, 1 << SUB_BITS);
            }
        }

        @Override
        public int keysInPartition(int partition) {
            return longKeys.keysInPartition(partition);
        }

        @Override
        public void releasePartition(CircuitBreaker breaker, int partition) {
            longKeys.releasePartition(breaker, partition);
            releaseReferenced(breaker, partition);
            final int dictionaryPartition = dictionaryPartitionOf(partition);
            if (dictionaryRefs.decrementAndGet(dictionaryPartition) == 0) {
                bytesKeys.releasePartition(breaker, dictionaryPartition);
            }
        }

        @Override
        public void releaseAll(CircuitBreaker breaker) {
            for (int p = 0; p < NUM_PARTITIONS; p++) {
                releaseReferenced(breaker, p);
            }
            longKeys.releaseAll(breaker);
            bytesKeys.releaseAll(breaker);
        }

        private void releaseReferenced(CircuitBreaker breaker, int partition) {
            final long[] bits = referenced[partition];
            if (bits != null) {
                referenced[partition] = null;
                breaker.addWithoutBreaking(-(long) bits.length * Long.BYTES);
            }
        }
    }

    @Override
    public boolean combinePartition(PartitionedHashKeys partitioned, int partitionIndex, int[] resultIds) {
        final BytesAndLongKeys combined = (BytesAndLongKeys) partitioned;
        final int dictionaryPartition = dictionaryPartitionOf(partitionIndex);
        final int sliceSize = combined.bytesKeys.keysInPartition(dictionaryPartition);
        ensureBytesIds(sliceSize);
        // Only the slice entries this pair partition references enter the dictionary; the slice itself stays intact for the other
        // pair partitions sharing it.
        bytesHash.combinePartition(combined.bytesKeys, dictionaryPartition, bytesIds, combined.referenced[partitionIndex]);
        final var longKeys = combined.longKeys;
        mapOnePartitionKeys(longKeys.partitionKeys[partitionIndex], longKeys.keysInPartition(partitionIndex), bytesIds, null);
        return longlongHash.combinePartition(combined.longKeys, partitionIndex, resultIds);
    }

    private void ensureBytesIds(int sliceSize) {
        if (bytesIds.length >= sliceSize) {
            return;
        }
        final int newLength = ArrayUtil.oversize(sliceSize, Integer.BYTES);
        longlongHash.breaker.addEstimateBytesAndMaybeBreak(
            (long) (newLength - bytesIds.length) * Integer.BYTES,
            "BytesLongPartitionedHash"
        );
        bytesIds = new int[newLength];
    }

    @Override
    public void close() {
        longlongHash.breaker.addWithoutBreaking(-(long) bytesIds.length * Integer.BYTES);
        bytesIds = new int[0];
    }
}
