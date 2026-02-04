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

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.LongLongHashTable;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Specialization for LongSwissHash, for LongLong. */
public class LongLongSwissHash extends SwissHash implements LongLongHashTable {

    static final VectorSpecies<Byte> BS = ByteVector.SPECIES_128;

    private static final int BYTE_VECTOR_LANES = BS.vectorByteSize();

    private static final int PAGE_SHIFT = 14;

    private static final int PAGE_MASK = PageCacheRecycler.PAGE_SIZE_IN_BYTES - 1;

    private static final int KEY_SIZE = Long.BYTES + Long.BYTES;

    private static final int ID_HASH_SIZE = Long.BYTES;

    static final int INITIAL_CAPACITY = PageCacheRecycler.PAGE_SIZE_IN_BYTES / KEY_SIZE;

    static {
        if (PageCacheRecycler.PAGE_SIZE_IN_BYTES >> PAGE_SHIFT != 1) {
            throw new AssertionError("bad constants");
        }
        if (Integer.highestOneBit(KEY_SIZE) != KEY_SIZE) {
            throw new AssertionError("not a power of two");
        }
        if (Integer.highestOneBit(ID_HASH_SIZE) != ID_HASH_SIZE) {
            throw new AssertionError("not a power of two");
        }
        if (Integer.highestOneBit(INITIAL_CAPACITY) != INITIAL_CAPACITY) {
            throw new AssertionError("not a power of two");
        }
        if (ID_HASH_SIZE > KEY_SIZE) {
            throw new AssertionError("key too small");
        }
    }

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(LongLongSwissHash.class);

    private static final VarHandle LONG_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.nativeOrder());

    /**
     * Pages of {@code keys}, vended by the {@link PageCacheRecycler}. It's
     * important that the size of keys be a power of two, so we can quickly
     * select the appropriate page and keys never span multiple pages.
     */
    private byte[][] keyPages;
    private long usedBytes = 0;

    private SmallCore smallCore;
    private BigCore bigCore;
    private final List<Releasable> toClose = new ArrayList<>();

    LongLongSwissHash(PageCacheRecycler recycler, CircuitBreaker breaker) {
        super(recycler, breaker, INITIAL_CAPACITY, LongSwissHash.SmallCore.FILL_FACTOR);
        boolean success = false;
        try {
            smallCore = new SmallCore();
            keyPages = new byte[0][];
            growKeyPages(capacity);
            success = true;
        } finally {
            if (success == false) {
                close();
            }
        }
    }

    private static int requiredPages(int capacity, int entrySize) {
        final long requiresBytes = (long) capacity * entrySize;
        return Math.toIntExact(requiresBytes + PageCacheRecycler.BYTE_PAGE_SIZE - 1) >> PAGE_SHIFT;
    }

    void growKeyPages(int newSize) {
        final int requiredPages = requiredPages(newSize, KEY_SIZE);
        final int currentPages = keyPages.length;
        final int extraPages = requiredPages - currentPages;
        breaker.addEstimateBytesAndMaybeBreak((long) extraPages * PageCacheRecycler.PAGE_SIZE_IN_BYTES, "LongLongSwissHash");
        usedBytes += (long) extraPages * PageCacheRecycler.PAGE_SIZE_IN_BYTES;
        keyPages = ArrayUtil.growExact(keyPages, requiredPages);
        for (int i = currentPages; i < keyPages.length; i++) {
            var page = recycler.bytePage(false);
            toClose.add(page);
            keyPages[i] = page.v();
        }
    }

    /**
     * Finds an {@code id} by a {@code key1} and a {@code key2}.
     */
    @Override
    public long find(final long key1, final long key2) {
        final int hash = hash(key1, key2);
        if (smallCore != null) {
            return smallCore.find(key1, key2, hash);
        } else {
            return bigCore.find(key1, key2, hash, control(hash));
        }
    }

    /**
     * Add a {@code key}, returning its {@code id}s. If it was already present
     * it's previous assigned {@code id} will be returned. If it wasn't present
     * it'll be assigned a new {@code id}.
     */
    @Override
    public long add(final long key1, final long key2) {
        final int hash = hash(key1, key2);
        if (smallCore != null) {
            if (size < nextGrowSize) {
                return smallCore.add(key1, key2, hash);
            }
            smallCore.transitionToBigCore();
        }
        return bigCore.add(key1, key2, hash);
    }

    @Override
    public void addBatch(long[] firstKeys, long[] secondKeys, int[] ids, int length) {
        if (smallCore != null) {
            for (int i = 0; i < length; i++) {
                long id = add(firstKeys[i], secondKeys[i]);
                id = id < 0 ? -1 - id : id;
                ids[i] = (int) id;
            }
            return;
        }
        if (size + length >= nextGrowSize) {
            // Juggle constants for the new page size
            growCount++;
            int oldCapacity = capacity;
            while (nextGrowSize <= size + length) {
                capacity <<= 1;
                if (capacity < 0) {
                    throw new IllegalArgumentException("overflow: oldCapacity=" + oldCapacity + ", new capacity=" + capacity);
                }
                nextGrowSize = (int) (capacity * LongSwissHash.BigCore.FILL_FACTOR);
            }
            mask = capacity - 1;
            bigCore.grow();
        }
        bigCore.addBatch(firstKeys, secondKeys, ids, length);
    }

    @Override
    public Status status() {
        return smallCore != null ? smallCore.status() : bigCore.status();
    }

    public abstract class Itr extends SwissHash.Itr {
        /** The first key the iterator current points to. */
        public abstract long key1();

        /** The second key the iterator current points to. */
        public abstract long key2();
    }

    @Override
    public Itr iterator() {
        return smallCore != null ? smallCore.iterator() : bigCore.iterator();
    }

    /**
     * Build the control byte for a populated entry out of the hash.
     * The control bytes for a populated entry has the high bit clear
     * and the remaining 7 bits contain the top 7 bits of the hash.
     * So it looks like {@code 0b0xxx_xxxx}.
     */
    private static byte control(int hash) {
        return (byte) (hash >>> (Integer.SIZE - 7));
    }

    @Override
    public void close() {
        Releasables.close(smallCore, bigCore);
        Releasables.close(toClose);
        toClose.clear();
    }

    private int growTracking() {
        // Juggle constants for the new page size
        growCount++;
        int oldCapacity = capacity;
        capacity <<= 1;
        if (capacity < 0) {
            throw new IllegalArgumentException("overflow: oldCapacity=" + oldCapacity + ", new capacity=" + capacity);
        }
        nextGrowSize = (int) (capacity * LongSwissHash.BigCore.FILL_FACTOR);
        mask = capacity - 1;
        return oldCapacity;
    }

    /**
     * Open addressed hash table with linear probing. Empty {@code id}s are
     * encoded as {@code -1}. This hash table can't grow, and is instead
     * replaced by a {@link LongSwissHash.BigCore}.
     *
     * <p> This uses one page from the {@link PageCacheRecycler} for the
     * {@code ids}.
     */
    final class SmallCore extends Core implements Accountable {
        static final float FILL_FACTOR = 0.6F;
        static final VarHandle INT_HANDLE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.nativeOrder());
        static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(SmallCore.class);

        private final byte[] idPage;

        private SmallCore() {
            boolean success = false;
            try {
                idPage = grabPage();
                Arrays.fill(idPage, (byte) 0xff);
                success = true;
            } finally {
                if (success == false) {
                    close();
                }
            }
        }

        int find(final long key1, final long key2, final int hash) {
            int slot = slot(hash);
            for (;;) {
                int id = id(slot);
                if (id < 0) {
                    return -1; // empty
                }
                final int offset = keyOffset(id);
                if (key1(offset) == key1 && key2(offset) == key2) {
                    return id;
                }
                slot = slot(slot + 1);
            }
        }

        int add(final long key1, final long key2, final int hash) {
            int slot = slot(hash);
            for (;;) {
                final int idOffset = slot * Integer.BYTES;
                final int currentId = (int) INT_HANDLE.get(idPage, idOffset);
                if (currentId >= 0) {
                    final int keyOffset = keyOffset(currentId);
                    if (key1(keyOffset) == key1 && key2(keyOffset) == key2) {
                        return -1 - currentId;
                    }
                    slot = slot(slot + 1);
                } else {
                    int id = size;
                    final int keyOffset = keyOffset(id);
                    INT_HANDLE.set(idPage, idOffset, id);
                    setKeys(keyOffset, key1, key2);
                    size++;
                    return id;
                }
            }
        }

        void transitionToBigCore() {
            growTracking();
            try {
                bigCore = new BigCore();
                rehash();
            } finally {
                close();
                smallCore = null;
            }
            growKeyPages(nextGrowSize + 1);
        }

        @Override
        protected Status status() {
            return new SmallCoreStatus(growCount, capacity, size, nextGrowSize);
        }

        @Override
        protected LongLongSwissHash.Itr iterator() {
            return new Itr() {
                @Override
                public boolean next() {
                    return ++keyId < size;
                }

                @Override
                public int id() {
                    return keyId;
                }

                @Override
                public long key1() {
                    return SmallCore.this.key1(keyOffset(keyId));
                }

                @Override
                public long key2() {
                    return SmallCore.this.key2(keyOffset(keyId));
                }
            };
        }

        private void rehash() {
            for (int i = 0; i < size; i++) {
                final int keyOffset = keyOffset(i);
                final int hash = hash(key1(keyOffset), key2(keyOffset));
                bigCore.insert(hash, control(hash), i);
            }
        }

        private long key1(int offset) {
            return (long) LONG_HANDLE.get(keyPages[0], offset);
        }

        private long key2(int offset) {
            return (long) LONG_HANDLE.get(keyPages[0], offset + Long.BYTES);
        }

        private void setKeys(int offset, long value1, long value2) {
            LONG_HANDLE.set(keyPages[0], offset, value1);
            LONG_HANDLE.set(keyPages[0], offset + Long.BYTES, value2);
        }

        private int keyOffset(final int id) {
            return id * KEY_SIZE;
        }

        private int id(int slot) {
            return (int) INT_HANDLE.get(idPage, slot * Integer.BYTES);
        }

        @Override
        public long ramBytesUsed() {
            return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(idPage);
        }
    }

    final class BigCore extends Core implements Accountable {
        static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(BigCore.class);

        static final float FILL_FACTOR = 0.875F;

        private static final byte EMPTY = (byte) 0x80; // empty slot

        private final byte[] controlData;

        private final byte[][] idPages;

        private int insertProbes;

        BigCore() {
            int controlLength = capacity + BYTE_VECTOR_LANES;
            breaker.addEstimateBytesAndMaybeBreak(controlLength, "LongLongSwissHash-bigCore");
            toClose.add(() -> breaker.addWithoutBreaking(-controlLength));
            controlData = new byte[controlLength];
            Arrays.fill(controlData, EMPTY);

            boolean success = false;
            try {
                int idPagesNeeded = requiredPages(capacity, ID_HASH_SIZE);
                idPages = new byte[idPagesNeeded][];
                for (int i = 0; i < idPagesNeeded; i++) {
                    idPages[i] = grabPage();
                }
                assert idPages[(int) (idOffset(mask) >> PAGE_SHIFT)] != null;
                success = true;
            } finally {
                if (false == success) {
                    close();
                }
            }
        }

        private int find(final long key1, final long key2, final int hash, final byte control) {
            int group = hash & mask;
            for (;;) {
                ByteVector vec = ByteVector.fromArray(BS, controlData, group);
                long matches = vec.eq(control).toLong();
                while (matches != 0) {
                    final int checkSlot = slot(group + Long.numberOfTrailingZeros(matches));
                    final long idAndHash = idAndHash(checkSlot);
                    if ((int) idAndHash == hash) {
                        final int id = id(idAndHash);
                        if (checkKeys(id, key1, key2)) {
                            return id;
                        }
                    }
                    matches &= matches - 1; // clear the first set bit and try again
                }
                long empty = vec.eq(EMPTY).toLong();
                if (empty != 0) {
                    return -1;
                }
                group = slot(group + BYTE_VECTOR_LANES);
            }
        }

        private static final int BATCH_SIZE = 64;
        private final int[] batchHashes = new int[BATCH_SIZE];
        private final int[] pendingKeyIndices = new int[BATCH_SIZE];

        // NEW: Buffers to hold metadata so we don't write to cold RAM in the hot loop
        private final int[] batchInsertSlots = new int[BATCH_SIZE];
        private final long[] batchIdAndHashes = new long[BATCH_SIZE];

        void addBatch(long[] firstKeys, long[] secondKeys, int[] ids, int length) {
            for (int offset = 0; offset < length; offset += BATCH_SIZE) {
                int limit = Math.min(BATCH_SIZE, length - offset);
                processChunk(firstKeys, secondKeys, ids, offset, limit);
            }
        }

        /**
         * The "Phase 5" Loop.
         * Separates L1 Cache operations (Probing) from Cold RAM operations (Writing).
         */
        private void processChunk(long[] firstKeys, long[] secondKeys, int[] ids, int offset, int limit) {
            final int batchStartSize = size;
            int accumulator = 0;

            // --- PHASES 1 & 2: HASH & PREFETCH ---
            for (int i = 0; i < limit; i++) {
                final int hash = hash(firstKeys[offset + i], secondKeys[offset + i]);
                batchHashes[i] = hash;
                // Prefetch Control Byte (L1 Hint)
                accumulator ^= controlData[hash & mask];
            }
            if (accumulator == -1) {
                System.err.println("avoid dead code");
            }

            // --- PHASE 3: PROBE & CLAIM (PURE L1 EXECUTION) ---
            // Goal: Do NOT touch idPages or keyPages here unless absolutely necessary.
            for (int i = 0; i < limit; i++) {
                final long k1 = firstKeys[offset + i];
                final long k2 = secondKeys[offset + i];
                final int hash = batchHashes[i];
                final byte control = control(hash);
                int group = hash & mask;

                search_loop:
                for (;;) {
                    // Vector Load (Hot L1)
                    ByteVector vec = ByteVector.fromArray(BS, controlData, group);

                    // A. Match Check
                    long matches = vec.eq(control).toLong();
                    while (matches != 0) {
                        final int bit = Long.numberOfTrailingZeros(matches);
                        final int checkSlot = slot(group + bit);

                        // COSTLY READ: Only happens on hash collision (rare)
                        // We must read Cold RAM here to verify the ID.
                        final long idAndHash = idAndHash(checkSlot);

                        if ((int) idAndHash == hash) {
                            final int id = (int) (idAndHash >>> 32);
                            boolean isMatch;

                            // Hybrid Check
                            if (id < batchStartSize) {
                                // Old Key (Cold RAM)
                                isMatch = checkKeys(id, k1, k2);
                            } else {
                                // Pending Key (Hot Input Array)
                                int keyIndex = offset + pendingKeyIndices[id - batchStartSize];
                                isMatch = (firstKeys[keyIndex] == k1 && secondKeys[keyIndex] == k2);
                            }

                            if (isMatch) {
                                ids[offset + i] = id; // Duplicate
                                // Mark slot as -1 so Phase 4 knows to skip writing
                                batchInsertSlots[i] = -1;
                                break search_loop;
                            }
                        }
                        matches &= (matches - 1);
                    }

                    // B. Empty Check
                    long empty = vec.eq(EMPTY).toLong();
                    if (empty != 0) {
                        int bit = Long.numberOfTrailingZeros(empty);
                        int insertSlot = slot(group + bit);
                        int id = size;

                        // 1. Update Hot Metadata (Control) immediately so future items in batch see it
                        // This stays in L1/L2 cache.
                        controlData[insertSlot] = control;
                        if (insertSlot < BYTE_VECTOR_LANES) {
                            controlData[insertSlot + capacity] = control;
                        }

                        // 2. Buffer the Cold Data (DEFERRED WRITE)
                        // We DO NOT write to idPages or keyPages yet.
                        batchInsertSlots[i] = insertSlot;
                        batchIdAndHashes[i] = ((long) id << 32) | Integer.toUnsignedLong(hash);

                        // Register pending key for intra-batch collisions
                        pendingKeyIndices[id - batchStartSize] = i;

                        size++;
                        ids[offset + i] = id;
                        break search_loop;
                    }

                    // C. Probe
                    group = (group + BYTE_VECTOR_LANES) & mask;
                }
            }

            // --- PHASE 4: BULK WRITE (SEQUENTIAL / BUFFERED) ---
            // Now we flush the buffered data to Cold RAM.
            // Doing this in a tight loop allows the CPU to use "Write Combining" buffers effectively.

            // We iterate 0..limit to map 'i' to the buffered data
            for (int i = 0; i < limit; i++) {
                final int slot = batchInsertSlots[i];

                // If slot != -1, it was a new insertion
                if (slot != -1) {
                    final long idAndHash = batchIdAndHashes[i];
                    final int id = (int) (idAndHash >>> 32);

                    // 1. Write ID & Hash (Random Access to Paged RAM)
                    long idOffset = (long) slot * ID_HASH_SIZE;
                    LONG_HANDLE.set(
                        idPages[(int) (idOffset >> PAGE_SHIFT)],
                        (int) (idOffset & PAGE_MASK),
                        idAndHash
                    );

                    // 2. Write Keys (Sequential Access to Paged RAM)
                    int absIndex = offset + i;
                    setKeys(id, firstKeys[absIndex], secondKeys[absIndex]);
                }
            }
        }

        private int add(final long key1, final long key2, final int hash) {
            maybeGrow();
            return bigCore.addImpl(key1, key2, hash);
        }

        private int addImpl(final long key1, final long key2, final int hash) {
            final byte control = control(hash);
            int group = hash & mask;
            for (;;) {
                ByteVector vec = ByteVector.fromArray(BS, controlData, group);
                long matches = vec.eq(control).toLong();
                while (matches != 0) {
                    final int checkSlot = slot(group + Long.numberOfTrailingZeros(matches));
                    final long idAndHash = idAndHash(checkSlot);
                    if ((int) idAndHash == hash) {
                        final int id = id(idAndHash);
                        if (checkKeys(id, key1, key2)) {
                            return -1 - id;
                        }
                    }
                    matches &= matches - 1; // clear the first set bit and try again
                }
                long empty = vec.eq(EMPTY).toLong();
                if (empty != 0) {
                    final int insertSlot = slot(group + Long.numberOfTrailingZeros(empty));
                    final int id = size;
                    setKeys(id, key1, key2);
                    final long idAndHash = ((long) id << 32) | Integer.toUnsignedLong(hash);
                    insertAtSlot(insertSlot, control, idAndHash);
                    size++;
                    return id;
                }
                group = (group + BYTE_VECTOR_LANES) & mask;
            }
        }

        private void insertAtSlot(final int insertSlot, final byte control, final long idAndHash) {
            final long idOffset = idOffset(insertSlot);
            LONG_HANDLE.set(idPages[(int) (idOffset >> PAGE_SHIFT)], (int) (idOffset & PAGE_MASK), idAndHash);
            controlData[insertSlot] = control;
            // mirror only if slot is within the first group size, to handle wraparound loads
            if (insertSlot < BYTE_VECTOR_LANES) {
                controlData[insertSlot + capacity] = control;
            }
        }

        @Override
        protected Status status() {
            return new BigCoreStatus(growCount, capacity, size, nextGrowSize, insertProbes, keyPages.length, idPages.length);
        }

        @Override
        protected LongLongSwissHash.Itr iterator() {
            return new LongLongSwissHash.Itr() {
                @Override
                public boolean next() {
                    return ++keyId < size;
                }

                @Override
                public int id() {
                    return keyId;
                }

                @Override
                public long key1() {
                    return BigCore.this.key1(keyOffset(keyId));
                }

                @Override
                public long key2() {
                    return BigCore.this.key2(keyOffset(keyId));
                }
            };
        }

        private void maybeGrow() {
            if (size >= nextGrowSize) {
                assert size == nextGrowSize;
                growTracking();
                grow();
            }
        }

        private void grow() {
            bigCore = null;
            try {
                var newBigCore = new BigCore();
                rehash(newBigCore);
                bigCore = newBigCore;
            } finally {
                close();
            }
            growKeyPages(nextGrowSize +1);
        }

        private void rehash(BigCore newBigCore) {
            for (int i = 0; i < size; i++) {
                final long keyOffset = keyOffset(i);
                final int hash = hash(key1(keyOffset), key2(keyOffset));
                newBigCore.insert(hash, control(hash), i);
            }
        }

        /**
         * Inserts the key into the first empty slot that allows it. Used
         * by {@link #rehash} because we know all keys are unique.
         */
        private void insert(final int hash, final byte control, final int id) {
            int group = hash & mask;
            for (;;) {
                for (int j = 0; j < BYTE_VECTOR_LANES; j++) {
                    int idx = group + j;
                    if (controlData[idx] == EMPTY) {
                        final int insertSlot = slot(group + j);
                        final long idAndHash = ((long) id << 32) | Integer.toUnsignedLong(hash);
                        insertAtSlot(insertSlot, control, idAndHash);
                        return;
                    }
                }
                group = (group + BYTE_VECTOR_LANES) & mask;
                insertProbes++;
            }
        }

        private long key1(final long keyOffset) {
            final int keyPageOffset = (int) (keyOffset >> PAGE_SHIFT);
            final int keyPageMask = (int) (keyOffset & PAGE_MASK);
            return (long) LONG_HANDLE.get(keyPages[keyPageOffset], keyPageMask);
        }

        private long key2(final long keyOffset) {
            final int keyPageOffset = Math.toIntExact(keyOffset >> PAGE_SHIFT);
            final int keyPageMask = Math.toIntExact((keyOffset + Long.BYTES) & PAGE_MASK);
            return (long) LONG_HANDLE.get(keyPages[keyPageOffset], keyPageMask);
        }

        private void setKeys(final int id, final long key1, final long key2) {
            final long keyOffset = keyOffset(id);
            final int pageIndex = (int) (keyOffset >> PAGE_SHIFT);
            final int indexInPage = (int) (keyOffset & PAGE_MASK);
            final byte[] page = keyPages[pageIndex];
            LONG_HANDLE.set(page, indexInPage, key1);
            LONG_HANDLE.set(page, indexInPage + Long.BYTES, key2);
        }

        private boolean checkKeys(int id, long key1, long key2) {
            final long keyOffset = keyOffset(id);
            final int pageIndex = (int) (keyOffset >> PAGE_SHIFT);
            final int indexInPage = (int) (keyOffset & PAGE_MASK);
            byte[] page = keyPages[pageIndex];
            return (long) LONG_HANDLE.get(page, indexInPage) == key1 && (long) LONG_HANDLE.get(page, indexInPage + Long.BYTES) == key2;
        }

        private long idAndHash(final int slot) {
            final long idOffset = idOffset(slot);
            final int pageIndex = (int) (idOffset >> PAGE_SHIFT);
            final int indexInPage = (int) (idOffset & PAGE_MASK);
            return (long) LONG_HANDLE.get(idPages[pageIndex], indexInPage);
        }

        private static int id(long idAndHash) {
            return (int) (idAndHash >>> 32);
        }

        @Override
        public long ramBytesUsed() {
            return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(controlData) + (long) idPages.length
                * PageCacheRecycler.PAGE_SIZE_IN_BYTES;
        }
    }

    @Override
    public long getKey1(final long id) {
        final int actualId = Math.toIntExact(id);
        Objects.checkIndex(actualId, size());
        final long keyOffset = keyOffset(actualId);
        return smallCore != null ? smallCore.key1(Math.toIntExact(keyOffset)) : bigCore.key1(keyOffset);
    }

    @Override
    public long getKey2(final long id) {
        final int actualId = Math.toIntExact(id);
        Objects.checkIndex(actualId, size());
        final long keyOffset = keyOffset(actualId);
        return smallCore != null ? smallCore.key2(Math.toIntExact(keyOffset)) : bigCore.key2(keyOffset);
    }

    private static long keyOffset(final int id) {
        return (long) id * KEY_SIZE;
    }

    private static long idOffset(final int slot) {
        return (long) slot * ID_HASH_SIZE;
    }

    private static int hash(long key1, long key2) {
        return 31 * BitMixer.mix(key1) + BitMixer.mix(key2);
    }

    private int slot(final int hash) {
        return hash & mask;
    }

    @Override
    public long ramBytesUsed() {
        return BASE_RAM_BYTES_USED + (smallCore != null ? smallCore.ramBytesUsed() : bigCore.ramBytesUsed());
    }
}
