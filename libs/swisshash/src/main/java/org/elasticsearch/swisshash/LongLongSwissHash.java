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
        throw new UnsupportedOperationException();
    }

    /**
     * Add a {@code key}, returning its {@code id}s. If it was already present
     * it's previous assigned {@code id} will be returned. If it wasn't present
     * it'll be assigned a new {@code id}.
     */
    @Override
    public long add(final long key1, final long key2) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addBatch(long[] firstKeys, long[] secondKeys, int[] ids, int length) {
        if (smallCore != null) {
            smallCore.transitionToBigCore();
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
        bigCore.batchAdd(firstKeys, secondKeys, ids, length);
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
    private static byte control(long hash) {
        return (byte) (hash >>> (Long.SIZE - 7));
    }

    @Override
    public void close() {
        breaker.addWithoutBreaking(-usedBytes);
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

        private final long[] controlData;

        private final byte[][] idPages;

        private int insertProbes;
        private final int CONTROL_HASH;

        BigCore() {
            int controlLength = (capacity >>> 3) + 1;
            CONTROL_HASH = mask >>> 3;
            breaker.addEstimateBytesAndMaybeBreak(controlLength, "LongLongSwissHash-bigCore");
            toClose.add(() -> breaker.addWithoutBreaking(-controlLength));
            controlData = new long[controlLength];
            Arrays.fill(controlData, 0x8080808080808080L); // fill with EMPTY

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

        final int CHUNK_LIMIT = 256;
        final long[] batchHash64s = new long[CHUNK_LIMIT];

        private void batchAdd(long[] key1s, long[] key2s, int[] batchIds, int length) {
            // Ensure the global result array can hold all IDs
            int offset = 0;
            int keysAddedAtStart = size;

            // Process in chunks that fit the internal BatchWork buffers

            while (offset < length) {
                final int batchSize = Math.min(length - offset, CHUNK_LIMIT);
                int nextId = size;

                // PHASE 1: Hash & Prefetch (Relative Indexing)
                long dummy = 0;
                for (int i = 0; i < batchSize; i++) {
                    int absIdx = offset + i;
                    long h64 = hash64(key1s[absIdx], key2s[absIdx]);
                    batchHash64s[i] = h64; // Relative 0..255
                    // SWAR Prefetch: Touch the 8-byte control block
                    dummy ^= controlData[(int) (h64 & CONTROL_HASH)];
                }
                if (dummy == -1) System.err.print("");

                // PHASE 2: Reserve Slots
                for (int i = 0; i < batchSize; i++) {
                    int absIdx = offset + i;
                    long h64 = batchHash64s[i];
                    batchIds[absIdx] = reserve(
                        key1s[absIdx],
                        key2s[absIdx],
                        (int)h64,
                        control(h64),
                        keysAddedAtStart
                    );
                }

                // PHASE 3: Flush Metadata (ID & Hash)
                for (int i = 0; i < batchSize; i++) {
                    int absIdx = offset + i;
                    int res = batchIds[absIdx];
                    if (res < 0) {
                        final int slot = -1 - res;
                        int id = nextId++;
                        batchIds[absIdx] = id; // Convert negative slot to actual ID

                        int hash = (int) batchHash64s[i];
                        final long idAndHash = ((long) id << 32) | Integer.toUnsignedLong(hash);
                        final long idOffset = idOffset(slot);
                        LONG_HANDLE.set(idPages[(int)(idOffset >> PAGE_SHIFT)], (int)(idOffset & PAGE_MASK), idAndHash);
                    }
                }

                // PHASE 4: Flush Keys (Sequential Writes)
                // Crucial: Only flush keys for the current chunk to stay O(N)
                for (int i = 0; i < batchSize; i++) {
                    int absIdx = offset + i;
                    int id = batchIds[absIdx];
                    if (id >= keysAddedAtStart) {
                        setKeys(id, key1s[absIdx], key2s[absIdx]);
                    }
                }

                size = nextId; // Commit the new IDs
                offset += batchSize;
            }
        }

        private static final long LSB_ONES = 0x0101010101010101L;
        private static final long MSB_ONES = 0x8080808080808080L;

        private int reserve(final long key1, final long key2, final int hash, final byte control, final int maxId) {
            int blockIdx = hash & CONTROL_HASH;
            final long pattern = (control & 0xFFL) * LSB_ONES;

            for (;;) {
                final long block = controlData[blockIdx];

                long match = block ^ pattern;
                match = (match - LSB_ONES) & ~match & MSB_ONES;
                while (match != 0) {
                    final int bitPos = Long.numberOfTrailingZeros(match);
                    final int slot = (blockIdx << 3) + (bitPos >>> 3);
                    final long idAndHash = idAndHash(slot);
                    if ((int) idAndHash == hash) {
                        final int id = id(idAndHash);
                        if (id != 0xFFFF_FFFF && id < maxId) {
                            final long keyOffset = keyOffset(id);
                            if (key1(keyOffset) == key1 && key2(keyOffset) == key2) {
                                return id;
                            }
                        }
                    }
                    match &= (match - 1);
                }

                final long emptyMatches = block & MSB_ONES;
                if (emptyMatches != 0) {
                    int bitPos = Long.numberOfTrailingZeros(emptyMatches);
                    int subSlot = bitPos >>> 3;
                    insertAtSlot(blockIdx, subSlot, control);
                    int slot = (blockIdx << 3) + subSlot;
                    return -1 - slot;
                }

                insertProbes++;
                blockIdx = (blockIdx + 1) & CONTROL_HASH;
            }
        }

        private void insertAtSlot(final int slot, final int subSlot, final byte control) {
            // Read-Modify-Write the specific byte
            long shift = (long)subSlot << 3;
            long longMask = 0xFFL << shift;
            long longVal = (control & 0xFFL) << shift;

            long block = controlData[slot];
            block = (block & ~longMask) | longVal;
            controlData[slot] = block;

            if (slot == 0) {
                controlData[controlData.length - 1] = block;
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
            int limit = controlData.length - 1;

            for (int i = 0; i < limit; i++) {
                long block = controlData[i];
                if (block == MSB_ONES) continue; // Skip empty blocks

                // Invert so populated slots have 1s
                long populated = (~block) & MSB_ONES;

                while (populated != 0) {
                    int bitPos = Long.numberOfTrailingZeros(populated);
                    int subSlot = bitPos >>> 3;
                    int oldSlot = (i << 3) + subSlot;

                    long packed = idAndHash(oldSlot);
                    int hash = (int) packed;
                    int id = (int) (packed >>> 32);
                    byte control = (byte) (block >>> bitPos);

                    newBigCore.insert(hash, control, id);

                    populated &= (populated - 1);
                }
            }
        }

        /**
         * Inserts the key into the first empty slot that allows it. Used
         * by {@link #rehash} because we know all keys are unique.
         */
        private void insert(final int hash, final byte control, final int id) {
            int blockIdx = hash & CONTROL_HASH;
            for (; ; ) {
                long block = controlData[blockIdx];
                final long emptyMatches = block & MSB_ONES;
                if (emptyMatches != 0) {
                    int bitPos = Long.numberOfTrailingZeros(emptyMatches);
                    int subSlot = bitPos >>> 3;
                    insertAtSlot(blockIdx, subSlot, control);

                    final long idAndHash = ((long) id << 32) | Integer.toUnsignedLong(hash);
                    int insertSlot = (blockIdx << 3) + subSlot;
                    final long offset = idOffset(insertSlot);
                    LONG_HANDLE.set(idPages[(int)(offset >> PAGE_SHIFT)], (int)(offset & PAGE_MASK), idAndHash);
                    return;
                }
                blockIdx = (blockIdx + 1) & CONTROL_HASH;
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

    private static long hash64(long key1, long key2) {
        return 31L * BitMixer.mix64(key1) + BitMixer.mix64(key2);
    }

    private int slot(final int hash) {
        return hash & mask;
    }

    @Override
    public long ramBytesUsed() {
        return BASE_RAM_BYTES_USED + (smallCore != null ? smallCore.ramBytesUsed() : bigCore.ramBytesUsed());
    }
}
