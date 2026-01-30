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
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.common.util.LongIntHashTable;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.Releasables;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;

public final class LongIntSwissHash extends AbstractBytesRefSwissHash implements LongIntHashTable {
    private final BytesRef fixed = new BytesRef(new byte[Long.BYTES + Integer.BYTES]);
    private static final VarHandle LONG_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.nativeOrder());
    private static final VarHandle INT_HANDLE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.nativeOrder());
    private static final int KEY_SIZE = Long.BYTES + Integer.BYTES;
    private static final int KEYS_IN_PAGES = 1 << 10;
    private static final int PAGE_SIZE = KEY_SIZE * KEYS_IN_PAGES;
    private static final int PAGE_SHIFT = 10;
    private static final int KEY_MASK = KEYS_IN_PAGES - 1;

    /**
     * Pages of {@code keys}, vended by the {@link PageCacheRecycler}. It's
     * important that the size of keys be a power of two, so we can quickly
     * select the appropriate page and keys never span multiple pages.
     */
    private byte[][] keyPages;
    private int keyMemoryUsed = 0;

    /**
     * Creates a new {@link LongIntSwissHash} that manages its own {@link BytesRefArray}.
     */
    LongIntSwissHash(PageCacheRecycler recycler, CircuitBreaker breaker) {
        super(recycler, breaker);
        boolean success = false;
        try {
            keyPages = new byte[][] { grabKeyPage() };
            success = true;
        } finally {
            if(success == false) {
                Releasables.close(this);
            }
        }
    }

    @Override
    public long find(long key1, int key2) {
        LONG_HANDLE.set(fixed.bytes, 0, key1);
        INT_HANDLE.set(fixed.bytes, Long.BYTES, key2);
        return findKey(fixed, hash(key1, key2));
    }

    @Override
    public long add(long key1, int key2) {
        LONG_HANDLE.set(fixed.bytes, 0, key1);
        INT_HANDLE.set(fixed.bytes, Long.BYTES, key2);
        return addKey(fixed, hash(key1, key2));
    }

    static int hash(long key1, int key2) {
        return 31 * BitMixer.mix(key1) + BitMixer.mix32(key2);
    }

    private int pageIndex(int id) {
        return id >>> PAGE_SHIFT;
    }

    private int keyOffset(int id) {
        return (id & KEY_MASK) * KEY_SIZE;
    }

    @Override
    public long getKey1(long id) {
        int n = Math.toIntExact(id);
        byte[] page = keyPages[pageIndex(n)];
        return (long) LONG_HANDLE.get(page, keyOffset(n));
    }

    @Override
    public int getKey2(long id) {
        int n = Math.toIntExact(id);
        byte[] page = keyPages[pageIndex(n)];
        return (int)INT_HANDLE.get(page, keyOffset(n) + Long.BYTES);
    }

    @Override
    protected boolean matches(BytesRef key, int id) {
        assert key == fixed;
        byte[] page = keyPages[pageIndex(id)];
        int offset = keyOffset(id);
        return Arrays.mismatch(
            page,
            offset,
            offset + KEY_SIZE,
            key.bytes,
            key.offset,
            key.offset + KEY_SIZE) < 0;
    }

    @Override
    protected int storeKey(BytesRef key) {
        assert key.length == KEY_SIZE;
        final int id = size;
        final int pageIndex = pageIndex(id);
        // grab more pages
        if (pageIndex >= keyPages.length) {
            final int currentSize = keyPages.length;
            keyPages = Arrays.copyOf(keyPages, ArrayUtil.oversize(currentSize + 1, KEY_SIZE));
            for (int i = currentSize; i < keyPages.length; i++) {
                keyPages[i] = grabKeyPage();
            }
        }
        System.arraycopy(key.bytes, key.offset, keyPages[pageIndex], keyOffset(id), KEY_SIZE);
        return id;
    }


    public abstract class Itr extends SwissHash.Itr {
        /** The first key the iterator current points to. */
        public abstract long key1();

        /** The second key the iterator current points to. */
        public abstract int key2();
    }

    @Override
    public Itr iterator() {
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
                return getKey1(keyId);
            }

            @Override
            public int key2() {
                return getKey2(keyId);
            }
        };
    }

    @Override
    public void close() {
        Releasables.close(super::close, () -> {
            breaker.addEstimateBytesAndMaybeBreak(-keyMemoryUsed, "LongIntSwissHash-keyPage");
        });
    }

    @Override
    public long ramBytesUsed() {
        return super.ramBytesUsed() + keyMemoryUsed;
    }


    private byte[] grabKeyPage() {
        breaker.addEstimateBytesAndMaybeBreak(PAGE_SIZE, "LongIntSwissHash-keyPage");
        keyMemoryUsed += PAGE_SIZE;
        return new byte[PAGE_SIZE];
    }
}
