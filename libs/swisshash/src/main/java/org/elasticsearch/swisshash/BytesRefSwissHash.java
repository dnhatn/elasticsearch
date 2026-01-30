/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.swisshash;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.common.util.BytesRefHashTable;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.Releasables;

import java.util.Objects;


public final class BytesRefSwissHash extends AbstractBytesRefSwissHash implements BytesRefHashTable {
    private final BytesRefArray bytesRefs;
    private final boolean ownsBytesRefs;
    private final BytesRef scratch = new BytesRef();

    /**
     * Creates a new {@link BytesRefSwissHash} that manages its own {@link BytesRefArray}.
     */
    BytesRefSwissHash(PageCacheRecycler recycler, CircuitBreaker breaker, BigArrays bigArrays) {
        super(recycler, breaker);
        boolean success = false;
        this.ownsBytesRefs = true;
        try {
            this.bytesRefs = new BytesRefArray(PageCacheRecycler.PAGE_SIZE_IN_BYTES, bigArrays);
            success = true;
        } finally {
            if(success == false) {
                Releasables.close(this);
            }
        }
    }

    /**
     * Creates a new {@link BytesRefSwissHash} that uses the provided {@link BytesRefArray}.
     * This allows multiple {@link BytesRefSwissHash} to share the same key storage and ID space.
     */
    BytesRefSwissHash(PageCacheRecycler recycler, CircuitBreaker breaker, BytesRefArray bytesRefs) {
        super(recycler, breaker);
        this.bytesRefs = bytesRefs;
        this.ownsBytesRefs = false;
    }

    /**
     * Finds an {@code id} by a {@code key}.
     */
    @Override
    public long find(BytesRef key) {
        return findKey(key, hash(key));
    }

    /**
     * Adds a {@code key}, returning its {@code id}. If it was already present
     * it's previous assigned {@code id} will be returned. If it wasn't present
     * it'll be assigned a new {@code id}.
     */
    @Override
    public long add(BytesRef key) {
        return addKey(key, hash(key));
    }

    static int hash(BytesRef v) {
        return BitMixer.mix32(v.hashCode());
    }

    public abstract class Itr extends SwissHash.Itr {
        /**
         * The key the iterator current points to.
         */
        public abstract BytesRef key(BytesRef dest);
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
            public BytesRef key(BytesRef dest) {
                return bytesRefs.get(keyId, dest);
            }
        };
    }

    @Override
    public void close() {
        super.close();
        if (ownsBytesRefs) {
            Releasables.close(bytesRefs);
        }
    }

    @Override
    protected boolean matches(BytesRef key, int id) {
        return bytesRefs.get(id, scratch).equals(key);
    }

    @Override
    protected int storeKey(BytesRef key) {
        long id = bytesRefs.size();
        bytesRefs.append(key);
        return Math.toIntExact(id);
    }

    /**
     * Returns the key at <code>0 &lt;= id &lt;= size()</code>.
     * The result is undefined if the id is unused.
     * @param id the id returned when the key was added
     * @return the key
     */
    @Override
    public BytesRef get(long id, BytesRef dest) {
        Objects.checkIndex(id, size());
        return bytesRefs.get(id, dest);
    }

    /** Returns the key array. */
    @Override
    public BytesRefArray getBytesRefs() {
        return bytesRefs;
    }

    @Override
    public long ramBytesUsed() {
        return super.ramBytesUsed() + bytesRefs.ramBytesUsed();
    }
}
