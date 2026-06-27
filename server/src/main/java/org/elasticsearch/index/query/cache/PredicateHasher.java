/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query.cache;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.common.util.ByteUtils;

import java.nio.charset.StandardCharsets;

/**
 * Generates a {@link PredicateKey} for a query predicate via MurmurHash3.
 */
public final class PredicateHasher {
    private final BytesRefBuilder buf = new BytesRefBuilder();

    public PredicateHasher(Class<?> queryClass) {
        addString(queryClass.getName());
    }

    public PredicateHasher addString(String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        addInt(bytes.length);
        buf.append(bytes, 0, bytes.length);
        return this;
    }

    public PredicateHasher addLong(long value) {
        buf.grow(buf.length() + Long.BYTES);
        ByteUtils.writeLongBE(value, buf.bytes(), buf.length());
        buf.setLength(buf.length() + Long.BYTES);
        return this;
    }

    public PredicateHasher addInt(int value) {
        buf.grow(buf.length() + Integer.BYTES);
        ByteUtils.writeIntBE(value, buf.bytes(), buf.length());
        buf.setLength(buf.length() + Integer.BYTES);
        return this;
    }

    public PredicateHasher addBoolean(boolean value) {
        buf.append(value ? (byte) 1 : (byte) 0);
        return this;
    }

    public PredicateHasher addDouble(double value) {
        return addLong(Double.doubleToRawLongBits(value));
    }

    public PredicateHasher addBytesRef(BytesRef value) {
        addInt(value.length);
        buf.append(value.bytes, value.offset, value.length);
        return this;
    }

    public PredicateHasher addObject(Object value) {
        addString(value.toString());
        return this;
    }

    public PredicateHasher addPredicateKey(PredicateKey key) {
        addLong(key.h1());
        addLong(key.h2());
        return this;
    }

    /**
     * Finalizes the hash computation and returns the predicate key.
     */
    public PredicateKey finish() {
        var hash = MurmurHash3.hash128(buf.bytes(), 0, buf.length(), 0, new MurmurHash3.Hash128());
        return new PredicateKey(hash.h1, hash.h2);
    }
}
