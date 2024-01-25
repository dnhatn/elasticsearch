/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.codec.postings;

import org.apache.lucene.util.BytesRefBuilder;

import java.security.AccessController;
import java.security.PrivilegedAction;

public final class Zstd {

    public static void compress(BytesRefBuilder raw, BytesRefBuilder out, int compressionLevel) {
        AccessController.doPrivileged(new PrivilegedAction<Void>() {
            @Override
            public Void run() {
                int estimatedBytes = Math.toIntExact(com.github.luben.zstd.Zstd.compressBound(raw.length()));
                out.grow(estimatedBytes);
                long len = com.github.luben.zstd.Zstd.compressByteArray(
                    out.bytes(),
                    0,
                    estimatedBytes,
                    raw.bytes(),
                    0,
                    raw.length(),
                    compressionLevel
                );
                out.setLength(Math.toIntExact(len));
                return null;
            }
        });
    }

    public static void decompress(byte[] dst, int dstSize, byte[] src, int srcSize) {
        AccessController.doPrivileged(new PrivilegedAction<Void>() {
            @Override
            public Void run() {
                com.github.luben.zstd.Zstd.decompressByteArray(dst, 0, dstSize, src, 0, srcSize);
                return null;
            }
        });
    }
}
