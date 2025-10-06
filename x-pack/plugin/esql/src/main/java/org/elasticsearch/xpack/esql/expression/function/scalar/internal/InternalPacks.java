/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.internal;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.topn.TopNEncoder;

final class InternalPacks {
    private static final TopNEncoder ENCODER = TopNEncoder.DEFAULT_UNSORTABLE;

    static BytesRefBlock packBytesValues(DriverContext driverContext, BytesRefBlock raw) {
        int positionCount = raw.getPositionCount();
        try (
            BytesRefBlock.Builder builder = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount);
            var work = new BreakingBytesRefBuilder(driverContext.breaker(), "pack_values", 1024)
        ) {
            BytesRef scratch = new BytesRef();
            for (int p = 0; p < positionCount; p++) {
                work.clear();
                int valueCount = raw.getValueCount(p);
                if (valueCount == 0) {
                    builder.appendNull();
                    continue;
                }
                ENCODER.encodeInt(valueCount, work);
                int first = raw.getFirstValueIndex(p);
                if (valueCount == 1) {
                    raw.getBytesRef(first, scratch);
                    ENCODER.encodeBytesRef(scratch, work);
                } else {
                    int end = first + valueCount;
                    for (int i = first; i < end; i++) {
                        raw.getBytesRef(i, scratch);
                        ENCODER.encodeBytesRef(scratch, work);
                    }
                }
                builder.appendBytesRef(work.bytesRefView());
            }
            return builder.build();
        }
    }

    static BytesRefBlock unpackBytesValues(DriverContext driverContext, BytesRefBlock encoded) {
        int positionCount = encoded.getPositionCount();
        try (BytesRefBlock.Builder builder = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
            BytesRef inScratch = new BytesRef();
            BytesRef outScratch = new BytesRef();
            for (int p = 0; p < positionCount; p++) {
                if (encoded.isNull(p)) {
                    builder.appendNull();
                    continue;
                }
                BytesRef row = encoded.getBytesRef(p, inScratch);
                int valueCount = ENCODER.decodeInt(row);
                if (valueCount == 1) {
                    var v = ENCODER.decodeBytesRef(row, outScratch);
                    builder.appendBytesRef(v);
                } else {
                    builder.beginPositionEntry();
                    for (int i = 0; i < valueCount; i++) {
                        var v = ENCODER.decodeBytesRef(row, outScratch);
                        builder.appendBytesRef(v);
                    }
                    builder.endPositionEntry();
                }
            }
            return builder.build();
        }
    }

    static BytesRefBlock packLongValues(DriverContext driverContext, LongBlock raw) {
        int positionCount = raw.getPositionCount();
        try (
            BytesRefBlock.Builder builder = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount * Long.BYTES);
            var work = new BreakingBytesRefBuilder(driverContext.breaker(), "pack_values", 32)
        ) {
            for (int p = 0; p < positionCount; p++) {
                work.clear();
                int valueCount = raw.getValueCount(p);
                if (valueCount == 0) {
                    builder.appendNull();
                    continue;
                }
                int first = raw.getFirstValueIndex(p);
                if (valueCount == 1) {
                    ENCODER.encodeLong(raw.getLong(first), work);
                } else {
                    int end = first + valueCount;
                    for (int i = first; i < end; i++) {
                        ENCODER.encodeLong(raw.getLong(i), work);
                    }
                }
                builder.appendBytesRef(work.bytesRefView());
            }
            return builder.build();
        }
    }

    static LongBlock unpackLongValues(DriverContext driverContext, BytesRefBlock encoded) {
        int positionCount = encoded.getPositionCount();
        try (LongBlock.Builder builder = driverContext.blockFactory().newLongBlockBuilder(positionCount)) {
            BytesRef inScratch = new BytesRef();
            for (int p = 0; p < positionCount; p++) {
                if (encoded.isNull(p)) {
                    builder.appendNull();
                    continue;
                }
                BytesRef row = encoded.getBytesRef(p, inScratch);
                int valueCount = row.length / Long.BYTES;
                if (valueCount == 1) {
                    builder.appendLong(ENCODER.decodeLong(row));
                } else {
                    builder.beginPositionEntry();
                    for (int i = 0; i < valueCount; i++) {
                        builder.appendLong(ENCODER.decodeLong(row));
                    }
                    builder.endPositionEntry();
                }
            }
            return builder.build();
        }
    }

    static BytesRefBlock packIntValues(DriverContext driverContext, IntBlock raw) {
        int positionCount = raw.getPositionCount();
        try (
            BytesRefBlock.Builder builder = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount * Long.BYTES);
            var work = new BreakingBytesRefBuilder(driverContext.breaker(), "pack_values", 32)
        ) {
            for (int p = 0; p < positionCount; p++) {
                work.clear();
                int valueCount = raw.getValueCount(p);
                if (valueCount == 0) {
                    builder.appendNull();
                    continue;
                }
                int first = raw.getFirstValueIndex(p);
                if (valueCount == 1) {
                    ENCODER.encodeInt(raw.getInt(first), work);
                } else {
                    int end = first + valueCount;
                    for (int i = first; i < end; i++) {
                        ENCODER.encodeInt(raw.getInt(i), work);
                    }
                }
                builder.appendBytesRef(work.bytesRefView());
            }
            return builder.build();
        }
    }

    static IntBlock unpackIntValues(DriverContext driverContext, BytesRefBlock encoded) {
        int positionCount = encoded.getPositionCount();
        try (IntBlock.Builder builder = driverContext.blockFactory().newIntBlockBuilder(positionCount)) {
            BytesRef inScratch = new BytesRef();
            for (int p = 0; p < positionCount; p++) {
                if (encoded.isNull(p)) {
                    builder.appendNull();
                    continue;
                }
                BytesRef row = encoded.getBytesRef(p, inScratch);
                int valueCount = row.length / Integer.BYTES;
                if (valueCount == 1) {
                    builder.appendInt(ENCODER.decodeInt(row));
                } else {
                    builder.beginPositionEntry();
                    for (int i = 0; i < valueCount; i++) {
                        builder.appendInt(ENCODER.decodeInt(row));
                    }
                    builder.endPositionEntry();
                }
            }
            return builder.build();
        }
    }

    static BytesRefBlock packBooleanValues(DriverContext driverContext, BooleanBlock raw) {
        int positionCount = raw.getPositionCount();
        try (
            BytesRefBlock.Builder builder = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount * Long.BYTES);
            var work = new BreakingBytesRefBuilder(driverContext.breaker(), "pack_values", 32)
        ) {
            for (int p = 0; p < positionCount; p++) {
                work.clear();
                int valueCount = raw.getValueCount(p);
                if (valueCount == 0) {
                    builder.appendNull();
                    continue;
                }
                int first = raw.getFirstValueIndex(p);
                if (valueCount == 1) {
                    ENCODER.encodeBoolean(raw.getBoolean(first), work);
                } else {
                    int end = first + valueCount;
                    for (int i = first; i < end; i++) {
                        ENCODER.encodeBoolean(raw.getBoolean(i), work);
                    }
                }
                builder.appendBytesRef(work.bytesRefView());
            }
            return builder.build();
        }
    }

    static BooleanBlock unpackBooleanValues(DriverContext driverContext, BytesRefBlock encoded) {
        int positionCount = encoded.getPositionCount();
        try (var builder = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
            BytesRef inScratch = new BytesRef();
            for (int p = 0; p < positionCount; p++) {
                if (encoded.isNull(p)) {
                    builder.appendNull();
                    continue;
                }
                BytesRef row = encoded.getBytesRef(p, inScratch);
                int valueCount = row.length;
                if (valueCount == 1) {
                    builder.appendBoolean(ENCODER.decodeBoolean(row));
                } else {
                    builder.beginPositionEntry();
                    for (int i = 0; i < valueCount; i++) {
                        builder.appendBoolean(ENCODER.decodeBoolean(row));
                    }
                    builder.endPositionEntry();
                }
            }
            return builder.build();
        }
    }
}
