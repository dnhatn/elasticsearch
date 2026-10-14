/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.internal;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * What one place moved, as a report of it rather than a running count. Carried whole so that a count added later
 * travels with the ones already here rather than as a field of its own.
 *
 * @param decoded     bytes a read turned into, which is the volume it handed on
 * @param processed    of those, the bytes a predicate walked
 * @param decodeNanos time spent turning what was read into those bytes, so that a rate over them needs no
 *                    arithmetic against the time around it
 */
public record ScanCounts(long decoded, long processed, long decodeNanos) implements Writeable, ToXContentFragment {

    public static final ScanCounts EMPTY = new ScanCounts(0, 0, 0);

    public static ScanCounts readFrom(StreamInput in) throws IOException {
        return new ScanCounts(in.readVLong(), in.readVLong(), in.readVLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(decoded);
        out.writeVLong(processed);
        out.writeVLong(decodeNanos);
    }

    /** The reports added, for a caller summing what several places moved. */
    public ScanCounts plus(ScanCounts other) {
        return other == EMPTY ? this : new ScanCounts(decoded + other.decoded, processed + other.processed, decodeNanos + other.decodeNanos);
    }

    /** What was counted between two reports, which is the work one call did. */
    public ScanCounts since(ScanCounts before) {
        return before == EMPTY
            ? this
            : new ScanCounts(decoded - before.decoded, processed - before.processed, decodeNanos - before.decodeNanos);
    }

    /** Whether anything was counted, which a caller leaving the counts out of a report goes by. */
    public boolean isEmpty() {
        return decoded == 0 && processed == 0 && decodeNanos == 0;
    }

    /** Writes the counts that were taken, leaving out the ones nobody reported. */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (decoded > 0) {
            builder.field("bytes_decoded", decoded);
        }
        if (processed > 0) {
            builder.field("bytes_processed", processed);
        }
        if (decodeNanos > 0) {
            builder.field("decode_nanos", decodeNanos);
        }
        return builder;
    }
}
