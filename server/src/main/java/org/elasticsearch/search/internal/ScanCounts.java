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
 * <p>The counts are tiers of the same work and are not added together. {@link #bytesIn} and {@link #bytesOut} are
 * the volume that flowed through, which whoever moved it between two places can see from outside; of what came out,
 * {@link #decoded} is the part made here rather than passed along, and of what came in, {@link #processed} is the
 * part something actually looked at, which only the work itself can say.
 *
 * @param bytesIn     bytes handed to this place
 * @param bytesOut    bytes it handed on
 * @param decoded     of what it handed on, the bytes it turned storage into
 * @param processed   of what it was handed, the bytes it looked at
 * @param decodeNanos time spent turning what was read into bytes, so that a rate over them needs no arithmetic
 *                    against the time around it
 */
public record ScanCounts(long bytesIn, long bytesOut, long decoded, long processed, long decodeNanos)
    implements
        Writeable,
        ToXContentFragment {

    public static final ScanCounts EMPTY = new ScanCounts(0, 0, 0, 0, 0);

    /** What was made and looked at in one place, which is what its counters report. */
    public static ScanCounts of(long decoded, long processed, long decodeNanos) {
        return new ScanCounts(0, 0, decoded, processed, decodeNanos);
    }

    /** Bytes handed to a place, which whoever handed them over counts. */
    public static ScanCounts handedIn(long bytes) {
        return new ScanCounts(bytes, 0, 0, 0, 0);
    }

    /** Bytes a place handed on, which whoever took them counts. */
    public static ScanCounts handedOn(long bytes) {
        return new ScanCounts(0, bytes, 0, 0, 0);
    }

    public static ScanCounts readFrom(StreamInput in) throws IOException {
        return new ScanCounts(in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(bytesIn);
        out.writeVLong(bytesOut);
        out.writeVLong(decoded);
        out.writeVLong(processed);
        out.writeVLong(decodeNanos);
    }

    /** The reports added, for a caller summing what several places moved. */
    public ScanCounts plus(ScanCounts other) {
        return other == EMPTY
            ? this
            : new ScanCounts(
                bytesIn + other.bytesIn,
                bytesOut + other.bytesOut,
                decoded + other.decoded,
                processed + other.processed,
                decodeNanos + other.decodeNanos
            );
    }

    /** What was counted between two reports, which is the work one call did. */
    public ScanCounts since(ScanCounts before) {
        return before == EMPTY
            ? this
            : new ScanCounts(
                bytesIn - before.bytesIn,
                bytesOut - before.bytesOut,
                decoded - before.decoded,
                processed - before.processed,
                decodeNanos - before.decodeNanos
            );
    }

    /** Whether anything was counted, which a caller leaving the counts out of a report goes by. */
    public boolean isEmpty() {
        return bytesIn == 0 && bytesOut == 0 && decoded == 0 && processed == 0 && decodeNanos == 0;
    }

    /** Writes the counts that were taken, leaving out the ones nobody reported. */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (bytesIn > 0) {
            builder.field("bytes_in", bytesIn);
        }
        if (bytesOut > 0) {
            builder.field("bytes_out", bytesOut);
        }
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
