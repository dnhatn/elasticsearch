/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.apache.lucene.index.SortedDocValues;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.codec.tsdb.PartitionedDocValues;

import java.io.IOException;
import java.util.Arrays;

public record TsidRange(int bits, int lower, int upper) implements Writeable {

    public TsidRange(StreamInput in) throws IOException {
        this(in.readVInt(), in.readVInt(), in.readVInt());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(bits);
        out.writeVInt(lower);
        out.writeVInt(upper);
    }

    public int firstPartition(PartitionedDocValues.PrefixPartitions partitions, int partitionBits) {
        return partition(partitions, scale(lower, partitionBits));
    }

    public int endPartition(PartitionedDocValues.PrefixPartitions partitions, int partitionBits) {
        return partition(partitions, scale(upper, partitionBits));
    }

    private int scale(int prefix, int partitionBits) {
        if (partitionBits >= bits) {
            return prefix << (partitionBits - bits);
        }
        final int shift = bits - partitionBits;
        return (prefix + (1 << shift) - 1) >>> shift;
    }

    private static int partition(PartitionedDocValues.PrefixPartitions partitions, int prefix) {
        final int found = Arrays.binarySearch(partitions.prefixes(), 0, partitions.numPartitions(), prefix);
        return found >= 0 ? found : -found - 1;
    }

    static int startDoc(PartitionedDocValues.PrefixPartitions partitions, int partition, int maxDoc) {
        return partition < partitions.numPartitions() ? partitions.startDocs()[partition] : maxDoc;
    }

    /**
     * The {@code _tsid} ordinal of {@code doc}, or the value count from {@code maxDoc} on. Segments are sorted by {@code _tsid},
     * so the ordinals between two docs' are the series between them.
     */
    static int ordAt(SortedDocValues tsid, int doc, int maxDoc) throws IOException {
        if (doc >= maxDoc) {
            return tsid.getValueCount();
        }
        final boolean found = tsid.advanceExact(doc);
        assert found : "every time-series doc has a _tsid";
        return tsid.ordValue();
    }
}
