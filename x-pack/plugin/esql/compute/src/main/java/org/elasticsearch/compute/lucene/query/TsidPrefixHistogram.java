/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.codec.tsdb.PartitionedDocValues;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Series-milliseconds per {@code _tsid} prefix cell. A cell's series-milliseconds sum, over the segments that
 * hold it, the series in the cell times the segment's time span, so dividing by the overall time span estimates how
 * many distinct series the cell holds even when a series spans many segments.
 */
public final class TsidPrefixHistogram implements Writeable {
    public static final TsidPrefixHistogram EMPTY = new TsidPrefixHistogram(0, new int[0], new long[0], Long.MAX_VALUE, Long.MIN_VALUE);

    private final int bits;
    private final int[] prefixes;
    private final long[] seriesMillis;
    private final long minTimestamp;
    private final long maxTimestamp;

    public TsidPrefixHistogram(int bits, int[] prefixes, long[] seriesMillis, long minTimestamp, long maxTimestamp) {
        assert prefixes.length == seriesMillis.length;
        this.bits = bits;
        this.prefixes = prefixes;
        this.seriesMillis = seriesMillis;
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
    }

    public TsidPrefixHistogram(StreamInput in) throws IOException {
        this(in.readVInt(), in.readVIntArray(), in.readVLongArray(), in.readLong(), in.readLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(bits);
        out.writeVIntArray(prefixes);
        out.writeVLongArray(seriesMillis);
        out.writeLong(minTimestamp);
        out.writeLong(maxTimestamp);
    }

    /**
     * Builds the histogram of one segment whose {@code @timestamp} values, clipped to the query, span
     * {@code minTimestamp..maxTimestamp}. A partition counts in full once {@code matching} hits any of its docs. Each
     * {@code bucketMillis} group holds at least one doc, so the cells scale down to at most one group per doc
     * {@code matching} may hit.
     */
    public static TsidPrefixHistogram ofLeaf(
        PartitionedDocValues.PrefixPartitions partitions,
        int bits,
        int maxDoc,
        @Nullable DocIdSetIterator matching,
        SortedDocValues tsid,
        long minTimestamp,
        long maxTimestamp,
        long bucketMillis
    ) throws IOException {
        assert minTimestamp <= maxTimestamp;
        final long spanMillis = maxTimestamp - minTimestamp + 1;
        final DocIdSetIterator iterator = matching == null ? DocIdSetIterator.all(maxDoc) : matching;
        final long docs = iterator.cost();
        final int numPartitions = partitions.numPartitions();
        final int[] startDocs = partitions.startDocs();
        final int[] prefixes = new int[numPartitions];
        final long[] seriesMillis = new long[numPartitions];
        int size = 0;
        int partition = 0;
        int lastDoc = -1;
        int lastOrd = -1;
        int doc = iterator.nextDoc();
        while (doc != DocIdSetIterator.NO_MORE_DOCS) {
            int found = Arrays.binarySearch(startDocs, partition, numPartitions, doc);
            partition = Math.max(partition, found >= 0 ? found : -found - 2);
            final int startDoc = startDocs[partition];
            final int endDoc = TsidRange.startDoc(partitions, partition + 1, maxDoc);
            // the next partition starts where this one ends, so walking partitions in order looks up each ordinal once
            final int startOrd = startDoc == lastDoc ? lastOrd : TsidRange.ordAt(tsid, startDoc, maxDoc);
            final int endOrd = TsidRange.ordAt(tsid, endDoc, maxDoc);
            lastDoc = endDoc;
            lastOrd = endOrd;
            prefixes[size] = partitions.prefixes()[partition];
            seriesMillis[size++] = (long) (endOrd - startOrd) * spanMillis;
            if (endDoc >= maxDoc) {
                break;
            }
            doc = iterator.advance(endDoc);
        }
        if (bucketMillis > 0) {
            long totalSeriesMillis = 0;
            for (int i = 0; i < size; i++) {
                totalSeriesMillis += seriesMillis[i];
            }
            final double scale = (double) docs * bucketMillis / totalSeriesMillis;
            if (scale < 1) {
                for (int i = 0; i < size; i++) {
                    seriesMillis[i] = (long) Math.ceil(seriesMillis[i] * scale);
                }
            }
        }
        return new TsidPrefixHistogram(bits, Arrays.copyOf(prefixes, size), Arrays.copyOf(seriesMillis, size), minTimestamp, maxTimestamp);
    }

    public int bits() {
        return bits;
    }

    public int size() {
        return prefixes.length;
    }

    public int prefix(int cell) {
        return prefixes[cell];
    }

    public long seriesMillis(int cell) {
        return seriesMillis[cell];
    }

    public long minTimestamp() {
        return minTimestamp;
    }

    public long maxTimestamp() {
        return maxTimestamp;
    }

    public long totalSeriesMillis() {
        return Arrays.stream(seriesMillis).sum();
    }

    /**
     * The estimated number of distinct series: the series-milliseconds over the time span they cover.
     */
    public long estimatedSeries() {
        if (size() == 0) {
            return 0;
        }
        return Math.ceilDiv(totalSeriesMillis(), maxTimestamp - minTimestamp + 1);
    }

    public TsidPrefixHistogram coarsen(int newBits) {
        if (newBits >= bits) {
            return this;
        }
        final int shift = bits - newBits;
        final int[] newPrefixes = new int[prefixes.length];
        final long[] newSeriesMillis = new long[prefixes.length];
        int size = 0;
        for (int i = 0; i < prefixes.length; i++) {
            final int prefix = prefixes[i] >>> shift;
            if (size == 0 || newPrefixes[size - 1] != prefix) {
                newPrefixes[size++] = prefix;
            }
            newSeriesMillis[size - 1] += seriesMillis[i];
        }
        return new TsidPrefixHistogram(
            newBits,
            Arrays.copyOf(newPrefixes, size),
            Arrays.copyOf(newSeriesMillis, size),
            minTimestamp,
            maxTimestamp
        );
    }

    /**
     * Drops the fewest low bits that leave at most {@code maxCells} cells. Dropping {@code shift} bits merges two adjacent
     * cells exactly when their highest differing bit is below {@code shift}, so one pass counts the cells for every shift.
     */
    public TsidPrefixHistogram capped(int maxCells) {
        if (size() <= maxCells) {
            return this;
        }
        final int[] mergedAtShift = new int[Integer.SIZE + 1];
        for (int i = 1; i < prefixes.length; i++) {
            mergedAtShift[Integer.SIZE - Integer.numberOfLeadingZeros(prefixes[i - 1] ^ prefixes[i])]++;
        }
        int cells = size();
        int shift = 0;
        while (cells > maxCells && shift < bits) {
            shift++;
            cells -= mergedAtShift[shift];
        }
        return coarsen(bits - shift);
    }

    public TsidPrefixHistogram merge(TsidPrefixHistogram other) {
        if (other.size() == 0) {
            return this;
        }
        if (size() == 0) {
            return other;
        }
        final int minBits = Math.min(bits, other.bits);
        final TsidPrefixHistogram a = coarsen(minBits);
        final TsidPrefixHistogram b = other.coarsen(minBits);
        final int[] newPrefixes = new int[a.size() + b.size()];
        final long[] newSeriesMillis = new long[a.size() + b.size()];
        int size = 0;
        int i = 0;
        int j = 0;
        while (i < a.size() || j < b.size()) {
            final int prefix;
            if (j == b.size() || (i < a.size() && a.prefixes[i] <= b.prefixes[j])) {
                prefix = a.prefixes[i];
            } else {
                prefix = b.prefixes[j];
            }
            newPrefixes[size] = prefix;
            if (i < a.size() && a.prefixes[i] == prefix) {
                newSeriesMillis[size] += a.seriesMillis[i++];
            }
            if (j < b.size() && b.prefixes[j] == prefix) {
                newSeriesMillis[size] += b.seriesMillis[j++];
            }
            size++;
        }
        return new TsidPrefixHistogram(
            minBits,
            Arrays.copyOf(newPrefixes, size),
            Arrays.copyOf(newSeriesMillis, size),
            Math.min(minTimestamp, other.minTimestamp),
            Math.max(maxTimestamp, other.maxTimestamp)
        );
    }

    /**
     * Cuts the prefix space into at most {@code numSplits} contiguous ranges of about equal series-milliseconds, or none if
     * the cells cannot be cut.
     */
    public List<TsidRange> splits(int numSplits) {
        final double total = totalSeriesMillis();
        if (numSplits <= 1 || total == 0) {
            return List.of();
        }
        final List<TsidRange> ranges = new ArrayList<>();
        int lower = 0;
        int next = 1;
        double cumulative = 0;
        for (int i = 0; i < prefixes.length - 1 && next < numSplits; i++) {
            cumulative += seriesMillis[i];
            if (cumulative * numSplits >= total * next) {
                ranges.add(new TsidRange(bits, lower, prefixes[i + 1]));
                lower = prefixes[i + 1];
                do {
                    next++;
                } while (next < numSplits && cumulative * numSplits >= total * next);
            }
        }
        if (ranges.isEmpty()) {
            return List.of();
        }
        ranges.add(new TsidRange(bits, lower, 1 << bits));
        return ranges;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof TsidPrefixHistogram that
            && bits == that.bits
            && minTimestamp == that.minTimestamp
            && maxTimestamp == that.maxTimestamp
            && Arrays.equals(prefixes, that.prefixes)
            && Arrays.equals(seriesMillis, that.seriesMillis);
    }

    @Override
    public int hashCode() {
        int result = 31 * bits + Arrays.hashCode(prefixes);
        result = 31 * result + Arrays.hashCode(seriesMillis);
        result = 31 * result + Long.hashCode(minTimestamp);
        return 31 * result + Long.hashCode(maxTimestamp);
    }

    @Override
    public String toString() {
        return "TsidPrefixHistogram[bits="
            + bits
            + ", cells="
            + prefixes.length
            + ", series="
            + estimatedSeries()
            + ", time="
            + minTimestamp
            + ".."
            + maxTimestamp
            + "]";
    }
}
