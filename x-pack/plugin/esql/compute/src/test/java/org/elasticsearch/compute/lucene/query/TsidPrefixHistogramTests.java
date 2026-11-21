/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.index.codec.tsdb.PartitionedDocValues.PrefixPartitions;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class TsidPrefixHistogramTests extends ESTestCase {

    public void testLeafWithoutFilter() throws IOException {
        var partitions = new PrefixPartitions(3, new int[] { 0, 2, 3 }, new int[] { 0, 100, 300 });
        var histogram = TsidPrefixHistogram.ofLeaf(partitions, 10, 450, null, tenDocsPerSeries(450), 1000, 1999, 0);
        assertThat(
            histogram,
            equalTo(new TsidPrefixHistogram(10, new int[] { 0, 2, 3 }, new long[] { 10_000, 20_000, 15_000 }, 1000, 1999))
        );
        assertThat(histogram.estimatedSeries(), equalTo(45L));
    }

    public void testLeafWithFilter() throws IOException {
        var partitions = new PrefixPartitions(4, new int[] { 0, 2, 3, 7 }, new int[] { 0, 100, 300, 400 });
        FixedBitSet matching = new FixedBitSet(450);
        matching.set(120);
        matching.set(130);
        matching.set(299);
        matching.set(449);
        var histogram = TsidPrefixHistogram.ofLeaf(
            partitions,
            10,
            450,
            new BitSetIterator(matching, 4),
            tenDocsPerSeries(450),
            1000,
            1999,
            0
        );
        assertThat(histogram, equalTo(new TsidPrefixHistogram(10, new int[] { 2, 7 }, new long[] { 20_000, 5_000 }, 1000, 1999)));

        var none = TsidPrefixHistogram.ofLeaf(
            partitions,
            10,
            450,
            new BitSetIterator(new FixedBitSet(450), 0),
            tenDocsPerSeries(450),
            1000,
            1999,
            0
        );
        assertThat(none.size(), equalTo(0));
        assertThat(none.estimatedSeries(), equalTo(0L));
    }

    public void testCappedByDocs() throws IOException {
        var partitions = new PrefixPartitions(3, new int[] { 0, 2, 3 }, new int[] { 0, 100, 300 });
        // 45 series over 1000ms in 25ms buckets would be 1800 groups, but 450 docs hold at most 450
        var dense = TsidPrefixHistogram.ofLeaf(partitions, 10, 450, null, tenDocsPerSeries(450), 1000, 1999, 25);
        assertThat(dense, equalTo(new TsidPrefixHistogram(10, new int[] { 0, 2, 3 }, new long[] { 2_500, 5_000, 3_750 }, 1000, 1999)));

        var coarse = TsidPrefixHistogram.ofLeaf(partitions, 10, 450, null, tenDocsPerSeries(450), 1000, 1999, 100);
        assertThat(coarse.totalSeriesMillis(), equalTo(45_000L));

        // a metric in one doc of three series matches every partition, yet holds at most three groups
        FixedBitSet matching = new FixedBitSet(450);
        matching.set(0);
        matching.set(100);
        matching.set(300);
        var sparse = TsidPrefixHistogram.ofLeaf(
            partitions,
            10,
            450,
            new BitSetIterator(matching, 3),
            tenDocsPerSeries(450),
            1000,
            1999,
            10
        );
        assertThat(sparse.size(), equalTo(3));
        long groups = Math.ceilDiv(sparse.totalSeriesMillis(), 10);
        assertThat(groups, greaterThanOrEqualTo(3L));
        assertThat(groups, lessThanOrEqualTo(3L + sparse.size()));
    }

    public void testSingleDocPartition() throws IOException {
        // partitions start at prefix changes, so they never split a series: the second one is the single doc of series 1
        var partitions = new PrefixPartitions(2, new int[] { 0, 5 }, new int[] { 0, 10 });
        var histogram = TsidPrefixHistogram.ofLeaf(partitions, 10, 11, null, tenDocsPerSeries(11), 0, 9, 0);
        assertThat(histogram.seriesMillis(0), equalTo(10L));
        assertThat(histogram.seriesMillis(1), equalTo(10L));
    }

    public void testSeriesAcrossSegments() throws IOException {
        var partitions = new PrefixPartitions(1, new int[] { 0 }, new int[] { 0 });
        var older = TsidPrefixHistogram.ofLeaf(partitions, 10, 100, null, tenDocsPerSeries(100), 0, 499, 0);
        var newer = TsidPrefixHistogram.ofLeaf(partitions, 10, 100, null, tenDocsPerSeries(100), 500, 999, 0);
        var merged = older.merge(newer);
        assertThat(merged.minTimestamp(), equalTo(0L));
        assertThat(merged.maxTimestamp(), equalTo(999L));
        assertThat(merged.estimatedSeries(), equalTo(10L));
    }

    public void testMergeAndCoarsen() {
        var a = new TsidPrefixHistogram(4, new int[] { 1, 4, 5 }, new long[] { 10, 20, 30 }, 0, 99);
        var b = new TsidPrefixHistogram(4, new int[] { 0, 4, 9 }, new long[] { 1, 2, 3 }, 50, 199);
        assertThat(a.merge(b), equalTo(new TsidPrefixHistogram(4, new int[] { 0, 1, 4, 5, 9 }, new long[] { 1, 10, 22, 30, 3 }, 0, 199)));
        assertThat(a.merge(TsidPrefixHistogram.EMPTY), equalTo(a));
        assertThat(TsidPrefixHistogram.EMPTY.merge(a), equalTo(a));

        assertThat(a.coarsen(3), equalTo(new TsidPrefixHistogram(3, new int[] { 0, 2 }, new long[] { 10, 50 }, 0, 99)));
        var coarse = new TsidPrefixHistogram(3, new int[] { 2 }, new long[] { 7 }, 0, 99);
        assertThat(a.merge(coarse), equalTo(new TsidPrefixHistogram(3, new int[] { 0, 2 }, new long[] { 10, 57 }, 0, 99)));
        assertThat(a.capped(2), equalTo(a.coarsen(3)));
        assertThat(a.capped(3), equalTo(a));
    }

    public void testCappedDropsFewestBits() {
        int bits = between(1, 20);
        int[] prefixes = IntStream.generate(() -> randomIntBetween(0, (1 << bits) - 1))
            .limit(between(1, 2000))
            .sorted()
            .distinct()
            .toArray();
        long[] weights = LongStream.generate(() -> randomLongBetween(1, 100)).limit(prefixes.length).toArray();
        TsidPrefixHistogram histogram = new TsidPrefixHistogram(bits, prefixes, weights, 0, 99);
        int maxCells = between(1, prefixes.length + 1);
        TsidPrefixHistogram expected = histogram;
        while (expected.size() > maxCells && expected.bits() > 0) {
            expected = expected.coarsen(expected.bits() - 1);
        }
        assertThat(histogram.capped(maxCells), equalTo(expected));
    }

    public void testSplits() {
        var uniform = new TsidPrefixHistogram(4, new int[] { 0, 1, 2, 3, 4, 5, 6, 7 }, new long[] { 10, 10, 10, 10, 10, 10, 10, 10 }, 0, 9);
        assertThat(uniform.splits(1), empty());
        assertThat(
            uniform.splits(4),
            equalTo(List.of(new TsidRange(4, 0, 2), new TsidRange(4, 2, 4), new TsidRange(4, 4, 6), new TsidRange(4, 6, 16)))
        );
        assertThat(uniform.splits(2), equalTo(List.of(new TsidRange(4, 0, 4), new TsidRange(4, 4, 16))));

        var skewed = new TsidPrefixHistogram(4, new int[] { 3, 5, 9 }, new long[] { 1000, 10, 10 }, 0, 9);
        assertThat(skewed.splits(11), equalTo(List.of(new TsidRange(4, 0, 5), new TsidRange(4, 5, 16))));

        assertThat(TsidPrefixHistogram.EMPTY.splits(4), empty());
    }

    public void testSplitsTileThePrefixSpace() {
        int bits = between(4, 12);
        int cells = between(1, 1 << bits);
        int[] prefixes = new int[cells];
        long[] weights = new long[cells];
        int prefix = 0;
        int size = 0;
        while (size < cells && prefix < 1 << bits) {
            prefixes[size] = prefix;
            weights[size++] = between(1, 1000);
            prefix += between(1, 3);
        }
        var histogram = new TsidPrefixHistogram(bits, Arrays.copyOf(prefixes, size), Arrays.copyOf(weights, size), 0, 9);
        int numSplits = between(2, 64);
        List<TsidRange> splits = histogram.splits(numSplits);
        if (splits.isEmpty()) {
            return;
        }
        assertThat(splits.size(), lessThanOrEqualTo(numSplits));
        assertThat(splits.getFirst().lower(), equalTo(0));
        assertThat(splits.getLast().upper(), equalTo(1 << bits));
        long total = 0;
        for (int s = 0; s < splits.size(); s++) {
            TsidRange split = splits.get(s);
            assertThat(split.bits(), equalTo(bits));
            assertThat(split.upper(), greaterThan(split.lower()));
            if (s > 0) {
                assertThat(split.lower(), equalTo(splits.get(s - 1).upper()));
            }
            long splitWeight = 0;
            for (int i = 0; i < histogram.size(); i++) {
                if (histogram.prefix(i) >= split.lower() && histogram.prefix(i) < split.upper()) {
                    splitWeight += histogram.seriesMillis(i);
                }
            }
            assertThat(splitWeight, greaterThan(0L));
            total += splitWeight;
        }
        assertThat(total, equalTo(histogram.totalSeriesMillis()));
    }

    /**
     * {@code _tsid} doc values for a segment sorted by {@code _tsid} where every series has ten docs. Building real
     * time-series segments with prefix partitions needs the TSDB codec; this only has to answer ordinal lookups.
     */
    private static SortedDocValues tenDocsPerSeries(int maxDoc) {
        return new SortedDocValues() {
            private int doc = -1;

            @Override
            public int ordValue() {
                return doc / 10;
            }

            @Override
            public BytesRef lookupOrd(int ord) {
                throw new UnsupportedOperationException();
            }

            @Override
            public int getValueCount() {
                return Math.ceilDiv(maxDoc, 10);
            }

            @Override
            public boolean advanceExact(int target) {
                assert target >= doc : target + " < " + doc;
                doc = target;
                return true;
            }

            @Override
            public int docID() {
                return doc;
            }

            @Override
            public int nextDoc() {
                return advance(doc + 1);
            }

            @Override
            public int advance(int target) {
                doc = target < maxDoc ? target : NO_MORE_DOCS;
                return doc;
            }

            @Override
            public long cost() {
                return maxDoc;
            }
        };
    }
}
