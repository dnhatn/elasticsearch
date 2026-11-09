/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromList;
import org.elasticsearch.compute.lucene.PartialLeafReaderContext;
import org.elasticsearch.compute.lucene.query.TsidRangeQueryTests.Layout;
import org.elasticsearch.compute.lucene.query.TsidRangeQueryTests.TsidIndex;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.elasticsearch.compute.lucene.query.TsidRangeQueryTests.matches;
import static org.elasticsearch.compute.lucene.query.TsidRangeQueryTests.scan;
import static org.elasticsearch.compute.lucene.query.TsidRangeQueryTests.tsidIndex;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

/**
 * Checks that the ranges {@link TsidRangePartitioner} cuts a shard into are disjoint, cover every doc, and keep each series
 * whole, and that a range index past the last range is empty.
 */
public class TsidRangePartitionerTests extends ESTestCase {

    public void testRanges() throws IOException {
        try (TsidIndex index = tsidIndex(Layout.random(), between(200, 500))) {
            assertThat(index.reader.leaves().size(), greaterThan(1));
            SortedDocValues reference = referenceSegment(index);
            int referenceSeries = reference.getValueCount();
            assertThat(TsidRangePartitioner.seriesInLargestSegment(index.reader), equalTo(referenceSeries));
            Query inner = randomBoolean()
                ? null
                : SortedNumericDocValuesField.newSlowRangeQuery("@timestamp", 0, randomLongBetween(0, 1_000_000));
            List<Integer> all = inner == null ? scan(index.reader, null, null) : matches(index.reader, inner);
            for (int wanted : new int[] { 1, 2, 4, 8 }) {
                int seriesPerRange = Math.ceilDiv(referenceSeries, wanted);
                // The cuts are at every seriesPerRange-th series, so a small reference segment can give fewer ranges than wanted.
                int ranges = TsidRangePartitioner.numRanges(referenceSeries, seriesPerRange);
                assertThat(ranges, lessThanOrEqualTo(wanted));
                assertThat(ranges, wanted == 1 ? equalTo(1) : greaterThan(1));
                TsidRangeQuery.Steps steps = new TsidRangeQuery.Steps();
                List<Integer> union = new ArrayList<>();
                Set<Integer> seen = new HashSet<>();
                for (int i = 0; i < ranges; i++) {
                    TsidRangeQuery range = TsidRangePartitioner.range(index.reader, seriesPerRange, i, steps);
                    assertThat(range.lower() == null, equalTo(i == 0));
                    assertThat(range.upper() == null, equalTo(i == ranges - 1));
                    List<Integer> docs = matches(index.reader, inner == null ? range : filtered(inner, range));
                    for (int doc : docs) {
                        assertTrue("doc [" + doc + "] matched by more than one range", seen.add(doc));
                    }
                    // Each matched doc's tsid is in the range, and the range holds at most seriesPerRange reference series.
                    assertThat(docs, equalTo(intersection(all, scan(index.reader, range.lower(), range.upper()))));
                    assertThat(seriesInReference(reference, range), lessThanOrEqualTo(seriesPerRange));
                    union.addAll(docs);
                }
                union.sort(null);
                assertThat(union, equalTo(all));
                if (ranges > 1) {
                    assertThat(steps.status().leaves(), equalTo((long) ranges * index.reader.leaves().size()));
                }
                // At or past the last range: an empty range, which still goes through the query but matches nothing.
                for (int i : new int[] { ranges, ranges + between(1, 10) }) {
                    TsidRangeQuery empty = TsidRangePartitioner.range(index.reader, seriesPerRange, i, steps);
                    assertThat(empty.lower(), equalTo(empty.upper()));
                    assertNotNull(empty.lower());
                    assertThat(matches(index.reader, empty), equalTo(List.of()));
                }
            }
        }
    }

    public void testSingleRange() throws IOException {
        try (TsidIndex index = tsidIndex(Layout.random(), between(10, 50))) {
            int seriesPerRange = between(index.tsids.size(), Integer.MAX_VALUE);
            assertThat(
                TsidRangePartitioner.numRanges(TsidRangePartitioner.seriesInLargestSegment(index.reader), seriesPerRange),
                equalTo(1)
            );
            TsidRangeQuery.Steps steps = new TsidRangeQuery.Steps();
            TsidRangeQuery range = TsidRangePartitioner.range(index.reader, seriesPerRange, 0, steps);
            assertThat(range.lower(), nullValue());
            assertThat(range.upper(), nullValue());
            assertThat(matches(index.reader, range), equalTo(scan(index.reader, null, null)));
            assertThat(matches(index.reader, TsidRangePartitioner.range(index.reader, seriesPerRange, 1, steps)), equalTo(List.of()));
        }
    }

    public void testInvalidArguments() {
        assertThat(TsidRangePartitioner.numRanges(0, 10), equalTo(1));
        expectThrows(IllegalArgumentException.class, () -> TsidRangePartitioner.numRanges(10, 0));
        expectThrows(IllegalArgumentException.class, () -> TsidRangePartitioner.numRanges(10, -5));
        try (TsidIndex index = tsidIndex(Layout.random(), between(1, 10))) {
            expectThrows(IllegalArgumentException.class, () -> TsidRangePartitioner.range(index.reader, 1, -1, new TsidRangeQuery.Steps()));
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * The slice queue cuts each leaf only over the interval of each range, under every partitioning strategy: the slices of
     * a range cover exactly the docs the range matches in each leaf, and a range that matches nothing in a leaf gets no
     * slice of it. It runs the query without the range, and one weight serves every range of the shard.
     */
    public void testSlicesFollowRange() throws IOException {
        Layout layout = Layout.random();
        try (TsidIndex index = tsidIndex(layout, between(200, 500))) {
            int referenceSeries = TsidRangePartitioner.seriesInLargestSegment(index.reader);
            int seriesPerRange = Math.ceilDiv(referenceSeries, between(2, 8));
            int ranges = TsidRangePartitioner.numRanges(referenceSeries, seriesPerRange);
            Query inner = randomBoolean()
                ? null
                : SortedNumericDocValuesField.newSlowRangeQuery("@timestamp", 0, randomLongBetween(0, 1_000_000));
            TsidRangeQuery.Steps steps = new TsidRangeQuery.Steps();
            List<TsidRangeQuery> rangeQueries = new ArrayList<>();
            List<LuceneSliceQueue.QueryAndTags> queries = new ArrayList<>();
            for (int i = 0; i <= ranges; i++) {
                TsidRangeQuery range = TsidRangePartitioner.range(index.reader, seriesPerRange, i, steps);
                rangeQueries.add(range);
                queries.add(new LuceneSliceQueue.QueryAndTags(inner == null ? range : filtered(inner, range), List.of(i)));
            }
            // Time-series partitioning needs the prefix table.
            List<LuceneSliceQueue.PartitioningStrategy> strategies = new ArrayList<>(
                List.of(
                    LuceneSliceQueue.PartitioningStrategy.SHARD,
                    LuceneSliceQueue.PartitioningStrategy.SEGMENT,
                    LuceneSliceQueue.PartitioningStrategy.DOC
                )
            );
            if (layout.prefixTable()) {
                strategies.add(LuceneSliceQueue.PartitioningStrategy.TIME_SERIES);
            }
            LuceneSliceQueue.PartitioningStrategy strategy = randomFrom(strategies);
            // The shard's searcher wraps the reader, so its leaves are the ones the queue resolves and cuts.
            LuceneSourceOperatorTests.MockShardContext shardContext = new LuceneSourceOperatorTests.MockShardContext(index.reader);
            List<LeafReaderContext> leaves = shardContext.searcher().getLeafContexts();
            LuceneSliceQueue queue = LuceneSliceQueue.create(
                new IndexedByShardIdFromList<>(List.of(shardContext)),
                ctx -> queries,
                DataPartitioning.AUTO,
                (ctx, query) -> strategy,
                0,
                between(1, 4),
                ctx -> ScoreMode.COMPLETE_NO_SCORES,
                LuceneSliceQueue.LeafSplitGuard.NEVER,
                between(1, 50)
            );
            Set<Weight> weights = Collections.newSetFromMap(new IdentityHashMap<>());
            Map<Object, List<PartialLeafReaderContext>> partsByRange = new HashMap<>();
            List<LuceneSlice> slices = new ArrayList<>();
            for (LuceneSlice slice = queue.nextSlice(null); slice != null; slice = queue.nextSlice(slice)) {
                weights.add(slice.weight());
                assertThat(TsidRangeQuery.requiredRange(slice.weight().getQuery()), nullValue());
                partsByRange.computeIfAbsent(slice.tags().getFirst(), k -> new ArrayList<>()).addAll(slice.leaves());
                slices.add(slice);
            }
            assertThat(strategy.toString(), weights, hasSize(1));
            if (strategy != LuceneSliceQueue.PartitioningStrategy.TIME_SERIES) {
                // In queue order the slices walk each leaf in doc order, so a driver streams a leaf as it would uncut.
                slices.sort(Comparator.comparingInt(LuceneSlice::slicePosition));
                for (int i = 1; i < slices.size(); i++) {
                    PartialLeafReaderContext prev = slices.get(i - 1).leaves().getFirst();
                    PartialLeafReaderContext next = slices.get(i).leaves().getFirst();
                    assertThat(
                        strategy + ": slice " + i,
                        next.leafReaderContext().ord > prev.leafReaderContext().ord
                            || (next.leafReaderContext().ord == prev.leafReaderContext().ord && next.minDoc() >= prev.maxDoc()),
                        equalTo(true)
                    );
                }
            }
            for (int i = 0; i <= ranges; i++) {
                for (LeafReaderContext leaf : leaves) {
                    String description = strategy + ": range " + i + " of leaf " + leaf.ord;
                    TsidRangeQuery.DocRange expected = rangeQueries.get(i).docRange(leaf);
                    List<PartialLeafReaderContext> parts = partsByRange.getOrDefault(i, List.of())
                        .stream()
                        .filter(p -> p.leafReaderContext() == leaf)
                        .sorted(Comparator.comparingInt(PartialLeafReaderContext::minDoc))
                        .toList();
                    if (expected.isEmpty()) {
                        assertThat(description, parts, empty());
                        continue;
                    }
                    assertThat(description, parts, not(empty()));
                    assertThat(description, parts.getFirst().minDoc(), equalTo(expected.minDoc()));
                    assertThat(description, parts.getLast().maxDoc(), equalTo(expected.maxDoc()));
                    for (int p = 1; p < parts.size(); p++) {
                        assertThat(description, parts.get(p).minDoc(), equalTo(parts.get(p - 1).maxDoc()));
                    }
                }
            }
            // The queue resolved each (range, leaf) once; the checks above reused it.
            assertThat(steps.status().leaves(), equalTo((long) (ranges + 1) * leaves.size()));
        }
    }

    private static Query filtered(Query inner, TsidRangeQuery range) {
        return new BooleanQuery.Builder().add(inner, BooleanClause.Occur.FILTER).add(range, BooleanClause.Occur.FILTER).build();
    }

    private static SortedDocValues referenceSegment(TsidIndex index) throws IOException {
        SortedDocValues reference = null;
        for (LeafReaderContext leaf : index.reader.leaves()) {
            SortedDocValues tsid = leaf.reader().getSortedDocValues(TimeSeriesIdFieldMapper.NAME);
            if (reference == null || tsid.getValueCount() > reference.getValueCount()) {
                reference = tsid;
            }
        }
        return reference;
    }

    /**
     * How many series of the reference segment are in {@code range}.
     */
    private static int seriesInReference(SortedDocValues reference, TsidRangeQuery range) throws IOException {
        Set<BytesRef> series = new TreeSet<>();
        for (int ord = 0; ord < reference.getValueCount(); ord++) {
            BytesRef tsid = reference.lookupOrd(ord);
            if ((range.lower() == null || tsid.compareTo(range.lower()) >= 0)
                && (range.upper() == null || tsid.compareTo(range.upper()) < 0)) {
                series.add(BytesRef.deepCopyOf(tsid));
            }
        }
        return series.size();
    }

    private static List<Integer> intersection(List<Integer> a, List<Integer> b) {
        Set<Integer> inB = new HashSet<>(b);
        return a.stream().filter(inB::contains).toList();
    }
}
