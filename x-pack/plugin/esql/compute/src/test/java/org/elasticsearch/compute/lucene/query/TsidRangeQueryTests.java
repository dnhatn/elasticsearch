/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.codec.tsdb.BinaryDVCompressionMode;
import org.elasticsearch.index.codec.tsdb.es819.ES819Version3TSDBDocValuesFormat;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Checks {@link TsidRangeQuery} against a scan of the {@code _tsid} doc values, on indices written with and without the
 * prefix table and the doc-values skipper, so that every step of the range resolution runs, alone and together.
 */
public class TsidRangeQueryTests extends ESTestCase {
    /**
     * The layout of a test index: which of the structures {@link TsidRangeQuery} can use are written.
     */
    record Layout(boolean prefixTable, boolean skipper) {
        static Layout random() {
            return new Layout(randomBoolean(), randomBoolean());
        }
    }

    /**
     * A test index sorted on {@code _tsid}, then {@code @timestamp} descending, in several segments.
     */
    static final class TsidIndex implements AutoCloseable {
        final Directory directory;
        final IndexReader reader;
        final NavigableSet<BytesRef> tsids;

        TsidIndex(Directory directory, IndexReader reader, NavigableSet<BytesRef> tsids) {
            this.directory = directory;
            this.reader = reader;
            this.tsids = tsids;
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(reader, directory);
        }
    }

    /**
     * Writes {@code numSeries} series, each with a few docs, spread over several segments. Series are random 16 byte
     * {@code _tsid}s like the real ones: a first byte from a few values, then hash bytes. Merges are off so the segments
     * stay as written.
     */
    static TsidIndex tsidIndex(Layout layout, int numSeries) throws IOException {
        Directory directory = newDirectory();
        // Small skip-index blocks and sometimes range-encoded ordinals, so that the skipper narrows the search and
        // both ordinal layouts, with and without random access, are read.
        ES819Version3TSDBDocValuesFormat format = new ES819Version3TSDBDocValuesFormat(
            between(4, 16),
            between(1, 32),
            randomBoolean(),
            randomFrom(BinaryDVCompressionMode.values()),
            randomBoolean(),
            randomBoolean() ? 7 : 9, // the default and the large numeric block shifts of the format
            layout.prefixTable()
        );
        IndexWriterConfig config = newIndexWriterConfig().setCodec(TestUtil.alwaysDocValuesFormat(format))
            .setIndexSort(
                new Sort(
                    new SortField(TimeSeriesIdFieldMapper.NAME, SortField.Type.STRING),
                    new SortedNumericSortField("@timestamp", SortField.Type.LONG, true)
                )
            )
            .setMergePolicy(NoMergePolicy.INSTANCE)
            // only the commits below make segments
            .setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH)
            .setRAMBufferSizeMB(1024);
        NavigableSet<BytesRef> tsids = new TreeSet<>();
        byte[] firstBytes = randomByteArrayOfLength(between(1, 3));
        boolean fewPrefixes = randomBoolean();
        while (tsids.size() < numSeries) {
            byte[] tsid = randomByteArrayOfLength(16);
            tsid[0] = firstBytes[between(0, firstBytes.length - 1)];
            if (fewPrefixes) {
                tsid[1] = (byte) between(0, 2);
            }
            tsids.add(new BytesRef(tsid));
        }
        record Doc(BytesRef tsid, long timestamp) {}
        List<Doc> docs = new ArrayList<>();
        for (BytesRef tsid : tsids) {
            int numDocs = between(1, 10);
            for (int i = 0; i < numDocs; i++) {
                docs.add(new Doc(tsid, randomLongBetween(0, 1_000_000)));
            }
        }
        Randomness.shuffle(docs);
        int docsPerSegment = Math.max(1, docs.size() / between(2, 6));
        try (IndexWriter writer = new IndexWriter(directory, config)) {
            for (int i = 0; i < docs.size(); i++) {
                Document doc = new Document();
                doc.add(
                    layout.skipper()
                        ? SortedDocValuesField.indexedField(TimeSeriesIdFieldMapper.NAME, docs.get(i).tsid())
                        : new SortedDocValuesField(TimeSeriesIdFieldMapper.NAME, docs.get(i).tsid())
                );
                doc.add(new SortedNumericDocValuesField("@timestamp", docs.get(i).timestamp()));
                writer.addDocument(doc);
                if ((i + 1) % docsPerSegment == 0) {
                    writer.commit();
                }
            }
            writer.commit();
        }
        return new TsidIndex(directory, DirectoryReader.open(directory), tsids);
    }

    /**
     * The docs of {@code reader} that {@code query} matches, as {@code docBase + doc}.
     */
    static List<Integer> matches(IndexReader reader, Query query) throws IOException {
        List<Integer> docs = new ArrayList<>();
        plainSearcher(reader).search(new ConstantScoreQuery(query), new SimpleCollector() {
            private int docBase;

            @Override
            protected void doSetNextReader(LeafReaderContext context) {
                docBase = context.docBase;
            }

            @Override
            public void collect(int doc) {
                docs.add(docBase + doc);
            }

            @Override
            public void setScorer(Scorable scorer) {}

            @Override
            public ScoreMode scoreMode() {
                return ScoreMode.COMPLETE_NO_SCORES;
            }
        });
        return docs;
    }

    /**
     * The docs of {@code reader} whose {@code _tsid} is in {@code [lower, upper)}, found by reading every doc.
     */
    static List<Integer> scan(IndexReader reader, @Nullable BytesRef lower, @Nullable BytesRef upper) throws IOException {
        List<Integer> docs = new ArrayList<>();
        for (LeafReaderContext leaf : reader.leaves()) {
            SortedDocValues tsid = leaf.reader().getSortedDocValues(TimeSeriesIdFieldMapper.NAME);
            for (int doc = 0; doc < leaf.reader().maxDoc(); doc++) {
                assertTrue(tsid.advanceExact(doc));
                BytesRef value = tsid.lookupOrd(tsid.ordValue());
                if ((lower == null || value.compareTo(lower) >= 0) && (upper == null || value.compareTo(upper) < 0)) {
                    docs.add(leaf.docBase + doc);
                }
            }
        }
        return docs;
    }

    /**
     * A searcher without a query cache, so that each search resolves the ranges again and the step counts are predictable.
     */
    static IndexSearcher plainSearcher(IndexReader reader) {
        IndexSearcher searcher = new IndexSearcher(reader);
        searcher.setQueryCache(null);
        return searcher;
    }

    /**
     * A random bound: open, an existing {@code _tsid}, or random bytes that can fall between, below, or above them.
     */
    @Nullable
    private static BytesRef randomBound(NavigableSet<BytesRef> tsids) {
        return switch (between(0, 2)) {
            case 0 -> null;
            case 1 -> randomFrom(tsids);
            case 2 -> new BytesRef(randomByteArrayOfLength(between(0, 17)));
            default -> throw new AssertionError();
        };
    }

    public void testMatchesScan() throws IOException {
        Layout layout = Layout.random();
        try (TsidIndex index = tsidIndex(layout, between(50, 300))) {
            assertThat(index.reader.leaves().size(), greaterThan(1));
            TsidRangeQuery.Steps steps = new TsidRangeQuery.Steps();
            int iterations = between(10, 50);
            for (int i = 0; i < iterations; i++) {
                BytesRef lower = randomBound(index.tsids);
                BytesRef upper = randomBound(index.tsids);
                if (lower != null && upper != null && lower.compareTo(upper) > 0 && randomBoolean()) {
                    // keep lower <= upper most of the time so that the ranges are not mostly empty
                    BytesRef swap = lower;
                    lower = upper;
                    upper = swap;
                }
                TsidRangeQuery query = new TsidRangeQuery(lower, upper, steps);
                List<Integer> expected = scan(index.reader, lower, upper);
                assertThat(query.toString(), matches(index.reader, query), equalTo(expected));
                assertThat(query.toString(), plainSearcher(index.reader).count(query), equalTo(expected.size()));
            }
            TsidRangeQuery.Status status = steps.status();
            // Each query resolves each leaf once; the count after the search reuses it.
            assertThat(status.leaves(), equalTo((long) iterations * index.reader.leaves().size()));
            assertThat(status.prefixTable() > 0, equalTo(layout.prefixTable()));
            assertThat(status.skipper() > 0, equalTo(layout.skipper()));
            if (layout.prefixTable() == false && layout.skipper() == false) {
                assertThat(status.binarySearch(), greaterThan(0L));
            }
        }
    }

    public void testEmptyAndFullRanges() throws IOException {
        try (TsidIndex index = tsidIndex(Layout.random(), between(10, 50))) {
            BytesRef key = randomFrom(index.tsids);
            TsidRangeQuery.Steps steps = new TsidRangeQuery.Steps();
            assertThat(matches(index.reader, new TsidRangeQuery(key, key, steps)), equalTo(List.of()));
            assertThat(matches(index.reader, new TsidRangeQuery(index.tsids.last(), index.tsids.first(), steps)), equalTo(List.of()));
            // Keys below and above every tsid: the empty key, and 17 bytes of 0xFF, which no 16 byte tsid reaches.
            byte[] max = new byte[17];
            Arrays.fill(max, (byte) 0xFF);
            assertThat(matches(index.reader, new TsidRangeQuery(null, new BytesRef(), steps)), equalTo(List.of()));
            assertThat(matches(index.reader, new TsidRangeQuery(new BytesRef(max), null, steps)), equalTo(List.of()));
            // The whole index, through open bounds, through the first tsid, and through the keys outside.
            List<Integer> all = scan(index.reader, null, null);
            assertThat(matches(index.reader, new TsidRangeQuery(null, null, steps)), equalTo(all));
            assertThat(matches(index.reader, new TsidRangeQuery(index.tsids.first(), null, steps)), equalTo(all));
            assertThat(matches(index.reader, new TsidRangeQuery(new BytesRef(), new BytesRef(max), steps)), equalTo(all));
        }
    }

    public void testEqualsAndHashCode() {
        TsidRangeQuery.Steps steps = new TsidRangeQuery.Steps();
        BytesRef a = new BytesRef("a");
        BytesRef b = new BytesRef("b");
        assertThat(new TsidRangeQuery(a, b, steps), equalTo(new TsidRangeQuery(BytesRef.deepCopyOf(a), b, new TsidRangeQuery.Steps())));
        assertThat(new TsidRangeQuery(a, b, steps).hashCode(), equalTo(new TsidRangeQuery(a, b, steps).hashCode()));
        assertFalse(new TsidRangeQuery(a, b, steps).equals(new TsidRangeQuery(a, null, steps)));
        assertFalse(new TsidRangeQuery(a, b, steps).equals(new TsidRangeQuery(null, b, steps)));
        assertThat(new TsidRangeQuery(null, null, steps).toString(), equalTo("_tsid:[* TO *)"));
    }

    public void testStrip() {
        TsidRangeQuery range = new TsidRangeQuery(new BytesRef("a"), new BytesRef("b"), new TsidRangeQuery.Steps());
        Query term = new TermQuery(new Term("f", "v"));
        Query other = new TermQuery(new Term("f", "w"));
        assertThat(TsidRangeQuery.strip(range), equalTo(new MatchAllDocsQuery()));
        assertThat(TsidRangeQuery.strip(new ConstantScoreQuery(range)), equalTo(new ConstantScoreQuery(new MatchAllDocsQuery())));
        assertThat(TsidRangeQuery.strip(new BoostQuery(range, 2f)), equalTo(new BoostQuery(new MatchAllDocsQuery(), 2f)));
        assertThat(
            TsidRangeQuery.strip(
                new BooleanQuery.Builder().add(term, BooleanClause.Occur.MUST).add(range, BooleanClause.Occur.FILTER).build()
            ),
            equalTo(new BooleanQuery.Builder().add(term, BooleanClause.Occur.MUST).build())
        );
        assertThat(
            TsidRangeQuery.strip(
                new BooleanQuery.Builder().add(range, BooleanClause.Occur.FILTER).add(other, BooleanClause.Occur.MUST_NOT).build()
            ),
            equalTo(new BooleanQuery.Builder().add(other, BooleanClause.Occur.MUST_NOT).build())
        );
        Query nested = new BooleanQuery.Builder().add(
            new BooleanQuery.Builder().add(term, BooleanClause.Occur.FILTER).add(range, BooleanClause.Occur.FILTER).build(),
            BooleanClause.Occur.MUST
        ).add(other, BooleanClause.Occur.SHOULD).build();
        assertThat(
            TsidRangeQuery.strip(nested),
            equalTo(
                new BooleanQuery.Builder().add(
                    new BooleanQuery.Builder().add(term, BooleanClause.Occur.FILTER).build(),
                    BooleanClause.Occur.MUST
                ).add(other, BooleanClause.Occur.SHOULD).build()
            )
        );
        Query noRange = new BooleanQuery.Builder().add(term, BooleanClause.Occur.MUST).add(range, BooleanClause.Occur.MUST_NOT).build();
        assertThat(TsidRangeQuery.strip(noRange), sameInstance(noRange));
        assertThat(TsidRangeQuery.strip(term), sameInstance(term));
    }
}
