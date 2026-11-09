/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.compute.operator.ProfileSchema;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.codec.tsdb.PartitionedDocValues;
import org.elasticsearch.index.codec.tsdb.PrefixedPartitionsWriter;
import org.elasticsearch.index.codec.tsdb.RandomAccessNumericValues;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * Matches the docs of a time-series shard whose {@code _tsid} is in {@code [lower, upper)}. A {@code null} bound is open.
 * <p>
 *     {@code _tsid} is the primary index sort of a time-series index and has doc values but no index, so the matching docs
 *     of a leaf are one contiguous doc interval, and this query finds it instead of testing every doc. Each leaf's interval
 *     is computed once per query, see {@link #docRange}, coarse to fine:
 * </p>
 * <ol>
 *     <li>the codec's prefix table ({@link PartitionedDocValues}), which gives the start doc of every {@code _tsid} prefix
 *     without reading a doc, and so the cell a bound falls in;</li>
 *     <li>the doc-values skipper, which narrows the cell to one block of docs;</li>
 *     <li>a binary search on doc ids over what is left, comparing ordinals.</li>
 * </ol>
 * <p>
 *     Each step is optional: older indices have no prefix table or skipper and go straight to the binary search over the
 *     whole leaf, which is still logarithmic. {@link Steps} counts, per leaf, which steps ran.
 * </p>
 * <p>
 *     The slice queue does not run this query: it takes it out of the shard query, see {@link #strip}, and cuts each leaf
 *     into slices over the interval only, see {@link #requiredRange}. The rest of the query then runs over those slices as
 *     it would uncut, with one weight for every range of the shard, and no driver gets docs the range cannot match. A
 *     conjunction with this query as a clause would score doc by doc, as the interval is too small a share of the leaf
 *     for Lucene's run-aware bulk scorer, and lose the bulk reads that follow dense doc runs. Every doc of a time-series
 *     index has a {@code _tsid}, so the interval holds no doc without one. On its own the query scores the interval, with
 *     its length as {@link ScorerSupplier#cost()}.
 * </p>
 */
public final class TsidRangeQuery extends Query {
    private static final String FIELD = TimeSeriesIdFieldMapper.NAME;

    @Nullable
    private final BytesRef lower;
    @Nullable
    private final BytesRef upper;
    private final Steps steps;
    /**
     * The interval of each leaf, keyed by {@link LeafReaderContext#id()}. Not part of the query's identity.
     */
    private final Map<Object, DocRange> ranges = new ConcurrentHashMap<>();

    /**
     * @param lower the smallest {@code _tsid} to match, or {@code null} for no lower bound
     * @param upper the first {@code _tsid} not to match, or {@code null} for no upper bound; a range whose
     *              {@code upper} is at or below {@code lower} matches nothing
     * @param steps counters of the steps used to resolve the range in each leaf
     */
    public TsidRangeQuery(@Nullable BytesRef lower, @Nullable BytesRef upper, Steps steps) {
        this.lower = lower;
        this.upper = upper;
        this.steps = steps;
    }

    @Nullable
    public BytesRef lower() {
        return lower;
    }

    @Nullable
    public BytesRef upper() {
        return upper;
    }

    /**
     * The doc interval {@code [minDoc, maxDoc)} of a leaf that this query matches. The slice queue cuts the leaf into slices
     * over this interval and the scorer iterates it. It is resolved once per leaf and shared by the weights of this query,
     * which serve one shard for one request.
     */
    public DocRange docRange(LeafReaderContext context) throws IOException {
        return docRange(context, new LeafResolver(context.reader()));
    }

    /**
     * Like {@link #docRange(LeafReaderContext)}, resolving with {@code resolver}, which holds the leaf's readers and the bounds
     * other ranges of the same leaf already found.
     */
    DocRange docRange(LeafReaderContext context, LeafResolver resolver) throws IOException {
        try {
            return ranges.computeIfAbsent(context.id(), k -> {
                try {
                    return resolver.resolve(lower, upper, steps);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    /**
     * The {@link TsidRangeQuery} that every doc matching {@code query} must match, or {@code null} if there is none. Only
     * required clauses and the wrappers that keep the guarantee are looked through, so the result's intervals are a superset
     * of the docs {@code query} matches and slicing over them is safe.
     */
    @Nullable
    public static TsidRangeQuery requiredRange(Query query) {
        if (query instanceof TsidRangeQuery range) {
            return range;
        }
        if (query instanceof ConstantScoreQuery constantScore) {
            return requiredRange(constantScore.getQuery());
        }
        if (query instanceof BoostQuery boost) {
            return requiredRange(boost.getQuery());
        }
        if (query instanceof BooleanQuery bool) {
            for (BooleanClause clause : bool) {
                if (clause.occur() == BooleanClause.Occur.MUST || clause.occur() == BooleanClause.Occur.FILTER) {
                    TsidRangeQuery range = requiredRange(clause.query());
                    if (range != null) {
                        return range;
                    }
                }
            }
        }
        return null;
    }

    /**
     * {@code query} without the {@link TsidRangeQuery} that {@link #requiredRange} finds, or {@code query} itself when there
     * is none. The slice queue runs the result over the range's intervals, where every doc is in the range already.
     */
    public static Query strip(Query query) {
        if (query instanceof TsidRangeQuery) {
            return new MatchAllDocsQuery();
        }
        if (query instanceof ConstantScoreQuery constantScore) {
            Query inner = strip(constantScore.getQuery());
            return inner == constantScore.getQuery() ? query : new ConstantScoreQuery(inner);
        }
        if (query instanceof BoostQuery boost) {
            Query inner = strip(boost.getQuery());
            return inner == boost.getQuery() ? query : new BoostQuery(inner, boost.getBoost());
        }
        if (query instanceof BooleanQuery bool) {
            BooleanQuery.Builder builder = new BooleanQuery.Builder().setMinimumNumberShouldMatch(bool.getMinimumNumberShouldMatch());
            boolean stripped = false;
            for (BooleanClause clause : bool) {
                boolean required = clause.occur() == BooleanClause.Occur.MUST || clause.occur() == BooleanClause.Occur.FILTER;
                if (stripped == false && required && requiredRange(clause.query()) != null) {
                    stripped = true;
                    Query inner = strip(clause.query());
                    if (inner instanceof MatchAllDocsQuery == false) {
                        builder.add(inner, clause.occur());
                    }
                } else {
                    builder.add(clause);
                }
            }
            if (stripped == false) {
                return query;
            }
            BooleanQuery result = builder.build();
            return result.clauses().isEmpty() ? new MatchAllDocsQuery() : result;
        }
        return query;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
        return new ConstantScoreWeight(this, boost) {
            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                DocRange range = docRange(context);
                if (range.isEmpty()) {
                    return null;
                }
                return new ScorerSupplier() {
                    @Override
                    public Scorer get(long leadCost) {
                        return new ConstantScoreScorer(score(), scoreMode, DocIdSetIterator.range(range.minDoc(), range.maxDoc()));
                    }

                    @Override
                    public long cost() {
                        return range.length();
                    }
                };
            }

            @Override
            public int count(LeafReaderContext context) throws IOException {
                return context.reader().hasDeletions() ? -1 : docRange(context).length();
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                // The interval is cheaper to find than a cached bit set is to read.
                return false;
            }
        };
    }

    /**
     * A doc interval {@code [minDoc, maxDoc)} within a leaf.
     */
    public record DocRange(int minDoc, int maxDoc) {
        static final DocRange EMPTY = new DocRange(0, 0);

        public int length() {
            return maxDoc - minDoc;
        }

        public boolean isEmpty() {
            return minDoc >= maxDoc;
        }
    }

    /**
     * The {@link LeafResolver} of each leaf of a shard, for every range of one request.
     */
    static final class LeafResolvers {
        private final Map<Object, LeafResolver> resolvers = new HashMap<>();

        LeafResolver get(LeafReaderContext context) throws IOException {
            LeafResolver resolver = resolvers.get(context.id());
            if (resolver == null) {
                resolver = new LeafResolver(context.reader());
                resolvers.put(context.id(), resolver);
            }
            return resolver;
        }
    }

    /**
     * Finds the first doc of a leaf whose {@code _tsid} is at or above a key. One instance serves every bound resolved on the
     * leaf: it opens the leaf's readers once and remembers the doc of each key, as adjacent ranges share a bound.
     */
    static final class LeafResolver {
        private final LeafReader reader;
        /**
         * {@code null} when no doc of the leaf has a {@code _tsid}.
         */
        @Nullable
        private final SortedDocValues tsid;
        private final int maxDoc;
        private final Map<BytesRef, Integer> docs = new HashMap<>();
        private boolean prefixTableLoaded;
        @Nullable
        private PartitionedDocValues.PrefixPartitions prefixTable;
        /**
         * Only moves forward, so it serves the keys in order and is reopened for a key below the last one.
         */
        @Nullable
        private DocValuesSkipper skipper;
        @Nullable
        private BytesRef skipperKey;
        private boolean usedPrefixTable;
        private boolean usedSkipper;
        private boolean usedBinarySearch;

        LeafResolver(LeafReader reader) throws IOException {
            this.reader = reader;
            SortedDocValues values = reader.getSortedDocValues(FIELD);
            this.tsid = values == null || values.getValueCount() == 0 ? null : values;
            this.maxDoc = reader.maxDoc();
        }

        /**
         * Finds the interval of {@code [lower, upper)} and records in {@code steps} which steps this call used.
         */
        DocRange resolve(@Nullable BytesRef lower, @Nullable BytesRef upper, Steps steps) throws IOException {
            usedPrefixTable = false;
            usedSkipper = false;
            usedBinarySearch = false;
            steps.leaves.increment();
            if (tsid == null) {
                return DocRange.EMPTY;
            }
            int minDoc = lower == null ? 0 : firstDocAtOrAbove(lower);
            int maxDoc = upper == null ? this.maxDoc : firstDocAtOrAbove(upper);
            if (usedPrefixTable) {
                steps.prefixTable.increment();
            }
            if (usedSkipper) {
                steps.skipper.increment();
            }
            if (usedBinarySearch) {
                steps.binarySearch.increment();
            }
            return new DocRange(minDoc, Math.max(minDoc, maxDoc));
        }

        /**
         * Returns the first doc whose {@code _tsid} is at or above {@code key}, or {@code maxDoc} if there is none.
         */
        int firstDocAtOrAbove(BytesRef key) throws IOException {
            Integer known = docs.get(key);
            if (known != null) {
                return known;
            }
            int doc = find(key);
            docs.put(BytesRef.deepCopyOf(key), doc);
            return doc;
        }

        private int find(BytesRef key) throws IOException {
            // The answer is in [lo, hi], where hi means "no such doc in the searched docs".
            int lo = 0;
            int hi = maxDoc;
            PartitionedDocValues.PrefixPartitions table = prefixTable();
            if (table != null) {
                usedPrefixTable = true;
                int keyPrefix = PrefixedPartitionsWriter.prefix(key);
                int cell = firstCellAtOrAbove(table, keyPrefix);
                if (cell == table.numPartitions()) {
                    return maxDoc;
                }
                if (table.prefixes()[cell] > keyPrefix) {
                    // No tsid has the key's prefix, so the key sorts right before this cell.
                    return table.startDocs()[cell];
                }
                lo = table.startDocs()[cell];
                hi = cell + 1 < table.numPartitions() ? table.startDocs()[cell + 1] : maxDoc;
            }
            if (lo >= hi) {
                return hi;
            }
            int ord = tsid.lookupTerm(key);
            if (ord < 0) {
                ord = -ord - 1;
            }
            if (ord >= tsid.getValueCount()) {
                return hi;
            }
            DocValuesSkipper skipper = skipper(key);
            if (skipper != null) {
                usedSkipper = true;
                if (lo > skipper.maxDocID(0)) {
                    skipper.advance(lo);
                }
                // The first block, from lo on, that can hold a doc with ordinal >= ord. Every doc before it is below.
                skipper.advance(ord, Long.MAX_VALUE);
                if (skipper.minDocID(0) == DocIdSetIterator.NO_MORE_DOCS || skipper.minDocID(0) >= hi) {
                    return hi;
                }
                int blockStart = Math.max(lo, skipper.minDocID(0));
                int blockEnd = Math.min(hi, skipper.maxDocID(0) + 1);
                if (skipper.minValue(0) >= ord) {
                    return blockStart;
                }
                usedBinarySearch = true;
                int doc = binarySearch(blockStart, blockEnd, ord);
                if (doc < blockEnd || blockEnd == hi) {
                    return doc;
                }
                // The block's max value is only an upper bound, so the doc can still be in a later block.
                lo = blockEnd;
            }
            if (lo >= hi) {
                return hi;
            }
            usedBinarySearch = true;
            return binarySearch(lo, hi, ord);
        }

        /**
         * The skipper positioned no later than {@code key}'s doc: the one of the last key when {@code key} is not below it,
         * as every block it passed holds only lower ordinals, otherwise a fresh one.
         */
        @Nullable
        private DocValuesSkipper skipper(BytesRef key) throws IOException {
            if (skipper == null || key.compareTo(skipperKey) < 0) {
                skipper = reader.getDocValuesSkipper(FIELD);
            }
            skipperKey = key;
            return skipper;
        }

        @Nullable
        private PartitionedDocValues.PrefixPartitions prefixTable() throws IOException {
            if (prefixTableLoaded == false) {
                prefixTableLoaded = true;
                if (tsid instanceof PartitionedDocValues partitioned
                    && partitioned.prefixPartitionBits() == PrefixedPartitionsWriter.PARTITION_PREFIX_BITS) {
                    prefixTable = partitioned.prefixPartitions(null);
                }
            }
            return prefixTable;
        }

        /**
         * The first cell whose prefix is at or above {@code keyPrefix}, or {@code numPartitions} if there is none.
         */
        private static int firstCellAtOrAbove(PartitionedDocValues.PrefixPartitions table, int keyPrefix) {
            int cell = Arrays.binarySearch(table.prefixes(), 0, table.numPartitions(), keyPrefix);
            return cell >= 0 ? cell : -cell - 1;
        }

        /**
         * The first doc in {@code [lo, hi)} whose ordinal is at or above {@code ord}, or {@code hi} if there is none.
         */
        private int binarySearch(int lo, int hi, int ord) throws IOException {
            RandomAccessNumericValues ords = tsid instanceof RandomAccessNumericValues.Provider provider
                ? provider.tryRandomAccess()
                : null;
            if (ords != null) {
                while (lo < hi) {
                    int mid = (lo + hi) >>> 1;
                    if (ords.valueAt(mid) >= ord) {
                        hi = mid;
                    } else {
                        lo = mid + 1;
                    }
                }
                return lo;
            }
            SortedDocValues values = tsid;
            while (lo < hi) {
                int mid = (lo + hi) >>> 1;
                if (mid < values.docID()) {
                    // Doc values only move forward; reload them to probe an earlier doc.
                    values = reader.getSortedDocValues(FIELD);
                }
                int doc = values.advanceExact(mid) ? mid : values.nextDoc();
                if (doc >= hi || values.ordValue() >= ord) {
                    hi = mid;
                } else {
                    lo = doc + 1;
                }
            }
            return lo;
        }
    }

    /**
     * Counts of the leaves whose interval each step helped to find. Shared by every {@link TsidRangeQuery} of one source, on
     * every shard, so every driver's status reports the same totals for the source.
     */
    public static final class Steps {
        private final LongAdder leaves = new LongAdder();
        private final LongAdder prefixTable = new LongAdder();
        private final LongAdder skipper = new LongAdder();
        private final LongAdder binarySearch = new LongAdder();

        public Status status() {
            return new Status(leaves.sum(), prefixTable.sum(), skipper.sum(), binarySearch.sum());
        }
    }

    /**
     * A snapshot of {@link Steps}: {@code leaves} is the number of leaf intervals resolved and the other fields the number of
     * them that used the prefix table, the doc-values skipper, and the binary search on doc ids. A leaf can count in several.
     */
    public record Status(long leaves, long prefixTable, long skipper, long binarySearch) implements Writeable, ToXContentObject {
        public static final ProfileSchema<Status> PROFILE_SCHEMA = ProfileSchema.of(
            Status.class,
            (builder, status, params) -> status.writeProfile(builder)
        )
            .required("leaves", ProfileSchema.integer())
            .required("prefix_table", ProfileSchema.integer())
            .required("skipper", ProfileSchema.integer())
            .required("binary_search", ProfileSchema.integer())
            .build();

        public static Status readFrom(StreamInput in) throws IOException {
            return new Status(in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(leaves);
            out.writeVLong(prefixTable);
            out.writeVLong(skipper);
            out.writeVLong(binarySearch);
        }

        private XContentBuilder writeProfile(XContentBuilder builder) throws IOException {
            builder.startObject();
            builder.field("leaves", leaves);
            builder.field("prefix_table", prefixTable);
            builder.field("skipper", skipper);
            builder.field("binary_search", binarySearch);
            return builder.endObject();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return PROFILE_SCHEMA.write(builder, this, params);
        }
    }

    @Override
    public String toString(String field) {
        return FIELD + ":[" + (lower == null ? "*" : lower) + " TO " + (upper == null ? "*" : upper) + ")";
    }

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(FIELD)) {
            visitor.visitLeaf(this);
        }
    }

    @Override
    public boolean equals(Object obj) {
        return sameClassAs(obj)
            && Objects.equals(lower, ((TsidRangeQuery) obj).lower)
            && Objects.equals(upper, ((TsidRangeQuery) obj).upper);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), lower, upper);
    }
}
