/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

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
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.codec.tsdb.PartitionedDocValues;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class TsidPrefixRangeQuery extends Query {
    private final TsidRange range;

    public TsidPrefixRangeQuery(TsidRange range) {
        this.range = range;
    }

    @Nullable
    public static TsidRange find(@Nullable Query query) {
        if (query instanceof TsidPrefixRangeQuery tsidRange) {
            return tsidRange.range;
        }
        if (query instanceof ConstantScoreQuery constantScore) {
            return find(constantScore.getQuery());
        }
        if (query instanceof BoostQuery boost) {
            return find(boost.getQuery());
        }
        if (query instanceof BooleanQuery bool) {
            for (BooleanClause clause : bool) {
                if (clause.occur() == BooleanClause.Occur.MUST || clause.occur() == BooleanClause.Occur.FILTER) {
                    TsidRange found = find(clause.query());
                    if (found != null) {
                        return found;
                    }
                }
            }
        }
        return null;
    }

    static Map<LeafReaderContext, PrimarySortRange.DocRange> docRanges(List<LeafReaderContext> leaves, TsidRange range) {
        final Map<LeafReaderContext, PrimarySortRange.DocRange> ranges = new IdentityHashMap<>();
        try {
            for (LeafReaderContext leaf : leaves) {
                final SortedDocValues tsid = leaf.reader().getSortedDocValues(TimeSeriesIdFieldMapper.NAME);
                final PrimarySortRange.DocRange docRange = tsid == null ? null : docRange(tsid, leaf.reader().maxDoc(), range);
                if (docRange != null) {
                    ranges.put(leaf, docRange);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return ranges;
    }

    @Nullable
    private static PrimarySortRange.DocRange docRange(SortedDocValues tsid, int maxDoc, TsidRange range) throws IOException {
        if (tsid instanceof PartitionedDocValues partitioned) {
            final PartitionedDocValues.PrefixPartitions partitions = partitioned.prefixPartitions();
            if (partitions != null) {
                final int partitionBits = partitioned.prefixPartitionBits();
                final int minDoc = TsidRange.startDoc(partitions, range.firstPartition(partitions, partitionBits), maxDoc);
                final int endDoc = TsidRange.startDoc(partitions, range.endPartition(partitions, partitionBits), maxDoc);
                return new PrimarySortRange.DocRange(minDoc, Math.max(minDoc, endDoc));
            }
        }
        return null;
    }

    private record Bounds(boolean docs, int min, int max) {
        static final Bounds NONE = new Bounds(true, 0, 0);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
        final Map<Object, Bounds> leafBounds = new ConcurrentHashMap<>();
        return new ConstantScoreWeight(this, boost) {
            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                Bounds bounds = leafBounds.get(context.id());
                SortedDocValues tsid = null;
                if (bounds == null) {
                    tsid = context.reader().getSortedDocValues(TimeSeriesIdFieldMapper.NAME);
                    bounds = tsid == null ? Bounds.NONE : bounds(tsid, context.reader().maxDoc());
                    leafBounds.put(context.id(), bounds);
                }
                if (bounds.min >= bounds.max) {
                    return null;
                }
                final DocIdSetIterator iterator;
                if (bounds.docs) {
                    iterator = DocIdSetIterator.range(bounds.min, bounds.max);
                } else {
                    final SortedDocValues ords = tsid != null ? tsid : context.reader().getSortedDocValues(TimeSeriesIdFieldMapper.NAME);
                    final int minOrd = bounds.min;
                    final int maxOrd = bounds.max;
                    iterator = TwoPhaseIterator.asDocIdSetIterator(new TwoPhaseIterator(ords) {
                        @Override
                        public boolean matches() throws IOException {
                            final int ord = ords.ordValue();
                            return ord >= minOrd && ord < maxOrd;
                        }

                        @Override
                        public float matchCost() {
                            return 2;
                        }
                    });
                }
                return new DefaultScorerSupplier(new ConstantScoreScorer(score(), scoreMode, iterator));
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return false;
            }
        };
    }

    private Bounds bounds(SortedDocValues tsid, int maxDoc) throws IOException {
        final PrimarySortRange.DocRange docRange = docRange(tsid, maxDoc, range);
        if (docRange != null) {
            return new Bounds(true, docRange.minDoc(), docRange.maxDoc());
        }
        return new Bounds(false, ord(tsid, range.lower()), ord(tsid, range.upper()));
    }

    private int ord(SortedDocValues tsid, int prefix) throws IOException {
        if (prefix >= 1 << range.bits()) {
            return tsid.getValueCount();
        }
        final byte[] bytes = new byte[Math.ceilDiv(range.bits(), Byte.SIZE)];
        final int value = prefix << (bytes.length * Byte.SIZE - range.bits());
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) (value >>> ((bytes.length - 1 - i) * Byte.SIZE));
        }
        final int ord = tsid.lookupTerm(new BytesRef(bytes));
        return ord >= 0 ? ord : -ord - 1;
    }

    @Override
    public void visit(QueryVisitor visitor) {
        visitor.visitLeaf(this);
    }

    @Override
    public String toString(String field) {
        return "TsidPrefixRangeQuery[" + range + "]";
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof TsidPrefixRangeQuery that && range.equals(that.range);
    }

    @Override
    public int hashCode() {
        return 31 * classHash() + range.hashCode();
    }
}
