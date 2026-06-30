/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query.cache;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.FilterWeight;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCache;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.core.Releasable;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Block-level predicate cache. Stores one skip-bit per ~8K-doc block per predicate.
 * A set bit means "block evaluated, no matches" (safe to skip).
 * Bits only flip 0→1 (one-directional, concurrent-safe).
 */
public final class BlockLevelQueryCache implements QueryCache {
    static int BLOCK_SIZE = 8 * 1024;
    final ConcurrentHashMap<IndexReader.CacheKey, SegmentCache> segmentCaches = new ConcurrentHashMap<>();

    public BlockLevelQueryCache() {

    }

    @Override
    public Weight doCache(Weight weight, QueryCachingPolicy policy) {
        return weight;
    }

    public QueryCache cacheSession(PredicateKeys predicateKeys) {
        return new QueryCacheSession(predicateKeys);
    }

    class QueryCacheSession implements QueryCache, Releasable {
        final PredicateKeys predicateKeys;

        QueryCacheSession(PredicateKeys predicateKeys) {
            this.predicateKeys = predicateKeys;
        }

        @Override
        public Weight doCache(Weight weight, QueryCachingPolicy policy) {
            Query query = weight.getQuery();
            PredicateKey key = predicateKeys.get(query);
            System.err.println("--> creating caching weight key=" + key);
            if (key == null) {
                return weight;
            }
            return new CachingWeight(weight, key);
        }

        @Override
        public void close() {
            // merge caches
        }
    }

    SegmentCache getOrCreateSegmentCache(LeafReaderContext leaf) {
        return segmentCaches.computeIfAbsent(segmentKey(leaf), k -> new SegmentCache(numBlocks(leaf)));
    }


    static int numBlocks(LeafReaderContext leaf) {
        return Math.toIntExact(((long) leaf.reader().maxDoc() + BLOCK_SIZE - 1L) / BLOCK_SIZE);
    }

    private static IndexReader.CacheKey segmentKey(LeafReaderContext leaf) {
        return leaf.reader().getCoreCacheHelper().getKey();
    }

    static final class SegmentCache {
        private final int numBlocks;
        private final ConcurrentHashMap<PredicateKey, FixedBitSet> skipBits = new ConcurrentHashMap<>();

        SegmentCache(int numBlocks) {
            this.numBlocks = numBlocks;
        }

         FixedBitSet getOrCreate(PredicateKey key) {
            return skipBits.computeIfAbsent(key, used -> new FixedBitSet(numBlocks));
        }

         void publish(PredicateKey key, int[] skippingBlocks, int numSkips) {
            if (numSkips == 0) {
                return;
            }
             System.err.println("--> skipping " + numSkips + " blocks");
            final FixedBitSet shared = skipBits.get(key);
            if (shared != null) {
                // using publish queue -> map<>
                synchronized (shared) {
                    for (int i = 0; i < numSkips; i++) {
                        shared.set(skippingBlocks[i]);
                    }
                }
            }
        }
    }

    class CachingWeight extends FilterWeight {
        private final PredicateKey key;

        CachingWeight(Weight weight, PredicateKey key) {
            super(weight);
            this.key = key;
        }

        @Override
        public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
            ScorerSupplier supplier = in.scorerSupplier(context);
            if (supplier == null) {
                return null;
            }
            IndexReader.CacheHelper cacheHelper = context.reader().getCoreCacheHelper();
            if (cacheHelper == null) {
                return supplier;
            }
            SegmentCache segmentCache = getOrCreateSegmentCache(context);
            return new CachingScorerSupplier(supplier, segmentCache, key);
        }
    }

    static class CachingScorerSupplier extends ScorerSupplier {
        private final ScorerSupplier delegate;
        private final SegmentCache segmentCache;
        private final PredicateKey key;

        CachingScorerSupplier(ScorerSupplier delegate, SegmentCache segmentCache, PredicateKey key) {
            this.delegate = delegate;
            this.segmentCache = segmentCache;
            this.key = key;
        }

        @Override
        public BulkScorer bulkScorer() throws IOException {
            return new CachingBulkScorer(super.bulkScorer(), segmentCache, key, segmentCache.getOrCreate(key));
        }

        @Override
        public org.apache.lucene.search.Scorer get(long leadCost) throws IOException {
            // TODO: wrap scorer with block-cache-aware TwoPhaseIterator
            // - reads from shared (skip known-empty blocks)
            // - writes to local (mark newly discovered empty blocks)
            // - on close/finish: segmentCache.publish(key, local)
            return delegate.get(leadCost);
        }

        @Override
        public long cost() {
            return delegate.cost();
        }
    }

    public static class CachingBulkScorer extends BulkScorer {
        private final BulkScorer scorer;
        private final SegmentCache segmentCache;
        private final PredicateKey key;
        private final FixedBitSet skipBits;
        private int[] skippingBlocks;
        private int numSkippingBlocks;

        private int nextDoc;
        private MatchedDocs matchedDocs;

        CachingBulkScorer(BulkScorer scorer, SegmentCache segmentCache, PredicateKey key, FixedBitSet skipBits) {
            this.scorer = scorer;
            this.segmentCache = segmentCache;
            this.key = key;
            this.skippingBlocks = new int[128];
            this.skipBits = skipBits;
        }

        private void addSkippingBlock(int blockIndex) {
            this.skippingBlocks = ArrayUtil.grow(skippingBlocks, numSkippingBlocks + 1);
            skippingBlocks[numSkippingBlocks++] = blockIndex;
        }

        @Override
        public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
            if (matchedDocs == null) {
                matchedDocs = new MatchedDocs(new FixedBitSet(BLOCK_SIZE));
            }
            int doc = min;
            if (doc / BLOCK_SIZE != matchedDocs.blockIndex) {
                doc = Math.max(doc, nextDoc);
            }
            while (doc < max) {
                final int blockIndex = doc / BLOCK_SIZE;
                final int blockEnd = (blockIndex + 1) * BLOCK_SIZE;
                if (matchedDocs.blockIndex != blockIndex) {
                    if (skipBits.get(blockIndex)) {
                        doc = nextDoc = blockEnd;
                        continue;
                    }
                    final int blockStart = blockIndex * BLOCK_SIZE;
                    final int scoreFrom = Math.max(blockStart, nextDoc);
                    matchedDocs.reset(blockIndex, blockStart);
                    doc = nextDoc = scorer.score(matchedDocs, null, scoreFrom, blockEnd);
                    if (matchedDocs.found == false && scoreFrom == blockStart) {
                        addSkippingBlock(blockIndex);
                    }
                }
                if (matchedDocs.found) {
                    int replayFrom = Math.max(min, blockIndex * BLOCK_SIZE);
                    int nextReplay = matchedDocs.replay(collector, acceptDocs, replayFrom, max);
                    if (nextReplay >= max) {
                        return nextReplay;
                    }
                }
                doc = Math.max(doc, blockEnd);
            }
            return nextDoc;
        }

        @Override
        public long cost() {
            return scorer.cost();
        }

        public void publishCache() {
            if (numSkippingBlocks > 0) {
                segmentCache.publish(key, skippingBlocks, numSkippingBlocks);
                numSkippingBlocks = 0;
            }
        }
    }

    static final class MatchedDocs implements LeafCollector {
        int blockIndex = -1;
        int docBase;
        final FixedBitSet bits;
        boolean found = false;

        MatchedDocs(FixedBitSet bits) {
            this.bits = bits;
        }

        @Override
        public void collectRange(int min, int max) {
            bits.set(min - docBase, max - docBase);
            found = true;
        }

        @Override
        public void collect(int doc) {
            bits.set(doc - docBase);
            found = true;
        }

        void reset(int blockIndex, int blockStart) {
            bits.clear(0, bits.length());
            this.blockIndex = blockIndex;
            this.docBase = blockStart;
            this.found = false;
        }

        int replay(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
            final int emitStart = Math.max(0, min - docBase);
            if (acceptDocs != null) {
                for (int doc = bits.nextSetBit(emitStart); doc != -1 && doc < BLOCK_SIZE; doc = bits.nextSetBit(doc + 1)) {
                    int actualDoc = doc + docBase;
                    if (actualDoc >= max) {
                        return actualDoc;
                    }
                    if (acceptDocs.get(actualDoc)) {
                        collector.collect(actualDoc);
                    }
                }
            } else {
                for (int doc = bits.nextSetBit(emitStart); doc != -1 && doc < BLOCK_SIZE; doc = bits.nextSetBit(doc + 1)) {
                    int actualDoc = doc + docBase;
                    if (actualDoc >= max) {
                        return actualDoc;
                    }
                    collector.collect(actualDoc);
                }
            }
            return -1;
        }

        @Override
        public void setScorer(Scorable scorer) {}
    }
}
