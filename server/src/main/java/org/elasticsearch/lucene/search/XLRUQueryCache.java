/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.lucene.search;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.ConstantScoreScorerSupplier;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCache;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.RoaringDocIdSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;

import static org.apache.lucene.util.RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY;
import static org.apache.lucene.util.RamUsageEstimator.LINKED_HASHTABLE_RAM_BYTES_PER_ENTRY;
import static org.apache.lucene.util.RamUsageEstimator.QUERY_DEFAULT_RAM_BYTES_USED;

/**
 * A {@link QueryCache} that evicts queries using a LRU (least-recently-used) eviction policy in
 * order to remain under a given maximum size and number of bytes used.
 *
 * <p>This class is thread-safe.
 *
 * <p>Note that query eviction runs in linear time with the total number of segments that have cache
 * entries so this cache works best with {@link QueryCachingPolicy caching policies} that only cache
 * on "large" segments, and it is advised to not share this cache across too many indices.
 *
 * <p>A default query cache and policy instance is used in IndexSearcher. If you want to replace
 * those defaults it is typically done like this:
 *
 * <pre class="prettyprint">
 *   final int maxNumberOfCachedQueries = 256;
 *   final long maxRamBytesUsed = 50 * 1024L * 1024L; // 50MB
 *   // these cache and policy instances can be shared across several queries and readers
 *   // it is fine to eg. store them into static variables
 *   final QueryCache queryCache = new LRUQueryCache(maxNumberOfCachedQueries, maxRamBytesUsed);
 *   final QueryCachingPolicy defaultCachingPolicy = new UsageTrackingQueryCachingPolicy();
 *   indexSearcher.setQueryCache(queryCache);
 *   indexSearcher.setQueryCachingPolicy(defaultCachingPolicy);
 * </pre>
 *
 * This cache exposes some global statistics ({@link #getHitCount() hit count}, {@link
 * #getMissCount() miss count}, {@link #getCacheSize() number of cache entries}, {@link
 * #getCacheCount() total number of DocIdSets that have ever been cached}, {@link
 * #getEvictionCount() number of evicted entries}). In case you would like to have more fine-grained
 * statistics, such as per-index or per-query-class statistics, it is possible to override various
 * callbacks: {@link #onHit}, {@link #onMiss}, {@link #onQueryCache}, {@link #onQueryEviction},
 * {@link #onDocIdSetCache}, {@link #onDocIdSetEviction} and {@link #onClear}. It is better to not
 * perform heavy computations in these methods though since they are called synchronously and under
 * a lock.
 */
// TODO: Remove this fork one after Lucene 10.5 or Lucene 11
public class XLRUQueryCache implements QueryCache, Accountable {

    private final int maxSize;
    private final long maxRamBytesUsed;
    private final Predicate<LeafReaderContext> leavesToCache;
    // maps queries that are contained in the cache to a singleton so that this
    // cache does not store several copies of the same query
    private final Map<Query, Query> uniqueQueries;
    // The contract between this set and the per-leaf caches is that per-leaf caches
    // are only allowed to store sub-sets of the queries that are contained in
    // mostRecentlyUsedQueries. This is why write operations are performed under a lock
    private final Set<Query> mostRecentlyUsedQueries;
    private final Map<IndexReader.CacheKey, LeafCache> cache;
    private final ReentrantReadWriteLock.ReadLock readLock;
    private final ReentrantReadWriteLock.WriteLock writeLock;
    private volatile float skipCacheFactor;
    private final LongAdder hitCount;
    private final LongAdder missCount;

    // these variables are volatile so that we do not need to sync reads
    // but increments need to be performed under the lock
    private volatile long ramBytesUsed;
    private volatile long cacheCount;
    private volatile long cacheSize;

    /**
     * Expert: Create a new instance that will cache at most <code>maxSize</code> queries with at most
     * <code>maxRamBytesUsed</code> bytes of memory, only on leaves that satisfy {@code
     * leavesToCache}.
     *
     * <p>Also, clauses whose cost is {@code skipCacheFactor} times more than the cost of the
     * top-level query will not be cached in order to not slow down queries too much.
     */
    public XLRUQueryCache(int maxSize, long maxRamBytesUsed, Predicate<LeafReaderContext> leavesToCache, float skipCacheFactor) {
        this.maxSize = maxSize;
        this.maxRamBytesUsed = maxRamBytesUsed;
        this.leavesToCache = leavesToCache;
        if (skipCacheFactor >= 1 == false) { // NaN >= 1 evaluates false
            throw new IllegalArgumentException("skipCacheFactor must be no less than 1, get " + skipCacheFactor);
        }
        this.skipCacheFactor = skipCacheFactor;

        // Note that reads on this LinkedHashMap trigger modifications on the linked list under the
        // hood, so reading from multiple threads is not thread-safe. This is why it is wrapped in a
        // Collections#synchronizedMap.
        uniqueQueries = Collections.synchronizedMap(new LinkedHashMap<>(16, 0.75f, true));
        mostRecentlyUsedQueries = uniqueQueries.keySet();
        cache = new IdentityHashMap<>();
        ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        writeLock = lock.writeLock();
        readLock = lock.readLock();
        ramBytesUsed = 0;
        hitCount = new LongAdder();
        missCount = new LongAdder();
    }

    /**
     * Get the skip cache factor
     *
     * @return #setSkipCacheFactor
     */
    public float getSkipCacheFactor() {
        return skipCacheFactor;
    }

    /**
     * This setter enables the skipCacheFactor to be updated dynamically.
     *
     * @param skipCacheFactor clauses whose cost is {@code skipCacheFactor} times more than the cost
     *     of the top-level query will not be cached in order to not slow down queries too much.
     */
    public void setSkipCacheFactor(float skipCacheFactor) {
        this.skipCacheFactor = skipCacheFactor;
    }

    /**
     * Create a new instance that will cache at most <code>maxSize</code> queries with at most <code>
     * maxRamBytesUsed</code> bytes of memory. Queries will only be cached on leaves that have more
     * than 10k documents and have more than half of the average documents per leave of the index.
     * This should guarantee that all leaves from the upper {@link TieredMergePolicy tier} will be
     * cached. Only clauses whose cost is at most 100x the cost of the top-level query will be cached
     * in order to not hurt latency too much because of caching.
     */
    public XLRUQueryCache(int maxSize, long maxRamBytesUsed) {
        this(maxSize, maxRamBytesUsed, new MinSegmentSizePredicate(10000), 10);
    }

    // pkg-private for testing
    static class MinSegmentSizePredicate implements Predicate<LeafReaderContext> {
        private final int minSize;

        MinSegmentSizePredicate(int minSize) {
            this.minSize = minSize;
        }

        @Override
        public boolean test(LeafReaderContext context) {
            final int maxDoc = context.reader().maxDoc();
            if (maxDoc < minSize) {
                return false;
            }
            final IndexReaderContext topLevelContext = ReaderUtil.getTopLevelContext(context);
            final int averageTotalDocs = topLevelContext.reader().maxDoc() / topLevelContext.leaves().size();
            return maxDoc * 2 > averageTotalDocs;
        }
    }

    /**
     * Expert: callback when there is a cache hit on a given query. Implementing this method is
     * typically useful in order to compute more fine-grained statistics about the query cache.
     */
    protected void onHit(Object readerCoreKey, Query query) {
        hitCount.add(1);
    }

    /**
     * Expert: callback when there is a cache miss on a given query.
     */
    protected void onMiss(Object readerCoreKey, Query query) {
        assert query != null;
        missCount.add(1);
    }

    /**
     * Expert: callback when a query is added to this cache. Implementing this method is typically
     * useful in order to compute more fine-grained statistics about the query cache.
     */
    protected void onQueryCache(Query query, long ramBytesUsed) {
        assert writeLock.isHeldByCurrentThread();
        this.ramBytesUsed += ramBytesUsed;
    }

    /**
     * Expert: callback when a query is evicted from this cache.
     */
    protected void onQueryEviction(Query query, long ramBytesUsed) {
        assert writeLock.isHeldByCurrentThread();
        this.ramBytesUsed -= ramBytesUsed;
    }

    /**
     * Expert: callback when a {@link DocIdSet} is added to this cache. Implementing this method is
     * typically useful in order to compute more fine-grained statistics about the query cache.
     */
    protected void onDocIdSetCache(Object readerCoreKey, long ramBytesUsed) {
        assert writeLock.isHeldByCurrentThread();
        cacheSize += 1;
        cacheCount += 1;
        this.ramBytesUsed += ramBytesUsed;
    }

    /**
     * Expert: callback when one or more {@link DocIdSet}s are removed from this cache.
     */
    protected void onDocIdSetEviction(Object readerCoreKey, int numEntries, long sumRamBytesUsed) {
        assert writeLock.isHeldByCurrentThread();
        this.ramBytesUsed -= sumRamBytesUsed;
        cacheSize -= numEntries;
    }

    /**
     * Expert: callback when the cache is completely cleared.
     */
    protected void onClear() {
        assert writeLock.isHeldByCurrentThread();
        ramBytesUsed = 0;
        cacheSize = 0;
    }

    /** Whether evictions are required. */
    boolean requiresEviction() {
        assert writeLock.isHeldByCurrentThread();
        final int size = mostRecentlyUsedQueries.size();
        if (size == 0) {
            return false;
        } else {
            return size > maxSize || ramBytesUsed() > maxRamBytesUsed;
        }
    }

    CacheAndCount get(Query key, IndexReader.CacheHelper cacheHelper) {
        assert key instanceof BoostQuery == false;
        assert key instanceof ConstantScoreQuery == false;
        final IndexReader.CacheKey readerKey = cacheHelper.getKey();
        final LeafCache leafCache = cache.get(readerKey);
        if (leafCache == null) {
            onMiss(readerKey, key);
            return null;
        }
        // this get call moves the query to the most-recently-used position
        final Query singleton = uniqueQueries.get(key);
        if (singleton == null) {
            onMiss(readerKey, key);
            return null;
        }
        final CacheAndCount cached = leafCache.get(singleton);
        if (cached == null) {
            onMiss(readerKey, singleton);
        } else {
            onHit(readerKey, singleton);
        }
        return cached;
    }

    private void putIfAbsent(Query query, CacheAndCount cached, IndexReader.CacheHelper cacheHelper) {
        assert query instanceof BoostQuery == false;
        assert query instanceof ConstantScoreQuery == false;
        // under a lock to make sure that mostRecentlyUsedQueries and cache remain sync'ed
        writeLock.lock();
        try {
            Query singleton = uniqueQueries.putIfAbsent(query, query);
            if (singleton == null) {
                onQueryCache(query, getRamBytesUsed(query));
            } else {
                query = singleton;
            }
            final IndexReader.CacheKey key = cacheHelper.getKey();
            LeafCache leafCache = cache.get(key);
            if (leafCache == null) {
                leafCache = new LeafCache(key);
                final LeafCache previous = cache.put(key, leafCache);
                ramBytesUsed += HASHTABLE_RAM_BYTES_PER_ENTRY;
                assert previous == null;
                // we just created a new leaf cache, need to register a close listener
                cacheHelper.addClosedListener(this::clearCoreCacheKey);
            }
            leafCache.putIfAbsent(query, cached);
            evictIfNecessary();
        } finally {
            writeLock.unlock();
        }
    }

    private void evictIfNecessary() {
        assert writeLock.isHeldByCurrentThread();
        // under a lock to make sure that mostRecentlyUsedQueries and cache keep sync'ed
        if (requiresEviction()) {

            Iterator<Query> iterator = mostRecentlyUsedQueries.iterator();
            do {
                final Query query = iterator.next();
                final int size = mostRecentlyUsedQueries.size();
                iterator.remove();
                if (size == mostRecentlyUsedQueries.size()) {
                    // size did not decrease, because the hash of the query changed since it has been
                    // put into the cache
                    throw new ConcurrentModificationException(
                        "Removal from the cache failed! This "
                            + "is probably due to a query which has been modified after having been put into "
                            + " the cache or a badly implemented clone(). Query class: ["
                            + query.getClass()
                            + "], query: ["
                            + query
                            + "]"
                    );
                }
                onEviction(query);
            } while (iterator.hasNext() && requiresEviction());
        }
    }

    /** Remove all cache entries for the given core cache key. */
    public void clearCoreCacheKey(Object coreKey) {
        writeLock.lock();
        try {
            final LeafCache leafCache = cache.remove(coreKey);
            if (leafCache != null) {
                ramBytesUsed -= HASHTABLE_RAM_BYTES_PER_ENTRY;
                final int numEntries = leafCache.cache.size();
                if (numEntries > 0) {
                    onDocIdSetEviction(coreKey, numEntries, leafCache.ramBytesUsed);
                } else {
                    assert numEntries == 0;
                    assert leafCache.ramBytesUsed == 0;
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    /** Remove all cache entries for the given query. */
    public void clearQuery(Query query) {
        writeLock.lock();
        try {
            final Query singleton = uniqueQueries.remove(query);
            if (singleton != null) {
                onEviction(singleton);
            }
        } finally {
            writeLock.unlock();
        }
    }

    private void onEviction(Query singleton) {
        assert writeLock.isHeldByCurrentThread();
        onQueryEviction(singleton, getRamBytesUsed(singleton));
        for (LeafCache leafCache : cache.values()) {
            leafCache.remove(singleton);
        }
    }

    /** Clear the content of this cache. */
    public void clear() {
        writeLock.lock();
        try {
            cache.clear();
            // Note that this also clears the uniqueQueries map since mostRecentlyUsedQueries is the
            // uniqueQueries.keySet view:
            mostRecentlyUsedQueries.clear();
            onClear();
        } finally {
            writeLock.unlock();
        }
    }

    private static long getRamBytesUsed(Query query) {
        return LINKED_HASHTABLE_RAM_BYTES_PER_ENTRY + (query instanceof Accountable accountableQuery
            ? accountableQuery.ramBytesUsed()
            : QUERY_DEFAULT_RAM_BYTES_USED);
    }

    // pkg-private for testing
    void assertConsistent() {
        writeLock.lock();
        try {
            if (requiresEviction()) {
                throw new AssertionError(
                    "requires evictions: size="
                        + mostRecentlyUsedQueries.size()
                        + ", maxSize="
                        + maxSize
                        + ", ramBytesUsed="
                        + ramBytesUsed()
                        + ", maxRamBytesUsed="
                        + maxRamBytesUsed
                );
            }
            for (LeafCache leafCache : cache.values()) {
                Set<Query> keys = Collections.newSetFromMap(new IdentityHashMap<>());
                keys.addAll(leafCache.cache.keySet());
                keys.removeAll(mostRecentlyUsedQueries);
                if (keys.isEmpty() == false) {
                    throw new AssertionError("One leaf cache contains more keys than the top-level cache: " + keys);
                }
            }
            long recomputedRamBytesUsed = HASHTABLE_RAM_BYTES_PER_ENTRY * cache.size();
            for (Query query : mostRecentlyUsedQueries) {
                recomputedRamBytesUsed += getRamBytesUsed(query);
            }
            for (LeafCache leafCache : cache.values()) {
                recomputedRamBytesUsed += HASHTABLE_RAM_BYTES_PER_ENTRY * leafCache.cache.size();
                for (CacheAndCount cached : leafCache.cache.values()) {
                    recomputedRamBytesUsed += cached.ramBytesUsed();
                }
            }
            if (recomputedRamBytesUsed != ramBytesUsed) {
                throw new AssertionError("ramBytesUsed mismatch : " + ramBytesUsed + " != " + recomputedRamBytesUsed);
            }

            long recomputedCacheSize = 0;
            for (LeafCache leafCache : cache.values()) {
                recomputedCacheSize += leafCache.cache.size();
            }
            if (recomputedCacheSize != getCacheSize()) {
                throw new AssertionError("cacheSize mismatch : " + getCacheSize() + " != " + recomputedCacheSize);
            }
        } finally {
            writeLock.unlock();
        }
    }

    // pkg-private for testing
    // return the list of cached queries in LRU order
    List<Query> cachedQueries() {
        readLock.lock();
        try {
            return new ArrayList<>(mostRecentlyUsedQueries);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Weight doCache(Weight weight, QueryCachingPolicy policy) {
        while (weight instanceof CachingWrapperWeight) {
            weight = ((CachingWrapperWeight) weight).in;
        }

        return new CachingWrapperWeight(weight, policy);
    }

    @Override
    public long ramBytesUsed() {
        return ramBytesUsed;
    }

    @Override
    public Collection<Accountable> getChildResources() {
        writeLock.lock();
        try {
            return Accountables.namedAccountables("segment", cache);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Default cache implementation: uses {@link RoaringDocIdSet} for sets that have a density &lt; 1%
     * and a {@link BitDocIdSet} over a {@link FixedBitSet} otherwise.
     */
    protected CacheAndCount cacheImpl(BulkScorer scorer, int maxDoc) throws IOException {
        if (scorer.cost() * 100 >= maxDoc) {
            // FixedBitSet is faster for dense sets and will enable the random-access
            // optimization in ConjunctionDISI
            return cacheIntoBitSet(scorer, maxDoc);
        } else {
            return cacheIntoRoaringDocIdSet(scorer, maxDoc);
        }
    }

    /**
     * Populates the cache for the given scorer and leaf reader context.
     *
     * <p>Subclasses can override this method to implement custom caching strategies, such as
     * deferring cache population to a background thread or skipping it when other threads are already
     * populating the cache. This can be useful for intra-segment searches.
     *
     * @return the cached result if available; otherwise {@code null}
     */
    protected CacheAndCount tryPopulateCache(
        IndexReader.CacheHelper cacheKey,
        Weight weight,
        ScorerSupplier scorerSupplier,
        LeafReaderContext context
    ) throws IOException {
        final CacheAndCount cached = cacheImpl(scorerSupplier.bulkScorer(), context.reader().maxDoc());
        putIfAbsent(weight.getQuery(), cached, cacheKey);
        return cached;
    }

    private static CacheAndCount cacheIntoBitSet(BulkScorer scorer, int maxDoc) throws IOException {
        final FixedBitSet bitSet = new FixedBitSet(maxDoc);
        int[] count = new int[1];
        scorer.score(new LeafCollector() {

            @Override
            public void setScorer(Scorable scorer) throws IOException {}

            @Override
            public void collect(int doc) throws IOException {
                count[0]++;
                bitSet.set(doc);
            }
        }, null, 0, DocIdSetIterator.NO_MORE_DOCS);
        return new CacheAndCount(new BitDocIdSet(bitSet, count[0]), count[0]);
    }

    private static CacheAndCount cacheIntoRoaringDocIdSet(BulkScorer scorer, int maxDoc) throws IOException {
        RoaringDocIdSet.Builder builder = new RoaringDocIdSet.Builder(maxDoc);
        scorer.score(new LeafCollector() {

            @Override
            public void setScorer(Scorable scorer) throws IOException {}

            @Override
            public void collect(int doc) throws IOException {
                builder.add(doc);
            }
        }, null, 0, DocIdSetIterator.NO_MORE_DOCS);
        RoaringDocIdSet cache = builder.build();
        return new CacheAndCount(cache, cache.cardinality());
    }

    /**
     * Return the total number of times that a {@link Query} has been looked up in this {@link
     * QueryCache}. Note that this number is incremented once per segment so running a cached query
     * only once will increment this counter by the number of segments that are wrapped by the
     * searcher. Note that by definition, {@link #getTotalCount()} is the sum of {@link
     * #getHitCount()} and {@link #getMissCount()}.
     *
     * @see #getHitCount()
     * @see #getMissCount()
     */
    public final long getTotalCount() {
        return getHitCount() + getMissCount();
    }

    /**
     * Over the {@link #getTotalCount() total} number of times that a query has been looked up, return
     * how many times a cached {@link DocIdSet} has been found and returned.
     *
     * @see #getTotalCount()
     * @see #getMissCount()
     */
    public final long getHitCount() {
        return hitCount.sum();
    }

    /**
     * Over the {@link #getTotalCount() total} number of times that a query has been looked up, return
     * how many times this query was not contained in the cache.
     *
     * @see #getTotalCount()
     * @see #getHitCount()
     */
    public final long getMissCount() {
        return missCount.sum();
    }

    /**
     * Return the total number of {@link DocIdSet}s which are currently stored in the cache.
     *
     * @see #getCacheCount()
     * @see #getEvictionCount()
     */
    public final long getCacheSize() {
        return cacheSize;
    }

    /**
     * Return the total number of cache entries that have been generated and put in the cache. It is
     * highly desirable to have a {@link #getHitCount() hit count} that is much higher than the {@link
     * #getCacheCount() cache count} as the opposite would indicate that the query cache makes efforts
     * in order to cache queries but then they do not get reused.
     *
     * @see #getCacheSize()
     * @see #getEvictionCount()
     */
    public final long getCacheCount() {
        return cacheCount;
    }

    /**
     * Return the number of cache entries that have been removed from the cache either in order to
     * stay under the maximum configured size/ram usage, or because a segment has been closed. High
     * numbers of evictions might mean that queries are not reused or that the {@link
     * QueryCachingPolicy caching policy} caches too aggressively on NRT segments which get merged
     * early.
     *
     * @see #getCacheCount()
     * @see #getCacheSize()
     */
    public final long getEvictionCount() {
        return getCacheCount() - getCacheSize();
    }

    // this class is not thread-safe, everything but ramBytesUsed needs to be called under a lock
    private class LeafCache implements Accountable {

        private final Object key;
        private final Map<Query, CacheAndCount> cache;
        private volatile long ramBytesUsed;

        LeafCache(Object key) {
            this.key = key;
            cache = new IdentityHashMap<>();
            ramBytesUsed = 0;
        }

        private void onDocIdSetCache(long ramBytesUsed) {
            assert writeLock.isHeldByCurrentThread();
            this.ramBytesUsed += ramBytesUsed;
            XLRUQueryCache.this.onDocIdSetCache(key, ramBytesUsed);
        }

        private void onDocIdSetEviction(long ramBytesUsed) {
            assert writeLock.isHeldByCurrentThread();
            this.ramBytesUsed -= ramBytesUsed;
            XLRUQueryCache.this.onDocIdSetEviction(key, 1, ramBytesUsed);
        }

        CacheAndCount get(Query query) {
            assert query instanceof BoostQuery == false;
            assert query instanceof ConstantScoreQuery == false;
            return cache.get(query);
        }

        void putIfAbsent(Query query, CacheAndCount cached) {
            assert writeLock.isHeldByCurrentThread();
            assert query instanceof BoostQuery == false;
            assert query instanceof ConstantScoreQuery == false;
            if (cache.putIfAbsent(query, cached) == null) {
                // the set was actually put
                onDocIdSetCache(HASHTABLE_RAM_BYTES_PER_ENTRY + cached.ramBytesUsed());
            }
        }

        void remove(Query query) {
            assert writeLock.isHeldByCurrentThread();
            assert query instanceof BoostQuery == false;
            assert query instanceof ConstantScoreQuery == false;
            CacheAndCount removed = cache.remove(query);
            if (removed != null) {
                onDocIdSetEviction(HASHTABLE_RAM_BYTES_PER_ENTRY + removed.ramBytesUsed());
            }
        }

        @Override
        public long ramBytesUsed() {
            return ramBytesUsed;
        }
    }

    private class CachingWrapperWeight extends ConstantScoreWeight {

        private final Weight in;
        private final QueryCachingPolicy policy;
        // we use an AtomicBoolean because Weight.scorer may be called from multiple
        // threads when IndexSearcher is created with threads
        private final AtomicBoolean used;

        CachingWrapperWeight(Weight in, QueryCachingPolicy policy) {
            super(in.getQuery(), 1f);
            this.in = in;
            this.policy = policy;
            used = new AtomicBoolean(false);
        }

        @Override
        public Matches matches(LeafReaderContext context, int doc) throws IOException {
            return in.matches(context, doc);
        }

        private boolean cacheEntryHasReasonableWorstCaseSize(int maxDoc) {
            // The worst-case (dense) is a bit set which needs one bit per document
            final long worstCaseRamUsage = maxDoc / 8;
            // Imagine the worst-case that a cache entry is large than the size of
            // the cache: not only will this entry be trashed immediately but it
            // will also evict all current entries from the cache. For this reason
            // we only cache on an IndexReader if we have available room for
            // 5 different filters on this reader to avoid excessive trashing
            return worstCaseRamUsage * 5 < maxRamBytesUsed;
        }

        /** Check whether this segment is eligible for caching, regardless of the query. */
        private boolean shouldCache(LeafReaderContext context) throws IOException {
            return cacheEntryHasReasonableWorstCaseSize(ReaderUtil.getTopLevelContext(context).reader().maxDoc())
                && leavesToCache.test(context);
        }

        @Override
        public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
            if (used.compareAndSet(false, true)) {
                policy.onUse(getQuery());
            }

            if (in.isCacheable(context) == false) {
                // this segment is not suitable for caching
                return in.scorerSupplier(context);
            }

            // Short-circuit: Check whether this segment is eligible for caching
            // before we take a lock because of #get
            if (shouldCache(context) == false) {
                return in.scorerSupplier(context);
            }

            final IndexReader.CacheHelper cacheHelper = context.reader().getCoreCacheHelper();
            if (cacheHelper == null) {
                // this reader has no cache helper
                return in.scorerSupplier(context);
            }

            // If the lock is already busy, prefer using the uncached version than waiting
            if (readLock.tryLock() == false) {
                return in.scorerSupplier(context);
            }

            CacheAndCount cached;
            try {
                cached = get(in.getQuery(), cacheHelper);
            } finally {
                readLock.unlock();
            }

            int maxDoc = context.reader().maxDoc();
            if (cached instanceof SlicedCache slicedCache) {
                final ScorerSupplier supplier = in.scorerSupplier(context);
                if (supplier == null) {
                    return null;
                }
                final long cost = supplier.cost();
                return new SlicedCacheScorerSupplier(supplier, slicedCache, cost, maxDoc);
            }

            if (cached == null) {
                if (policy.shouldCache(in.getQuery())) {
                    final ScorerSupplier supplier = in.scorerSupplier(context);
                    if (supplier == null) {
                        putIfAbsent(in.getQuery(), CacheAndCount.EMPTY, cacheHelper);
                        return null;
                    }

                    final long cost = supplier.cost();
                    final List<RangeCache> registry = getCachingScorerRegistry();
                    if (registry != null) {
                        SlicedCache slicedCache = new SlicedCache(maxDoc);
                        putIfAbsent(in.getQuery(), slicedCache, cacheHelper);
                        return new SlicedCacheScorerSupplier(supplier, slicedCache, cost, maxDoc);
                    }

                    return new ConstantScoreScorerSupplier(0f, ScoreMode.COMPLETE_NO_SCORES, maxDoc) {
                        @Override
                        public DocIdSetIterator iterator(long leadCost) throws IOException {
                            if (cost / skipCacheFactor > leadCost) {
                                return supplier.get(leadCost).iterator();
                            }
                            CacheAndCount cached = tryPopulateCache(cacheHelper, in, supplier, context);
                            if (cached == null) {
                                return supplier.get(leadCost).iterator();
                            }
                            DocIdSetIterator disi = cached.iterator();
                            if (disi == null) {
                                disi = DocIdSetIterator.empty();
                            }
                            return disi;
                        }

                        @Override
                        public long cost() {
                            return cost;
                        }
                    };
                } else {
                    return in.scorerSupplier(context);
                }
            }

            assert cached != null;
            if (cached == CacheAndCount.EMPTY) {
                return null;
            }
            final DocIdSetIterator disi = cached.iterator();
            if (disi == null) {
                return null;
            }

            return ConstantScoreScorerSupplier.fromIterator(disi, 0f, ScoreMode.COMPLETE_NO_SCORES, maxDoc);
        }

        @Override
        public int count(LeafReaderContext context) throws IOException {
            // Our cache won't have an accurate count if there are deletions
            if (context.reader().hasDeletions()) {
                return in.count(context);
            }

            // Otherwise check if the count is in the cache
            if (used.compareAndSet(false, true)) {
                policy.onUse(getQuery());
            }

            if (in.isCacheable(context) == false) {
                // this segment is not suitable for caching
                return in.count(context);
            }

            // Short-circuit: Check whether this segment is eligible for caching
            // before we take a lock because of #get
            if (shouldCache(context) == false) {
                return in.count(context);
            }

            final IndexReader.CacheHelper cacheHelper = context.reader().getCoreCacheHelper();
            if (cacheHelper == null) {
                // this reader has no cacheHelper
                return in.count(context);
            }

            // If the lock is already busy, prefer using the uncached version than waiting
            if (readLock.tryLock() == false) {
                return in.count(context);
            }

            CacheAndCount cached;
            try {
                cached = get(in.getQuery(), cacheHelper);
            } finally {
                readLock.unlock();
            }
            if (cached != null) {
                // cached
                return cached.count();
            }
            // Not cached, check if the wrapped weight can count quickly then use that
            return in.count(context);
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
            return in.isCacheable(ctx);
        }
    }

    /**
     * Returns the thread-local registry for collecting {@link RangeCache} scorers during
     * scorer construction, or {@code null} if we are not in a sliced caching context.
     */
    protected static List<RangeCache> getCachingScorerRegistry() {
        return org.elasticsearch.indices.IndicesQueryCache.CACHING_SCORER.get();
    }

    /**
     * A {@link ScorerSupplier} that uses a {@link SlicedCache} to skip empty blocks
     * and tracks which blocks have matches for future executions.
     */
    protected static class SlicedCacheScorerSupplier extends ScorerSupplier implements RangeCache {
        private final ScorerSupplier delegate;
        private final SlicedCache slicedCache;
        private final long cost;
        private final int maxDoc;
        private int fromInclusive;
        private int toExclusive;

        SlicedCacheScorerSupplier(ScorerSupplier delegate, SlicedCache slicedCache, long cost, int maxDoc) {
            this.delegate = delegate;
            this.slicedCache = slicedCache;
            this.cost = cost;
            this.maxDoc = maxDoc;
            this.fromInclusive = 0;
            this.toExclusive = maxDoc;
        }

        @Override
        public Scorer get(long leadCost) throws IOException {
            Scorer realScorer = delegate.get(leadCost);
            SlicedCacheScorer scorer = new SlicedCacheScorer(realScorer, slicedCache);
            List<RangeCache> registry = getCachingScorerRegistry();
            if (registry != null) {
                registry.add(scorer);
            }
            return scorer;
        }

        @Override
        public void cacheRange(int from, int to) {
            this.fromInclusive = from;
            this.toExclusive = to;
        }

        @Override
        public BulkScorer bulkScorer() throws IOException {
            BulkScorer realBulkScorer = delegate.bulkScorer();
            SlicedCacheBulkScorer bulkScorer = new SlicedCacheBulkScorer(realBulkScorer, slicedCache);
            List<RangeCache> registry = getCachingScorerRegistry();
            if (registry != null) {
                registry.add(bulkScorer);
            }
            return bulkScorer;
        }

        @Override
        public long cost() {
            return cost;
        }
    }

    /**
     * A {@link Scorer} that wraps a real scorer and uses {@link SlicedCache} to skip
     * confirmed-empty blocks and track which blocks have matches.
     * When the delegate has a {@link TwoPhaseIterator}, it is unwrapped so that
     * expensive {@code matches()} calls are avoided for confirmed-empty blocks.
     * For non-TwoPhase delegates, the scorer passes through directly (block caching
     * is handled by the BulkScorer at the top level).
     */
    protected static class SlicedCacheScorer extends Scorer implements RangeCache {
        private final Scorer delegate;
        private final SlicedCache slicedCache;
        private final boolean hasTwoPhase;
        private int currentBlock;
        private int blockVisits;
        private boolean blockMatched;

        SlicedCacheScorer(Scorer delegate, SlicedCache slicedCache) {
            this.delegate = delegate;
            this.slicedCache = slicedCache;
            this.hasTwoPhase = delegate.twoPhaseIterator() != null;
            this.currentBlock = -1;
            this.blockVisits = 0;
            this.blockMatched = false;
        }

        @Override
        public void cacheRange(int from, int to) {
            flushBlock();
        }

        private void onApproximationDoc(int docId) {
            int block = docId >>> SlicedCache.BLOCK_SHIFT;
            if (block != currentBlock) {
                flushBlock();
                currentBlock = block;
                blockVisits = 0;
                blockMatched = false;
            }
            blockVisits++;
        }

        private void flushBlock() {
            if (currentBlock >= 0 && blockVisits == SlicedCache.BLOCK_SIZE) {
                slicedCache.markBlockScored(currentBlock);
            }
        }

        @Override
        public TwoPhaseIterator twoPhaseIterator() {
            TwoPhaseIterator realTwoPhase = delegate.twoPhaseIterator();
            if (realTwoPhase == null) {
                return null;
            }
            DocIdSetIterator approximation = realTwoPhase.approximation();
            return new TwoPhaseIterator(approximation) {
                @Override
                public boolean matches() throws IOException {
                    int docId = approximation.docID();
                    onApproximationDoc(docId);
                    int block = docId >>> SlicedCache.BLOCK_SHIFT;
                    if (slicedCache.blockScored(block) && slicedCache.blockHasMatch(block) == false) {
                        return false;
                    }
                    boolean matched = realTwoPhase.matches();
                    if (matched) {
                        blockMatched = true;
                        slicedCache.markBlockMatch(docId);
                    }
                    return matched;
                }

                @Override
                public float matchCost() {
                    return realTwoPhase.matchCost();
                }
            };
        }

        @Override
        public DocIdSetIterator iterator() {
            if (hasTwoPhase) {
                return TwoPhaseIterator.asDocIdSetIterator(twoPhaseIterator());
            }
            return delegate.iterator();
        }

        @Override
        public float getMaxScore(int upTo) throws IOException {
            return delegate.getMaxScore(upTo);
        }

        @Override
        public float score() throws IOException {
            return delegate.score();
        }

        @Override
        public int docID() {
            return delegate.docID();
        }
    }

    /**
     * A {@link BulkScorer} that wraps a real bulk scorer and uses {@link SlicedCache} skip bits
     * to skip empty blocks during scoring. Marks blocks as non-empty when hits are observed.
     */
    protected static class SlicedCacheBulkScorer extends BulkScorer implements RangeCache {
        private final BulkScorer delegate;
        private final SlicedCache slicedCache;

        SlicedCacheBulkScorer(BulkScorer delegate, SlicedCache slicedCache) {
            this.delegate = delegate;
            this.slicedCache = slicedCache;
        }

        @Override
        public void cacheRange(int from, int to) {
            // no-op for bulk scorer — range is already passed via score(collector, acceptDocs, min, max)
        }

        @Override
        public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
            LeafCollector trackingCollector = new LeafCollector() {
                @Override
                public void setScorer(Scorable scorer) throws IOException {
                    collector.setScorer(scorer);
                }

                @Override
                public void collect(int doc) throws IOException {
                    slicedCache.markBlockMatch(doc);
                    collector.collect(doc);
                }
            };
            int pos = min;
            int skippedBlocks = 0;
            int scoredBlocks = 0;
            while (pos < max) {
                int block = pos >>> SlicedCache.BLOCK_SHIFT;
                int blockStart = block << SlicedCache.BLOCK_SHIFT;
                int blockEnd = Math.min((block + 1) << SlicedCache.BLOCK_SHIFT, slicedCache.maxDoc());
                if (slicedCache.blockScored(block) && slicedCache.blockHasMatch(block) == false) {
                    skippedBlocks++;
                    pos = Math.min(blockEnd, max);
                    continue;
                }
                int batchEnd = Math.min(blockEnd, max);
                int nextBlock = block + 1;
                while (batchEnd < max) {
                    int nBlockEnd = Math.min((nextBlock + 1) << SlicedCache.BLOCK_SHIFT, slicedCache.maxDoc());
                    if (slicedCache.blockScored(nextBlock) && slicedCache.blockHasMatch(nextBlock) == false) {
                        break;
                    }
                    batchEnd = Math.min(nBlockEnd, max);
                    nextBlock++;
                }
                scoredBlocks += (nextBlock - block);
                pos = delegate.score(trackingCollector, acceptDocs, pos, batchEnd);
                for (int b = block; b < nextBlock; b++) {
                    int bStart = b << SlicedCache.BLOCK_SHIFT;
                    int bEnd = Math.min((b + 1) << SlicedCache.BLOCK_SHIFT, slicedCache.maxDoc());
                    if (min <= bStart && max >= bEnd) {
                        slicedCache.markBlockScored(b);
                    }
                }
            }
            if (skippedBlocks > 0 || scoredBlocks > 0) {
                System.out.println(
                    "[SlicedCacheBulkScorer] min=" + min + " max=" + max + " skipped=" + skippedBlocks + " scored=" + scoredBlocks
                );
            }
            return pos;
        }

        @Override
        public long cost() {
            return delegate.cost();
        }
    }

    /** Cache of doc ids with a count. */
    protected static class CacheAndCount implements Accountable {
        protected static final CacheAndCount EMPTY = new CacheAndCount(DocIdSet.EMPTY, 0);

        private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(CacheAndCount.class);
        private final DocIdSet cache;
        private final int count;

        public CacheAndCount(DocIdSet cache, int count) {
            this.cache = cache;
            this.count = count;
        }

        public DocIdSetIterator iterator() throws IOException {
            return cache.iterator();
        }

        public int count() {
            return count;
        }

        @Override
        public long ramBytesUsed() {
            return BASE_RAM_BYTES_USED + cache.ramBytesUsed();
        }
    }

    /**
     * A block-level skip cache that stores one bit per {@link #BLOCK_SIZE}-doc block.
     * A set bit means the block has at least one matching document; a clear bit means the block
     * is confirmed empty (within observed ranges). Bits are monotone (0→1 only), so concurrent
     * writers are safe without synchronization.
     */
    protected static class SlicedCache extends CacheAndCount {
        private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(SlicedCache.class);
        static final int BLOCK_SHIFT = 10;
        static final int BLOCK_SIZE = 1 << BLOCK_SHIFT;

        private final long[] matchBits;
        private final long[] scoredBits;
        private final int maxDoc;

        SlicedCache(int maxDoc) {
            super(DocIdSet.EMPTY, -1);
            this.maxDoc = maxDoc;
            int numBlocks = (maxDoc + BLOCK_SIZE - 1) >>> BLOCK_SHIFT;
            int words = (numBlocks + 63) >>> 6;
            this.matchBits = new long[words];
            this.scoredBits = new long[words];
        }

        /** Marks the block containing {@code docId} as having at least one match. */
        void markBlockMatch(int docId) {
            int block = docId >>> BLOCK_SHIFT;
            matchBits[block >>> 6] |= 1L << (block & 63);
        }

        /** Marks the given block index as fully scored (authoritative). */
        void markBlockScored(int block) {
            scoredBits[block >>> 6] |= 1L << (block & 63);
        }

        /** Returns true if the given block has been fully scored. */
        boolean blockScored(int block) {
            return (scoredBits[block >>> 6] & (1L << (block & 63))) != 0;
        }

        /** Returns true if the given block has at least one match. Only meaningful when {@link #blockScored} is true. */
        boolean blockHasMatch(int block) {
            return (matchBits[block >>> 6] & (1L << (block & 63))) != 0;
        }

        int maxDoc() {
            return maxDoc;
        }

        @Override
        public DocIdSetIterator iterator() {
            return null;
        }

        @Override
        public int count() {
            return -1;
        }

        @Override
        public long ramBytesUsed() {
            return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(matchBits) + RamUsageEstimator.sizeOf(scoredBits);
        }
    }

    /**
     * Interface for scorers that participate in range-aware block caching.
     * The operator calls {@link #cacheRange} before scoring each partition range,
     * allowing the scorer to load skip bits and flush observed blocks.
     */
    public interface RangeCache {
        /**
         * Signals that scoring will proceed over {@code [from, to)}.
         * Implementations should load skip bits for this range and prepare to track hits.
         */
        void cacheRange(int from, int to);
    }
}
