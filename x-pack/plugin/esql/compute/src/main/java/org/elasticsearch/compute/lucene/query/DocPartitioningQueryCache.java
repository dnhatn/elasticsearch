/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.FilterWeight;
import org.apache.lucene.search.QueryCache;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.Weight;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.indices.IndicesQueryCache;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A query cache for doc partitioning that tries to prevent multiple threads from populating the cache for the same segment.
 * Other threads pause at the operator level (via {@link SubscribableListener}) until caching completes, then use the cached result.
 * This is best-effort as other threads might also fall back to an uncached scorer.
 */
final class DocPartitioningQueryCache implements QueryCache {
    private final QueryCache actual;

    DocPartitioningQueryCache(QueryCache actual) {
        this.actual = actual;
    }

    @Override
    public Weight doCache(Weight weight, QueryCachingPolicy policy) {
        if (weight instanceof AlreadyCachedWeight) {
            return weight;
        }
        return new AlreadyCachedWeight(actual.doCache(new WrappedWeight(weight), policy));
    }

    /**
     * Returns a listener that completes when all cache population finishes for the given leaf,
     * or {@code null} if no caching is in progress for this leaf.
     */
    SubscribableListener<Void> blockedOnCaching(LeafReaderContext leaf) {
        return null;
    }


    static class WrappedWeight extends IndicesQueryCache.OptionalCachingWeight {
        private final Set<Object> cached = ConcurrentCollections.newConcurrentSet();

        WrappedWeight(Weight weight) {
            super(weight);
        }

        @Override
        public Releasable startCaching(LeafReaderContext leaf) {
            if (cached.add(leaf.id())) {
                return () -> {};
            }
            return null;
        }
    }

    /**
     * Marker to prevent double-wrapping a {@link Weight} that already passed through {@link #doCache}.
     * Queries like {@link org.apache.lucene.search.ConstantScoreQuery} can invoke {@code doCache(doCache(w))};
     * other QueryCache implementations also unwrap the incoming Weight before wrapping.
     */
    private static class AlreadyCachedWeight extends FilterWeight {
        AlreadyCachedWeight(Weight weight) {
            super(weight);
        }
    }
}
