/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query.cache;

import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.index.query.DateRangeIncludingNowQuery;

import java.util.IdentityHashMap;
import java.util.Map;

/**
 * Stores {@link Query} to {@link PredicateKey} mappings for block-level predicate caching.
 * Only leaf predicates register keys here; composite keys are built from leaf keys at cache-lookup time.
 * Populated during query building (single-threaded) then frozen before being used in multiple Drivers.
 * After freezing, only {@link #get(Query)} is permitted.
 */
public final class PredicateKeys {
    private final Map<Query, PredicateKey> keys = new IdentityHashMap<>();
    private final Thread creationThread;
    private final SetOnce<Boolean> frozen = new SetOnce<>();

    public PredicateKeys() {
        this.creationThread = Thread.currentThread();
    }

    public void put(Query query, PredicateKey key) {
        assert assertUpdatable();
        keys.put(query, key);
    }

    /**
     * A rewritten query is considered equivalent in terms of matching docunents so they both can share the same {@link PredicateKey}
     */
    public void updateRewrittenQuery(Query query, Query rewritten) {
        assert assertUpdatable();
        if (query != rewritten && shouldCache(rewritten)) {
            PredicateKey key = keys.get(query);
            if (key != null) {
                keys.put(rewritten, key);
            }
        }
    }

    public PredicateKey get(Query query) {
        assert frozen.get() : "PredicateKeys must be frozen before calling get()";
        PredicateKey key = keys.get(query);
        if (key != null) {
            return key;
        }
        return switch (query) {
            case BooleanQuery bq -> booleanQuery(bq);
            default -> null;
        };
    }

    private PredicateKey booleanQuery(BooleanQuery bq) {
        PredicateHasher hasher = new PredicateHasher(BooleanQuery.class);
        hasher.addInt(bq.getMinimumNumberShouldMatch());
        var clauses = bq.clauses();
        hasher.addInt(clauses.size());
        for (var clause : clauses) {
            PredicateKey k = get(clause.query());
            if (k == null) {
                return null;
            }
            hasher.addInt(clause.occur().ordinal());
            hasher.addPredicateKey(k);
        }
        return hasher.finish();
    }

    public boolean shouldCache(Query query) {
        if (query instanceof DateRangeIncludingNowQuery) {
            return false;
        }
        if (query instanceof TermQuery) {
            return false;
        }
        if (query instanceof FieldExistsQuery) {
            return false;
        }
        if (query instanceof MatchAllDocsQuery) {
            return false;
        }
        if (query instanceof MatchNoDocsQuery) {
            return false;
        }
        if (query instanceof BooleanQuery bq && bq.clauses().isEmpty()) {
            return false;
        }
        if (query instanceof DisjunctionMaxQuery dmq && dmq.getDisjuncts().isEmpty()) {
            return false;
        }
        return true;
    }

    public void freeze() {
        frozen.set(Boolean.TRUE);
    }

    private boolean assertUpdatable() {
        assert frozen.get() == null;
        assert Thread.currentThread() == creationThread : Thread.currentThread() + " != " + creationThread;
        return true;
    }
}
