
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromSingleton;
import org.elasticsearch.compute.lucene.ShardContext;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.index.cache.query.TrivialQueryCachingPolicy;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesQueryCache;
import org.elasticsearch.search.internal.ContextIndexSearcher;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.elasticsearch.indices.IndicesQueryCache.INDICES_QUERIES_CACHE_ALL_SEGMENTS_SETTING;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class LuceneSourceCachingTests extends ComputeTestCase {

    public void testCacheOnce() throws Exception {
        var dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());
        final int numThreads = between(2, 10);
        final int numDocs = 100 * numThreads;
        for (int d = 0; d < numDocs; d++) {
            writer.addDocument(new Document());
        }
        var reader = DirectoryReader.open(writer);
        ShardId shard = new ShardId("index", "_na_", 0);
        reader = ElasticsearchDirectoryReader.wrap(reader, shard);
        writer.close();
        var queryCache = new IndicesQueryCache(Settings.builder().put(INDICES_QUERIES_CACHE_ALL_SEGMENTS_SETTING.getKey(), true).build());
        var searcher = new ContextIndexSearcher(
            reader,
            IndexSearcher.getDefaultSimilarity(),
            queryCache,
            TrivialQueryCachingPolicy.ALWAYS,
            false
        );
        ShardContext ctx = new LuceneSourceOperatorTests.MockShardContext(searcher, 0);
        AtomicInteger reads = new AtomicInteger();
        Function<ShardContext, List<LuceneSliceQueue.QueryAndTags>> queryFunction = c -> List.of(
            new LuceneSliceQueue.QueryAndTags(new ReadCountingQuery(reads), List.of())
        );
        int maxPageSize = between(10, numDocs);
        LuceneSourceOperator.Factory sourceFactory = new LuceneSourceOperator.Factory(
            new IndexedByShardIdFromSingleton<>(ctx),
            queryFunction,
            DataPartitioning.DOC,
            DataPartitioning.AutoStrategy.DEFAULT,
            numThreads,
            maxPageSize,
            Integer.MAX_VALUE,
            false
        );
        Thread[] threads = new Thread[numThreads];
        CountDownLatch latch = new CountDownLatch(numThreads);
        Set<Integer> visitedDocs = ConcurrentCollections.newConcurrentSet();
        for (int i = 0; i < numThreads; i++) {
            threads[i] = new Thread(() -> {
                BlockFactory blockFactory = blockFactory();
                DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
                try (SourceOperator operator = sourceFactory.get(driverContext)) {
                    latch.countDown();
                    safeAwait(latch);
                    while (operator.isFinished() == false) {
                        try (Page output = operator.getOutput()) {
                            if (output != null) {
                                DocBlock docBlock = output.getBlock(0);
                                IntVector docs = docBlock.asVector().docs();
                                for (int d = 0; d < docs.getPositionCount(); d++) {
                                    assertTrue(visitedDocs.add(docs.getInt(d)));
                                }
                            }
                        }
                    }
                }
            });
            threads[i].start();
        }
        for (Thread thread : threads) {
            thread.join(30_000);
        }
        reader.close();
        dir.close();
        // best effort, sometimes we still run uncached iterators
        assertThat(reads.get(), lessThanOrEqualTo(numDocs * 2));
    }

    static class ReadCountingQuery extends Query {
        final AtomicInteger reads;

        ReadCountingQuery(AtomicInteger reads) {
            this.reads = reads;
        }

        @Override
        public String toString(String field) {
            return "ReadCountingQuery";
        }

        @Override
        public void visit(QueryVisitor visitor) {

        }

        @Override
        public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
            return new ReadCountingWeight(this, reads);
        }

        @Override
        public boolean equals(Object other) {
            return sameClassAs(other);
        }

        @Override
        public int hashCode() {
            return 0;
        }
    }

    static class ReadCountingWeight extends Weight {
        final AtomicInteger reads;

        ReadCountingWeight(Query query, AtomicInteger reads) {
            super(query);
            this.reads = reads;
        }

        @Override
        public Explanation explain(LeafReaderContext context, int doc) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
            int maxDoc = context.reader().maxDoc();
            DocIdSetIterator iterator = new DocIdSetIterator() {
                int docId = -1;

                @Override
                public int docID() {
                    return docId;
                }

                @Override
                public int nextDoc() throws IOException {
                    return advance(docId + 1);
                }

                @Override
                public int advance(int target) throws IOException {
                    if (target < maxDoc) {
                        reads.incrementAndGet();
                        docId = target;
                    } else {
                        docId = DocIdSetIterator.NO_MORE_DOCS;
                    }
                    return docId;
                }

                @Override
                public long cost() {
                    return maxDoc;
                }
            };
            return new ScorerSupplier() {
                @Override
                public Scorer get(long leadCost) {
                    return new ConstantScoreScorer(1f, ScoreMode.COMPLETE_NO_SCORES, iterator);
                }

                @Override
                public long cost() {
                    return maxDoc;
                }
            };
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
            return true;
        }
    }
}
