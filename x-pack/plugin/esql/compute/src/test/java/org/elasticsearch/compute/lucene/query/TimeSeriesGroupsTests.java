/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromSingleton;
import org.elasticsearch.compute.querydsl.query.QueryWarnings;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;

public class TimeSeriesGroupsTests extends ESTestCase {

    public void testReadsTsidRange() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), directory)) {
                writer.addDocument(new Document());
            }
            try (IndexReader reader = DirectoryReader.open(directory)) {
                Query split = new BooleanQuery.Builder().add(new MatchAllDocsQuery(), BooleanClause.Occur.FILTER)
                    .add(new TsidPrefixRangeQuery(new TsidRange(8, 1, 2)), BooleanClause.Occur.FILTER)
                    .build();
                assertTrue(TimeSeriesGroups.readsTsidRange(factory(reader, split)));
                assertFalse(TimeSeriesGroups.readsTsidRange(factory(reader, new MatchAllDocsQuery())));
            }
        }
    }

    private static LuceneSourceOperator.Factory factory(IndexReader reader, Query query) {
        return new LuceneSourceOperator.Factory(
            new IndexedByShardIdFromSingleton<>(new LuceneSourceOperatorTests.MockShardContext(reader, 0)),
            ctx -> List.of(new LuceneSliceQueue.QueryAndTags(query, List.of())),
            randomFrom(DataPartitioning.SHARD, DataPartitioning.SEGMENT, DataPartitioning.DOC),
            DataPartitioning.AutoStrategy.DEFAULT,
            LuceneOperator.SMALL_INDEX_BOUNDARY,
            between(1, 4),
            100,
            LuceneOperator.NO_LIMIT,
            false,
            () -> 0L,
            LuceneSliceQueue.MIN_DOCS_PER_SLICE,
            QueryWarnings.EMIT
        );
    }
}
