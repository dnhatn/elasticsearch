/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ConstantScoreQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

record TimeSeriesSliceQueue(LuceneSliceQueue queue) {

    static LuceneSliceQueue createQueue(List<? extends ShardContext> contexts, List<TimeSeriesSourceOperator.QueryBuilderAndTags> queryAndTags, int maxDocsPerSlice, int taskConcurrency){
        List<LuceneSlice> sliceList = new ArrayList<>(contexts.size() * queryAndTags.size());
        List<Tuple<QueryBuilder, TimeRange>> queryAndTimeRanges = new ArrayList<>(queryAndTags.size());
        for (var queryAndTag : queryAndTags) {
            queryAndTimeRanges.add(removeTimeRange(queryAndTag.query()));
        }
        final Map<LeafReaderContext, TimeRange> leafTimeRanges = new HashMap<>();
        final Map<Integer, TimeRange> shardTimeRanges = new HashMap<>();
        for (ShardContext context : contexts) {
            TimeRange shardTimeRange = null;
            for (LeafReaderContext leaf : context.searcher().getIndexReader().leaves()) {
                TimeRange timeRange = null;
                try {
                    byte[] minPackedValue = PointValues.getMinPackedValue(leaf.reader(), "@timestamp");
                    if (minPackedValue != null) {
                        byte[] maxPackedValue = PointValues.getMaxPackedValue(leaf.reader(), "@timestamp");
                        if (maxPackedValue != null) {
                            long min = LongPoint.decodeDimension(minPackedValue, 0);
                            long max = LongPoint.decodeDimension(maxPackedValue, 0);
                            timeRange = new TimeRange(min, max);
                        }
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                if (timeRange != null) {
                    leafTimeRanges.put(leaf, timeRange);
                    shardTimeRange = shardTimeRange == null ? timeRange : TimeRange.or(shardTimeRange, timeRange);
                }
            }
            if (shardTimeRange != null) {
                shardTimeRanges.put(context.index(), shardTimeRange);
            }
        }
        long totalDocs = 0;
        for (ShardContext context : contexts) {
            TimeRange shardTimeRange = shardTimeRanges.get(context.index());
            int numDocs = context.searcher().getIndexReader().numDocs();
            if (shardTimeRange == null || numDocs == 0) {
                continue;
            }
            long dataLength = shardTimeRange.max - shardTimeRange.min + 1;
            // 829_440_000
            for (int i = 0; i < queryAndTags.size(); i++) {
                TimeRange queryTimeRange = queryAndTimeRanges.get(i).v2();
                var intersected = queryTimeRange == null ? shardTimeRange : TimeRange.and(queryTimeRange, shardTimeRange);
                // TODO: nulls issue
                if (intersected.canMatch() == false) {
                    continue;
                }
                assert intersected.min != null && intersected.max != null;
                long queryLength = intersected.max - intersected.min + 1;
                System.err.println("--> query length " + queryLength + " data length " + dataLength + " num docs " + numDocs);
                long queryingDocs = Math.ceilDiv(numDocs * queryLength, dataLength);
                totalDocs += queryingDocs;
            }
        }
        maxDocsPerSlice = Math.clamp(totalDocs / taskConcurrency, Math.max(1, maxDocsPerSlice / 100), maxDocsPerSlice);
        int slicePosition = 0;
        for (ShardContext context : contexts) {
            TimeRange shardTimeRange = shardTimeRanges.get(context.index());
            int numDocs = context.searcher().getIndexReader().numDocs();
            if (shardTimeRange == null || numDocs == 0) {
                continue;
            }
            var partialLeaves = context.searcher().getLeafContexts().stream().map(PartialLeafReaderContext::new).toList();
            long dataLength = shardTimeRange.max - shardTimeRange.min + 1;
            for (int i = 0; i < queryAndTags.size(); i++) {
                TimeRange queryTimeRange = queryAndTimeRanges.get(i).v2();
                var intersected = queryTimeRange == null ? shardTimeRange : TimeRange.and(queryTimeRange, shardTimeRange);
                if (intersected.canMatch() == false) {
                    continue;
                }
                assert intersected.min != null && intersected.max != null;
                long queryLength = intersected.max - intersected.min + 1;
                long queryingDocs = Math.ceilDiv(numDocs * queryLength, dataLength);
                System.err.println("--> query length " + queryLength + " data length " + dataLength + " num docs " + numDocs + " querying docs " + queryingDocs);
                if (queryAndTags.size() > 1 && queryingDocs <= maxDocsPerSlice) {
                    QueryBuilder subQuery = withTimeRange(queryAndTimeRanges.get(i).v1(), intersected);
                    Weight weight = createWeight(subQuery, context);
                    LuceneSlice slice = new LuceneSlice(
                        slicePosition++,
                        i == 0,
                        context,
                        partialLeaves,
                        weight,
                        queryAndTags.get(i).tags()
                    );
                    sliceList.add(slice);
                } else {
                    int numSlices = Math.toIntExact(queryingDocs / maxDocsPerSlice);
                    long sliceLength = Math.max(1, Math.ceilDiv(queryLength, numSlices));
                    long sliceMin = intersected.min;
                    while (sliceMin <= intersected.max) {
                        long sliceMax = Math.min(intersected.max, sliceMin + sliceLength - 1);
                        QueryBuilder subQuery = withTimeRange(queryAndTimeRanges.get(i).v1(), new TimeRange(sliceMin, sliceMax));
                        Weight weight = createWeight(subQuery, context);
                        LuceneSlice slice = new LuceneSlice(
                            slicePosition++,
                            i == 0,
                            context,
                            partialLeaves,
                            weight,
                            queryAndTags.get(i).tags()
                        );
                        sliceList.add(slice);
                        sliceMin = Math.addExact(sliceMax, sliceLength);
                    }
                }
            }
            // stick one query per shard so that we can leverage cache
        }
        return new LuceneSliceQueue(sliceList, Map.of());
    }

    static QueryBuilder withTimeRange(QueryBuilder withoutTimeRange, TimeRange timeRange) {
        RangeQueryBuilder timeRangeQuery = new RangeQueryBuilder("@timestamp")
            .from(timeRange.min)
            .to(timeRange.max)
            .includeLower(true)
            .includeUpper(true);
        if (withoutTimeRange == null) {
            return timeRangeQuery;
        } else if (withoutTimeRange instanceof BoolQueryBuilder bq) {
            BoolQueryBuilder clone = bq.shallowCopy();
            clone.filter().add(timeRangeQuery);
            return clone;
        } else {
            BoolQueryBuilder bq = new BoolQueryBuilder();
            bq.filter().add(withoutTimeRange);
            bq.filter().add(timeRangeQuery);
            return bq;
        }
    }

    static Weight createWeight(QueryBuilder queryBuilder, ShardContext shard) {
        var query = shard.toQuery(queryBuilder);
        try {
            query = shard.searcher().rewrite(query);
            return shard.searcher().createWeight(query, ScoreMode.COMPLETE_NO_SCORES, 1);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static Tuple<QueryBuilder, TimeRange> removeTimeRange(QueryBuilder originalQuery) {
        QueryBuilder queryWithoutTimeRange = null;
        TimeRange timeRange = null;
        if (originalQuery instanceof BoolQueryBuilder bq) {
            BoolQueryBuilder builder = bq.shallowCopy();
            builder.must().clear();
            builder.filter().clear();
            builder.mustNot().clear();
            moveClausesToTopLevel(builder, bq);
            Iterator<QueryBuilder> it = builder.filter().iterator();
            while (it.hasNext()) {
                RangeQueryBuilder rangeQuery = extractTimestampRangeQuery(it.next());
                if (rangeQuery != null) {
                    it.remove();
                    if (timeRange == null) {
                        timeRange = extractTimeRangeFromQuery(rangeQuery);
                    } else {
                        timeRange = TimeRange.and(timeRange, extractTimeRangeFromQuery(rangeQuery));
                    }
                }
            }
            queryWithoutTimeRange = builder;
        } else {
            RangeQueryBuilder rangeQuery = extractTimestampRangeQuery(originalQuery);
            if (rangeQuery != null) {
                timeRange = extractTimeRangeFromQuery(rangeQuery);
            }
        }
        System.err.println("--> removed time range " + timeRange + " from query " + originalQuery);
        return Tuple.tuple(queryWithoutTimeRange, timeRange);
    }

    static void moveClausesToTopLevel(BoolQueryBuilder top, QueryBuilder query) {
        if (query instanceof BoolQueryBuilder bq) {
            for (QueryBuilder c : bq.must()) {
                moveClausesToTopLevel(top, c);
            }
            bq.must().clear();
            for (QueryBuilder c : bq.filter()) {
                moveClausesToTopLevel(top, c);
            }
            bq.filter().clear();
            top.mustNot().addAll(bq.mustNot());
            top.mustNot().clear();
            if (bq.should().isEmpty() == false) {
                top.should().add(bq);
            }
        } else if (query instanceof ConstantScoreQueryBuilder c) {
            top.filter().add(c.innerQuery());
        } else {
            top.filter().add(query);
        }
    }

    static RangeQueryBuilder extractTimestampRangeQuery(QueryBuilder query) {
        while (query != null) {
            switch (query) {
                case RangeQueryBuilder r when r.fieldName().equals("@timestamp") -> {
                    return r;
                }
                case ConstantScoreQueryBuilder c -> query = c.innerQuery();
                case SingleValueQueryBuilder s -> query = s.subQuery();
                default -> {
                    return null;
                }
            }
        }
        return null;
    }

    static TimeRange extractTimeRangeFromQuery(RangeQueryBuilder r) {
        Long min = null;
        Long max = null;
        if (r.from() != null) {
            if (r.includeLower()) {
                min = ((Number) r.from()).longValue();
            } else {
                min = Math.addExact(((Number) r.from()).longValue(), 1L);
            }
        }
        if (r.to() != null) {
            if (r.includeUpper()) {
                max = ((Number) r.to()).longValue();
            } else {
                max = Math.subtractExact(((Number) r.to()).longValue(), 1L);
            }
        }
        return new TimeRange(min, max);
    }

    record TimeRange(Long min, Long max) {
        static TimeRange and(TimeRange a, TimeRange b) {
            final Long min;
            if (a.min == null) {
                min = b.min;
            } else if (b.min == null) {
                min = a.min;
            } else {
                min = Math.max(a.min, b.min);
            }
            final Long max;
            if (a.max == null) {
                max = b.max;
            } else if (b.max == null) {
                max = a.max;
            } else {
                max = Math.min(a.max, b.max);
            }
            return new TimeRange(min, max);
        }

        static TimeRange or(TimeRange a, TimeRange b) {
            final Long min;
            if (a.min == null || b.min == null) {
                min = null;
            } else {
                min = Math.min(a.min, b.min);
            }
            final Long max;
            if (a.max == null || b.max == null) {
                max = null;
            } else {
                max = Math.max(a.max, b.max);
            }
            return new TimeRange(min, max);
        }

        boolean canMatch() {
            return min == null || max == null || min <= max;
        }
    }
}
