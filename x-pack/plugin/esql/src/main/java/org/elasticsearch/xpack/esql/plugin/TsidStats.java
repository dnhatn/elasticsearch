/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchShardsGroup;
import org.elasticsearch.action.search.SearchShardsRequest;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.compute.lucene.query.TimeSeriesGroups;
import org.elasticsearch.compute.lucene.query.TsidPrefixHistogram;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.codec.tsdb.PartitionedDocValues;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.EsqlSearchShardsAction;
import org.elasticsearch.xpack.esql.querydsl.query.SingleValueQuery;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

public final class TsidStats {
    public static final String ACTION_NAME = "indices:data/read/esql/tsid_stats";
    private static final int MAX_CELLS = 4096;

    private TsidStats() {}

    public static void registerHandler(TransportService transportService, IndicesService indicesService) {
        transportService.registerRequestHandler(
            ACTION_NAME,
            transportService.getThreadPool().executor(EsqlPlugin.ESQL_WORKER_THREAD_POOL_NAME),
            NodeRequest::new,
            (request, channel, task) -> ActionListener.completeWith(
                new ChannelActionListener<>(channel),
                () -> collect(indicesService, request, (CancellableTask) task)
            )
        );
    }

    /**
     * The histogram of the shards that match {@code shardFilter}, clipped to the {@code @timestamp} bounds, or an empty one
     * if any shard cannot be probed. Shard discovery and collection are children of the query so cancellation can stop both phases.
     */
    public static void probe(
        Client client,
        TransportService transportService,
        CancellableTask queryTask,
        String[] indices,
        @Nullable QueryBuilder shardFilter,
        @Nullable QueryBuilder metricFilter,
        long minTimestamp,
        long maxTimestamp,
        long bucketMillis,
        ActionListener<TsidPrefixHistogram> listener
    ) {
        if (queryTask.notifyIfCancelled(listener)) {
            return;
        }
        var searchShards = new SearchShardsRequest(indices, SearchRequest.DEFAULT_INDICES_OPTIONS, shardFilter, null, null, true, null);
        searchShards.setParentTask(transportService.getLocalNode().getId(), queryTask.getId());
        client.execute(EsqlSearchShardsAction.TYPE, searchShards, listener.delegateFailureAndWrap((l, shards) -> {
            queryTask.ensureNotCancelled();
            Map<String, List<ShardId>> shardsPerNode = new HashMap<>();
            for (SearchShardsGroup group : shards.getGroups()) {
                if (group.skipped()) {
                    continue;
                }
                if (group.allocatedNodes().isEmpty()) {
                    l.onResponse(TsidPrefixHistogram.EMPTY);
                    return;
                }
                shardsPerNode.computeIfAbsent(group.allocatedNodes().getFirst(), k -> new ArrayList<>()).add(group.shardId());
            }
            if (shardsPerNode.isEmpty()) {
                l.onResponse(TsidPrefixHistogram.EMPTY);
                return;
            }
            long nowInMillis = transportService.getThreadPool().absoluteTimeInMillis();
            ActionListener<NodeResponse> nodeListener = new GroupedActionListener<>(shardsPerNode.size(), l.map(responses -> {
                queryTask.ensureNotCancelled();
                return merge(responses);
            }));
            Map<String, DiscoveryNode> nodes = new HashMap<>();
            for (DiscoveryNode node : shards.getNodes()) {
                nodes.put(node.getId(), node);
            }
            Executor responseExecutor = transportService.getThreadPool().executor(ThreadPool.Names.SEARCH_COORDINATION);
            for (var entry : shardsPerNode.entrySet()) {
                DiscoveryNode node = nodes.get(entry.getKey());
                if (node == null) {
                    nodeListener.onFailure(new IllegalStateException("node [" + entry.getKey() + "] not found"));
                    continue;
                }
                transportService.sendChildRequest(
                    node,
                    ACTION_NAME,
                    new NodeRequest(entry.getValue(), metricFilter, nowInMillis, minTimestamp, maxTimestamp, bucketMillis),
                    queryTask,
                    TransportRequestOptions.EMPTY,
                    new ActionListenerResponseHandler<>(nodeListener, NodeResponse::new, responseExecutor)
                );
            }
        }));
    }

    /**
     * The {@code @timestamp} range allowed by the filter's top-level conjunction, unbounded where it says nothing.
     */
    public static long[] timestampBounds(@Nullable QueryBuilder filter, long nowInMillis) {
        long[] bounds = { Long.MIN_VALUE, Long.MAX_VALUE };
        if (filter != null) {
            collectTimestampBounds(filter, nowInMillis, bounds);
        }
        return bounds;
    }

    private static void collectTimestampBounds(QueryBuilder filter, long nowInMillis, long[] bounds) {
        if (filter instanceof BoolQueryBuilder bool) {
            for (QueryBuilder clause : bool.filter()) {
                collectTimestampBounds(clause, nowInMillis, bounds);
            }
            for (QueryBuilder clause : bool.must()) {
                collectTimestampBounds(clause, nowInMillis, bounds);
            }
        } else if (filter instanceof SingleValueQuery.AbstractBuilder singleValue) {
            collectTimestampBounds(singleValue.next(), nowInMillis, bounds);
        } else if (filter instanceof RangeQueryBuilder range && range.fieldName().equals(DataStream.TIMESTAMP_FIELD_NAME)) {
            try {
                DateFormatter formatter = range.format() == null
                    ? DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER
                    : DateFormatter.forPattern(range.format());
                DateMathParser parser = formatter.toDateMathParser();
                ZoneId zone = range.timeZone() == null ? null : ZoneId.of(range.timeZone());
                if (range.from() != null) {
                    long lower = parser.parse(range.from().toString(), () -> nowInMillis, range.includeLower() == false, zone)
                        .toEpochMilli();
                    bounds[0] = Math.max(bounds[0], lower);
                }
                if (range.to() != null) {
                    long upper = parser.parse(range.to().toString(), () -> nowInMillis, range.includeUpper(), zone).toEpochMilli();
                    bounds[1] = Math.min(bounds[1], upper);
                }
            } catch (RuntimeException e) {
                // a bound that cannot be parsed here only loses the clipping; the shards still apply the filter
            }
        }
    }

    private static TsidPrefixHistogram merge(Collection<NodeResponse> responses) {
        TsidPrefixHistogram histogram = TsidPrefixHistogram.EMPTY;
        for (NodeResponse response : responses) {
            if (response.histogram == null) {
                return TsidPrefixHistogram.EMPTY;
            }
            histogram = histogram.merge(response.histogram);
        }
        return histogram.capped(MAX_CELLS);
    }

    private static NodeResponse collect(IndicesService indicesService, NodeRequest request, CancellableTask task) throws IOException {
        TsidPrefixHistogram histogram = TsidPrefixHistogram.EMPTY;
        for (ShardId shardId : request.shardIds) {
            task.ensureNotCancelled();
            IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
            MappedFieldType timestampType = indexService.mapperService().fieldType(DataStream.TIMESTAMP_FIELD_NAME);
            try (Engine.Searcher searcher = indexService.getShard(shardId.id()).acquireSearcher("esql_tsid_stats")) {
                Weight weight = null;
                if (request.metricFilter != null) {
                    SearchExecutionContext context = indexService.newSearchExecutionContext(
                        shardId.id(),
                        0,
                        searcher,
                        () -> request.nowInMillis,
                        null,
                        Map.of(),
                        null,
                        null
                    );
                    Query query = searcher.rewrite(context.toQuery(request.metricFilter).query());
                    weight = searcher.createWeight(query, ScoreMode.COMPLETE_NO_SCORES, 1f);
                }
                for (LeafReaderContext leaf : searcher.getIndexReader().leaves()) {
                    task.ensureNotCancelled();
                    SortedDocValues tsid = leaf.reader().getSortedDocValues(TimeSeriesIdFieldMapper.NAME);
                    if (tsid == null) {
                        continue;
                    }
                    long[] timestamps = TimeSeriesGroups.timestampRange(leaf.reader(), timestampType);
                    if (timestamps == null) {
                        return new NodeResponse((TsidPrefixHistogram) null);
                    }
                    long minTimestamp = Math.max(timestamps[0], request.minTimestamp);
                    long maxTimestamp = Math.min(timestamps[1], request.maxTimestamp);
                    if (minTimestamp > maxTimestamp) {
                        continue;
                    }
                    if (tsid instanceof PartitionedDocValues == false) {
                        return new NodeResponse((TsidPrefixHistogram) null);
                    }
                    DocIdSetIterator matching = null;
                    if (weight != null) {
                        Scorer scorer = weight.scorer(leaf);
                        if (scorer == null) {
                            continue;
                        }
                        matching = scorer.iterator();
                    }
                    PartitionedDocValues partitioned = (PartitionedDocValues) tsid;
                    PartitionedDocValues.PrefixPartitions partitions = partitioned.prefixPartitions();
                    if (partitions == null) {
                        return new NodeResponse((TsidPrefixHistogram) null);
                    }
                    histogram = histogram.merge(
                        TsidPrefixHistogram.ofLeaf(
                            partitions,
                            partitioned.prefixPartitionBits(),
                            leaf.reader().maxDoc(),
                            matching,
                            tsid,
                            minTimestamp,
                            maxTimestamp,
                            request.bucketMillis
                        )
                    );
                }
            }
        }
        return new NodeResponse(histogram.capped(MAX_CELLS));
    }

    static final class NodeRequest extends AbstractTransportRequest implements IndicesRequest {
        private final List<ShardId> shardIds;
        @Nullable
        private final QueryBuilder metricFilter;
        private final long nowInMillis;
        private final long minTimestamp;
        private final long maxTimestamp;
        private final long bucketMillis;

        NodeRequest(
            List<ShardId> shardIds,
            @Nullable QueryBuilder metricFilter,
            long nowInMillis,
            long minTimestamp,
            long maxTimestamp,
            long bucketMillis
        ) {
            this.shardIds = shardIds;
            this.metricFilter = metricFilter;
            this.nowInMillis = nowInMillis;
            this.minTimestamp = minTimestamp;
            this.maxTimestamp = maxTimestamp;
            this.bucketMillis = bucketMillis;
        }

        NodeRequest(StreamInput in) throws IOException {
            super(in);
            this.shardIds = in.readCollectionAsList(ShardId::new);
            this.metricFilter = in.readOptionalNamedWriteable(QueryBuilder.class);
            this.nowInMillis = in.readVLong();
            this.minTimestamp = in.readLong();
            this.maxTimestamp = in.readLong();
            this.bucketMillis = in.readVLong();
        }

        @Override
        public CancellableTask createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, getDescription(), parentTaskId, headers);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeCollection(shardIds);
            out.writeOptionalNamedWriteable(metricFilter);
            out.writeVLong(nowInMillis);
            out.writeLong(minTimestamp);
            out.writeLong(maxTimestamp);
            out.writeVLong(bucketMillis);
        }

        @Override
        public String[] indices() {
            return shardIds.stream().map(ShardId::getIndexName).distinct().toArray(String[]::new);
        }

        @Override
        public IndicesOptions indicesOptions() {
            return IndicesOptions.strictExpandOpenAndForbidClosed();
        }
    }

    static final class NodeResponse extends TransportResponse {
        @Nullable
        private final TsidPrefixHistogram histogram;

        NodeResponse(@Nullable TsidPrefixHistogram histogram) {
            this.histogram = histogram;
        }

        NodeResponse(StreamInput in) throws IOException {
            this.histogram = in.readOptionalWriteable(TsidPrefixHistogram::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalWriteable(histogram);
        }
    }
}
