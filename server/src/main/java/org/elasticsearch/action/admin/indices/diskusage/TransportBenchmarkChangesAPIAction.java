/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.diskusage;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.LuceneBatchChangesSnapshot;
import org.elasticsearch.index.engine.LuceneChangesSnapshot;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;

public class TransportBenchmarkChangesAPIAction extends HandledTransportAction<AnalyzeIndexDiskUsageRequest, AnalyzeIndexDiskUsageResponse> {
    public static final ActionType<AnalyzeIndexDiskUsageResponse> TYPE = new ActionType<>("indices:admin/benchmark_changes_api");
    private final IndicesService indicesService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final ClusterService clusterService;

    @Inject
    public TransportBenchmarkChangesAPIAction(
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indexServices,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            TYPE.name(),
            true,
            transportService,
            actionFilters,
            AnalyzeIndexDiskUsageRequest::new,
            transportService.getThreadPool().executor(ThreadPool.Names.SEARCH)
        );
        this.indicesService = indexServices;
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
    }

    @Override
    protected void doExecute(Task task, AnalyzeIndexDiskUsageRequest request, ActionListener<AnalyzeIndexDiskUsageResponse> listener) {
        ClusterState state = clusterService.state();
        Index[] concreteIndices = indexNameExpressionResolver.concreteIndices(state, request);
        for (Index index : concreteIndices) {
            IndexService indexService = indicesService.indexService(index);
            if (indexService != null) {
                for (IndexShard indexShard : indexService) {
                    benchmarkChanges((CancellableTask) task, indexShard);
                }
            }
        }
        listener.onResponse(new AnalyzeIndexDiskUsageResponse(0, 0, 0, List.of(), Map.of()));
    }

    void benchmarkChanges(CancellableTask cancellableTask, IndexShard shard) {
        int times = 1;
        for (int i = 0; i < times; i++) {
            for (int batchSize : List.of(1024, 256, 64)) {
                cancellableTask.ensureNotCancelled();
                logger.info("--> benchmark {} batch_size {}", shard.shardId(), batchSize);
                benchmarkChanges(shard, batchSize);
                cancellableTask.ensureNotCancelled();
                benchmarkBatchedChanges(shard, batchSize);
            }
        }
    }

    void benchmarkChanges(IndexShard shard, int batchSize) {
        Engine.Searcher searcher = shard.acquireSearcher("test");
        long checkpoint = shard.seqNoStats().getLocalCheckpoint();
        MappingLookup mappingLookup = null;
        boolean synthetic = shard.mapperService().mappingLookup().isSourceSynthetic();
        if (synthetic) {
            mappingLookup = shard.mapperService().mappingLookup();
        }
        long fromSeqNo = 5001;
        long toSeqNo = 205_000;
        long startTime = System.nanoTime();
        long totalOps = 0;
        try (var snapshot = new LuceneChangesSnapshot(
            mappingLookup,
            searcher,
            batchSize,
            fromSeqNo,
            toSeqNo,
            false,
            true,
            false,
            IndexVersion.current()
        )) {
            while (snapshot.next() != null) {
                totalOps++;
                if (totalOps % 10_000 == 0) {
                    logger.info("--> un-batched fetched synthetic={} {}/{} ops", synthetic, totalOps, checkpoint);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        long endTime = System.nanoTime();
        long ms = (endTime - startTime) / 1000_000;
        double speed = (double) totalOps / ms;
        logger.info("--> un-batched size {}, total ops {} total time {} speed={}", batchSize, totalOps, endTime - startTime, speed);
    }

    void benchmarkBatchedChanges(IndexShard shard, int batchSize) {
        Engine.Searcher searcher = shard.acquireSearcher("test");
        long checkpoint = shard.seqNoStats().getLocalCheckpoint();
        MappingLookup mappingLookup = null;
        boolean synthetic = shard.mapperService().mappingLookup().isSourceSynthetic();
        if (synthetic) {
            mappingLookup = shard.mapperService().mappingLookup();
        }
        long fromSeqNo = 5001;
        long toSeqNo = 205_000;
        long startTime = System.nanoTime();
        long totalOps = 0;
        try (var snapshot = new LuceneBatchChangesSnapshot(
            mappingLookup,
            searcher,
            batchSize,
            fromSeqNo,
            toSeqNo,
            false,
            false,
            IndexVersion.current()
        )) {
            while (snapshot.next() != null) {
                totalOps++;
                if (totalOps % 10_000 == 0) {
                    logger.info("--> batched fetched synthetic={} {}/{} ops", synthetic, totalOps, checkpoint);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        long endTime = System.nanoTime();
        long ms = (endTime - startTime) / 1000_000;
        double speed = (double) totalOps / ms;
        logger.info("--> batched size {}, total ops {} total time {} speed={}", batchSize, totalOps, endTime - startTime, speed);
    }
}
