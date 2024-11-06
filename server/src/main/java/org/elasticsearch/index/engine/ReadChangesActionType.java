/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public class ReadChangesActionType extends ActionType<ReadChangesActionType.ReadChangeResponse> {

    public static final ReadChangesActionType INSTANCE = new ReadChangesActionType();
    private static final IndicesOptions INDICES_OPTIONS = IndicesOptions.fromOptions(false, false, true, true);

    public ReadChangesActionType() {
        super("indices:data/read/changes");
    }

    public static final class ReadChangeRequest extends ActionRequest implements IndicesRequest {
        private final long fromSeqNo;
        private final long toSeqNo;
        private final int batchSize;
        private final String[] indices;

        public ReadChangeRequest(String[] indices, long fromSeqNo, long toSeqNo, int batchSize) {
            this.fromSeqNo = fromSeqNo;
            this.toSeqNo = toSeqNo;
            this.batchSize = batchSize;
            this.indices = indices;
        }

        public ReadChangeRequest(StreamInput in) throws IOException {
            super(in);
            this.indices = in.readStringArray();
            fromSeqNo = in.readVLong();
            toSeqNo = in.readVLong();
            batchSize = in.readVInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringArray(indices);
            out.writeVLong(fromSeqNo);
            out.writeVLong(toSeqNo);
            out.writeVInt(batchSize);
        }

        @Override
        public String[] indices() {
            return indices;
        }

        @Override
        public IndicesOptions indicesOptions() {
            return INDICES_OPTIONS;
        }

        @Override
        public boolean includeDataStreams() {
            return true;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static final class ReadChangeResponse extends ActionResponse implements ToXContentObject {
        private final long tookNanos;
        private final long totalOps;

        public ReadChangeResponse(long tookNanos, long totalOps) {
            this.tookNanos = tookNanos;
            this.totalOps = totalOps;
        }

        public ReadChangeResponse(StreamInput in) throws IOException {
            super(in);
            this.tookNanos = in.readVLong();
            this.totalOps = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(tookNanos);
            out.writeVLong(totalOps);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("took_nanos", tookNanos);
            builder.field("took", TimeValue.timeValueNanos(tookNanos));
            builder.field("total_ops", totalOps);
            builder.endObject();
            return builder;
        }
    }

    public static class TransportAction extends HandledTransportAction<ReadChangeRequest, ReadChangeResponse> {
        private final IndicesService indicesService;
        private final IndexNameExpressionResolver indexNameExpressionResolver;
        private final ClusterService clusterService;

        @Inject
        public TransportAction(TransportService transportService, IndicesService indicesService, ClusterService clusterService, IndexNameExpressionResolver indexNameExpressionResolver, ActionFilters actionFilters) {
            super(INSTANCE.name(), transportService, actionFilters, ReadChangeRequest::new, transportService.getThreadPool().executor(ThreadPool.Names.SEARCH));
            this.indicesService = indicesService;
            this.clusterService = clusterService;
            this.indexNameExpressionResolver = indexNameExpressionResolver;
        }

        @Override
        protected void doExecute(Task task, ReadChangeRequest request, ActionListener<ReadChangeResponse> listener) {
            final ClusterState state = clusterService.state();
            final Index[] indices = indexNameExpressionResolver.concreteIndices(state, request);
            if (state.nodes().size() > 1) {
                throw new IllegalArgumentException("api requires single node cluster");
            }
            ActionListener.completeWith(listener, () -> {
                final long startTimeInNanos = System.nanoTime();
                long totalOps = 0;
                long minSeqNo = Long.MAX_VALUE;
                long maxSeqNo = Long.MIN_VALUE;
                for (Index index : indices) {
                    for (IndexShard shard : indicesService.indexServiceSafe(index)) {
                        try(Translog.Snapshot snapshot = acquireSnapshot(shard, request)){
                            Translog.Operation op;
                            while ((op = snapshot.next()) != null) {
                                totalOps++;
                                minSeqNo = Math.min(op.seqNo(), minSeqNo);
                                maxSeqNo = Math.max(op.seqNo(), maxSeqNo);
                            }
                        }
                        logger.info("--> reading {} total operations {} from {} to {}", index.getName(), totalOps, minSeqNo, maxSeqNo);
                    }
                }
                final long tookTimeInNanos = System.nanoTime() - startTimeInNanos;
                return new ReadChangeResponse(tookTimeInNanos, totalOps);
            });
        }
        private Translog.Snapshot acquireSnapshot(IndexShard shard, ReadChangeRequest request) throws IOException {
            Engine engine = shard.getEngineOrNull();
            Engine.Searcher searcher = engine.acquireSearcher("api", Engine.SearcherScope.INTERNAL);
            boolean useRecoverySource = RecoverySettings.INDICES_RECOVERY_SOURCE_ENABLED_SETTING.get(
                engine.config().getIndexSettings().getSettings()
            );
            var mappingLookup = useRecoverySource ? shard.mapperService().mappingLookup() : null;
            try {
                Translog.Snapshot snapshot = new LuceneBatchChangesSnapshot(
                    mappingLookup,
                    searcher,
                    request.batchSize,
                    request.fromSeqNo,
                    request.toSeqNo,
                    false,
                    false,
                    engine.config().getIndexSettings().getIndexVersionCreated()
                );
                searcher = null;
                return snapshot;
            } finally {
                IOUtils.close(searcher);
            }
        }
    }

}
