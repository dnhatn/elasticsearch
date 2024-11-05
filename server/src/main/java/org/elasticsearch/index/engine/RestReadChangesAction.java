/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestReadChangesAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/{index}/_changes"), new Route(GET, "/{index}/_changes"));
    }

    @Override
    public String getName() {
        return "read_changes";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        long fromSeqNo = request.paramAsLong("from_seq_no", 0L);
        long toSeqNo = request.paramAsLong("to_seq_no", 0L);
        int batchSize = request.paramAsInt("batch_size", 256);
        final String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        final var analyzeRequest = new ReadChangesActionType.ReadChangeRequest(indices, fromSeqNo, toSeqNo, batchSize);
        return channel -> {
            client.execute(ReadChangesActionType.INSTANCE, analyzeRequest, new RestToXContentListener<>(channel));
        };
    }
}
