/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

/**
 * Options used when fetching pages from a remote sink.
 *
 * @param toFinishSink  if true, the remote sink can finish because the sources have enough pages.
 * @param keepAliveOnly if true, the remote sink should not return pages but consider the request as a keep alive ping from the source
 */
public record FetchOptions(boolean toFinishSink, boolean keepAliveOnly) implements Writeable {
    public FetchOptions(StreamInput in) throws IOException {
        this(in.readBoolean(), in.getTransportVersion().onOrAfter(TransportVersions.ESQL_EXCHANGE_KEEP_ALIVE_ONLY) && in.readBoolean());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(toFinishSink);
        if (out.getTransportVersion().onOrAfter(TransportVersions.ESQL_EXCHANGE_KEEP_ALIVE_ONLY)) {
            out.writeBoolean(keepAliveOnly);
        }
    }
}
