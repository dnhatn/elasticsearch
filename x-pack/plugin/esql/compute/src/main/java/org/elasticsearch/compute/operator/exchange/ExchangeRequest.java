/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public final class ExchangeRequest extends TransportRequest {
    private final String exchangeId;
    private final FetchOptions fetchOptions;

    public ExchangeRequest(String exchangeId, FetchOptions fetchOptions) {
        this.exchangeId = exchangeId;
        this.fetchOptions = fetchOptions;
    }

    public ExchangeRequest(StreamInput in) throws IOException {
        super(in);
        this.exchangeId = in.readString();
        this.fetchOptions = new FetchOptions(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(exchangeId);
        fetchOptions.writeTo(out);
    }

    /**
     * Returns the fetch options of this exchange request
     */
    public FetchOptions fetchOptions() {
        return fetchOptions;
    }

    /**
     * Returns the exchange ID. We don't use the parent task id because it can be overwritten by a proxy node.
     */
    public String exchangeId() {
        return exchangeId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExchangeRequest that = (ExchangeRequest) o;
        return fetchOptions.equals(that.fetchOptions) && exchangeId.equals(that.exchangeId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(exchangeId, fetchOptions);
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        if (parentTaskId.isSet() == false) {
            assert false : "ExchangeRequest must have a parent task";
            throw new IllegalStateException("ExchangeRequest must have a parent task");
        }
        return new CancellableTask(id, type, action, "", parentTaskId, headers) {
            @Override
            public String getDescription() {
                return "exchange request id=" + exchangeId;
            }
        };
    }
}
