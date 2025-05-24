/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.IsBlockedResult;

public final class DirectExchange {
    private final ExchangeSource source;
    private final ExchangeSink sink;

    public DirectExchange(int bufferSize) {
        ExchangeBuffer buffer = new ExchangeBuffer(bufferSize);
        this.source = new DirectExchangeSource(buffer);
        this.sink = new DirectExchangeSink(buffer);
    }

    static final class DirectExchangeSource implements ExchangeSource {
        private final ExchangeBuffer buffer;

        DirectExchangeSource(ExchangeBuffer buffer) {
            this.buffer = buffer;
        }

        @Override
        public Page pollPage() {
            return buffer.pollPage();
        }

        @Override
        public void finish() {
            buffer.finish(true);
        }

        @Override
        public boolean isFinished() {
            return buffer.isFinished();
        }

        @Override
        public int bufferSize() {
            return buffer.size();
        }

        @Override
        public IsBlockedResult waitForReading() {
            return buffer.waitForReading();
        }
    }

    static final class DirectExchangeSink implements ExchangeSink {
        private final ExchangeBuffer buffer;
        private boolean finished = false;

        DirectExchangeSink(ExchangeBuffer buffer) {
            this.buffer = buffer;
        }

        @Override
        public void addPage(Page page) {
            buffer.addPage(page);
        }

        @Override
        public void finish() {
            buffer.finish(false);
            finished = true;
        }

        @Override
        public boolean isFinished() {
            return finished || buffer.isFinished();
        }

        @Override
        public void addCompletionListener(ActionListener<Void> listener) {
            buffer.addCompletionListener(listener);
        }

        @Override
        public IsBlockedResult waitForWriting() {
            return buffer.waitForWriting();
        }
    }

    public ExchangeSource exchangeSource() {
        return source;
    }

    public ExchangeSink exchangeSink() {
        return sink;
    }
}
