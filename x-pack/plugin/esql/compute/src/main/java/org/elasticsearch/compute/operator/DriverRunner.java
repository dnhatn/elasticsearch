/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.List;
import java.util.Set;

/**
 * Run a set of drivers to completion.
 */
public abstract class DriverRunner {
    private final static Logger LOGGER = LogManager.getLogger(DriverRunner.class);
    private final ThreadContext threadContext;

    public DriverRunner(ThreadContext threadContext) {
        this.threadContext = threadContext;
    }

    /**
     * Start a driver.
     */
    protected abstract void start(Driver driver, ActionListener<Void> driverListener);

    /**
     * Run all drivers to completion asynchronously.
     */
    public void runToCompletion(List<Driver> drivers, ActionListener<Void> listener) {
        var failure = new FailureCollector();
        CountDown counter = new CountDown(drivers.size());
        Set<String> warnings = ConcurrentCollections.newConcurrentSet();
        for (int i = 0; i < drivers.size(); i++) {
            Driver driver = drivers.get(i);
            ActionListener<Void> driverListener = new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {
                    done();
                }

                @Override
                public void onFailure(Exception e) {
                    failure.unwrapAndCollect(e);
                    for (Driver d : drivers) {
                        if (driver != d) {
                            d.cancel("Driver [" + driver.sessionId() + "] was cancelled or failed");
                        }
                    }
                    done();
                }

                private void done() {
                    warnings.addAll(driver.driverContext().getWarnings());
                    if (counter.countDown()) {
                        if (warnings.isEmpty() == false) {
                            LOGGER.debug("warnings [{}]", warnings);
                            for (String warning : warnings) {
                                threadContext.addResponseHeader("Warning", HeaderWarning.formatWarning(warning));
                            }
                        }
                        Exception error = failure.getFailure();
                        if (error != null) {
                            listener.onFailure(error);
                        } else {
                            listener.onResponse(null);
                        }
                    }
                }
            };

            start(driver, driverListener);
        }
    }
}
