/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.UntypedActionRequest;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;

/**
 * A {@link DriverRunner} that executes {@link Driver} with a child task so that we can retrieve the progress with the Task API.
 */
public class DriverTaskRunner {
    public static final String ACTION_NAME = "indices:data/read/esql/compute";
    private final TransportService transportService;
    private final Executor searchExecutor;

    public DriverTaskRunner(TransportService transportService, Executor searchExecutor) {
        this.transportService = transportService;
        this.searchExecutor = searchExecutor;
        transportService.registerRequestHandler(
            ACTION_NAME,
            searchExecutor,
            DriverRequest::new,
            new DriverRequestHandler(transportService)
        );
    }

    public void executeDrivers(Task parentTask, List<Driver> drivers, Executor workerExecutor, ActionListener<Void> listener) {
        var runner = new DriverRunner(transportService.getThreadPool().getThreadContext()) {
            @Override
            protected void start(Driver driver, ActionListener<Void> driverListener) {
                // HACK (latency experiment): register the driver's task directly and start the driver on the worker pool,
                // skipping the local child transport request and its two thread hops (send -> SEARCH -> worker).
                // Completion still hops to SEARCH because downstream listeners assert that pool.
                System.err.println("--> driver start direct [" + driver.shortDescription + "] " + System.nanoTime());
                var taskManager = transportService.getTaskManager();
                var request = new DriverRequest(driver, workerExecutor);
                request.setParentTask(transportService.getLocalNode().getId(), parentTask.getId());
                final Task task = taskManager.register("transport", ACTION_NAME, request);
                ActionListener<Void> completion = ActionListener.runBefore(
                    new ThreadedActionListener<>(searchExecutor, ActionListener.wrap(v -> {
                        System.err.println(
                            "--> driver completion delivered (on SEARCH) [" + driver.shortDescription + "] " + System.nanoTime()
                        );
                        driverListener.onResponse(v);
                    }, driverListener::onFailure)),
                    () -> taskManager.unregister(task)
                );
                Driver.start(
                    transportService.getThreadPool().getThreadContext(),
                    workerExecutor,
                    driver,
                    Driver.DEFAULT_MAX_ITERATIONS,
                    completion
                );
            }

            @SuppressWarnings("unused")
            void startViaChildRequest(Driver driver, ActionListener<Void> driverListener) {
                transportService.sendChildRequest(
                    transportService.getLocalNode(),
                    ACTION_NAME,
                    new DriverRequest(driver, workerExecutor),
                    parentTask,
                    TransportRequestOptions.EMPTY,
                    TransportResponseHandler.empty(
                        searchExecutor,
                        // The TransportResponseHandler can be notified while the Driver is still running during node shutdown
                        // or the Driver hasn't started when the parent task is canceled. In such cases, we should abort
                        // the Driver and wait for it to finish.
                        ActionListener.wrap(driverListener::onResponse, e -> driver.abort(e, driverListener))
                    )
                );
            }
        };
        runner.runToCompletion(drivers, listener);
    }

    private static class DriverRequest extends UntypedActionRequest implements CompositeIndicesRequest {
        private final Driver driver;
        private final Executor executor;

        DriverRequest(Driver driver, Executor executor) {
            this.driver = driver;
            this.executor = executor;
        }

        DriverRequest(StreamInput in) {
            throw new UnsupportedOperationException("Driver request should never leave the current node");
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            throw new UnsupportedOperationException("Driver request should never leave the current node");
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            if (parentTaskId.isSet() == false) {
                assert false : "DriverRequest must have a parent task";
                throw new IllegalStateException("DriverRequest must have a parent task");
            }
            return new CancellableTask(id, type, action, "", parentTaskId, headers) {
                @Override
                protected void onCancelled() {
                    String reason = Objects.requireNonNullElse(getReasonCancelled(), "cancelled");
                    driver.cancel(reason);
                }

                @Override
                public String getDescription() {
                    return driver.describe();
                }

                @Override
                public Status getStatus() {
                    return driver.status();
                }
            };
        }
    }

    private record DriverRequestHandler(TransportService transportService) implements TransportRequestHandler<DriverRequest> {
        @Override
        public void messageReceived(DriverRequest request, TransportChannel channel, Task task) {
            System.err.println("--> driver request received (on SEARCH) [" + request.driver.shortDescription + "] " + System.nanoTime());
            var listener = new ChannelActionListener<ActionResponse.Empty>(channel);
            Driver.start(
                transportService.getThreadPool().getThreadContext(),
                request.executor,
                request.driver,
                Driver.DEFAULT_MAX_ITERATIONS,
                listener.map(unused -> ActionResponse.Empty.INSTANCE)
            );
        }
    }
}
