/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.test;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.DriverRunner;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.PageConsumerOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;

/**
 * Superclass for testing with blocks and operators
 */
public abstract class ComputeTestCase extends ESTestCase {

    private final List<CircuitBreaker> breakers = new ArrayList<>();
    private final List<BlockFactory> blockFactories = new ArrayList<>();

    /**
     * A {@link BigArrays} that won't throw {@link CircuitBreakingException}.
     * <p>
     * Rather than using the {@link NoneCircuitBreakerService} we use a
     * very large limit so tests can call {@link CircuitBreaker#getUsed()}.
     * </p>
     */
    protected final BigArrays nonBreakingBigArrays() {
        return new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofBytes(Integer.MAX_VALUE)).withCircuitBreaking();
    }

    /**
     * Build a {@link BlockFactory} with a huge limit.
     */
    protected final BlockFactory blockFactory() {
        return blockFactory(ByteSizeValue.ofGb(1));
    }

    /**
     * Build a {@link BlockFactory} with a configured limit.
     */
    protected final BlockFactory blockFactory(ByteSizeValue limit) {
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, limit).withCircuitBreaking();
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        breakers.add(breaker);
        BlockFactory factory = new MockBlockFactory(breaker, bigArrays);
        blockFactories.add(factory);
        return factory;
    }

    /**
     * Build a {@link BlockFactory} that randomly fails.
     */
    protected final BlockFactory crankyBlockFactory() {
        CrankyCircuitBreakerService cranky = new CrankyCircuitBreakerService();
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, cranky).withCircuitBreaking();
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        breakers.add(breaker);
        BlockFactory blockFactory = new MockBlockFactory(breaker, bigArrays);
        blockFactories.add(blockFactory);
        return blockFactory;
    }

    protected final void testWithCrankyBlockFactory(Consumer<BlockFactory> run) {
        try {
            run.accept(crankyBlockFactory());
            logger.info("cranky let us finish!");
        } catch (CircuitBreakingException e) {
            logger.info("cranky", e);
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    @After
    public final void allBreakersEmpty() throws Exception {
        // first check that all big arrays are released, which can affect breakers
        MockBigArrays.ensureAllArraysAreReleased();
        for (var factory : blockFactories) {
            if (factory instanceof MockBlockFactory mockBlockFactory) {
                mockBlockFactory.ensureAllBlocksAreReleased();
            }
        }
        for (CircuitBreaker breaker : breakers) {
            assertThat("Unexpected used in breaker: " + breaker, breaker.getUsed(), equalTo(0L));
        }
    }

    /**
     * A {@link DriverContext} with a nonBreakingBigArrays.
     */
    protected DriverContext driverContext() { // TODO make this final once all operators support memory tracking
        BlockFactory blockFactory = blockFactory();
        return new DriverContext(blockFactory.bigArrays(), blockFactory);
    }

    protected final DriverContext crankyDriverContext() {
        BlockFactory blockFactory = crankyBlockFactory();
        return new DriverContext(blockFactory.bigArrays(), blockFactory);
    }

    protected final List<Page> drive(Operator operator, Iterator<Page> input, DriverContext driverContext) {
        return drive(List.of(operator), input, driverContext);
    }

    protected final List<Page> drive(List<Operator> operators, Iterator<Page> input, DriverContext driverContext) {
        List<Page> results = new ArrayList<>();
        boolean success = false;
        try (
            Driver d = TestDriverFactory.create(
                driverContext,
                new CannedSourceOperator(input),
                operators,
                new TestResultPageSinkOperator(results::add)
            )
        ) {
            runDriver(d);
            success = true;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(Releasables.wrap(() -> Iterators.map(results.iterator(), p -> p::releaseBlocks)));
            }
        }
        return results;
    }

    public static void runDriver(Driver driver) {
        runDriver(List.of(driver));
    }

    public static void runDriver(List<Driver> drivers) {
        drivers = new ArrayList<>(drivers);
        int dummyDrivers = between(0, 10);
        for (int i = 0; i < dummyDrivers; i++) {
            drivers.add(
                TestDriverFactory.create(
                    new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, TestBlockFactory.getNonBreakingInstance()),
                    new SequenceLongBlockSourceOperator(
                        TestBlockFactory.getNonBreakingInstance(),
                        LongStream.range(0, between(1, 100)),
                        between(1, 100)
                    ),
                    List.of(),
                    new PageConsumerOperator(Page::releaseBlocks)
                )
            );
        }
        Randomness.shuffle(drivers);
        int numThreads = between(1, 16);
        ThreadPool threadPool = new TestThreadPool(
            getTestClass().getSimpleName(),
            new FixedExecutorBuilder(Settings.EMPTY, "esql", numThreads, 1024, "esql", EsExecutors.TaskTrackingConfig.DEFAULT)
        );
        var driverRunner = new DriverRunner(threadPool.getThreadContext()) {
            @Override
            protected void start(Driver driver, ActionListener<Void> driverListener) {
                Driver.start(threadPool.getThreadContext(), threadPool.executor("esql"), driver, between(1, 10000), driverListener);
            }
        };
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        try {
            driverRunner.runToCompletion(drivers, future);
            future.actionGet(TimeValue.timeValueSeconds(30));
        } finally {
            terminate(threadPool);
        }
    }
}
