/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.MaxLongAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.SumLongAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.ValuesBytesRefAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.OrdinalBytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.AbstractBlockSourceOperator;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.core.TimeValue;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.index.mapper.DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER;
import static org.hamcrest.Matchers.equalTo;

public class TimeSeriesAggregationOperatorTests extends ComputeTestCase {

    static final long timeSeriesEndDate = DEFAULT_DATE_TIME_FORMATTER.parseMillis("2025-05-02T10:00:00Z");
    static final long timeSeriesStartDate = DEFAULT_DATE_TIME_FORMATTER.parseMillis("2025-05-02T01:00:00Z");

    public void testBasic() {
        List<DataPoint> dataPoints = generateDataPoints();
        Set<Row> actualRows = runWithTimeSeries(dataPoints);
        Set<Row> expectedRows = runWithHashAggregation(dataPoints);
        assertThat(actualRows, equalTo(expectedRows));
    }

    private Set<Row> runWithTimeSeries(List<DataPoint> dataPoints) {
        DriverContext driverContext = driverContext();
        Rounding.Prepared timeBucket = Rounding.builder(TimeValue.timeValueMillis(100)).build().prepareForUnknown();
        DataPointSourceOperator sourceOperator = new DataPointSourceOperator(driverContext.blockFactory(), timeBucket, dataPoints);
        var maxOverTime = new MaxLongAggregatorFunctionSupplier();
        var sum = new SumLongAggregatorFunctionSupplier();
        var values = new ValuesBytesRefAggregatorFunctionSupplier();

        Instant timeSeriesEndTime = Instant.ofEpochMilli(timeSeriesEndDate - between(0, 3600 * 1000));
        Instant timeSeriesStartTime = Instant.ofEpochMilli(timeSeriesStartDate + between(0, 3600 * 1000));
        var initialAgg = new TimeSeriesAggregationOperator.Factory(
            randomBoolean(),
            timeBucket,
            timeSeriesStartTime,
            timeSeriesEndTime,
            List.of(new BlockHash.GroupSpec(0, ElementType.BYTES_REF), new BlockHash.GroupSpec(1, ElementType.LONG)),
            AggregatorMode.INITIAL,
            List.of(
                maxOverTime.groupingAggregatorFactory(AggregatorMode.INITIAL, List.of(2)),
                values.groupingAggregatorFactory(AggregatorMode.INITIAL, List.of(3))
            ),
            randomIntBetween(10, 1024)
        );

        var finalAgg = new TimeSeriesAggregationOperator.Factory(
            false,
            timeBucket,
            null,
            null,
            List.of(new BlockHash.GroupSpec(0, ElementType.BYTES_REF), new BlockHash.GroupSpec(1, ElementType.LONG)),
            AggregatorMode.FINAL,
            List.of(
                maxOverTime.groupingAggregatorFactory(AggregatorMode.FINAL, List.of(2, 3)),
                values.groupingAggregatorFactory(AggregatorMode.FINAL, List.of(4))
            ),
            randomIntBetween(10, 1024)
        );

        var secondAgg = new HashAggregationOperator.HashAggregationOperatorFactory(
            List.of(new BlockHash.GroupSpec(3, ElementType.BYTES_REF), new BlockHash.GroupSpec(1, ElementType.LONG)),
            AggregatorMode.SINGLE,
            List.of(sum.groupingAggregatorFactory(AggregatorMode.SINGLE, List.of(2))),
            randomIntBetween(10, 1024),
            null
        );
        Set<Row> actualRows = new HashSet<>();
        Driver driver = new Driver(
            "unset",
            "test",
            "cluster",
            "node",
            0,
            0,
            driverContext,
            () -> "test",
            sourceOperator,
            List.of(initialAgg.get(driverContext), finalAgg.get(driverContext), secondAgg.get(driverContext)),
            new PageConsumerOperator(page -> {
                try {
                    BytesRefBlock hosts = page.getBlock(0);
                    LongBlock timestamps = page.getBlock(1);
                    LongBlock memoryUsages = page.getBlock(2);
                    BytesRef spare = new BytesRef();
                    for (int p = 0; p < page.getPositionCount(); p++) {
                        spare = hosts.getBytesRef(p, spare);
                        assertTrue(actualRows.add(new Row(spare.utf8ToString(), timestamps.getLong(p), memoryUsages.getLong(p))));
                    }
                } finally {
                    page.releaseBlocks();
                }
            }),
            TimeValue.timeValueMillis(1),
            () -> {

            }
        );
        runDriver(driver);
        return actualRows;
    }

    private Set<Row> runWithHashAggregation(List<DataPoint> dataPoints) {
        DriverContext driverContext = driverContext();
        Rounding.Prepared timeBucket = Rounding.builder(TimeValue.timeValueMillis(100)).build().prepareForUnknown();
        DataPointSourceOperator sourceOperator = new DataPointSourceOperator(driverContext.blockFactory(), timeBucket, dataPoints);
        var maxOverTime = new MaxLongAggregatorFunctionSupplier();
        var sum = new SumLongAggregatorFunctionSupplier();
        var values = new ValuesBytesRefAggregatorFunctionSupplier();

        var firstAgg = new HashAggregationOperator.HashAggregationOperatorFactory(
            List.of(new BlockHash.GroupSpec(0, ElementType.BYTES_REF), new BlockHash.GroupSpec(1, ElementType.LONG)),
            AggregatorMode.SINGLE,
            List.of(
                maxOverTime.groupingAggregatorFactory(AggregatorMode.SINGLE, List.of(2)),
                values.groupingAggregatorFactory(AggregatorMode.SINGLE, List.of(3))
            ),
            randomIntBetween(10, 1024),
            null
        );
        var secondAgg = new HashAggregationOperator.HashAggregationOperatorFactory(
            List.of(new BlockHash.GroupSpec(3, ElementType.BYTES_REF), new BlockHash.GroupSpec(1, ElementType.LONG)),
            AggregatorMode.SINGLE,
            List.of(sum.groupingAggregatorFactory(AggregatorMode.SINGLE, List.of(2))),
            randomIntBetween(10, 1024),
            null
        );
        Set<Row> actualRows = new HashSet<>();
        Driver driver = new Driver(
            "unset",
            "test",
            "cluster",
            "node",
            0,
            0,
            driverContext,
            () -> "test",
            sourceOperator,
            List.of(firstAgg.get(driverContext), secondAgg.get(driverContext)),
            new PageConsumerOperator(page -> {
                try {
                    BytesRefBlock hosts = page.getBlock(0);
                    LongBlock timestamps = page.getBlock(1);
                    LongBlock memoryUsages = page.getBlock(2);
                    BytesRef spare = new BytesRef();
                    for (int p = 0; p < page.getPositionCount(); p++) {
                        spare = hosts.getBytesRef(p, spare);
                        assertTrue(actualRows.add(new Row(spare.utf8ToString(), timestamps.getLong(p), memoryUsages.getLong(p))));
                    }
                } finally {
                    page.releaseBlocks();
                }
            }),
            TimeValue.timeValueMillis(1),
            () -> {}
        );
        runDriver(driver);
        return actualRows;
    }

    static class DataPointSourceOperator extends AbstractBlockSourceOperator {
        static final int DEFAULT_MAX_PAGE_POSITIONS = 8 * 1024;
        private final List<DataPoint> dataPoints;
        private final Rounding.Prepared timeBucket;

        DataPointSourceOperator(BlockFactory blockFactory, Rounding.Prepared timeBucket, List<DataPoint> dataPoints) {
            super(blockFactory, DEFAULT_MAX_PAGE_POSITIONS);
            this.dataPoints = dataPoints;
            this.timeBucket = timeBucket;
        }

        @Override
        protected int remaining() {
            return dataPoints.size() - currentPosition;
        }

        @Override
        protected Page createPage(int positionOffset, int length) {
            try (
                BytesRefVector.Builder dict = blockFactory.newBytesRefVectorBuilder(1);
                IntVector.Builder uidOrdsBuilder = blockFactory.newIntVectorBuilder(1);
                LongVector.Builder timestampsBuilder = blockFactory.newLongVectorBuilder(1);
                LongVector.Builder memoryUsageBuilder = blockFactory.newLongVectorBuilder(1);
                BytesRefBlock.Builder hostsBuilder = blockFactory.newBytesRefBlockBuilder(1)
            ) {
                Set<String> uids = new HashSet<>();
                int ord = -1;
                for (int i = 0; i < length; i++) {
                    var d = dataPoints.get(positionOffset + i);
                    if (uids.add(d.uid)) {
                        dict.appendBytesRef(new BytesRef(d.uid));
                        ord++;
                    }
                    uidOrdsBuilder.appendInt(ord);
                    timestampsBuilder.appendLong(timeBucket.round(d.timestamp()));
                    memoryUsageBuilder.appendLong(d.memoryUsage());
                    hostsBuilder.appendBytesRef(new BytesRef(d.host()));
                    currentPosition++;
                }
                OrdinalBytesRefVector tsid = new OrdinalBytesRefVector(uidOrdsBuilder.build(), dict.build());
                LongVector timestamps = timestampsBuilder.build();
                LongVector memoryUsage = memoryUsageBuilder.build();
                BytesRefBlock hosts = hostsBuilder.build();
                return new Page(tsid.asBlock(), timestamps.asBlock(), memoryUsage.asBlock(), hosts);
            }
        }

    }

    record Row(String host, long timestamp, long totalMemoryUsage) {

    }

    record DataPoint(long timestamp, String host, String uid, long memoryUsage) {

    }

    List<DataPoint> generateDataPoints() {
        Map<String, String> uid = new HashMap<>();
        int numUIDs = between(1, 20);
        for (int i = 0; i < numUIDs; i++) {
            uid.put(String.format(Locale.ROOT, "id-%02d", (i + 1)), randomFrom("host1", "host2", "host3"));
        }
        List<DataPoint> dataPoints = new ArrayList<>();
        int numDataPoints = between(1, 10_000);
        long timestamp = timeSeriesEndDate;
        for (int i = 0; i < numDataPoints; i++) {
            if (randomBoolean()) {
                timestamp -= 1000L * between(1, 10);
            }
            for (String app : randomSubsetOf(uid.keySet())) {
                String host = uid.get(app);
                long memoryUsage = randomLongBetween(0, 1000);
                dataPoints.add(new DataPoint(timestamp, host, app, memoryUsage));
            }
        }
        dataPoints.sort(Comparator.comparing(DataPoint::uid).thenComparing(Comparator.comparing(DataPoint::timestamp).reversed()));
        return dataPoints;
    }
}
