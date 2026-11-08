/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.compute.lucene.query.LuceneSourceOperator;
import org.elasticsearch.compute.lucene.query.TsidRangeQuery;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.plugin.ComputeService;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.index.mapper.DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

/**
 * Runs time-series aggregations with the source queries cut into {@code _tsid} ranges by the {@code tsid_partition_series}
 * pragma and checks that they return what the uncut queries return, both with all ranges in one request and with one range
 * per request through {@code tsid_partition_index}. The index has a few hundred series over several segments, so the cuts
 * come from one segment while the other segments hold series the cut segment lacks.
 */
@LuceneTestCase.SuppressCodecs("*") // require production codecs
public class TimeSeriesTsidPartitionIT extends AbstractEsqlIntegTestCase {
    private static final String INDEX = "hosts";
    private static final List<String> QUERIES = List.of(
        "TS hosts | STATS s = sum(rate(request_count)) BY tb = tbucket(1 minute) | SORT tb",
        "TS hosts | STATS load = avg(avg_over_time(cpu)) BY cluster, tb = tbucket(1 minute) | SORT cluster, tb",
        "TS hosts | STATS peak = max(max_over_time(cpu)) BY cluster | SORT cluster",
        // a scoring query over the same index: the cut must leave the scores alone
        "FROM hosts METADATA _score | WHERE cluster:\"prod\" | STATS score = sum(_score), c = count(*) BY host | SORT host"
    );

    private int numHosts;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        // registers index.merge.enabled
        return CollectionUtils.appendToCopy(super.nodePlugins(), InternalSettingsPlugin.class);
    }

    @Before
    public void populateIndex() {
        assumeTrue("query pragmas require a snapshot build", canUseQueryPragmas());
        Settings.Builder settings = Settings.builder()
            .put("mode", "time_series")
            .putList("routing_path", List.of("host"))
            // One copy of one shard: the cuts come from a shard copy's own segments, so a replica, which flushes on its
            // own, could cut different ranges than the primary and the requests of testOneRangePerRequest would not line up.
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            // keep the segments of the batches below, so the series are spread over several of them
            .put(MergePolicyConfig.INDEX_MERGE_ENABLED, false);
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(INDEX)
                .setSettings(settings)
                .setMapping(
                    "@timestamp",
                    "type=date",
                    "host",
                    "type=keyword,time_series_dimension=true",
                    "cluster",
                    "type=keyword,time_series_dimension=true",
                    "cpu",
                    "type=double,time_series_metric=gauge",
                    "request_count",
                    "type=long,time_series_metric=counter"
                )
        );
        numHosts = between(200, 400);
        Map<String, String> hostToCluster = new HashMap<>();
        Map<String, Long> requestCounts = new HashMap<>();
        for (int i = 0; i < numHosts; i++) {
            hostToCluster.put("host-" + i, randomFrom("qa", "staging", "prod"));
        }
        long timestamp = DEFAULT_DATE_TIME_FORMATTER.parseMillis("2024-04-15T00:00:00Z");
        int numBatches = between(3, 6);
        for (int batch = 0; batch < numBatches; batch++) {
            BulkRequestBuilder bulk = client().prepareBulk(INDEX);
            for (Map.Entry<String, String> host : hostToCluster.entrySet()) {
                if (randomInt(10) < 2) {
                    continue; // not every series is in every segment
                }
                int numDocs = between(1, 3);
                for (int d = 0; d < numDocs; d++) {
                    timestamp += between(1, 10) * 1000L;
                    long requestCount = requestCounts.merge(host.getKey(), (long) between(1, 20), Long::sum);
                    bulk.add(
                        client().prepareIndex(INDEX)
                            .setSource(
                                "@timestamp",
                                timestamp,
                                "host",
                                host.getKey(),
                                "cluster",
                                host.getValue(),
                                "cpu",
                                randomIntBetween(0, 100),
                                "request_count",
                                requestCount
                            )
                    );
                }
            }
            BulkResponse response = bulk.get();
            assertFalse(response.buildFailureMessage(), response.hasFailures());
            client().admin().indices().prepareRefresh(INDEX).get();
        }
    }

    /**
     * All ranges in one request return the rows of the uncut query, under {@code auto} and {@code shard} partitioning.
     * {@code rate} runs on the time-series source; {@code avg_over_time} and {@code max_over_time} on a standard source
     * over the same index, which the rule cuts too.
     */
    public void testAllRangesMatchUncut() {
        int seriesPerRange = numHosts / 4;
        for (String dataPartitioning : List.of("auto", "shard")) {
            for (String query : QUERIES) {
                List<List<Object>> expected = rows(query, pragmas(dataPartitioning, 0, -1));
                EsqlQueryResponse.Profile profile;
                List<List<Object>> actual;
                try (EsqlQueryResponse resp = run(request(query, pragmas(dataPartitioning, seriesPerRange, -1)))) {
                    actual = EsqlTestUtils.getValuesList(resp);
                    profile = resp.profile();
                }
                assertThat(
                    "[" + query + "] with data_partitioning [" + dataPartitioning + "]",
                    canonicalize(actual),
                    equalTo(canonicalize(expected))
                );
                List<LuceneSourceOperator.Status> sources = sourceStatuses(profile);
                String description = "[" + query + "] with data_partitioning [" + dataPartitioning + "]";
                assertThat(description, sources, not(empty()));
                for (LuceneSourceOperator.Status source : sources) {
                    TsidRangeQuery.Status tsidRange = source.tsidRange();
                    assertNotNull(description, tsidRange);
                    assertThat(description, tsidRange.leaves(), greaterThan(0L));
                    assertThat(description, tsidRange.prefixTable() + tsidRange.skipper() + tsidRange.binarySearch(), greaterThan(0L));
                }
                // Each range is its own query to the slice queue.
                long ranges = sources.stream()
                    .flatMap(s -> s.processedQueries().stream())
                    .filter(q -> q.contains("_tsid:["))
                    .distinct()
                    .count();
                assertThat(description, ranges, greaterThanOrEqualTo(2L));
            }
        }
        try (EsqlQueryResponse resp = run(request(QUERIES.getFirst(), pragmas("auto", 0, -1)))) {
            for (LuceneSourceOperator.Status source : sourceStatuses(resp.profile())) {
                assertThat(source.tsidRange(), nullValue());
            }
        }
    }

    /**
     * Without any pragma, a shard whose largest segment holds more than {@code tsid_partition_series} (10k) series is cut.
     */
    public void testDefaultCutsLargeSegment() {
        String index = "many-hosts";
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(index)
                .setSettings(
                    Settings.builder()
                        .put("mode", "time_series")
                        .putList("routing_path", List.of("host"))
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                )
                .setMapping(
                    "@timestamp",
                    "type=date",
                    "host",
                    "type=keyword,time_series_dimension=true",
                    "cpu",
                    "type=double,time_series_metric=gauge"
                )
        );
        int numSeries = between(10_001, 12_000);
        long timestamp = DEFAULT_DATE_TIME_FORMATTER.parseMillis("2024-04-15T00:00:00Z");
        BulkRequestBuilder bulk = client().prepareBulk(index);
        for (int i = 0; i < numSeries; i++) {
            bulk.add(client().prepareIndex(index).setSource("@timestamp", timestamp + i, "host", "host-" + i, "cpu", i % 100));
        }
        BulkResponse response = bulk.get();
        assertFalse(response.buildFailureMessage(), response.hasFailures());
        // one segment, so its series count is the whole index's
        client().admin().indices().prepareForceMerge(index).setMaxNumSegments(1).get();
        client().admin().indices().prepareRefresh(index).get();

        String query = "TS "
            + index
            + " | STATS load = avg(avg_over_time(cpu)), c = sum(count_over_time(cpu)) BY tb = tbucket(1 minute) | SORT tb";
        EsqlQueryRequest request = new EsqlQueryRequest();
        request.query(query);
        request.profile(true);
        try (EsqlQueryResponse resp = run(request)) {
            List<List<Object>> rows = EsqlTestUtils.getValuesList(resp);
            assertThat(rows, hasSize(1));
            assertThat(((Number) rows.getFirst().get(1)).longValue(), equalTo((long) numSeries));
            List<LuceneSourceOperator.Status> sources = sourceStatuses(resp.profile());
            assertThat(sources, not(empty()));
            for (LuceneSourceOperator.Status source : sources) {
                assertNotNull(source.tsidRange());
                assertThat(source.tsidRange().leaves(), greaterThan(0L));
            }
            long ranges = sources.stream()
                .flatMap(s -> s.processedQueries().stream())
                .filter(q -> q.contains("_tsid:["))
                .distinct()
                .count();
            assertThat(ranges, equalTo(2L));
        }
        // Turned off, the same rows and no ranges.
        try (EsqlQueryResponse resp = run(request(query, pragmas(randomFrom("auto", "shard"), 0, -1)))) {
            assertThat(canonicalize(EsqlTestUtils.getValuesList(resp)), equalTo(canonicalize(rows(query, QueryPragmas.EMPTY))));
            for (LuceneSourceOperator.Status source : sourceStatuses(resp.profile())) {
                assertThat(source.tsidRange(), nullValue());
            }
        }
    }

    /**
     * One range per request: the sums of each bucket over the ranges add up to the sums of the uncut query, and the first
     * index past the last range matches nothing.
     */
    public void testOneRangePerRequest() {
        String query = "TS hosts | STATS s = sum(rate(request_count)), m = sum(max_over_time(cpu)) BY tb = tbucket(1 minute) | SORT tb";
        int seriesPerRange = numHosts / 4;
        String dataPartitioning = randomFrom("auto", "shard");
        Map<Object, double[]> expected = sumsByBucket(rows(query, pragmas(dataPartitioning, 0, -1)));
        Map<Object, double[]> summed = new HashMap<>();
        int ranges = 0;
        while (true) {
            List<List<Object>> rows = rows(query, pragmas(dataPartitioning, seriesPerRange, ranges));
            if (rows.isEmpty()) {
                break;
            }
            sumsByBucket(rows).forEach((bucket, sums) -> {
                double[] total = summed.computeIfAbsent(bucket, k -> new double[2]);
                total[0] += sums[0];
                total[1] += sums[1];
            });
            ranges++;
            assertThat("more ranges than series", ranges, lessThanOrEqualTo(numHosts));
        }
        assertThat(ranges, greaterThanOrEqualTo(2));
        assertThat(summed.keySet(), equalTo(expected.keySet()));
        for (Map.Entry<Object, double[]> bucket : expected.entrySet()) {
            double[] actual = summed.get(bucket.getKey());
            assertThat("sum(rate) of " + bucket.getKey(), actual[0], closeTo(bucket.getValue()[0], 1e-6));
            assertThat("sum(max_over_time) of " + bucket.getKey(), actual[1], closeTo(bucket.getValue()[1], 1e-6));
        }
    }

    private static QueryPragmas pragmas(String dataPartitioning, int seriesPerRange, int rangeIndex) {
        return new QueryPragmas(
            Settings.builder()
                .put(QueryPragmas.DATA_PARTITIONING.getKey(), dataPartitioning)
                // let a small index pick time-series partitioning under "auto"
                .put(PlannerSettings.DOC_THRESHOLD_AUTO_PARTITIONING.getKey(), 1)
                .put(QueryPragmas.TASK_CONCURRENCY.getKey(), between(1, 4))
                .put(QueryPragmas.TSID_PARTITION_SERIES.getKey(), seriesPerRange)
                .put(QueryPragmas.TSID_PARTITION_INDEX.getKey(), rangeIndex)
                .build()
        );
    }

    private static EsqlQueryRequest request(String query, QueryPragmas pragmas) {
        EsqlQueryRequest request = new EsqlQueryRequest();
        request.query(query);
        request.pragmas(pragmas);
        request.acceptedPragmaRisks(true);
        request.profile(true);
        return request;
    }

    private List<List<Object>> rows(String query, QueryPragmas pragmas) {
        try (EsqlQueryResponse resp = run(request(query, pragmas))) {
            return EsqlTestUtils.getValuesList(resp);
        }
    }

    private static List<LuceneSourceOperator.Status> sourceStatuses(EsqlQueryResponse.Profile profile) {
        List<LuceneSourceOperator.Status> statuses = new ArrayList<>();
        for (DriverProfile driver : profile.drivers()) {
            if (driver.description().equals(ComputeService.DATA_DESCRIPTION)) {
                for (OperatorStatus operator : driver.operators()) {
                    if (operator.status() instanceof LuceneSourceOperator.Status status) {
                        statuses.add(status);
                    }
                }
            }
        }
        return statuses;
    }

    /**
     * The two sums of each row, by bucket; a {@code null} sum counts as zero.
     */
    private static Map<Object, double[]> sumsByBucket(List<List<Object>> rows) {
        Map<Object, double[]> sums = new HashMap<>();
        for (List<Object> row : rows) {
            assertNull(sums.put(row.get(2), new double[] { doubleValue(row.get(0)), doubleValue(row.get(1)) }));
        }
        return sums;
    }

    private static double doubleValue(Object value) {
        return value == null ? 0 : ((Number) value).doubleValue();
    }

    /**
     * Rounds floating-point cells, as the order of the partial sums changes with the ranges, and sorts the rows.
     */
    private static List<List<Object>> canonicalize(List<List<Object>> rows) {
        return rows.stream()
            .map(row -> row.stream().map(v -> v instanceof Double d ? (Object) (Math.round(d * 1_000_000d) / 1_000_000d) : v).toList())
            .sorted(Comparator.comparing(Objects::toString))
            .toList();
    }
}
