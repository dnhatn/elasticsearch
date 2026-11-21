/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesIndexResponse;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.fieldcaps.TimeSeriesStats;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.lucene.query.TimeSeriesGroups;
import org.elasticsearch.compute.lucene.query.TsidPrefixHistogram;
import org.elasticsearch.compute.operator.PlanProfile;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TestTransportChannel;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.core.querydsl.query.RangeQuery;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.querydsl.query.SingleValueQuery;
import org.junit.Before;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

@LuceneTestCase.SuppressCodecs("*")
public class TsidStatsIT extends AbstractEsqlIntegTestCase {
    private static final String[] INDICES = { "ts-1", "ts-2" };
    private static final int HOSTS = 50;
    private static final int POINTS = 20;
    private static final long START = 1_700_000_000_000L;

    private long cpuDocs;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(MockTransportService.TestPlugin.class);
        return plugins;
    }

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    @Before
    public void indexDocs() {
        for (String index : INDICES) {
            Settings settings = Settings.builder()
                .put("mode", "time_series")
                .putList("index.dimensions", List.of("host"))
                .put("number_of_shards", between(1, 3))
                .put("number_of_replicas", 0)
                .build();
            client().admin()
                .indices()
                .prepareCreate(index)
                .setSettings(settings)
                .setMapping(
                    "@timestamp",
                    "type=date",
                    "host",
                    "type=keyword,time_series_dimension=true",
                    "cpu",
                    "type=long,time_series_metric=gauge",
                    "memory",
                    "type=long,time_series_metric=gauge",
                    "requests",
                    "type=long,time_series_metric=counter"
                )
                .get();
            for (int host = 0; host < HOSTS; host++) {
                String metric = host % 2 == 0 ? "cpu" : "memory";
                for (int point = 0; point < POINTS; point++) {
                    client().prepareIndex(index)
                        .setSource(
                            "@timestamp",
                            START + point * 1000L,
                            "host",
                            "host-" + host,
                            metric,
                            host + point,
                            "requests",
                            point * (host + 1)
                        )
                        .get();
                    if (metric.equals("cpu")) {
                        cpuDocs++;
                    }
                }
            }
            client().admin().indices().prepareRefresh(index).get();
        }
    }

    public void testTimeSeriesStats() {
        for (String index : INDICES) {
            client().admin().indices().prepareForceMerge(index).setMaxNumSegments(1).get();
            client().admin().indices().prepareRefresh(index).get();
        }
        FieldCapabilitiesRequest request = new FieldCapabilitiesRequest().indices(INDICES).fields("cpu");
        request.setMergeResults(false);
        for (FieldCapabilitiesIndexResponse response : client().fieldCaps(request).actionGet().getIndexResponses()) {
            assertNull(response.getTimeSeriesStats());
        }
        request.includeTimeSeriesStats(true);
        for (FieldCapabilitiesIndexResponse response : client().fieldCaps(request).actionGet().getIndexResponses()) {
            TimeSeriesStats stats = response.getTimeSeriesStats();
            assertThat(stats.minTimestamp(), equalTo(START));
            assertThat(stats.maxTimestamp(), equalTo(START + (POINTS - 1) * 1000L));
            // one shard stands for all of them, so the series count is exact only when the hosts split evenly
            long series = stats.seriesMillis() / (stats.maxTimestamp() - stats.minTimestamp() + 1);
            assertThat(series, greaterThanOrEqualTo((long) HOSTS / 3));
            assertThat(series, lessThanOrEqualTo((long) HOSTS * 3));
        }
    }

    public void testProbe() {
        // one segment per shard: a series that a refresh cut into two segments covering the same time range would count twice
        for (String index : INDICES) {
            client().admin().indices().prepareForceMerge(index).setMaxNumSegments(1).get();
            client().admin().indices().prepareRefresh(index).get();
        }
        // both indices hold every host over the same time range, so each host counts once per index
        TsidPrefixHistogram all = probe(null, null);
        assertThat(all.size(), greaterThan(1));
        assertThat(all.minTimestamp(), equalTo(START));
        assertThat(all.maxTimestamp(), equalTo(START + (POINTS - 1) * 1000L));
        assertThat(all.estimatedSeries(), equalTo(2L * HOSTS));

        TsidPrefixHistogram cpu = probe(null, QueryBuilders.existsQuery("cpu"));
        assertThat(cpu.size(), greaterThan(0));
        assertThat(cpu.size(), lessThanOrEqualTo(all.size()));
        assertThat(cpu.minTimestamp(), equalTo(START));
        assertThat(cpu.maxTimestamp(), equalTo(START + (POINTS - 1) * 1000L));
        assertThat(cpu.estimatedSeries(), greaterThanOrEqualTo((long) HOSTS));
        assertThat(cpu.estimatedSeries(), lessThan(all.estimatedSeries()));

        // cpu is reported once a second, so 100ms buckets hold no more groups than there are cpu docs
        TsidPrefixHistogram cpuFine = probe(null, QueryBuilders.existsQuery("cpu"), 100);
        long fineGroups = Math.ceilDiv(cpuFine.totalSeriesMillis(), 100);
        assertThat(fineGroups, greaterThanOrEqualTo(cpuDocs));
        assertThat(fineGroups, lessThanOrEqualTo(cpuDocs + cpuFine.size()));
        assertThat(Math.ceilDiv(cpu.totalSeriesMillis(), 100), greaterThan(fineGroups));

        TsidPrefixHistogram missing = probe(null, QueryBuilders.existsQuery("unknown"));
        assertThat(missing.size(), equalTo(0));
        assertThat(missing.estimatedSeries(), equalTo(0L));

        QueryBuilder range = QueryBuilders.rangeQuery("@timestamp").gte(START + 5000).lt(START + 10_000);
        TsidPrefixHistogram clipped = probe(range, null);
        assertThat(clipped.minTimestamp(), equalTo(START + 5000));
        assertThat(clipped.maxTimestamp(), equalTo(START + 10_000));
        assertThat(clipped.estimatedSeries(), equalTo(2L * HOSTS));

        TsidPrefixHistogram openEnded = probe(QueryBuilders.rangeQuery("@timestamp").gte(START + 5000), null);
        assertThat(openEnded.minTimestamp(), equalTo(START + 5000));
        assertThat(openEnded.maxTimestamp(), equalTo(START + (POINTS - 1) * 1000L));

        // the shape ES|QL pushes for WHERE @timestamp >= ... AND @timestamp < ...
        QueryBuilder pushed = QueryBuilders.boolQuery()
            .must(singleValueRange(Instant.ofEpochMilli(START + 5000).toString(), null))
            .must(singleValueRange(null, Instant.ofEpochMilli(START + 10_000).toString()));
        TsidPrefixHistogram pushedClipped = probe(pushed, null);
        assertThat(pushedClipped.minTimestamp(), equalTo(START + 5000));
        assertThat(pushedClipped.maxTimestamp(), equalTo(START + 10_000));
        assertThat(pushedClipped.estimatedSeries(), equalTo(2L * HOSTS));
    }

    private static QueryBuilder singleValueRange(String lower, String upper) {
        RangeQuery range = new RangeQuery(
            Source.EMPTY,
            "@timestamp",
            lower,
            true,
            upper,
            false,
            "strict_date_optional_time",
            ZoneOffset.UTC
        );
        return new SingleValueQuery(range, "@timestamp", false).toQueryBuilder();
    }

    public void testQueryWithSplits() {
        assumeTrue("pragmas require snapshot builds", canUseQueryPragmas());
        QueryPragmas noSplits = new QueryPragmas(Settings.builder().put("time_series_split_memory", "1tb").build());
        List<String> queries = List.of(
            "TS ts-* | STATS c = sum(count_over_time(cpu))",
            "TS ts-* | STATS s = sum(sum_over_time(cpu)), c = sum(count_over_time(cpu)) BY host | SORT host | LIMIT 100",
            "TS ts-* | STATS s = sum(sum_over_time(memory)) BY host, b = bucket(@timestamp, 5 seconds) | SORT host, b | LIMIT 1000",
            "TS ts-* | STATS r = max(rate(requests)) BY host | SORT host | LIMIT 100",
            "TS ts-* | STATS m = max(avg_over_time(cpu)), n = min(avg_over_time(memory)) BY host | SORT host | LIMIT 100"
        );
        for (boolean intraSegment : new boolean[] { false, true }) {
            Settings.Builder settings = Settings.builder()
                .put("time_series_single_pass", false)
                .put("time_series_split_memory", ByteSizeValue.ofBytes(HOSTS / 4 * TimeSeriesGroups.ESTIMATED_BYTES_PER_GROUP));
            if (intraSegment) {
                settings.put(PlannerSettings.DOC_THRESHOLD_AUTO_PARTITIONING.getKey(), 1);
            }
            QueryPragmas splits = new QueryPragmas(settings.build());
            for (String query : queries) {
                try (
                    EsqlQueryResponse expected = run(syncEsqlQueryRequest(query).pragmas(noSplits));
                    EsqlQueryResponse actual = run(syncEsqlQueryRequest(query).pragmas(splits).profile(true))
                ) {
                    assertThat(EsqlTestUtils.getValuesList(actual), equalTo(EsqlTestUtils.getValuesList(expected)));
                    List<String> ranges = actual.profile()
                        .plans()
                        .stream()
                        .map(PlanProfile::logicalPlanTree)
                        .filter(plan -> plan != null && plan.contains("[tsid="))
                        .map(plan -> plan.substring(plan.indexOf("[tsid="), plan.indexOf(']', plan.indexOf("[tsid="))))
                        .distinct()
                        .toList();
                    assertThat(ranges.toString(), ranges.size(), greaterThan(1));
                    for (PlanProfile plan : actual.profile().plans()) {
                        String tree = plan.planTree().replace("\n", "");
                        if (plan.description().startsWith("subplan-")) {
                            assertThat(tree, tree.contains("\\_AggregateExec["), equalTo(false));
                        } else if (plan.description().equals("main.final")) {
                            assertThat(tree, tree.contains(",SINGLE,"), equalTo(true));
                        }
                    }
                }
            }
            try (EsqlQueryResponse total = run(syncEsqlQueryRequest(queries.getFirst()).pragmas(splits))) {
                assertThat(EsqlTestUtils.getValuesList(total), equalTo(List.of(List.of(cpuDocs))));
            }
        }
    }

    public void testCancellingQueryStopsProbe() throws Exception {
        assumeTrue("pragmas require snapshot builds", canUseQueryPragmas());
        String coordinator = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        var probeTask = new AtomicReference<CancellableTask>();
        var probeStarted = new CountDownLatch(1);
        var releaseProbe = new CountDownLatch(1);
        var probeResponse = new PlainActionFuture<TransportResponse>();
        var queryResponse = new PlainActionFuture<EsqlQueryResponse>();
        for (String node : internalCluster().getNodeNames()) {
            MockTransportService.getInstance(node).addRequestHandlingBehavior(TsidStats.ACTION_NAME, (handler, request, channel, task) -> {
                if (probeTask.compareAndSet(null, asInstanceOf(CancellableTask.class, task))) {
                    probeStarted.countDown();
                    safeAwait(releaseProbe);
                    handler.messageReceived(request, new TestTransportChannel(ActionListener.wrap(response -> {
                        probeResponse.onResponse(response);
                        channel.sendResponse(response);
                    }, e -> {
                        probeResponse.onFailure(e);
                        channel.sendResponse(e);
                    })), task);
                } else {
                    handler.messageReceived(request, channel, task);
                }
            });
        }
        try {
            var client = internalCluster().getInstance(NodeClient.class, coordinator);
            var transport = MockTransportService.getInstance(coordinator);
            var request = syncEsqlQueryRequest("TS ts-* | STATS c = sum(count_over_time(cpu))").pragmas(
                new QueryPragmas(Settings.builder().put("time_series_single_pass", false).put("time_series_split_memory", "1b").build())
            );
            var queryTask = asInstanceOf(
                CancellableTask.class,
                client.executeAndReturnTask(EsqlQueryAction.INSTANCE, request, queryResponse)
            );
            safeAwait(probeStarted);
            assertEquals(new TaskId(client.getLocalNodeId(), queryTask.getId()), probeTask.get().getParentTaskId());

            var cancelled = new PlainActionFuture<Void>();
            transport.getTaskManager().cancelTaskAndDescendants(queryTask, "test cancellation", false, cancelled);
            cancelled.actionGet(DEFAULT_REQUEST_TIMEOUT);
            assertBusy(() -> assertTrue(probeTask.get().isCancelled()));
            releaseProbe.countDown();

            expectThrows(TaskCancelledException.class, () -> probeResponse.actionGet(DEFAULT_REQUEST_TIMEOUT));
            Exception failure = expectThrows(Exception.class, () -> {
                try (var response = queryResponse.actionGet(DEFAULT_REQUEST_TIMEOUT)) {
                    fail("cancelled query returned a response");
                }
            });
            assertNotNull(ExceptionsHelper.unwrap(failure, TaskCancelledException.class));
        } finally {
            releaseProbe.countDown();
            for (String node : internalCluster().getNodeNames()) {
                MockTransportService.getInstance(node).clearAllRules();
            }
        }
    }

    private TsidPrefixHistogram probe(QueryBuilder shardFilter, QueryBuilder metricFilter) {
        return probe(shardFilter, metricFilter, 0);
    }

    private TsidPrefixHistogram probe(QueryBuilder shardFilter, QueryBuilder metricFilter, long bucketMillis) {
        long[] bounds = TsidStats.timestampBounds(shardFilter, System.currentTimeMillis());
        PlainActionFuture<TsidPrefixHistogram> future = new PlainActionFuture<>();
        var transport = internalCluster().getInstance(TransportService.class);
        var taskManager = transport.getTaskManager();
        var queryTask = asInstanceOf(
            CancellableTask.class,
            taskManager.register("transport", EsqlQueryAction.NAME, syncEsqlQueryRequest("TS ts-*"))
        );
        try {
            TsidStats.probe(
                client(transport.getLocalNode().getName()),
                transport,
                queryTask,
                INDICES,
                shardFilter,
                metricFilter,
                bounds[0],
                bounds[1],
                bucketMillis,
                future
            );
            return future.actionGet(DEFAULT_REQUEST_TIMEOUT);
        } finally {
            taskManager.unregister(queryTask);
        }
    }
}
