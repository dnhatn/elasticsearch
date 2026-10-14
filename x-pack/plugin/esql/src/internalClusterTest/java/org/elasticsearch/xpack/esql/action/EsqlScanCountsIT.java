/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.search.internal.ScanCounts;
import org.elasticsearch.test.ESIntegTestCase;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * End to end coverage for what a query reports moving: the bytes a column read decoded, the time it spent decoding
 * them, and the bytes a predicate walked. A query of known volume says what the counts should be, so these hold the
 * counts to the data rather than to each other.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class EsqlScanCountsIT extends AbstractEsqlIntegTestCase {

    private static final int DOCS = 200;
    private static final int VALUE_LENGTH = 1000;

    /** A column read counts the bytes it decoded, which is the volume it handed on, and how long that took. */
    public void testAColumnReadCountsWhatItDecoded() {
        final String index = "esql_scan_counts";
        assertAcked(
            prepareCreate(index).setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .setMapping("body", "type=keyword")
        );
        index(index, doc -> "x".repeat(VALUE_LENGTH - 8) + String.format(java.util.Locale.ROOT, "%08d", doc));

        // Grouping by the value reads every one of them: a length would be read from the column without them.
        final EsqlQueryRequest request = syncEsqlQueryRequest(
            "FROM " + index + " | STATS rows = COUNT(*) BY body | STATS groups = COUNT(*)"
        );
        request.profile(true);
        try (EsqlQueryResponse response = run(request)) {
            assertThat(response.profile(), notNullValue());
            final long volume = (long) DOCS * VALUE_LENGTH;
            assertThat("every value is its own group", asLong(response), greaterThanOrEqualTo((long) DOCS));

            final StringBuilder reported = new StringBuilder("\n");
            reported.append("drivers=").append(response.profile().drivers().size()).append('\n');
            long decoded = 0;
            long decodeNanos = 0;
            for (DriverProfile driver : response.profile().drivers()) {
                reported.append("  driver ")
                    .append(driver.description())
                    .append(' ')
                    .append(driver.operators().size())
                    .append(" operators\n");
                for (OperatorStatus operator : driver.operators()) {
                    final ScanCounts counts = operator.scanCounts();
                    reported.append("    ").append(operator.operator()).append(' ').append(counts).append('\n');
                    decoded += counts.decoded();
                    decodeNanos += counts.decodeNanos();
                }
            }
            assertThat("every value decoded is counted" + reported, decoded, greaterThanOrEqualTo(volume));
            assertThat("decoding them took time" + reported, decodeNanos, greaterThan(0L));
        }
    }

    /** A predicate reading a document's values counts the bytes it walked, which no column read counts for it. */
    public void testAPredicateCountsWhatItWalked() {
        final String index = "esql_scan_counts_text";
        assertAcked(
            prepareCreate(index).setSettings(
                Settings.builder().put("index.mode", "columnar").put("index.number_of_shards", 1).put("index.number_of_replicas", 0)
            ).setMapping("body", "type=text,index=false")
        );
        // A term none of them spells, so the predicate walks every value and answers no document.
        index(index, doc -> "the quick brown fox ".repeat(VALUE_LENGTH / 20) + doc);

        final EsqlQueryRequest request = syncEsqlQueryRequest(
            "FROM " + index + " | WHERE MATCH(body, \"qqzzx\") | STATS matched = COUNT(*)"
        );
        request.profile(true);
        try (EsqlQueryResponse response = run(request)) {
            assertThat(response.profile(), notNullValue());
            assertThat("no document spells it", asLong(response), greaterThanOrEqualTo(0L));

            final StringBuilder reported = new StringBuilder("\n");
            long processed = 0;
            for (DriverProfile driver : response.profile().drivers()) {
                for (OperatorStatus operator : driver.operators()) {
                    final ScanCounts counts = operator.scanCounts();
                    reported.append("    ").append(operator.operator()).append(' ').append(counts).append('\n');
                    processed += counts.processed();
                }
            }
            assertThat("the predicate walked the values it was handed" + reported, processed, greaterThan(0L));
        }
    }

    private void index(String index, java.util.function.IntFunction<String> body) {
        final BulkRequestBuilder bulk = client().prepareBulk();
        for (int doc = 0; doc < DOCS; doc++) {
            bulk.add(prepareIndex(index).setSource("body", body.apply(doc)));
        }
        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        client().admin().indices().prepareForceMerge(index).setMaxNumSegments(1).get();
        client().admin().indices().prepareRefresh(index).get();
    }

    private static long asLong(EsqlQueryResponse response) {
        final Object value = response.values().next().next();
        return value instanceof Number number ? number.longValue() : 0L;
    }
}
