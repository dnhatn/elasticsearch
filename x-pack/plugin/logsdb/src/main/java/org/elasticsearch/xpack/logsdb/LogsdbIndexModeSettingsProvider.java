/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettingProvider;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.time.Instant;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.xpack.cluster.settings.ClusterSettings.CLUSTER_LOGSDB_ENABLED;

final class LogsdbIndexModeSettingsProvider implements IndexSettingProvider {
    public static final Logger LOGGER = LogManager.getLogger(LogsdbIndexModeSettingsProvider.class);

    private static final String LOGS_PATTERN = "logs-*-*";
    private volatile boolean isLogsdbEnabled;

    LogsdbIndexModeSettingsProvider(final Settings settings) {
        this.isLogsdbEnabled = CLUSTER_LOGSDB_ENABLED.get(settings);
    }

    void updateClusterIndexModeLogsdbEnabled(boolean isLogsdbEnabled) {
        this.isLogsdbEnabled = isLogsdbEnabled;
    }

    @Override
    public Settings getAdditionalIndexSettings(
        final String indexName,
        final String dataStreamName,
        boolean isTimeSeries,
        final Metadata metadata,
        final Instant resolvedAt,
        final Settings settings,
        final List<CompressedXContent> combinedTemplateMappings
    ) {
        if (isLogsdbEnabled == false || dataStreamName == null) {
            return Settings.EMPTY;
        }

        final IndexMode indexMode = resolveIndexMode(settings.get(IndexSettings.MODE.getKey()));
        if (indexMode != null) {
            return Settings.EMPTY;
        }

        if (usesLogsAtSettingsComponentTemplate(metadata, dataStreamName) && matchesLogsPattern(dataStreamName)) {
            return Settings.builder().put("index.mode", IndexMode.LOGSDB.getName()).build();
        }

        return Settings.EMPTY;
    }

    private static boolean matchesLogsPattern(final String name) {
        boolean matched = Regex.simpleMatch(LOGS_PATTERN, name);
        LOGGER.info("--> data_stream {} log pattern", name, matched);
        return matched;
    }

    private IndexMode resolveIndexMode(final String mode) {
        return mode != null ? Enum.valueOf(IndexMode.class, mode.toUpperCase(Locale.ROOT)) : null;
    }

    private boolean usesLogsAtSettingsComponentTemplate(final Metadata metadata, final String name) {
        final String template = MetadataIndexTemplateService.findV2Template(metadata, name, false);
        LOGGER.info("--> data_stream {} matching template {}", name, template);
        if (template == null) {
            return false;
        }
        final ComposableIndexTemplate composableIndexTemplate = metadata.templatesV2().get(template);
        LOGGER.info("--> data_stream {} matching compose template {}", name, composableIndexTemplate == null ? null : composableIndexTemplate.composedOf());
        if (composableIndexTemplate == null) {
            return false;
        }
        for (final String componentTemplate : composableIndexTemplate.composedOf()) {
            if ("logs@settings".equals(componentTemplate)) {
                return true;
            }
        }
        return false;
    }

}
