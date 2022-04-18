/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.shard;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.store.NIOFSDirectory;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.LoggingAwareMultiCommand;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.common.lucene.Lucene;

import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

/**
 * Class encapsulating and dispatching commands from the {@code elasticsearch-shard} command line tool
 */
public class ShardToolCli extends LoggingAwareMultiCommand {

    private ShardToolCli() {
        super("A CLI tool to remove corrupted parts of unrecoverable shards");
        subcommands.put("remove-corrupted-data", new RemoveCorruptedShardDataCommand());
        subcommands.put("forcemerge", new ForceMergeCommand());
    }

    public static void main(String[] args) throws Exception {
        exit(new ShardToolCli().main(args, Terminal.DEFAULT));
    }

    private static class ForceMergeCommand extends Command {
        final OptionSpec<String> indexDirOption;

        ForceMergeCommand() {
            super("A CLI tool to force merge a Lucene index to a single segment", () -> {
            });
            indexDirOption = parser.accepts("index-dir", "Index directory location on disk").withRequiredArg();
        }


        private void warnAboutIndexBackup(Terminal terminal) {
            terminal.println("-----------------------------------------------------------------------");
            terminal.println("");
            terminal.println("  Please make a complete backup of your index before using this tool.");
            terminal.println("");
            terminal.println("-----------------------------------------------------------------------");
        }

        @Override
        protected void execute(Terminal terminal, OptionSet options) throws Exception {
            warnAboutIndexBackup(terminal);
            final String indexDir = indexDirOption.value(options);
            terminal.println("Prepare to force merge Lucene index at [" + indexDir + "]; confirm [y/N]");
            terminal.flush();
            String text = terminal.readText("Confirm [y/N]");
            if (text.equalsIgnoreCase("y") == false) {
                throw new ElasticsearchException("aborted by user");
            }
            try (NIOFSDirectory dir = new NIOFSDirectory(Paths.get(indexDir))) {
                List<IndexCommit> commits = DirectoryReader.listCommits(dir);
                if (commits.isEmpty()) {
                    throw new IllegalStateException("No Lucene index commit found at directory [" + indexDir + "]");
                }
                IndexCommit lastCommit = commits.get(commits.size() - 1);
                Map<String, String> userData = lastCommit.getUserData();
                IndexWriterConfig config = new IndexWriterConfig();
                config.setOpenMode(IndexWriterConfig.OpenMode.APPEND);
                config.setSoftDeletesField(Lucene.SOFT_DELETES_FIELD);
                TieredMergePolicy mergePolicy = new TieredMergePolicy();
                mergePolicy.setMaxMergedSegmentMB(Double.POSITIVE_INFINITY);
                config.setMergePolicy(mergePolicy);
                config.setCommitOnClose(false);
                config.setIndexCommit(lastCommit);
                try (IndexWriter writer = new IndexWriter(dir, config)) {
                    terminal.println("force merging...");
                    writer.forceMerge(1);
                    writer.setLiveCommitData(userData.entrySet());
                    writer.commit();
                }
            }
        }
    }
}
