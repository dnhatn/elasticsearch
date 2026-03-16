/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;

import java.io.IOException;

public interface PartitionedDocValues {
    record LeadingPartition(BytesRef[] keys, int[] sliceDocs, int numPartitions) {}

    LeadingPartition leadingPartitions(int docsPerSlice) throws IOException;

    void partitionDocCounts(BytesRef[] leadKeys, int[] sliceDocs, int[] scratch) throws IOException;

    static boolean canPartitionByTsid(IndexSearcher searcher, int docsPerSlice) throws IOException {
        int maxDoc = 0;
        for (LeafReaderContext leafContext : searcher.getLeafContexts()) {
            if (leafContext.reader().maxDoc() == 0) {
                continue;
            }
            SortedDocValues docValues = leafContext.reader().getSortedDocValues(TimeSeriesIdFieldMapper.NAME);
            if (docValues == null) {
                continue;
            }
            if (docValues instanceof PartitionedDocValues == false) {
                return false;
            }
            maxDoc = Math.max(maxDoc, leafContext.reader().maxDoc());
        }
        return maxDoc >= docsPerSlice * 4L;
    }
}
