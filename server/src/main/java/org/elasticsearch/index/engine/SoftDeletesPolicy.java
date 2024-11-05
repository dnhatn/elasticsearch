/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.search.Query;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.seqno.RetentionLease;
import org.elasticsearch.index.seqno.RetentionLeases;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.translog.Translog;

import java.util.Objects;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * A policy that controls how many soft-deleted documents should be retained for peer-recovery and querying history changes purpose.
 */
final class SoftDeletesPolicy {
    private final LongSupplier globalCheckpointSupplier;
    private long localCheckpointOfSafeCommit;
    // This lock count is used to prevent `minRetainedSeqNo` from advancing.
    private int retentionLockCount;
    // The extra number of operations before the global checkpoint are retained
    private long retentionOperations;
    // The min seq_no value that is retained - ops after this seq# should exist in the Lucene index.
    private long minRetainedSeqNo;
    // provides the retention leases used to calculate the minimum sequence number to retain
    private final Supplier<RetentionLeases> retentionLeasesSupplier;

    SoftDeletesPolicy(
        final LongSupplier globalCheckpointSupplier,
        final long minRetainedSeqNo,
        final long retentionOperations,
        final Supplier<RetentionLeases> retentionLeasesSupplier
    ) {
        this.globalCheckpointSupplier = globalCheckpointSupplier;
        this.retentionOperations = retentionOperations;
        this.minRetainedSeqNo = minRetainedSeqNo;
        this.retentionLeasesSupplier = Objects.requireNonNull(retentionLeasesSupplier);
        this.localCheckpointOfSafeCommit = SequenceNumbers.NO_OPS_PERFORMED;
        this.retentionLockCount = 0;
    }

    /**
     * Updates the number of soft-deleted documents prior to the global checkpoint to be retained
     * See {@link org.elasticsearch.index.IndexSettings#INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING}
     */
    synchronized void setRetentionOperations(long retentionOperations) {
        this.retentionOperations = retentionOperations;
    }

    /**
     * Sets the local checkpoint of the current safe commit
     */
    synchronized void setLocalCheckpointOfSafeCommit(long newCheckpoint) {
        if (newCheckpoint < this.localCheckpointOfSafeCommit) {
            throw new IllegalArgumentException(
                "Local checkpoint can't go backwards; "
                    + "new checkpoint ["
                    + newCheckpoint
                    + "],"
                    + "current checkpoint ["
                    + localCheckpointOfSafeCommit
                    + "]"
            );
        }
        this.localCheckpointOfSafeCommit = newCheckpoint;
    }

    /**
     * Acquires a lock on soft-deleted documents to prevent them from cleaning up in merge processes. This is necessary to
     * make sure that all operations that are being retained will be retained until the lock is released.
     * This is a analogy to the translog's retention lock; see {@link Translog#acquireRetentionLock()}
     */
    synchronized Releasable acquireRetentionLock() {
        assert retentionLockCount >= 0 : "Invalid number of retention locks [" + retentionLockCount + "]";
        retentionLockCount++;
        return Releasables.releaseOnce(this::releaseRetentionLock);
    }

    private synchronized void releaseRetentionLock() {
        assert retentionLockCount > 0 : "Invalid number of retention locks [" + retentionLockCount + "]";
        retentionLockCount--;
    }

    /**
     * Returns the min seqno that is retained in the Lucene index.
     * Operations whose seq# is least this value should exist in the Lucene index.
     */
    synchronized long getMinRetainedSeqNo() {
        return 0;
    }

    /**
     * Returns a soft-deletes retention query that will be used in {@link org.apache.lucene.index.SoftDeletesRetentionMergePolicy}
     * Documents including tombstones are soft-deleted and matched this query will be retained and won't cleaned up by merges.
     */
    Query getRetentionQuery() {
        return LongPoint.newRangeQuery(SeqNoFieldMapper.NAME, getMinRetainedSeqNo(), Long.MAX_VALUE);
    }

}
