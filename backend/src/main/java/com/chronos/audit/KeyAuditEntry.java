package com.chronos.audit;

import com.chronos.model.OperationType;
import com.chronos.model.VersionStatus;

import java.util.List;

/**
 * Single chronological mutation event in a key's complete audit trail.
 */
public record KeyAuditEntry(
        String versionId,
        long versionSequence,
        long timestamp,
        long raftLogIndex,
        String branchId,
        List<String> parentVersionIds,
        VersionStatus status,
        OperationType operationType,
        String key,
        boolean oldPresent,
        String oldValue,
        boolean newPresent,
        String newValue,
        Long deltaAmount,
        String writerId,
        String transactionId,
        boolean merge,
        String mergePolicy
) {}

