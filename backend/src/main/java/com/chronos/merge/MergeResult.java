package com.chronos.merge;

import com.chronos.model.ChronosVersion;
import com.chronos.model.ConflictRecord;

import java.util.List;

/**
 * Outcome of an executed branch merge.
 */
public record MergeResult(
        boolean success,
        String mergedVersionId,
        long sequence,
        long timestamp,
        String targetBranch,
        String sourceBranch,
        List<String> parentVersionIds,
        MergePolicy policy,
        int resolvedConflictsCount,
        List<ConflictRecord> conflictRecords,
        ChronosVersion version,
        String message
) {}
