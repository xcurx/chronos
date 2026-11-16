package com.chronos.audit;

import java.util.List;

/**
 * Full structural diff between two Chronos versions ({@code fromVersionId -> toVersionId}).
 */
public record VersionDiffResult(
        String fromVersionId,
        long fromSequence,
        long fromTimestamp,
        String toVersionId,
        long toSequence,
        long toTimestamp,
        int addedCount,
        int modifiedCount,
        int deletedCount,
        List<VersionDiffEntry> changes
) {}

