package com.chronos.merge;

import com.chronos.model.ConflictType;

/**
 * Detailed conflict description for an individual key across divergent branches.
 */
public record DetectedConflict(
        String key,
        ConflictType conflictType,
        String leftBranchId,
        String rightBranchId,
        String baseValue,
        String leftValue,
        String rightValue,
        String description,
        boolean resolvable
) {}

