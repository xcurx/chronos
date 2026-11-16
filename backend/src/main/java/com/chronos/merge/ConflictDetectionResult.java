package com.chronos.merge;

import java.util.List;
import java.util.Map;

/**
 * Output of the Conflict Detection phase before resolution.
 */
public record ConflictDetectionResult(
        String lcaVersionId,
        String branchAId,
        String branchATipId,
        String branchBId,
        String branchBTipId,
        boolean hasConflicts,
        boolean hasUnresolvableConflicts,
        List<DetectedConflict> conflicts,
        Map<String, String> nonConflictingMergedState,
        Map<String, Long> deltaSums
) {}

