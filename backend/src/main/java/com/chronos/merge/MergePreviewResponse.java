package com.chronos.merge;

import java.util.List;
import java.util.Map;

/**
 * Detailed preview of a branch merge, including detected conflicts, invariant violations,
 * and tentative reconciled state.
 */
public record MergePreviewResponse(
        boolean canMerge,
        String sourceBranch,
        String targetBranch,
        String sourceTipId,
        String targetTipId,
        String lcaVersionId,
        MergePolicy policy,
        List<DetectedConflict> conflicts,
        List<InvariantViolation> invariantViolations,
        Map<String, String> tentativeMergedState,
        Map<String, String> finalResolutions,
        int totalConflicts,
        int totalViolations,
        String reason
) {}

