package com.chronos.dag;

import java.util.Map;

/**
 * Aggregated key transitions along the DAG path from an ancestor version to a tip version.
 */
public record AggregatedPathDiff(
        String ancestorVersionId,
        String tipVersionId,
        Map<String, AggregatedKeyDiff> changes,
        int totalKeysChanged
) {}

