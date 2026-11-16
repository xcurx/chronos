package com.chronos.dag;

import com.chronos.model.OperationType;

import java.util.List;

/**
 * Summary of all mutations applied to a single key between a DAG ancestor and a tip version.
 */
public record AggregatedKeyDiff(
        String key,
        String baseValue,
        String tipValue,
        boolean deleted,
        List<OperationType> operations,
        Long netDelta
) {}

