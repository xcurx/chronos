package com.chronos.crdt;

/**
 * Result of deterministically resolving a conflict via CRDT semantics.
 */
public record CrdtResolutionResult(
        String resolvedValue,
        CrdtType crdtType,
        String strategy,
        String winningWriter,
        String explanation
) {}
