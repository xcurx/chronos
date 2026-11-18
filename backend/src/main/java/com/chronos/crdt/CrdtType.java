package com.chronos.crdt;

/**
 * Supported Conflict-free Replicated Data Types (CRDT) in Chronos.
 */
public enum CrdtType {
    /**
     * Positive-Negative Counter: allows concurrent increments and decrements across branches.
     * Merges by taking the max positive and max negative vectors per replica.
     */
    PN_COUNTER,

    /**
     * Last-Write-Wins Register: stores value with timestamp, sequence, and writer tie-breakers.
     * Guarantees deterministic commutative selection.
     */
    LWW_REGISTER,

    /**
     * Observed-Remove Set (OR-Set): Add-Wins set where each addition is tracked with a unique tag.
     * Concurrent additions win over concurrent removals.
     */
    OR_SET,

    /**
     * Observed-Remove Map (OR-Map): multi-field JSON map where concurrent edits to disjoint fields
     * merge cleanly and overlapping fields resolve via LWW-Register.
     */
    OR_MAP
}
