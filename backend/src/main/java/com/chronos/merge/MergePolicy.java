package com.chronos.merge;

/**
 * Deterministic policies for reconciling conflicting mutations during branch merges.
 */
public enum MergePolicy {
    /** Commutatively sum numeric DELTA mutations (base + deltaA + deltaB). */
    COMMUTATIVE_DELTA,
    /** Last-Write-Wins based on physical/logical timestamp and sequence. */
    LAST_WRITE_WINS,
    /** Abort merge if any WRITE_WRITE, UPDATE_DELETE, or INVARIANT_VIOLATION exists. */
    FAIL_ON_CONFLICT,
    /** Use explicit caller-provided key-level resolution values. */
    CUSTOM_RESOLUTIONS
}

