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
    CUSTOM_RESOLUTIONS,
    /** Automatic CRDT-style reconciliation (PN-Counter, LWW-Register, OR-Set, OR-Map). */
    CRDT,
    /** Force PN-Counter commutative merge on conflicting keys. */
    PN_COUNTER,
    /** Force Observed-Remove Set (Add-Wins) merge on conflicting keys. */
    OR_SET,
    /** Force LWW-Register timestamp/sequence tie-break merge. */
    LWW_REGISTER
}

