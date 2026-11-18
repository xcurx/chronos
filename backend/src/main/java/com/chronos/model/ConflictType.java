package com.chronos.model;

/**
 * Classification of conflicts detected when reconciling divergent Chronos branches.
 */
public enum ConflictType {
    NONE,
    COMPATIBLE_DELTA,
    COMPATIBLE_CRDT,
    IDENTICAL_WRITE,
    WRITE_WRITE,
    DELETE_MODIFY,
    MODIFY_DELETE,
    UPDATE_DELETE,
    DELETE_UPDATE,
    CAUSAL_DIVERGENCE,
    INVARIANT_VIOLATION
}

