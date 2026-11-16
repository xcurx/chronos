package com.chronos.model;

/**
 * Lifecycle status of a first-class Chronos database version.
 */
public enum VersionStatus {
    /** Written locally under speculative MVCC, awaiting Raft majority commit. */
    SPECULATIVE,
    /** Durably committed through Raft consensus and visible to linearizable/historical reads. */
    COMMITTED,
    /** Speculative write rolled back after leader step-down or consensus abort. */
    ROLLED_BACK,
    /** Merge or candidate version rejected by conflict policy or invariant validation. */
    REJECTED
}

