package com.chronos.gc;

/**
 * Retention strategies controlling which historical Chronos versions become eligible
 * for safe low-watermark garbage collection.
 */
public enum RetentionPolicy {
    /** Purge versions older than {@code windowMs} (subject to active read pins and safety invariants). */
    TIME_WINDOW,
    /** Retain at least the most recent {@code maxVersions} committed versions. */
    LAST_N_VERSIONS,
    /** Retain versions within BOTH {@code maxVersions} and {@code windowMs} bounds. */
    HYBRID,
    /** Never purge committed versions automatically (only ROLLED_BACK entries are cleaned). */
    UNBOUNDED
}

