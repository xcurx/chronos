package com.chronos.audit;

/**
 * Single key difference between two versions in a version diff comparison.
 */
public record VersionDiffEntry(
        String key,
        DiffType diffType,
        String fromValue,
        String toValue
) {
    public enum DiffType {
        ADDED,
        MODIFIED,
        DELETED
    }
}

