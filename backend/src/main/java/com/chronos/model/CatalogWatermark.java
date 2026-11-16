package com.chronos.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * Low-watermark bounds persisted in RocksDB for safe historical garbage collection
 * and deterministic HISTORY_UNAVAILABLE reporting.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class CatalogWatermark {

    private final long earliestAvailableSeq;
    private final long earliestAvailableTimestamp;
    private final long totalVersions;
    private final long lastPurgedAt;

    @JsonCreator
    public CatalogWatermark(
            @JsonProperty("earliestAvailableSeq") long earliestAvailableSeq,
            @JsonProperty("earliestAvailableTimestamp") long earliestAvailableTimestamp,
            @JsonProperty("totalVersions") long totalVersions,
            @JsonProperty("lastPurgedAt") long lastPurgedAt) {
        this.earliestAvailableSeq = earliestAvailableSeq;
        this.earliestAvailableTimestamp = earliestAvailableTimestamp;
        this.totalVersions = totalVersions;
        this.lastPurgedAt = lastPurgedAt;
    }

    public static CatalogWatermark empty() {
        return new CatalogWatermark(0L, 0L, 0L, 0L);
    }

    public long getEarliestAvailableSeq() { return earliestAvailableSeq; }
    public long getEarliestAvailableTimestamp() { return earliestAvailableTimestamp; }
    public long getTotalVersions() { return totalVersions; }
    public long getLastPurgedAt() { return lastPurgedAt; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CatalogWatermark that)) return false;
        return earliestAvailableSeq == that.earliestAvailableSeq
                && earliestAvailableTimestamp == that.earliestAvailableTimestamp
                && totalVersions == that.totalVersions
                && lastPurgedAt == that.lastPurgedAt;
    }

    @Override
    public int hashCode() {
        return Objects.hash(earliestAvailableSeq, earliestAvailableTimestamp, totalVersions, lastPurgedAt);
    }
}

