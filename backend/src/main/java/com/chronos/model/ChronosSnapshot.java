package com.chronos.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * Named logical checkpoint anchored to a specific {@link ChronosVersion}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class ChronosSnapshot {

    private final String snapshotId;
    private final String versionId;
    private final String branchId;
    private final long timestamp;
    private final long raftCommitIndex;
    private final long createdAt;
    private final String description;

    @JsonCreator
    public ChronosSnapshot(
            @JsonProperty("snapshotId") String snapshotId,
            @JsonProperty("versionId") String versionId,
            @JsonProperty("branchId") String branchId,
            @JsonProperty("timestamp") long timestamp,
            @JsonProperty("raftCommitIndex") long raftCommitIndex,
            @JsonProperty("createdAt") long createdAt,
            @JsonProperty("description") String description) {
        if (snapshotId == null || snapshotId.isBlank()) {
            throw new IllegalArgumentException("snapshotId must be non-blank");
        }
        if (versionId == null || versionId.isBlank()) {
            throw new IllegalArgumentException("versionId must be non-blank");
        }
        if (timestamp < 0) {
            throw new IllegalArgumentException("timestamp must be >= 0");
        }
        this.snapshotId = snapshotId;
        this.versionId = versionId;
        this.branchId = (branchId != null && !branchId.isBlank()) ? branchId : "main";
        this.timestamp = timestamp;
        this.raftCommitIndex = raftCommitIndex;
        this.createdAt = createdAt;
        this.description = description;
    }

    public String getSnapshotId() { return snapshotId; }
    public String getVersionId() { return versionId; }
    public String getBranchId() { return branchId; }
    public long getTimestamp() { return timestamp; }
    public long getRaftCommitIndex() { return raftCommitIndex; }
    public long getCreatedAt() { return createdAt; }
    public String getDescription() { return description; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ChronosSnapshot that)) return false;
        return timestamp == that.timestamp
                && raftCommitIndex == that.raftCommitIndex
                && createdAt == that.createdAt
                && Objects.equals(snapshotId, that.snapshotId)
                && Objects.equals(versionId, that.versionId)
                && Objects.equals(branchId, that.branchId)
                && Objects.equals(description, that.description);
    }

    @Override
    public int hashCode() {
        return Objects.hash(snapshotId, versionId, branchId, timestamp, raftCommitIndex, createdAt, description);
    }

    @Override
    public String toString() {
        return "ChronosSnapshot{snapshotId='" + snapshotId + "', versionId='" + versionId
                + "', branchId='" + branchId + "', timestamp=" + timestamp + "}";
    }
}

