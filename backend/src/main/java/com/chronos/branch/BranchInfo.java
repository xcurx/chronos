package com.chronos.branch;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * Metadata and tracking state for an isolated Chronos branch.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class BranchInfo {

    private final String branchId;
    private final String baseVersionId;
    private final String headVersionId;
    private final String writerId;
    private final BranchStatus status;
    private final long createdAt;
    private final long updatedAt;
    private final String description;

    @JsonCreator
    public BranchInfo(
            @JsonProperty("branchId") String branchId,
            @JsonProperty("baseVersionId") String baseVersionId,
            @JsonProperty("headVersionId") String headVersionId,
            @JsonProperty("writerId") String writerId,
            @JsonProperty("status") BranchStatus status,
            @JsonProperty("createdAt") long createdAt,
            @JsonProperty("updatedAt") long updatedAt,
            @JsonProperty("description") String description) {
        if (branchId == null || branchId.isBlank()) {
            throw new IllegalArgumentException("branchId must be non-blank");
        }
        this.branchId = branchId;
        this.baseVersionId = baseVersionId;
        this.headVersionId = headVersionId;
        this.writerId = writerId;
        this.status = status != null ? status : BranchStatus.ACTIVE;
        this.createdAt = createdAt > 0 ? createdAt : System.currentTimeMillis();
        this.updatedAt = updatedAt > 0 ? updatedAt : this.createdAt;
        this.description = description;
    }

    public static BranchInfo create(String branchId, String baseVersionId, String writerId, String description) {
        long now = System.currentTimeMillis();
        return new BranchInfo(branchId, baseVersionId, baseVersionId, writerId, BranchStatus.ACTIVE, now, now, description);
    }

    public BranchInfo withHeadVersionId(String newHeadVersionId) {
        return new BranchInfo(branchId, baseVersionId, newHeadVersionId, writerId, status, createdAt, System.currentTimeMillis(), description);
    }

    public BranchInfo withStatus(BranchStatus newStatus) {
        return new BranchInfo(branchId, baseVersionId, headVersionId, writerId, newStatus, createdAt, System.currentTimeMillis(), description);
    }

    public String getBranchId() { return branchId; }
    public String getBaseVersionId() { return baseVersionId; }
    public String getHeadVersionId() { return headVersionId; }
    public String getWriterId() { return writerId; }
    public BranchStatus getStatus() { return status; }
    public long getCreatedAt() { return createdAt; }
    public long getUpdatedAt() { return updatedAt; }
    public String getDescription() { return description; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BranchInfo that)) return false;
        return createdAt == that.createdAt
                && updatedAt == that.updatedAt
                && Objects.equals(branchId, that.branchId)
                && Objects.equals(baseVersionId, that.baseVersionId)
                && Objects.equals(headVersionId, that.headVersionId)
                && Objects.equals(writerId, that.writerId)
                && status == that.status
                && Objects.equals(description, that.description);
    }

    @Override
    public int hashCode() {
        return Objects.hash(branchId, baseVersionId, headVersionId, writerId, status, createdAt, updatedAt, description);
    }

    @Override
    public String toString() {
        return "BranchInfo{branchId='" + branchId + "', baseVersionId='" + baseVersionId
                + "', headVersionId='" + headVersionId + "', status=" + status + "}";
    }
}

