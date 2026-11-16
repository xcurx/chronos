package com.chronos.model;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * First-class immutable database version node in the Chronos version timeline and DAG.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class ChronosVersion {

    private final String versionId;
    private final long sequence;
    private final List<String> parentVersionIds;
    private final String branchId;
    private final long timestamp;
    private final long raftLogIndex;
    private final String writerId;
    private final String transactionId;
    private final VersionStatus status;
    private final boolean isMerge;
    private final String mergePolicy;
    private final List<ConflictRecord> conflictMetadata;
    private final List<ChangeEntry> changeSet;

    @JsonCreator
    public ChronosVersion(
            @JsonProperty("versionId") String versionId,
            @JsonProperty("sequence") long sequence,
            @JsonProperty("parentVersionIds") List<String> parentVersionIds,
            @JsonProperty("branchId") String branchId,
            @JsonProperty("timestamp") long timestamp,
            @JsonProperty("raftLogIndex") long raftLogIndex,
            @JsonProperty("writerId") String writerId,
            @JsonProperty("transactionId") String transactionId,
            @JsonProperty("status") VersionStatus status,
            @JsonProperty("isMerge") @JsonAlias("merge") boolean isMerge,
            @JsonProperty("mergePolicy") String mergePolicy,
            @JsonProperty("conflictMetadata") List<ConflictRecord> conflictMetadata,
            @JsonProperty("changeSet") List<ChangeEntry> changeSet) {
        if (versionId == null || versionId.isBlank()) {
            throw new IllegalArgumentException("versionId must be non-blank");
        }
        if (sequence < 1) {
            throw new IllegalArgumentException("sequence must be >= 1");
        }
        if (branchId == null || branchId.isBlank()) {
            throw new IllegalArgumentException("branchId must be non-blank");
        }
        if (timestamp < 0) {
            throw new IllegalArgumentException("timestamp must be >= 0");
        }
        this.versionId = versionId;
        this.sequence = sequence;
        this.parentVersionIds = parentVersionIds != null
                ? List.copyOf(parentVersionIds)
                : Collections.emptyList();
        this.branchId = branchId;
        this.timestamp = timestamp;
        this.raftLogIndex = raftLogIndex;
        this.writerId = writerId;
        this.transactionId = transactionId;
        this.status = Objects.requireNonNull(status, "status must not be null");
        this.isMerge = isMerge;
        this.mergePolicy = mergePolicy;
        this.conflictMetadata = conflictMetadata != null
                ? List.copyOf(conflictMetadata)
                : Collections.emptyList();
        this.changeSet = changeSet != null
                ? List.copyOf(changeSet)
                : Collections.emptyList();
    }

    public static Builder builder() {
        return new Builder();
    }

    public Builder toBuilder() {
        return new Builder()
                .versionId(versionId)
                .sequence(sequence)
                .parentVersionIds(parentVersionIds)
                .branchId(branchId)
                .timestamp(timestamp)
                .raftLogIndex(raftLogIndex)
                .writerId(writerId)
                .transactionId(transactionId)
                .status(status)
                .isMerge(isMerge)
                .mergePolicy(mergePolicy)
                .conflictMetadata(conflictMetadata)
                .changeSet(changeSet);
    }

    public ChronosVersion withStatus(VersionStatus newStatus) {
        return toBuilder().status(newStatus).build();
    }

    /**
     * Compare all immutable content and lineage fields, excluding mutable lifecycle status.
     */
    public boolean sameImmutablePayload(ChronosVersion other) {
        if (other == null) return false;
        return sequence == other.sequence
                && timestamp == other.timestamp
                && raftLogIndex == other.raftLogIndex
                && isMerge == other.isMerge
                && Objects.equals(versionId, other.versionId)
                && Objects.equals(parentVersionIds, other.parentVersionIds)
                && Objects.equals(branchId, other.branchId)
                && Objects.equals(writerId, other.writerId)
                && Objects.equals(transactionId, other.transactionId)
                && Objects.equals(mergePolicy, other.mergePolicy)
                && Objects.equals(conflictMetadata, other.conflictMetadata)
                && Objects.equals(changeSet, other.changeSet);
    }

    public String getVersionId() { return versionId; }
    public long getSequence() { return sequence; }
    public List<String> getParentVersionIds() { return parentVersionIds; }
    public String getBranchId() { return branchId; }
    public long getTimestamp() { return timestamp; }
    public long getRaftLogIndex() { return raftLogIndex; }
    public String getWriterId() { return writerId; }
    public String getTransactionId() { return transactionId; }
    public VersionStatus getStatus() { return status; }

    @JsonProperty("isMerge")
    public boolean isMerge() { return isMerge; }

    public String getMergePolicy() { return mergePolicy; }
    public List<ConflictRecord> getConflictMetadata() { return conflictMetadata; }
    public List<ChangeEntry> getChangeSet() { return changeSet; }

    @JsonIgnore public boolean isCommitted() { return status == VersionStatus.COMMITTED; }
    @JsonIgnore public boolean isSpeculative() { return status == VersionStatus.SPECULATIVE; }
    @JsonIgnore public boolean isRolledBack() { return status == VersionStatus.ROLLED_BACK; }
    @JsonIgnore public boolean isRejected() { return status == VersionStatus.REJECTED; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ChronosVersion that)) return false;
        return status == that.status && sameImmutablePayload(that);
    }

    @Override
    public int hashCode() {
        return Objects.hash(versionId, sequence, parentVersionIds, branchId, timestamp,
                raftLogIndex, writerId, transactionId, status, isMerge, mergePolicy,
                conflictMetadata, changeSet);
    }

    @Override
    public String toString() {
        return "ChronosVersion{versionId='" + versionId + "', seq=" + sequence
                + ", branch='" + branchId + "', ts=" + timestamp + ", status=" + status
                + ", changes=" + changeSet.size() + "}";
    }

    public static final class Builder {
        private String versionId;
        private long sequence;
        private List<String> parentVersionIds = Collections.emptyList();
        private String branchId = "main";
        private long timestamp;
        private long raftLogIndex;
        private String writerId;
        private String transactionId;
        private VersionStatus status = VersionStatus.COMMITTED;
        private boolean isMerge;
        private String mergePolicy;
        private List<ConflictRecord> conflictMetadata = Collections.emptyList();
        private List<ChangeEntry> changeSet = Collections.emptyList();

        public Builder versionId(String versionId) { this.versionId = versionId; return this; }
        public Builder sequence(long sequence) { this.sequence = sequence; return this; }
        public Builder parentVersionIds(List<String> parentVersionIds) { this.parentVersionIds = parentVersionIds; return this; }
        public Builder branchId(String branchId) { this.branchId = branchId; return this; }
        public Builder timestamp(long timestamp) { this.timestamp = timestamp; return this; }
        public Builder raftLogIndex(long raftLogIndex) { this.raftLogIndex = raftLogIndex; return this; }
        public Builder writerId(String writerId) { this.writerId = writerId; return this; }
        public Builder transactionId(String transactionId) { this.transactionId = transactionId; return this; }
        public Builder status(VersionStatus status) { this.status = status; return this; }
        public Builder isMerge(boolean isMerge) { this.isMerge = isMerge; return this; }
        public Builder mergePolicy(String mergePolicy) { this.mergePolicy = mergePolicy; return this; }
        public Builder conflictMetadata(List<ConflictRecord> conflictMetadata) { this.conflictMetadata = conflictMetadata; return this; }
        public Builder changeSet(List<ChangeEntry> changeSet) { this.changeSet = changeSet; return this; }

        public ChronosVersion build() {
            return new ChronosVersion(versionId, sequence, parentVersionIds, branchId, timestamp,
                    raftLogIndex, writerId, transactionId, status, isMerge, mergePolicy,
                    conflictMetadata, changeSet);
        }
    }
}

