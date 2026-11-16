package com.chronos.model;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Deterministic replicated payload stored inside Chronos Raft log entries
 * ({@code CHRONOS_WRITE}, {@code CHRONOS_TXN}, {@code CHRONOS_BRANCH_WRITE}, {@code CHRONOS_MERGE}).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class ChronosWritePayload {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String txnId;
    private final String writerId;
    private final String branchId;
    private final List<String> parentVersionIds;
    private final boolean isMerge;
    private final String mergePolicy;
    private final List<ConflictRecord> conflictMetadata;
    private final List<WriteOp> operations;

    @JsonCreator
    public ChronosWritePayload(
            @JsonProperty("txnId") String txnId,
            @JsonProperty("writerId") String writerId,
            @JsonProperty("branchId") String branchId,
            @JsonProperty("parentVersionIds") List<String> parentVersionIds,
            @JsonProperty("isMerge") @JsonAlias("merge") boolean isMerge,
            @JsonProperty("mergePolicy") String mergePolicy,
            @JsonProperty("conflictMetadata") List<ConflictRecord> conflictMetadata,
            @JsonProperty("operations") List<WriteOp> operations) {
        this.txnId = txnId;
        this.writerId = writerId;
        this.branchId = (branchId != null && !branchId.isBlank()) ? branchId : "main";
        this.parentVersionIds = parentVersionIds != null ? List.copyOf(parentVersionIds) : Collections.emptyList();
        this.isMerge = isMerge;
        this.mergePolicy = mergePolicy;
        this.conflictMetadata = conflictMetadata != null ? List.copyOf(conflictMetadata) : Collections.emptyList();
        this.operations = operations != null ? List.copyOf(operations) : Collections.emptyList();
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getTxnId() { return txnId; }
    public String getWriterId() { return writerId; }
    public String getBranchId() { return branchId; }
    public List<String> getParentVersionIds() { return parentVersionIds; }

    @JsonProperty("isMerge")
    public boolean isMerge() { return isMerge; }

    public String getMergePolicy() { return mergePolicy; }
    public List<ConflictRecord> getConflictMetadata() { return conflictMetadata; }
    public List<WriteOp> getOperations() { return operations; }

    public byte[] toJsonBytes() {
        try {
            return MAPPER.writeValueAsBytes(this);
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize ChronosWritePayload", e);
        }
    }

    public static ChronosWritePayload fromJsonBytes(byte[] bytes) {
        try {
            return MAPPER.readValue(bytes, ChronosWritePayload.class);
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize ChronosWritePayload", e);
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static final class WriteOp {
        private final String key;
        private final byte[] value;
        private final OperationType opType;
        private final Long deltaAmount;

        @JsonCreator
        public WriteOp(
                @JsonProperty("key") String key,
                @JsonProperty("value") byte[] value,
                @JsonProperty("opType") OperationType opType,
                @JsonProperty("deltaAmount") Long deltaAmount) {
            if (key == null || key.isBlank()) {
                throw new IllegalArgumentException("WriteOp key must be non-blank");
            }
            this.key = key;
            this.value = value != null ? value.clone() : null;
            this.opType = Objects.requireNonNull(opType, "opType must not be null");
            this.deltaAmount = deltaAmount;
        }

        public static WriteOp put(String key, byte[] value) {
            Objects.requireNonNull(value, "PUT value must not be null");
            return new WriteOp(key, value, OperationType.PUT, null);
        }

        public static WriteOp delete(String key) {
            return new WriteOp(key, null, OperationType.DELETE, null);
        }

        public static WriteOp delta(String key, byte[] resolvedValue, long deltaAmount) {
            Objects.requireNonNull(resolvedValue, "DELTA value must not be null");
            return new WriteOp(key, resolvedValue, OperationType.DELTA, deltaAmount);
        }

        public String getKey() { return key; }
        public byte[] getValue() { return value != null ? value.clone() : null; }
        public OperationType getOpType() { return opType; }
        public Long getDeltaAmount() { return deltaAmount; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof WriteOp writeOp)) return false;
            return Objects.equals(key, writeOp.key)
                    && Arrays.equals(value, writeOp.value)
                    && opType == writeOp.opType
                    && Objects.equals(deltaAmount, writeOp.deltaAmount);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(key, opType, deltaAmount);
            result = 31 * result + Arrays.hashCode(value);
            return result;
        }
    }

    public static final class Builder {
        private String txnId;
        private String writerId;
        private String branchId = "main";
        private List<String> parentVersionIds = Collections.emptyList();
        private boolean isMerge;
        private String mergePolicy;
        private List<ConflictRecord> conflictMetadata = Collections.emptyList();
        private List<WriteOp> operations = Collections.emptyList();

        public Builder txnId(String txnId) { this.txnId = txnId; return this; }
        public Builder writerId(String writerId) { this.writerId = writerId; return this; }
        public Builder branchId(String branchId) { this.branchId = branchId; return this; }
        public Builder parentVersionIds(List<String> parentVersionIds) { this.parentVersionIds = parentVersionIds; return this; }
        public Builder isMerge(boolean isMerge) { this.isMerge = isMerge; return this; }
        public Builder mergePolicy(String mergePolicy) { this.mergePolicy = mergePolicy; return this; }
        public Builder conflictMetadata(List<ConflictRecord> conflictMetadata) { this.conflictMetadata = conflictMetadata; return this; }
        public Builder operations(List<WriteOp> operations) { this.operations = operations; return this; }

        public ChronosWritePayload build() {
            return new ChronosWritePayload(txnId, writerId, branchId, parentVersionIds,
                    isMerge, mergePolicy, conflictMetadata, operations);
        }
    }
}

