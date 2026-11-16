package com.chronos.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

/**
 * Immutable conflict audit record attached to merge {@link ChronosVersion} instances.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class ConflictRecord {

    private final String key;
    private final ConflictType conflictType;
    private final String leftBranchId;
    private final String rightBranchId;
    private final String baseVersionId;
    private final byte[] leftValue;
    private final byte[] rightValue;
    private final String resolutionStrategy;
    private final byte[] resolvedValue;
    private final String winningWriterId;
    private final String notes;

    @JsonCreator
    public ConflictRecord(
            @JsonProperty("key") String key,
            @JsonProperty("conflictType") ConflictType conflictType,
            @JsonProperty("leftBranchId") String leftBranchId,
            @JsonProperty("rightBranchId") String rightBranchId,
            @JsonProperty("baseVersionId") String baseVersionId,
            @JsonProperty("leftValue") byte[] leftValue,
            @JsonProperty("rightValue") byte[] rightValue,
            @JsonProperty("resolutionStrategy") String resolutionStrategy,
            @JsonProperty("resolvedValue") byte[] resolvedValue,
            @JsonProperty("winningWriterId") String winningWriterId,
            @JsonProperty("notes") String notes) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("ConflictRecord key must be non-blank");
        }
        this.key = key;
        this.conflictType = Objects.requireNonNull(conflictType, "conflictType must not be null");
        this.leftBranchId = leftBranchId;
        this.rightBranchId = rightBranchId;
        this.baseVersionId = baseVersionId;
        this.leftValue = leftValue != null ? leftValue.clone() : null;
        this.rightValue = rightValue != null ? rightValue.clone() : null;
        this.resolutionStrategy = resolutionStrategy;
        this.resolvedValue = resolvedValue != null ? resolvedValue.clone() : null;
        this.winningWriterId = winningWriterId;
        this.notes = notes;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getKey() { return key; }
    public String key() { return key; }
    public ConflictType getConflictType() { return conflictType; }
    public ConflictType type() { return conflictType; }
    public String getLeftBranchId() { return leftBranchId; }
    public String getRightBranchId() { return rightBranchId; }
    public String getBaseVersionId() { return baseVersionId; }
    public byte[] getLeftValue() { return leftValue != null ? leftValue.clone() : null; }
    public byte[] getRightValue() { return rightValue != null ? rightValue.clone() : null; }
    public String getResolutionStrategy() { return resolutionStrategy; }
    public byte[] getResolvedValue() { return resolvedValue != null ? resolvedValue.clone() : null; }
    public String getWinningWriterId() { return winningWriterId; }
    public String getNotes() { return notes; }

    @JsonIgnore
    public String getLeftValueAsString() {
        return leftValue != null ? new String(leftValue, StandardCharsets.UTF_8) : null;
    }

    @JsonIgnore
    public String getRightValueAsString() {
        return rightValue != null ? new String(rightValue, StandardCharsets.UTF_8) : null;
    }

    @JsonIgnore
    public String getResolvedValueAsString() {
        return resolvedValue != null ? new String(resolvedValue, StandardCharsets.UTF_8) : null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ConflictRecord that)) return false;
        return Objects.equals(key, that.key)
                && conflictType == that.conflictType
                && Objects.equals(leftBranchId, that.leftBranchId)
                && Objects.equals(rightBranchId, that.rightBranchId)
                && Objects.equals(baseVersionId, that.baseVersionId)
                && Arrays.equals(leftValue, that.leftValue)
                && Arrays.equals(rightValue, that.rightValue)
                && Objects.equals(resolutionStrategy, that.resolutionStrategy)
                && Arrays.equals(resolvedValue, that.resolvedValue)
                && Objects.equals(winningWriterId, that.winningWriterId)
                && Objects.equals(notes, that.notes);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(key, conflictType, leftBranchId, rightBranchId, baseVersionId,
                resolutionStrategy, winningWriterId, notes);
        result = 31 * result + Arrays.hashCode(leftValue);
        result = 31 * result + Arrays.hashCode(rightValue);
        result = 31 * result + Arrays.hashCode(resolvedValue);
        return result;
    }

    public static final class Builder {
        private String key;
        private ConflictType conflictType;
        private String leftBranchId;
        private String rightBranchId;
        private String baseVersionId;
        private byte[] leftValue;
        private byte[] rightValue;
        private String resolutionStrategy;
        private byte[] resolvedValue;
        private String winningWriterId;
        private String notes;

        public Builder key(String key) { this.key = key; return this; }
        public Builder conflictType(ConflictType conflictType) { this.conflictType = conflictType; return this; }
        public Builder leftBranchId(String leftBranchId) { this.leftBranchId = leftBranchId; return this; }
        public Builder rightBranchId(String rightBranchId) { this.rightBranchId = rightBranchId; return this; }
        public Builder baseVersionId(String baseVersionId) { this.baseVersionId = baseVersionId; return this; }
        public Builder leftValue(byte[] leftValue) { this.leftValue = leftValue; return this; }
        public Builder rightValue(byte[] rightValue) { this.rightValue = rightValue; return this; }
        public Builder resolutionStrategy(String resolutionStrategy) { this.resolutionStrategy = resolutionStrategy; return this; }
        public Builder resolvedValue(byte[] resolvedValue) { this.resolvedValue = resolvedValue; return this; }
        public Builder winningWriterId(String winningWriterId) { this.winningWriterId = winningWriterId; return this; }
        public Builder notes(String notes) { this.notes = notes; return this; }

        public ConflictRecord build() {
            return new ConflictRecord(key, conflictType, leftBranchId, rightBranchId, baseVersionId,
                    leftValue, rightValue, resolutionStrategy, resolvedValue, winningWriterId, notes);
        }
    }
}

