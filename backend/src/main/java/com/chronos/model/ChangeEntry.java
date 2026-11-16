package com.chronos.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

/**
 * Immutable record of a single key mutation captured within a {@link ChronosVersion}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class ChangeEntry {

    private final String key;
    private final byte[] oldValue;
    private final byte[] newValue;
    private final boolean oldPresent;
    private final boolean newPresent;
    private final OperationType opType;
    private final Long deltaAmount;
    private final String writerId;

    @JsonCreator
    public ChangeEntry(
            @JsonProperty("key") String key,
            @JsonProperty("oldValue") byte[] oldValue,
            @JsonProperty("newValue") byte[] newValue,
            @JsonProperty("oldPresent") boolean oldPresent,
            @JsonProperty("newPresent") boolean newPresent,
            @JsonProperty("opType") OperationType opType,
            @JsonProperty("deltaAmount") Long deltaAmount,
            @JsonProperty("writerId") String writerId) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("ChangeEntry key must be non-blank");
        }
        this.key = key;
        this.oldValue = oldValue != null ? oldValue.clone() : null;
        this.newValue = newValue != null ? newValue.clone() : null;
        this.oldPresent = oldPresent;
        this.newPresent = newPresent;
        this.opType = Objects.requireNonNull(opType, "opType must not be null");
        this.deltaAmount = deltaAmount;
        this.writerId = writerId;
    }

    public static ChangeEntry put(String key, byte[] oldValue, byte[] newValue, String writerId) {
        Objects.requireNonNull(newValue, "PUT newValue must not be null");
        return new ChangeEntry(key, oldValue, newValue, oldValue != null, true, OperationType.PUT, null, writerId);
    }

    public static ChangeEntry delete(String key, byte[] oldValue, String writerId) {
        return new ChangeEntry(key, oldValue, null, oldValue != null, false, OperationType.DELETE, null, writerId);
    }

    public static ChangeEntry delta(String key, byte[] oldValue, byte[] newValue, long deltaAmount, String writerId) {
        Objects.requireNonNull(newValue, "DELTA newValue must not be null");
        return new ChangeEntry(key, oldValue, newValue, oldValue != null, true, OperationType.DELTA, deltaAmount, writerId);
    }

    public String getKey() {
        return key;
    }

    public byte[] getOldValue() {
        return oldValue != null ? oldValue.clone() : null;
    }

    public byte[] getNewValue() {
        return newValue != null ? newValue.clone() : null;
    }

    public boolean isOldPresent() {
        return oldPresent;
    }

    public boolean isNewPresent() {
        return newPresent;
    }

    public OperationType getOpType() {
        return opType;
    }

    public Long getDeltaAmount() {
        return deltaAmount;
    }

    public String getWriterId() {
        return writerId;
    }

    @JsonIgnore
    public String getOldValueAsString() {
        return oldValue != null ? new String(oldValue, StandardCharsets.UTF_8) : null;
    }

    @JsonIgnore
    public String getNewValueAsString() {
        return newValue != null ? new String(newValue, StandardCharsets.UTF_8) : null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ChangeEntry that)) return false;
        return oldPresent == that.oldPresent
                && newPresent == that.newPresent
                && Objects.equals(key, that.key)
                && Arrays.equals(oldValue, that.oldValue)
                && Arrays.equals(newValue, that.newValue)
                && opType == that.opType
                && Objects.equals(deltaAmount, that.deltaAmount)
                && Objects.equals(writerId, that.writerId);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(key, oldPresent, newPresent, opType, deltaAmount, writerId);
        result = 31 * result + Arrays.hashCode(oldValue);
        result = 31 * result + Arrays.hashCode(newValue);
        return result;
    }

    @Override
    public String toString() {
        return "ChangeEntry{key='" + key + "', opType=" + opType
                + ", oldPresent=" + oldPresent + ", newPresent=" + newPresent
                + ", deltaAmount=" + deltaAmount + ", writerId='" + writerId + "'}";
    }
}

