package com.chronos.crdt;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Objects;
import java.util.Optional;

/**
 * Last-Write-Wins Register (LWW-Register) CRDT.
 * Assigns each write a monotonic hybrid timestamp (physical time + sequence + writerId tie-breaker).
 * Merging is commutative, associative, and total: concurrent writes always resolve to the highest timestamp.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class LwwRegister {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @JsonProperty("_crdt")
    private final String crdtType = "lww-register";

    @JsonProperty("value")
    private final String value;

    @JsonProperty("timestamp")
    private final long timestamp;

    @JsonProperty("sequence")
    private final long sequence;

    @JsonProperty("writerId")
    private final String writerId;

    @JsonCreator
    public LwwRegister(
            @JsonProperty("value") String value,
            @JsonProperty("timestamp") long timestamp,
            @JsonProperty("sequence") long sequence,
            @JsonProperty("writerId") String writerId) {
        this.value = value;
        this.timestamp = timestamp;
        this.sequence = sequence;
        this.writerId = writerId != null ? writerId : "unknown";
    }

    public static LwwRegister of(String value, long timestamp, long sequence, String writerId) {
        return new LwwRegister(value, timestamp, sequence, writerId);
    }

    public String getValue() {
        return value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public long getSequence() {
        return sequence;
    }

    public String getWriterId() {
        return writerId;
    }

    /**
     * Commutative, total-order merge between two registers.
     */
    public LwwRegister merge(LwwRegister other) {
        if (other == null) return this;
        if (this.timestamp != other.timestamp) {
            return this.timestamp > other.timestamp ? this : other;
        }
        if (this.sequence != other.sequence) {
            return this.sequence > other.sequence ? this : other;
        }
        return this.writerId.compareTo(other.writerId) >= 0 ? this : other;
    }

    public static LwwRegister resolve(
            String leftVal,
            String rightVal,
            long leftTs,
            long rightTs,
            long leftSeq,
            long rightSeq,
            String leftWriter,
            String rightWriter) {
        Optional<LwwRegister> leftReg = tryParse(leftVal);
        Optional<LwwRegister> rightReg = tryParse(rightVal);

        if (leftReg.isPresent() && rightReg.isPresent()) {
            return leftReg.get().merge(rightReg.get());
        }
        if (leftReg.isPresent()) {
            LwwRegister r = of(rightVal, rightTs, rightSeq, rightWriter);
            return leftReg.get().merge(r);
        }
        if (rightReg.isPresent()) {
            LwwRegister l = of(leftVal, leftTs, leftSeq, leftWriter);
            return l.merge(rightReg.get());
        }

        LwwRegister l = of(leftVal, leftTs, leftSeq, leftWriter);
        LwwRegister r = of(rightVal, rightTs, rightSeq, rightWriter);
        return l.merge(r);
    }

    public static Optional<LwwRegister> tryParse(String raw) {
        if (raw == null || raw.isBlank() || !raw.trim().startsWith("{")) {
            return Optional.empty();
        }
        if (!raw.contains("\"_crdt\"") && !raw.contains("\"lww-register\"")) {
            return Optional.empty();
        }
        try {
            LwwRegister reg = MAPPER.readValue(raw, LwwRegister.class);
            if ("lww-register".equalsIgnoreCase(reg.crdtType) || reg.timestamp > 0) {
                return Optional.of(reg);
            }
        } catch (Exception ignored) {
        }
        return Optional.empty();
    }

    public String toJson() {
        try {
            return MAPPER.writeValueAsString(this);
        } catch (Exception e) {
            return "{\"_crdt\":\"lww-register\",\"value\":\"" + value + "\",\"timestamp\":" + timestamp + "}";
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LwwRegister that = (LwwRegister) o;
        return timestamp == that.timestamp &&
                sequence == that.sequence &&
                Objects.equals(value, that.value) &&
                Objects.equals(writerId, that.writerId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, timestamp, sequence, writerId);
    }
}
