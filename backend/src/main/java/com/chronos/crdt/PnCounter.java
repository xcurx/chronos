package com.chronos.crdt;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.*;

/**
 * State-based Positive-Negative Counter (PN-Counter) CRDT.
 * Tracks concurrent positive increments and negative decrements across divergent replicas/branches.
 * Merging is commutative, associative, and idempotent:
 *   P_merged[r] = max(P_1[r], P_2[r])
 *   N_merged[r] = max(N_1[r], N_2[r])
 *   Value = sum(P_merged) - sum(N_merged)
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class PnCounter {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @JsonProperty("_crdt")
    private final String crdtType = "pn-counter";

    @JsonProperty("p")
    private final Map<String, Long> p;

    @JsonProperty("n")
    private final Map<String, Long> n;

    public PnCounter() {
        this.p = new LinkedHashMap<>();
        this.n = new LinkedHashMap<>();
    }

    @JsonCreator
    public PnCounter(
            @JsonProperty("p") Map<String, Long> p,
            @JsonProperty("n") Map<String, Long> n) {
        this.p = p != null ? new LinkedHashMap<>(p) : new LinkedHashMap<>();
        this.n = n != null ? new LinkedHashMap<>(n) : new LinkedHashMap<>();
    }

    public synchronized PnCounter increment(String replicaId, long amount) {
        if (amount < 0) {
            return decrement(replicaId, -amount);
        }
        if (amount > 0) {
            p.merge(replicaId != null ? replicaId : "default", amount, Long::sum);
        }
        return this;
    }

    public synchronized PnCounter decrement(String replicaId, long amount) {
        if (amount < 0) {
            return increment(replicaId, -amount);
        }
        if (amount > 0) {
            n.merge(replicaId != null ? replicaId : "default", amount, Long::sum);
        }
        return this;
    }

    @JsonProperty("value")
    public long value() {
        long sumP = p.values().stream().mapToLong(Long::longValue).sum();
        long sumN = n.values().stream().mapToLong(Long::longValue).sum();
        return sumP - sumN;
    }

    public Map<String, Long> getP() {
        return Collections.unmodifiableMap(p);
    }

    public Map<String, Long> getN() {
        return Collections.unmodifiableMap(n);
    }

    /**
     * Commutative, associative state merge between two PN-Counters.
     */
    public PnCounter merge(PnCounter other) {
        if (other == null) {
            return new PnCounter(this.p, this.n);
        }
        Map<String, Long> mergedP = new LinkedHashMap<>(this.p);
        for (Map.Entry<String, Long> entry : other.p.entrySet()) {
            mergedP.merge(entry.getKey(), entry.getValue(), Math::max);
        }

        Map<String, Long> mergedN = new LinkedHashMap<>(this.n);
        for (Map.Entry<String, Long> entry : other.n.entrySet()) {
            mergedN.merge(entry.getKey(), entry.getValue(), Math::max);
        }

        return new PnCounter(mergedP, mergedN);
    }

    /**
     * Three-way PN-Counter resolution relative to base value.
     */
    public static PnCounter resolve(String baseVal, String leftVal, String rightVal, String leftReplica, String rightReplica) {
        Optional<PnCounter> leftCrdt = tryParse(leftVal);
        Optional<PnCounter> rightCrdt = tryParse(rightVal);

        if (leftCrdt.isPresent() && rightCrdt.isPresent()) {
            return leftCrdt.get().merge(rightCrdt.get());
        }

        if (leftCrdt.isPresent()) {
            PnCounter res = leftCrdt.get();
            long rNum = parseNumber(rightVal, 0L);
            long bNum = parseNumber(baseVal, 0L);
            long deltaR = rNum - bNum;
            if (deltaR > 0) res.increment(rightReplica, deltaR);
            else if (deltaR < 0) res.decrement(rightReplica, -deltaR);
            return res;
        }

        if (rightCrdt.isPresent()) {
            PnCounter res = rightCrdt.get();
            long lNum = parseNumber(leftVal, 0L);
            long bNum = parseNumber(baseVal, 0L);
            long deltaL = lNum - bNum;
            if (deltaL > 0) res.increment(leftReplica, deltaL);
            else if (deltaL < 0) res.decrement(leftReplica, -deltaL);
            return res;
        }

        // Standard numeric scalar 3-way delta resolution
        long baseNum = parseNumber(baseVal, 0L);
        long leftNum = parseNumber(leftVal, baseNum);
        long rightNum = parseNumber(rightVal, baseNum);
        long deltaL = leftNum - baseNum;
        long deltaR = rightNum - baseNum;

        PnCounter counter = new PnCounter();
        if (baseNum > 0) counter.increment("base", baseNum);
        else if (baseNum < 0) counter.decrement("base", -baseNum);

        if (deltaL > 0) counter.increment(leftReplica, deltaL);
        else if (deltaL < 0) counter.decrement(leftReplica, -deltaL);

        if (deltaR > 0) counter.increment(rightReplica, deltaR);
        else if (deltaR < 0) counter.decrement(rightReplica, -deltaR);

        return counter;
    }

    public static Optional<PnCounter> tryParse(String raw) {
        if (raw == null || raw.isBlank() || !raw.trim().startsWith("{")) {
            return Optional.empty();
        }
        if (!raw.contains("\"_crdt\"") && !raw.contains("\"pn-counter\"") && !raw.contains("\"p\"")) {
            return Optional.empty();
        }
        try {
            PnCounter counter = MAPPER.readValue(raw, PnCounter.class);
            if ("pn-counter".equalsIgnoreCase(counter.crdtType) && (!counter.p.isEmpty() || !counter.n.isEmpty())) {
                return Optional.of(counter);
            }
        } catch (Exception ignored) {
        }
        return Optional.empty();
    }

    public String toJson() {
        try {
            return MAPPER.writeValueAsString(this);
        } catch (Exception e) {
            return "{\"_crdt\":\"pn-counter\",\"value\":" + value() + "}";
        }
    }

    private static long parseNumber(String s, long def) {
        if (s == null || s.isBlank()) return def;
        try {
            return Long.parseLong(s.trim());
        } catch (NumberFormatException e) {
            return def;
        }
    }
}
