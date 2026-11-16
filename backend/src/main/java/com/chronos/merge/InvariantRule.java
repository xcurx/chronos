package com.chronos.merge;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Domain invariant rule evaluated on tentative merged state prior to committing a merge.
 */
public final class InvariantRule {

    private final String name;
    private final InvariantType type;
    private final String prefix;
    private final Long min;
    private final Long max;

    public InvariantRule(String name, InvariantType type, String prefix, Long min, Long max) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Rule name must be non-blank");
        }
        this.name = name;
        this.type = Objects.requireNonNull(type, "type must not be null");
        this.prefix = prefix != null ? prefix : "";
        this.min = min;
        this.max = max;
    }

    public static InvariantRule nonNegative(String name, String prefix) {
        return new InvariantRule(name, InvariantType.NON_NEGATIVE_NUMERIC, prefix, 0L, null);
    }

    public static InvariantRule bounds(String name, String prefix, long min, long max) {
        return new InvariantRule(name, InvariantType.CUSTOM_BOUNDS, prefix, min, max);
    }

    public static InvariantRule conservation(String name, String prefix) {
        return new InvariantRule(name, InvariantType.CONSERVATION_SUM, prefix, null, null);
    }

    public List<InvariantViolation> validate(Map<String, String> tentativeState, Map<String, String> baseState) {
        List<InvariantViolation> violations = new ArrayList<>();

        if (type == InvariantType.NON_NEGATIVE_NUMERIC || type == InvariantType.CUSTOM_BOUNDS) {
            for (Map.Entry<String, String> entry : tentativeState.entrySet()) {
                String key = entry.getKey();
                if (!prefix.isEmpty() && !key.startsWith(prefix)) {
                    continue;
                }
                String valStr = entry.getValue();
                if (valStr == null || valStr.isBlank()) {
                    continue;
                }
                try {
                    long numericVal = Long.parseLong(valStr.trim());
                    if (min != null && numericVal < min) {
                        violations.add(new InvariantViolation(
                                name, key, valStr, ">= " + min,
                                "Value for key '" + key + "' violates rule '" + name + "': " + numericVal + " < " + min
                        ));
                    }
                    if (max != null && numericVal > max) {
                        violations.add(new InvariantViolation(
                                name, key, valStr, "<= " + max,
                                "Value for key '" + key + "' violates rule '" + name + "': " + numericVal + " > " + max
                        ));
                    }
                } catch (NumberFormatException ignored) {
                    // Non-numeric values under numeric-checked prefix can be ignored or flagged
                }
            }
        } else if (type == InvariantType.CONSERVATION_SUM && baseState != null) {
            long baseSum = sumKeysWithPrefix(baseState, prefix);
            long tentativeSum = sumKeysWithPrefix(tentativeState, prefix);
            if (baseSum != tentativeSum) {
                violations.add(new InvariantViolation(
                        name, prefix + "*", String.valueOf(tentativeSum), "= " + baseSum,
                        "Conservation sum invariant violated for prefix '" + prefix + "': expected "
                                + baseSum + " but found " + tentativeSum
                ));
            }
        }
        return violations;
    }

    private long sumKeysWithPrefix(Map<String, String> state, String prefix) {
        long sum = 0L;
        for (Map.Entry<String, String> e : state.entrySet()) {
            if (!prefix.isEmpty() && !e.getKey().startsWith(prefix)) {
                continue;
            }
            try {
                if (e.getValue() != null && !e.getValue().isBlank()) {
                    sum += Long.parseLong(e.getValue().trim());
                }
            } catch (NumberFormatException ignored) {
            }
        }
        return sum;
    }

    public String getName() { return name; }
    public InvariantType getType() { return type; }
    public String getPrefix() { return prefix; }
    public Long getMin() { return min; }
    public Long getMax() { return max; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof InvariantRule that)) return false;
        return Objects.equals(name, that.name)
                && type == that.type
                && Objects.equals(prefix, that.prefix)
                && Objects.equals(min, that.min)
                && Objects.equals(max, that.max);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, prefix, min, max);
    }
}

