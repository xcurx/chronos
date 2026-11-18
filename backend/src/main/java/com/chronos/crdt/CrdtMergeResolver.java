package com.chronos.crdt;

import com.chronos.merge.MergePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * Universal conflict-free deterministic resolution engine for Chronos branches.
 * Automatically identifies and reconciles PN-Counters, LWW-Registers, OR-Sets, and OR-Maps.
 */
@Component
public class CrdtMergeResolver {

    private static final Logger log = LoggerFactory.getLogger(CrdtMergeResolver.class);

    /**
     * Determine if a conflict is eligible for automatic CRDT resolution.
     */
    public boolean canResolveAsCrdt(String key, String baseVal, String leftVal, String rightVal, MergePolicy policy) {
        if (policy == MergePolicy.CRDT
                || policy == MergePolicy.PN_COUNTER
                || policy == MergePolicy.OR_SET
                || policy == MergePolicy.LWW_REGISTER) {
            return true;
        }
        if (isCrdtPayload(leftVal) || isCrdtPayload(rightVal) || isCrdtPayload(baseVal)) {
            return true;
        }
        return isCrdtKey(key);
    }

    /**
     * Identify the specific CRDT type applicable for this key and payload pair.
     */
    public CrdtType detectCrdtType(String key, String baseVal, String leftVal, String rightVal, MergePolicy policy) {
        if (policy == MergePolicy.PN_COUNTER) return CrdtType.PN_COUNTER;
        if (policy == MergePolicy.OR_SET) return CrdtType.OR_SET;
        if (policy == MergePolicy.LWW_REGISTER) return CrdtType.LWW_REGISTER;

        String k = key != null ? key.toLowerCase() : "";
        if (k.startsWith("crdt:counter:") || k.startsWith("counter:") || containsCrdtType(leftVal, "pn-counter") || containsCrdtType(rightVal, "pn-counter")) {
            return CrdtType.PN_COUNTER;
        }
        if (k.startsWith("crdt:set:") || k.startsWith("set:") || containsCrdtType(leftVal, "or-set") || containsCrdtType(rightVal, "or-set")) {
            return CrdtType.OR_SET;
        }
        if (k.startsWith("crdt:map:") || k.startsWith("map:") || containsCrdtType(leftVal, "or-map") || containsCrdtType(rightVal, "or-map")) {
            return CrdtType.OR_MAP;
        }
        if (k.startsWith("crdt:reg:") || k.startsWith("reg:") || containsCrdtType(leftVal, "lww-register") || containsCrdtType(rightVal, "lww-register")) {
            return CrdtType.LWW_REGISTER;
        }

        // If both values are numeric, infer PN-Counter
        if (isNumeric(baseVal) && isNumeric(leftVal) && isNumeric(rightVal)) {
            return CrdtType.PN_COUNTER;
        }

        // If both values are JSON maps, infer OR-Map
        if (isJsonMap(leftVal) && isJsonMap(rightVal)) {
            return CrdtType.OR_MAP;
        }

        // If values appear to be JSON arrays or lists, infer OR-Set
        if (isJsonList(leftVal) || isJsonList(rightVal)) {
            return CrdtType.OR_SET;
        }

        return CrdtType.LWW_REGISTER;
    }

    /**
     * Deterministically resolve divergent mutations between two branches using the appropriate CRDT algorithm.
     */
    public CrdtResolutionResult resolve(
            String key,
            String baseVal,
            String leftVal,
            String rightVal,
            String leftBranch,
            String rightBranch,
            long leftTs,
            long rightTs,
            long leftSeq,
            long rightSeq,
            String leftWriter,
            String rightWriter,
            MergePolicy policy) {

        CrdtType crdtType = detectCrdtType(key, baseVal, leftVal, rightVal, policy);

        switch (crdtType) {
            case PN_COUNTER -> {
                PnCounter resolved = PnCounter.resolve(baseVal, leftVal, rightVal, leftBranch, rightBranch);
                boolean wasJson = isCrdtPayload(leftVal) || isCrdtPayload(rightVal);
                String resultStr = wasJson ? resolved.toJson() : String.valueOf(resolved.value());
                String exp = String.format("PN-Counter reconciled: value=%d (p=%s, n=%s)",
                        resolved.value(), resolved.getP(), resolved.getN());
                return new CrdtResolutionResult(resultStr, CrdtType.PN_COUNTER, "CRDT_PN_COUNTER", "crdt-counter-engine", exp);
            }

            case OR_SET -> {
                OrSet resolved = OrSet.resolve(baseVal, leftVal, rightVal);
                boolean wasJson = isCrdtPayload(leftVal) || isCrdtPayload(rightVal);
                String resultStr = wasJson ? resolved.toJson() : resolved.readElements().toString();
                String exp = String.format("OR-Set (Add-Wins) merged %d active elements: %s",
                        resolved.readElements().size(), resolved.readElements());
                return new CrdtResolutionResult(resultStr, CrdtType.OR_SET, "CRDT_OR_SET", "crdt-set-engine", exp);
            }

            case OR_MAP -> {
                OrMap resolved = OrMap.resolve(baseVal, leftVal, rightVal, leftTs, rightTs, leftSeq, rightSeq, leftWriter, rightWriter);
                boolean wasJson = containsCrdtType(leftVal, "or-map") || containsCrdtType(rightVal, "or-map");
                String resultStr;
                if (wasJson) {
                    resultStr = resolved.toJson();
                } else {
                    try {
                        resultStr = new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(resolved.asPlainMap());
                    } catch (Exception e) {
                        resultStr = resolved.toJson();
                    }
                }
                String exp = String.format("OR-Map field-level reconciliation merged %d fields: %s",
                        resolved.asPlainMap().size(), resolved.asPlainMap().keySet());
                return new CrdtResolutionResult(resultStr, CrdtType.OR_MAP, "CRDT_OR_MAP", "crdt-map-engine", exp);
            }

            case LWW_REGISTER -> {
                LwwRegister resolved = LwwRegister.resolve(leftVal, rightVal, leftTs, rightTs, leftSeq, rightSeq, leftWriter, rightWriter);
                boolean wasJson = containsCrdtType(leftVal, "lww-register") || containsCrdtType(rightVal, "lww-register");
                String resultStr = wasJson ? resolved.toJson() : resolved.getValue();
                String exp = String.format("LWW-Register selected value '%s' from writer '%s' (ts=%d, seq=%d)",
                        resolved.getValue(), resolved.getWriterId(), resolved.getTimestamp(), resolved.getSequence());
                return new CrdtResolutionResult(resultStr, CrdtType.LWW_REGISTER, "CRDT_LWW_REGISTER", resolved.getWriterId(), exp);
            }

            default -> throw new IllegalStateException("Unsupported CRDT type: " + crdtType);
        }
    }

    public static boolean isCrdtPayload(String raw) {
        if (raw == null || raw.isBlank()) return false;
        String trimmed = raw.trim();
        return trimmed.startsWith("{") && (
                trimmed.contains("\"_crdt\"") ||
                trimmed.contains("\"pn-counter\"") ||
                trimmed.contains("\"or-set\"") ||
                trimmed.contains("\"or-map\"") ||
                trimmed.contains("\"lww-register\"")
        );
    }

    public static boolean isCrdtKey(String key) {
        if (key == null || key.isBlank()) return false;
        String k = key.toLowerCase();
        return k.startsWith("crdt:")
                || k.startsWith("counter:")
                || k.startsWith("set:")
                || k.startsWith("reg:")
                || k.startsWith("map:");
    }

    private static boolean containsCrdtType(String raw, String type) {
        return raw != null && raw.contains(type);
    }

    private static boolean isNumeric(String s) {
        if (s == null || s.isBlank()) return false;
        try {
            Long.parseLong(s.trim());
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private static boolean isJsonMap(String s) {
        if (s == null || s.isBlank()) return false;
        String t = s.trim();
        return t.startsWith("{") && t.endsWith("}") && t.contains(":");
    }

    private static boolean isJsonList(String s) {
        if (s == null || s.isBlank()) return false;
        String t = s.trim();
        return t.startsWith("[") && t.endsWith("]");
    }
}
