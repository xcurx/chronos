package com.chronos.crdt;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.*;

/**
 * Observed-Remove Map (OR-Map) CRDT for JSON document / multi-field conflict-free reconciliation.
 * Each field is tracked via an independent LwwRegister.
 * Disjoint field updates from concurrent branches merge with zero conflicts.
 * Overlapping field updates resolve deterministically via field-level LWW timestamps.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class OrMap {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @JsonProperty("_crdt")
    private final String crdtType = "or-map";

    @JsonProperty("fields")
    private final Map<String, LwwRegister> fields;

    public OrMap() {
        this.fields = new LinkedHashMap<>();
    }

    @JsonCreator
    public OrMap(@JsonProperty("fields") Map<String, LwwRegister> fields) {
        this.fields = fields != null ? new LinkedHashMap<>(fields) : new LinkedHashMap<>();
    }

    public synchronized OrMap put(String field, String value, long timestamp, long sequence, String writerId) {
        LwwRegister reg = LwwRegister.of(value, timestamp, sequence, writerId);
        fields.merge(field, reg, LwwRegister::merge);
        return this;
    }

    public synchronized OrMap remove(String field, long timestamp, long sequence, String writerId) {
        return put(field, null, timestamp, sequence, writerId);
    }

    public String get(String field) {
        LwwRegister reg = fields.get(field);
        return reg != null ? reg.getValue() : null;
    }

    @JsonProperty("values")
    public Map<String, String> asPlainMap() {
        Map<String, String> plain = new LinkedHashMap<>();
        for (Map.Entry<String, LwwRegister> entry : fields.entrySet()) {
            if (entry.getValue() != null && entry.getValue().getValue() != null) {
                plain.put(entry.getKey(), entry.getValue().getValue());
            }
        }
        return Collections.unmodifiableMap(plain);
    }

    public Map<String, LwwRegister> getFields() {
        return Collections.unmodifiableMap(fields);
    }

    /**
     * Commutative, associative state merge between two OR-Maps.
     */
    public OrMap merge(OrMap other) {
        if (other == null) {
            return new OrMap(this.fields);
        }
        Map<String, LwwRegister> mergedFields = new LinkedHashMap<>(this.fields);
        for (Map.Entry<String, LwwRegister> entry : other.fields.entrySet()) {
            mergedFields.merge(entry.getKey(), entry.getValue(), LwwRegister::merge);
        }
        return new OrMap(mergedFields);
    }

    /**
     * Three-way OR-Map resolution supporting structured JSON maps and plain JSON objects.
     */
    public static OrMap resolve(
            String baseVal,
            String leftVal,
            String rightVal,
            long leftTs,
            long rightTs,
            long leftSeq,
            long rightSeq,
            String leftWriter,
            String rightWriter) {
        Optional<OrMap> leftMap = tryParse(leftVal);
        Optional<OrMap> rightMap = tryParse(rightVal);

        if (leftMap.isPresent() && rightMap.isPresent()) {
            return leftMap.get().merge(rightMap.get());
        }

        Map<String, String> baseRaw = parseJsonMap(baseVal);
        Map<String, String> leftRaw = parseJsonMap(leftVal);
        Map<String, String> rightRaw = parseJsonMap(rightVal);

        OrMap result = new OrMap();

        // Populate base fields
        for (Map.Entry<String, String> e : baseRaw.entrySet()) {
            result.put(e.getKey(), e.getValue(), 0L, 0L, "base");
        }

        // Apply left updates/deletes
        for (Map.Entry<String, String> e : leftRaw.entrySet()) {
            if (!Objects.equals(e.getValue(), baseRaw.get(e.getKey()))) {
                result.put(e.getKey(), e.getValue(), leftTs, leftSeq, leftWriter);
            }
        }
        for (String k : baseRaw.keySet()) {
            if (!leftRaw.containsKey(k)) {
                result.remove(k, leftTs, leftSeq, leftWriter);
            }
        }

        // Apply right updates/deletes (field-level LWW merge happens automatically)
        for (Map.Entry<String, String> e : rightRaw.entrySet()) {
            if (!Objects.equals(e.getValue(), baseRaw.get(e.getKey()))) {
                result.put(e.getKey(), e.getValue(), rightTs, rightSeq, rightWriter);
            }
        }
        for (String k : baseRaw.keySet()) {
            if (!rightRaw.containsKey(k)) {
                result.remove(k, rightTs, rightSeq, rightWriter);
            }
        }

        return result;
    }

    public static Optional<OrMap> tryParse(String raw) {
        if (raw == null || raw.isBlank() || !raw.trim().startsWith("{")) {
            return Optional.empty();
        }
        if (!raw.contains("\"_crdt\"") && !raw.contains("\"fields\"")) {
            return Optional.empty();
        }
        try {
            OrMap map = MAPPER.readValue(raw, OrMap.class);
            if ("or-map".equalsIgnoreCase(map.crdtType) && !map.fields.isEmpty()) {
                return Optional.of(map);
            }
        } catch (Exception ignored) {
        }
        return Optional.empty();
    }

    public static Map<String, String> parseJsonMap(String raw) {
        if (raw == null || raw.isBlank() || !raw.trim().startsWith("{")) {
            return Collections.emptyMap();
        }
        try {
            return MAPPER.readValue(raw, new TypeReference<Map<String, String>>() {});
        } catch (Exception e) {
            return Collections.emptyMap();
        }
    }

    public String toJson() {
        try {
            return MAPPER.writeValueAsString(this);
        } catch (Exception e) {
            return "{\"_crdt\":\"or-map\",\"values\":" + asPlainMap() + "}";
        }
    }
}
