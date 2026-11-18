package com.chronos.crdt;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.*;

/**
 * State-based Observed-Remove Set (OR-Set) CRDT with Add-Wins semantics.
 * Each addition is assigned a globally unique tag (UUID).
 * Removals tombstone all tags observed up to that point.
 * Concurrent additions generate distinct unobserved tags and thus win over concurrent removals.
 *
 * Merging is commutative, associative, and idempotent:
 *   Elements_merged = Elements_A ∪ Elements_B
 *   Tombstones_merged = Tombstones_A ∪ Tombstones_B
 *   Active = { e | ∃ (tag, e) ∈ Elements_merged : tag ∉ Tombstones_merged }
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class OrSet {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @JsonProperty("_crdt")
    private final String crdtType = "or-set";

    @JsonProperty("elements")
    private final Map<String, String> elements; // tagId -> element

    @JsonProperty("tombstones")
    private final Set<String> tombstones; // tagIds

    public OrSet() {
        this.elements = new LinkedHashMap<>();
        this.tombstones = new LinkedHashSet<>();
    }

    @JsonCreator
    public OrSet(
            @JsonProperty("elements") Map<String, String> elements,
            @JsonProperty("tombstones") Set<String> tombstones) {
        this.elements = elements != null ? new LinkedHashMap<>(elements) : new LinkedHashMap<>();
        this.tombstones = tombstones != null ? new LinkedHashSet<>(tombstones) : new LinkedHashSet<>();
    }

    public synchronized String add(String element) {
        String tagId = UUID.randomUUID().toString().substring(0, 8);
        elements.put(tagId, element);
        return tagId;
    }

    public synchronized OrSet addWithTag(String element, String tagId) {
        elements.put(tagId, element);
        return this;
    }

    public synchronized boolean remove(String element) {
        boolean found = false;
        for (Map.Entry<String, String> entry : elements.entrySet()) {
            if (Objects.equals(entry.getValue(), element) && !tombstones.contains(entry.getKey())) {
                tombstones.add(entry.getKey());
                found = true;
            }
        }
        return found;
    }

    public boolean contains(String element) {
        return readElements().contains(element);
    }

    @JsonProperty("active")
    public Set<String> readElements() {
        Set<String> active = new TreeSet<>();
        for (Map.Entry<String, String> entry : elements.entrySet()) {
            if (!tombstones.contains(entry.getKey())) {
                active.add(entry.getValue());
            }
        }
        return Collections.unmodifiableSet(active);
    }

    public Map<String, String> getElements() {
        return Collections.unmodifiableMap(elements);
    }

    public Set<String> getTombstones() {
        return Collections.unmodifiableSet(tombstones);
    }

    /**
     * Commutative, associative state merge between two OR-Sets.
     */
    public OrSet merge(OrSet other) {
        if (other == null) {
            return new OrSet(this.elements, this.tombstones);
        }
        Map<String, String> mergedElements = new LinkedHashMap<>(this.elements);
        mergedElements.putAll(other.elements);

        Set<String> mergedTombstones = new LinkedHashSet<>(this.tombstones);
        mergedTombstones.addAll(other.tombstones);

        return new OrSet(mergedElements, mergedTombstones);
    }

    /**
     * Three-way OR-Set resolution supporting both structured JSON and collection string representations.
     */
    public static OrSet resolve(String baseVal, String leftVal, String rightVal) {
        Optional<OrSet> leftSet = tryParse(leftVal);
        Optional<OrSet> rightSet = tryParse(rightVal);

        if (leftSet.isPresent() && rightSet.isPresent()) {
            return leftSet.get().merge(rightSet.get());
        }

        if (leftSet.isPresent()) {
            OrSet res = leftSet.get();
            Set<String> rItems = parseElementList(rightVal);
            Set<String> bItems = parseElementList(baseVal);
            for (String item : rItems) {
                if (!bItems.contains(item)) res.add(item);
            }
            for (String item : bItems) {
                if (!rItems.contains(item)) res.remove(item);
            }
            return res;
        }

        if (rightSet.isPresent()) {
            OrSet res = rightSet.get();
            Set<String> lItems = parseElementList(leftVal);
            Set<String> bItems = parseElementList(baseVal);
            for (String item : lItems) {
                if (!bItems.contains(item)) res.add(item);
            }
            for (String item : bItems) {
                if (!lItems.contains(item)) res.remove(item);
            }
            return res;
        }

        // Both are comma-separated or plain set strings: build initial base tags, then apply additions/removals
        OrSet set = new OrSet();
        Set<String> baseItems = parseElementList(baseVal);
        Set<String> leftItems = parseElementList(leftVal);
        Set<String> rightItems = parseElementList(rightVal);

        Map<String, String> baseTags = new HashMap<>();
        for (String item : baseItems) {
            String tag = "base-" + UUID.nameUUIDFromBytes(item.getBytes());
            set.addWithTag(item, tag);
            baseTags.put(item, tag);
        }

        // Apply left changes
        for (String item : leftItems) {
            if (!baseItems.contains(item)) {
                set.add(item);
            }
        }
        for (String item : baseItems) {
            if (!leftItems.contains(item)) {
                set.remove(item);
            }
        }

        // Apply right changes
        for (String item : rightItems) {
            if (!baseItems.contains(item)) {
                set.add(item);
            }
        }
        for (String item : baseItems) {
            if (!rightItems.contains(item)) {
                set.remove(item);
            }
        }

        return set;
    }

    public static Optional<OrSet> tryParse(String raw) {
        if (raw == null || raw.isBlank() || !raw.trim().startsWith("{")) {
            return Optional.empty();
        }
        if (!raw.contains("\"_crdt\"") && !raw.contains("\"or-set\"") && !raw.contains("\"elements\"")) {
            return Optional.empty();
        }
        try {
            OrSet set = MAPPER.readValue(raw, OrSet.class);
            if ("or-set".equalsIgnoreCase(set.crdtType) && !set.elements.isEmpty()) {
                return Optional.of(set);
            }
        } catch (Exception ignored) {
        }
        return Optional.empty();
    }

    public String toJson() {
        try {
            return MAPPER.writeValueAsString(this);
        } catch (Exception e) {
            return "{\"_crdt\":\"or-set\",\"active\":" + readElements() + "}";
        }
    }

    public static Set<String> parseElementList(String raw) {
        if (raw == null || raw.isBlank()) return Collections.emptySet();
        Set<String> items = new TreeSet<>();
        String cleaned = raw.trim();
        if (cleaned.startsWith("[") && cleaned.endsWith("]")) {
            cleaned = cleaned.substring(1, cleaned.length() - 1);
        }
        for (String part : cleaned.split(",")) {
            String token = part.trim().replaceAll("^\"|\"$", "");
            if (!token.isBlank()) {
                items.add(token);
            }
        }
        return items;
    }
}
