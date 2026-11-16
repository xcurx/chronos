package com.chronos.dag;

import com.chronos.model.ChangeEntry;
import com.chronos.model.ChronosVersion;
import com.chronos.model.OperationType;
import com.chronos.query.TemporalQueryService;
import com.chronos.storage.VersionCatalog;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

/**
 * Service for querying and traversing the Version DAG, computing Lowest Common Ancestors (LCA),
 * and computing aggregated key transitions along DAG lineage paths.
 */
@Service
public class VersionDagService {

    private static final Comparator<ChronosVersion> TOPOLOGICAL_ASC =
            Comparator.comparingLong(ChronosVersion::getSequence)
                    .thenComparingLong(ChronosVersion::getTimestamp)
                    .thenComparing(ChronosVersion::getVersionId);

    private final VersionCatalog versionCatalog;
    private final TemporalQueryService temporalQueryService;

    @Autowired
    public VersionDagService(VersionCatalog versionCatalog, TemporalQueryService temporalQueryService) {
        this.versionCatalog = Objects.requireNonNull(versionCatalog, "versionCatalog must not be null");
        this.temporalQueryService = Objects.requireNonNull(temporalQueryService, "temporalQueryService must not be null");
    }

    /**
     * Retrieve DAG nodes and directed parent-to-child edges, optionally filtered by branch and limit.
     */
    public VersionDagResponse getDag(int limit, String branchFilter) {
        List<ChronosVersion> nodes;
        if (branchFilter != null && !branchFilter.isBlank()) {
            nodes = versionCatalog.listVersionsByBranch(branchFilter.trim(), limit);
        } else {
            nodes = versionCatalog.listVersions(limit);
        }

        Set<String> presentNodeIds = new HashSet<>();
        for (ChronosVersion v : nodes) {
            presentNodeIds.add(v.getVersionId());
        }

        List<DagEdge> edges = new ArrayList<>();
        for (ChronosVersion child : nodes) {
            for (String parentId : child.getParentVersionIds()) {
                if (parentId != null && !parentId.isBlank()) {
                    edges.add(new DagEdge(parentId, child.getVersionId()));
                }
            }
        }

        Map<String, String> branchHeads = versionCatalog.listBranchHeads();
        return new VersionDagResponse(nodes, edges, branchHeads, nodes.size(), edges.size());
    }

    /**
     * Return all topological ancestors of {@code versionId} (including the version itself).
     */
    public List<ChronosVersion> getAncestors(String versionId) {
        return getAncestors(versionId, true);
    }

    /**
     * Return all topological ancestors of {@code versionId}, optionally including the version itself.
     */
    public List<ChronosVersion> getAncestors(String versionId, boolean includeSelf) {
        if (versionId == null || versionId.isBlank()) {
            return Collections.emptyList();
        }
        Optional<ChronosVersion> start = versionCatalog.getVersion(versionId);
        if (start.isEmpty()) {
            return Collections.emptyList();
        }

        List<ChronosVersion> ancestors = new ArrayList<>();
        Set<String> visited = new HashSet<>();
        Deque<ChronosVersion> queue = new ArrayDeque<>();

        visited.add(start.get().getVersionId());
        if (includeSelf) {
            ancestors.add(start.get());
        }

        for (String parentId : start.get().getParentVersionIds()) {
            if (parentId != null && !parentId.isBlank() && visited.add(parentId)) {
                versionCatalog.getVersion(parentId).ifPresent(queue::add);
            }
        }

        while (!queue.isEmpty()) {
            ChronosVersion curr = queue.poll();
            ancestors.add(curr);
            for (String parentId : curr.getParentVersionIds()) {
                if (parentId != null && !parentId.isBlank() && visited.add(parentId)) {
                    versionCatalog.getVersion(parentId).ifPresent(queue::add);
                }
            }
        }

        ancestors.sort(TOPOLOGICAL_ASC);
        return ancestors;
    }

    /**
     * Find the Lowest Common Ancestor (LCA) of two versions in the Version DAG.
     * Handles linear history, forks, and diamond merges.
     */
    public Optional<ChronosVersion> findLowestCommonAncestor(String versionAId, String versionBId) {
        if (versionAId == null || versionAId.isBlank() || versionBId == null || versionBId.isBlank()) {
            return Optional.empty();
        }
        if (versionAId.equals(versionBId)) {
            return versionCatalog.getVersion(versionAId);
        }

        Optional<ChronosVersion> verA = versionCatalog.getVersion(versionAId);
        Optional<ChronosVersion> verB = versionCatalog.getVersion(versionBId);
        if (verA.isEmpty() || verB.isEmpty()) {
            return Optional.empty();
        }

        // Anc(A) and Anc(B) include self
        List<ChronosVersion> ancAList = getAncestors(versionAId, true);
        List<ChronosVersion> ancBList = getAncestors(versionBId, true);

        Map<String, ChronosVersion> ancAMap = new LinkedHashMap<>();
        for (ChronosVersion v : ancAList) {
            ancAMap.put(v.getVersionId(), v);
        }

        Map<String, ChronosVersion> ancBMap = new LinkedHashMap<>();
        for (ChronosVersion v : ancBList) {
            ancBMap.put(v.getVersionId(), v);
        }

        // Fast path: if A is ancestor of B
        if (ancBMap.containsKey(versionAId)) {
            return verA;
        }
        // Fast path: if B is ancestor of A
        if (ancAMap.containsKey(versionBId)) {
            return verB;
        }

        // Common ancestors CA = Anc(A) ∩ Anc(B)
        Map<String, ChronosVersion> commonAncestors = new LinkedHashMap<>();
        for (Map.Entry<String, ChronosVersion> e : ancAMap.entrySet()) {
            if (ancBMap.containsKey(e.getKey())) {
                commonAncestors.put(e.getKey(), e.getValue());
            }
        }

        if (commonAncestors.isEmpty()) {
            return Optional.empty();
        }

        // Precompute ancestor sets for all nodes in CA to eliminate any node in CA that is an ancestor of another node in CA
        Set<String> subAncestors = new HashSet<>();
        for (ChronosVersion ca : commonAncestors.values()) {
            for (String parentId : ca.getParentVersionIds()) {
                if (parentId != null && !parentId.isBlank()) {
                    collectAllAncestorIds(parentId, subAncestors);
                }
            }
        }

        List<ChronosVersion> lcaCandidates = new ArrayList<>();
        for (ChronosVersion ca : commonAncestors.values()) {
            if (!subAncestors.contains(ca.getVersionId())) {
                lcaCandidates.add(ca);
            }
        }

        if (lcaCandidates.isEmpty()) {
            lcaCandidates.addAll(commonAncestors.values());
        }

        // If multiple diamond candidates remain, pick the one with highest sequence / timestamp
        lcaCandidates.sort(TOPOLOGICAL_ASC.reversed());
        return Optional.of(lcaCandidates.get(0));
    }

    /**
     * Compute aggregated key transitions along the DAG path between {@code ancestorId} and {@code tipId}.
     */
    public AggregatedPathDiff computeAggregatedChangeSet(String ancestorId, String tipId) {
        if (ancestorId == null || ancestorId.isBlank() || tipId == null || tipId.isBlank()) {
            throw new IllegalArgumentException("ancestorId and tipId must be non-blank");
        }

        ChronosVersion ancestor = versionCatalog.getVersion(ancestorId)
                .orElseThrow(() -> new IllegalArgumentException("Unknown ancestor version: " + ancestorId));
        ChronosVersion tip = versionCatalog.getVersion(tipId)
                .orElseThrow(() -> new IllegalArgumentException("Unknown tip version: " + tipId));

        Map<String, String> baseState = temporalQueryService.reconstructKeyValuesAtVersion(ancestor);
        Map<String, String> tipState = temporalQueryService.reconstructKeyValuesAtVersion(tip);

        // Collect all versions strictly along the DAG paths between ancestor and tip
        List<ChronosVersion> pathVersions = findVersionsOnPath(ancestor.getVersionId(), tip.getVersionId());

        Map<String, List<OperationType>> keyOps = new TreeMap<>();
        Map<String, Long> keyDeltas = new TreeMap<>();

        for (ChronosVersion v : pathVersions) {
            if (v.getVersionId().equals(ancestor.getVersionId())) {
                continue; // Do not include ancestor's own changes
            }
            for (ChangeEntry change : v.getChangeSet()) {
                String key = change.getKey();
                if (key == null) continue;

                keyOps.computeIfAbsent(key, k -> new ArrayList<>()).add(change.getOpType());
                if (change.getOpType() == OperationType.DELTA && change.getDeltaAmount() != null) {
                    keyDeltas.merge(key, change.getDeltaAmount(), Long::sum);
                }
            }
        }

        Set<String> allTouchedKeys = new HashSet<>();
        allTouchedKeys.addAll(keyOps.keySet());
        for (String k : baseState.keySet()) {
            if (!Objects.equals(baseState.get(k), tipState.get(k))) {
                allTouchedKeys.add(k);
            }
        }
        for (String k : tipState.keySet()) {
            if (!Objects.equals(baseState.get(k), tipState.get(k))) {
                allTouchedKeys.add(k);
            }
        }

        Map<String, AggregatedKeyDiff> result = new LinkedHashMap<>();
        for (String key : allTouchedKeys) {
            String baseVal = baseState.get(key);
            String tipVal = tipState.get(key);
            boolean isDeleted = (tipVal == null && baseVal != null);
            List<OperationType> ops = keyOps.getOrDefault(key, Collections.emptyList());
            Long delta = keyDeltas.get(key);

            result.put(key, new AggregatedKeyDiff(key, baseVal, tipVal, isDeleted, ops, delta));
        }

        return new AggregatedPathDiff(ancestorId, tipId, result, result.size());
    }

    private List<ChronosVersion> findVersionsOnPath(String ancestorId, String tipId) {
        if (ancestorId.equals(tipId)) {
            return versionCatalog.getVersion(tipId).map(List::of).orElse(Collections.emptyList());
        }

        // Backwards traversal from tip to ancestor
        Set<String> onPath = new HashSet<>();
        onPath.add(ancestorId);

        // First find all ancestors of tip that can reach ancestorId
        Map<String, ChronosVersion> tipAncestors = new LinkedHashMap<>();
        for (ChronosVersion v : getAncestors(tipId, true)) {
            tipAncestors.put(v.getVersionId(), v);
        }

        if (!tipAncestors.containsKey(ancestorId)) {
            throw new IllegalArgumentException("Version '" + ancestorId + "' is not an ancestor of '" + tipId + "'");
        }

        // Forward filter: a node is on path if it is reachable from ancestor and reaches tip
        List<ChronosVersion> pathNodes = new ArrayList<>();
        for (ChronosVersion v : tipAncestors.values()) {
            if (v.getVersionId().equals(ancestorId)) {
                pathNodes.add(v);
                continue;
            }
            if (isReachable(ancestorId, v.getVersionId())) {
                pathNodes.add(v);
            }
        }

        pathNodes.sort(TOPOLOGICAL_ASC);
        return pathNodes;
    }

    private boolean isReachable(String fromId, String toId) {
        if (fromId.equals(toId)) return true;
        Deque<String> queue = new ArrayDeque<>();
        Set<String> visited = new HashSet<>();
        queue.add(toId);
        visited.add(toId);

        while (!queue.isEmpty()) {
            String currId = queue.poll();
            Optional<ChronosVersion> v = versionCatalog.getVersion(currId);
            if (v.isEmpty()) continue;
            for (String p : v.get().getParentVersionIds()) {
                if (fromId.equals(p)) return true;
                if (p != null && !p.isBlank() && visited.add(p)) {
                    queue.add(p);
                }
            }
        }
        return false;
    }

    private void collectAllAncestorIds(String startId, Set<String> out) {
        if (startId == null || startId.isBlank() || !out.add(startId)) return;
        versionCatalog.getVersion(startId).ifPresent(v -> {
            for (String p : v.getParentVersionIds()) {
                collectAllAncestorIds(p, out);
            }
        });
    }
}

