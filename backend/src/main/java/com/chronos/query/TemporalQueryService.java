package com.chronos.query;

import com.chronos.gc.ReadWatermarkRegistry;
import com.chronos.model.CatalogWatermark;
import com.chronos.model.ChangeEntry;
import com.chronos.model.ChronosSnapshot;
import com.chronos.model.ChronosVersion;
import com.chronos.model.OperationType;
import com.chronos.model.VersionStatus;
import com.chronos.storage.VersionCatalog;
import com.minidb.mvcc.MvccStore;
import com.minidb.mvcc.ReadMode;
import com.minidb.raft.RaftNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Lineage-aware Temporal Query Engine supporting CURRENT, AS OF VERSION, AS OF TIME,
 * and AS OF SNAPSHOT queries with causal DAG reconstruction and safe watermark bounds checking.
 */
@Service
public class TemporalQueryService {

    private static final String DEFAULT_BRANCH = "main";
    private static final Comparator<ChronosVersion> ANCESTOR_ASCENDING =
            Comparator.comparingLong(ChronosVersion::getSequence)
                    .thenComparingLong(ChronosVersion::getTimestamp)
                    .thenComparing(ChronosVersion::getVersionId);

    private final RaftNode raftNode;
    private final VersionCatalog versionCatalog;
    private final MvccStore mvccStore;
    private final ReadWatermarkRegistry watermarkRegistry;

    @Autowired
    public TemporalQueryService(RaftNode raftNode,
                                VersionCatalog versionCatalog,
                                ReadWatermarkRegistry watermarkRegistry) {
        this(raftNode, versionCatalog, raftNode != null ? raftNode.getMvccStore() : null, watermarkRegistry);
    }

    public TemporalQueryService(RaftNode raftNode, VersionCatalog versionCatalog, MvccStore mvccStore) {
        this(raftNode, versionCatalog, mvccStore, new ReadWatermarkRegistry());
    }

    public TemporalQueryService(RaftNode raftNode,
                                VersionCatalog versionCatalog,
                                MvccStore mvccStore,
                                ReadWatermarkRegistry watermarkRegistry) {
        this.raftNode = raftNode;
        this.versionCatalog = versionCatalog;
        this.mvccStore = mvccStore != null ? mvccStore : (raftNode != null ? raftNode.getMvccStore() : null);
        this.watermarkRegistry = watermarkRegistry != null ? watermarkRegistry : new ReadWatermarkRegistry();
    }

    /**
     * Execute a single-key temporal lookup while holding a read pin on the target version/timestamp.
     */
    public TemporalQueryResult queryKey(TemporalQueryRequest request) {
        if (request == null || request.key() == null || request.key().isBlank()) {
            throw new IllegalArgumentException("key must be non-blank");
        }
        long start = System.currentTimeMillis();
        maybeAwaitConsistentBarrier(request.consistent());

        QueryMode effectiveMode = resolveMode(request);
        String effectiveBranch = effectiveBranch(request.branchId(), effectiveMode);
        ReadMode readMode = request.readMode() != null ? request.readMode() : ReadMode.LINEARIZABLE;

        try (ReadWatermarkRegistry.ReadLease lease = acquireInitialLease(request, effectiveMode)) {
            ChronosVersion target = resolveTargetVersion(request, effectiveMode, effectiveBranch, readMode);
            if (target == null) {
                return fallbackWhenNoCatalogTarget(request.key(), effectiveMode, effectiveBranch, readMode, start);
            }
            lease.updateBounds(target.getSequence(), target.getTimestamp());

            AncestorWalkResult walk = collectOrderedAncestors(target);
            List<ChronosVersion> ancestorsAsc = walk.ancestors();

            Long targetVt = (effectiveMode == QueryMode.AS_OF_VALID_TIME || effectiveMode == QueryMode.BITEMPORAL)
                    ? parseTimestamp(request.validTime() != null ? request.validTime() : String.valueOf(System.currentTimeMillis()))
                    : null;

            // Walk backwards from target towards root so we stop at the most recent causal touch of key.
            for (int i = ancestorsAsc.size() - 1; i >= 0; i--) {
                ChronosVersion v = ancestorsAsc.get(i);
                ChangeEntry match = findLastChangeForKey(v, request.key());
                if (match == null) {
                    continue;
                }
                if (targetVt != null && !match.isValidAt(targetVt)) {
                    continue;
                }
                long latency = System.currentTimeMillis() - start;
                if (match.getOpType() == OperationType.DELETE || !match.isNewPresent() || match.getNewValue() == null) {
                    return TemporalQueryResult.ofNotFound(
                            request.key(),
                            true,
                            v.getVersionId(),
                            v.getSequence(),
                            v.getTimestamp(),
                            v.getBranchId(),
                            v.getWriterId(),
                            v.getTransactionId(),
                            effectiveMode,
                            latency,
                            match.getValidFrom(),
                            match.getValidTo()
                    );
                }
                return TemporalQueryResult.ofFound(
                        request.key(),
                        match.getNewValue(),
                        v.getStatus() == VersionStatus.SPECULATIVE,
                        v.getVersionId(),
                        v.getSequence(),
                        v.getTimestamp(),
                        v.getBranchId(),
                        match.getWriterId() != null ? match.getWriterId() : v.getWriterId(),
                        v.getTransactionId(),
                        effectiveMode,
                        latency,
                        match.getValidFrom(),
                        match.getValidTo()
                );
            }

            // If older ancestors were purged below GC watermark, fall back to MVCC store at target timestamp
            Optional<TemporalQueryResult> mvccFallback = tryMvccFallbackAtTarget(request.key(), target, effectiveMode, start);
            if (mvccFallback.isPresent()) {
                return mvccFallback.get();
            }

            return TemporalQueryResult.ofNotFound(
                    request.key(),
                    false,
                    target.getVersionId(),
                    target.getSequence(),
                    target.getTimestamp(),
                    target.getBranchId(),
                    target.getWriterId(),
                    target.getTransactionId(),
                    effectiveMode,
                    System.currentTimeMillis() - start
            );
        }
    }

    /**
     * Reconstruct full key-value state (optionally filtered by prefix and limit) at any point in time.
     */
    public DatabaseStateResult reconstructState(TemporalQueryRequest request) {
        long start = System.currentTimeMillis();
        TemporalQueryRequest effectiveReq = request != null
                ? request
                : TemporalQueryRequest.current(null, DEFAULT_BRANCH, ReadMode.LINEARIZABLE, false);

        maybeAwaitConsistentBarrier(effectiveReq.consistent());

        QueryMode effectiveMode = resolveMode(effectiveReq);
        String effectiveBranch = effectiveBranch(effectiveReq.branchId(), effectiveMode);
        ReadMode readMode = effectiveReq.readMode() != null ? effectiveReq.readMode() : ReadMode.LINEARIZABLE;

        try (ReadWatermarkRegistry.ReadLease lease = acquireInitialLease(effectiveReq, effectiveMode)) {
            ChronosVersion target = resolveTargetVersion(effectiveReq, effectiveMode, effectiveBranch, readMode);
            if (target == null) {
                return fallbackStateWhenNoCatalogTarget(effectiveReq, effectiveMode, effectiveBranch, readMode, start);
            }
            lease.updateBounds(target.getSequence(), target.getTimestamp());

            Long targetVt = (effectiveMode == QueryMode.AS_OF_VALID_TIME || effectiveMode == QueryMode.BITEMPORAL)
                    ? parseTimestamp(effectiveReq.validTime() != null ? effectiveReq.validTime() : String.valueOf(System.currentTimeMillis()))
                    : null;

            Map<String, DatabaseStateResult.StateEntry> state = buildCausalStateMap(target, targetVt);
            return toDatabaseStateResult(target, effectiveMode, state, effectiveReq.prefix(), effectiveReq.limit(), start);
        }
    }

    /**
     * Reconstruct raw string key-value map for a specific known {@link ChronosVersion}.
     * Used internally by {@link com.chronos.audit.AuditService} for version diffs.
     */
    public Map<String, String> reconstructKeyValuesAtVersion(ChronosVersion target) {
        Map<String, DatabaseStateResult.StateEntry> full = buildCausalStateMap(target);
        Map<String, String> result = new LinkedHashMap<>(full.size());
        for (Map.Entry<String, DatabaseStateResult.StateEntry> e : full.entrySet()) {
            result.put(e.getKey(), e.getValue().value());
        }
        return result;
    }

    /**
     * Create a persistent named {@link ChronosSnapshot} anchored to the current head of {@code branchId}.
     */
    public ChronosSnapshot createSnapshot(String snapshotId, String description, String branchId) {
        String effectiveBranch = (branchId != null && !branchId.isBlank()) ? branchId : DEFAULT_BRANCH;
        ChronosVersion head = versionCatalog.getBranchHeadVersion(effectiveBranch)
                .orElseThrow(() -> new IllegalStateException(
                        "No committed versions on branch '" + effectiveBranch + "' to snapshot"));

        String resolvedId = (snapshotId != null && !snapshotId.isBlank())
                ? snapshotId
                : "snap-" + UUID.randomUUID().toString().substring(0, 8);

        long commitIndex = raftNode != null ? raftNode.getCommitIndex() : head.getRaftLogIndex();
        ChronosSnapshot snapshot = new ChronosSnapshot(
                resolvedId,
                head.getVersionId(),
                head.getBranchId(),
                head.getTimestamp(),
                commitIndex,
                System.currentTimeMillis(),
                description
        );
        versionCatalog.saveSnapshot(snapshot);
        return snapshot;
    }

    public List<ChronosSnapshot> listSnapshots() {
        return versionCatalog.listSnapshots();
    }

    public Optional<ChronosSnapshot> getSnapshot(String snapshotId) {
        return versionCatalog.getSnapshot(snapshotId);
    }

    /**
     * Parse either epoch milliseconds (digits) or ISO-8601 instant/offset timestamp string.
     */
    public static long parseTimestamp(String raw) {
        if (raw == null || raw.isBlank()) {
            throw new IllegalArgumentException("timestamp must be non-blank");
        }
        String trimmed = raw.trim();
        try {
            return Long.parseLong(trimmed);
        } catch (NumberFormatException ignored) {
            // Try ISO-8601
        }
        try {
            return Instant.parse(trimmed).toEpochMilli();
        } catch (Exception ignored) {
            return OffsetDateTime.parse(trimmed).toInstant().toEpochMilli();
        }
    }

    private void maybeAwaitConsistentBarrier(boolean consistent) {
        if (!consistent) {
            return;
        }
        if (raftNode == null) {
            throw new IllegalStateException("RaftNode unavailable for consistent read");
        }
        try {
            raftNode.awaitLinearizableReadBarrier().get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            throw new IllegalStateException("Consistent read barrier failed: " + cause.getMessage(), cause);
        }
    }

    private QueryMode resolveMode(TemporalQueryRequest req) {
        if (req.mode() != null) {
            return req.mode();
        }
        if (req.validTime() != null && !req.validTime().isBlank()) {
            if ((req.timestamp() != null && !req.timestamp().isBlank())
                    || (req.versionId() != null && !req.versionId().isBlank())) {
                return QueryMode.BITEMPORAL;
            }
            return QueryMode.AS_OF_VALID_TIME;
        }
        if (req.versionId() != null && !req.versionId().isBlank()) {
            return QueryMode.AS_OF_VERSION;
        }
        if (req.snapshotId() != null && !req.snapshotId().isBlank()) {
            return QueryMode.AS_OF_SNAPSHOT;
        }
        if (req.timestamp() != null && !req.timestamp().isBlank()) {
            return QueryMode.AS_OF_TIME;
        }
        return QueryMode.CURRENT;
    }

    private String effectiveBranch(String requestedBranch, QueryMode mode) {
        if (requestedBranch != null && !requestedBranch.isBlank()) {
            return requestedBranch;
        }
        return mode == QueryMode.CURRENT ? DEFAULT_BRANCH : null;
    }

    private ChronosVersion resolveTargetVersion(TemporalQueryRequest req,
                                                QueryMode mode,
                                                String effectiveBranch,
                                                ReadMode readMode) {
        CatalogWatermark watermark = versionCatalog.getWatermark();
        boolean hasPurged = watermark != null && watermark.getLastPurgedAt() > 0;

        return switch (mode) {
            case CURRENT, AS_OF_VALID_TIME -> resolveCurrentVersion(effectiveBranch, readMode == ReadMode.SPECULATIVE);
            case AS_OF_VERSION -> resolveVersionById(req.versionId(), watermark, hasPurged);
            case AS_OF_TIME -> {
                long targetTs = parseTimestamp(req.timestamp());
                if (hasPurged && targetTs < watermark.getEarliestAvailableTimestamp()) {
                    throw new HistoryUnavailableException(
                            "Requested timestamp " + targetTs + " precedes earliest retained timestamp "
                                    + watermark.getEarliestAvailableTimestamp(),
                            QueryMode.AS_OF_TIME.name(),
                            req.timestamp(),
                            watermark.getEarliestAvailableSeq(),
                            watermark.getEarliestAvailableTimestamp()
                    );
                }
                yield versionCatalog.findVersionAtOrBeforeTimestamp(targetTs, effectiveBranch, false)
                        .orElse(null);
            }
            case BITEMPORAL -> {
                if (req.versionId() != null && !req.versionId().isBlank()) {
                    yield resolveVersionById(req.versionId(), watermark, hasPurged, QueryMode.BITEMPORAL.name(), req.versionId());
                } else if (req.timestamp() != null && !req.timestamp().isBlank()) {
                    long targetTs = parseTimestamp(req.timestamp());
                    yield versionCatalog.findVersionAtOrBeforeTimestamp(targetTs, effectiveBranch, false).orElse(null);
                } else {
                    yield resolveCurrentVersion(effectiveBranch, readMode == ReadMode.SPECULATIVE);
                }
            }
            case AS_OF_SNAPSHOT -> {
                if (req.snapshotId() == null || req.snapshotId().isBlank()) {
                    throw new IllegalArgumentException("snapshotId is required for AS_OF_SNAPSHOT");
                }
                ChronosSnapshot snapshot = versionCatalog.getSnapshot(req.snapshotId())
                        .orElseThrow(() -> new IllegalArgumentException("Unknown snapshot: " + req.snapshotId()));
                yield resolveVersionById(snapshot.getVersionId(), watermark, hasPurged, QueryMode.AS_OF_SNAPSHOT.name(), req.snapshotId());
            }
        };
    }

    private ChronosVersion resolveCurrentVersion(String branchId, boolean includeSpeculative) {
        String branch = (branchId != null && !branchId.isBlank()) ? branchId : DEFAULT_BRANCH;
        Optional<ChronosVersion> committedHead = versionCatalog.getBranchHeadVersion(branch);
        if (committedHead.isEmpty() && !DEFAULT_BRANCH.equals(branch)) {
            String baseVerId = versionCatalog.getBranchBase(branch).orElse(null);
            if (baseVerId != null) {
                return versionCatalog.getVersion(baseVerId).orElse(null);
            }
        }
        if (!includeSpeculative) {
            return committedHead.orElse(null);
        }

        long headSeq = committedHead.map(ChronosVersion::getSequence).orElse(0L);
        List<ChronosVersion> recent = versionCatalog.listVersionsDescending(32);
        for (ChronosVersion candidate : recent) {
            if (!branch.equals(candidate.getBranchId())) {
                continue;
            }
            if (candidate.getStatus() == VersionStatus.SPECULATIVE && candidate.getSequence() > headSeq) {
                return candidate;
            }
        }
        return committedHead.orElse(null);
    }

    private ChronosVersion resolveVersionById(String versionId, CatalogWatermark watermark, boolean hasPurged) {
        return resolveVersionById(versionId, watermark, hasPurged, QueryMode.AS_OF_VERSION.name(), versionId);
    }

    private ChronosVersion resolveVersionById(String versionId,
                                              CatalogWatermark watermark,
                                              boolean hasPurged,
                                              String selectorName,
                                              String selectorValue) {
        if (versionId == null || versionId.isBlank()) {
            throw new IllegalArgumentException("versionId is required for " + selectorName);
        }

        Optional<ChronosVersion> found = versionCatalog.getVersion(versionId);
        if (found.isPresent()) {
            ChronosVersion v = found.get();
            if (hasPurged && v.getSequence() < watermark.getEarliestAvailableSeq()) {
                throw new HistoryUnavailableException(
                        "Version " + versionId + " (seq=" + v.getSequence()
                                + ") is below earliest retained sequence " + watermark.getEarliestAvailableSeq(),
                        selectorName,
                        selectorValue,
                        watermark.getEarliestAvailableSeq(),
                        watermark.getEarliestAvailableTimestamp()
                );
            }
            return v;
        }

        if (hasPurged) {
            Optional<Long> parsedSeq = tryParseVersionSequence(versionId);
            if (parsedSeq.isEmpty() || parsedSeq.get() < watermark.getEarliestAvailableSeq()) {
                throw new HistoryUnavailableException(
                        "Requested history (" + selectorValue + ") is no longer available; purged before sequence "
                                + watermark.getEarliestAvailableSeq(),
                        selectorName,
                        selectorValue,
                        watermark.getEarliestAvailableSeq(),
                        watermark.getEarliestAvailableTimestamp()
                );
            }
        }

        throw new IllegalArgumentException("Unknown version: " + versionId);
    }

    private Optional<Long> tryParseVersionSequence(String versionId) {
        String numPart = null;
        if (versionId.startsWith("v-") || versionId.startsWith("V-")) {
            numPart = versionId.substring(2);
        } else if (versionId.startsWith("V") || versionId.startsWith("v")) {
            numPart = versionId.substring(1);
        }
        if (numPart != null && !numPart.isBlank()) {
            try {
                return Optional.of(Long.parseLong(numPart));
            } catch (NumberFormatException ignored) {
                return Optional.empty();
            }
        }
        return Optional.empty();
    }

    private ReadWatermarkRegistry.ReadLease acquireInitialLease(TemporalQueryRequest req, QueryMode mode) {
        return switch (mode) {
            case CURRENT, AS_OF_VALID_TIME -> watermarkRegistry.acquirePin(Long.MAX_VALUE, Long.MAX_VALUE);
            case AS_OF_TIME -> {
                long ts = (req.timestamp() != null && !req.timestamp().isBlank())
                        ? parseTimestamp(req.timestamp())
                        : Long.MAX_VALUE;
                yield watermarkRegistry.acquirePin(Long.MAX_VALUE, ts);
            }
            case BITEMPORAL -> {
                long ts = (req.timestamp() != null && !req.timestamp().isBlank())
                        ? parseTimestamp(req.timestamp())
                        : Long.MAX_VALUE;
                long seq = (req.versionId() != null && !req.versionId().isBlank())
                        ? tryParseVersionSequence(req.versionId()).orElse(0L)
                        : 0L;
                yield watermarkRegistry.acquirePin(seq, ts);
            }
            case AS_OF_VERSION -> {
                long seq = (req.versionId() != null && !req.versionId().isBlank())
                        ? tryParseVersionSequence(req.versionId()).orElse(0L)
                        : 0L;
                yield watermarkRegistry.acquirePin(seq, 0L);
            }
            case AS_OF_SNAPSHOT -> watermarkRegistry.acquirePin(0L, 0L);
        };
    }

    private record AncestorWalkResult(List<ChronosVersion> ancestors, boolean missingParent) {}

    private AncestorWalkResult collectOrderedAncestors(ChronosVersion target) {
        List<ChronosVersion> ancestors = new ArrayList<>();
        Set<String> visited = new HashSet<>();
        Deque<ChronosVersion> queue = new ArrayDeque<>();
        boolean missingParent = false;

        visited.add(target.getVersionId());
        queue.add(target);

        while (!queue.isEmpty()) {
            ChronosVersion current = queue.poll();
            ancestors.add(current);
            for (String parentId : current.getParentVersionIds()) {
                if (parentId == null || parentId.isBlank() || !visited.add(parentId)) {
                    continue;
                }
                Optional<ChronosVersion> parent = versionCatalog.getVersion(parentId);
                if (parent.isPresent()) {
                    queue.add(parent.get());
                } else {
                    missingParent = true;
                }
            }
        }

        ancestors.sort(ANCESTOR_ASCENDING);
        return new AncestorWalkResult(ancestors, missingParent);
    }

    private Map<String, DatabaseStateResult.StateEntry> buildCausalStateMap(ChronosVersion target) {
        return buildCausalStateMap(target, null);
    }

    private Map<String, DatabaseStateResult.StateEntry> buildCausalStateMap(ChronosVersion target, Long targetValidTime) {
        AncestorWalkResult walk = collectOrderedAncestors(target);
        List<ChronosVersion> ancestorsAsc = walk.ancestors();
        Map<String, DatabaseStateResult.StateEntry> state = new TreeMap<>();

        CatalogWatermark watermark = versionCatalog.getWatermark();
        if (walk.missingParent() && watermark != null && watermark.getLastPurgedAt() > 0 && mvccStore != null && !ancestorsAsc.isEmpty()) {
            long preBoundaryTs = ancestorsAsc.get(0).getTimestamp() - 1L;
            if (preBoundaryTs > 0) {
                for (MvccStore.MvccResult r : mvccStore.scan("", "\uffff", 0, preBoundaryTs, ReadMode.LINEARIZABLE)) {
                    state.put(r.getKey(), new DatabaseStateResult.StateEntry(
                            r.getKey(),
                            r.getValueAsString(),
                            null,
                            watermark.getEarliestAvailableSeq(),
                            r.getTimestamp(),
                            DEFAULT_BRANCH,
                            null,
                            null
                    ));
                }
            }
        }

        for (ChronosVersion v : ancestorsAsc) {
            if (v.getStatus() == VersionStatus.ROLLED_BACK || v.getStatus() == VersionStatus.REJECTED) {
                if (!v.getVersionId().equals(target.getVersionId())) {
                    continue;
                }
            }
            for (ChangeEntry entry : v.getChangeSet()) {
                if (entry.getKey() == null) {
                    continue;
                }
                if (targetValidTime != null && !entry.isValidAt(targetValidTime)) {
                    continue;
                }
                if (entry.getOpType() == OperationType.DELETE || !entry.isNewPresent() || entry.getNewValue() == null) {
                    state.remove(entry.getKey());
                } else {
                    String valStr = new String(entry.getNewValue(), StandardCharsets.UTF_8);
                    String writer = entry.getWriterId() != null ? entry.getWriterId() : v.getWriterId();
                    state.put(entry.getKey(), new DatabaseStateResult.StateEntry(
                            entry.getKey(),
                            valStr,
                            v.getVersionId(),
                            v.getSequence(),
                            v.getTimestamp(),
                            v.getBranchId(),
                            writer,
                            v.getTransactionId(),
                            entry.getValidFrom(),
                            entry.getValidTo()
                    ));
                }
            }
        }
        return state;
    }

    /**
     * Return the complete 2D bitemporal mutation history of a key.
     */
    public List<Map<String, Object>> getKeyBitemporalTimeline(String key) {
        if (key == null || key.isBlank()) {
            return Collections.emptyList();
        }
        List<ChronosVersion> versions = versionCatalog.findVersionsByKeyBitemporal(key);
        if (versions.isEmpty()) {
            versions = versionCatalog.listVersions(0);
        }
        List<Map<String, Object>> timeline = new ArrayList<>();
        for (ChronosVersion v : versions) {
            for (ChangeEntry c : v.getChangeSet()) {
                if (key.equals(c.getKey())) {
                    Map<String, Object> point = new LinkedHashMap<>();
                    point.put("versionId", v.getVersionId());
                    point.put("sequence", v.getSequence());
                    point.put("txnTime", v.getTimestamp());
                    point.put("branchId", v.getBranchId());
                    point.put("writerId", c.getWriterId() != null ? c.getWriterId() : v.getWriterId());
                    point.put("opType", c.getOpType().name());
                    point.put("validFrom", c.getValidFrom() != null ? c.getValidFrom() : v.getTimestamp());
                    point.put("validTo", c.getValidTo());
                    point.put("value", c.getNewValueAsString());
                    timeline.add(point);
                }
            }
        }
        return timeline;
    }

    private ChangeEntry findLastChangeForKey(ChronosVersion v, String key) {
        List<ChangeEntry> changes = v.getChangeSet();
        for (int i = changes.size() - 1; i >= 0; i--) {
            ChangeEntry entry = changes.get(i);
            if (key.equals(entry.getKey())) {
                return entry;
            }
        }
        return null;
    }

    private Optional<TemporalQueryResult> tryMvccFallbackAtTarget(String key,
                                                                  ChronosVersion target,
                                                                  QueryMode mode,
                                                                  long startMs) {
        CatalogWatermark watermark = versionCatalog.getWatermark();
        if (watermark == null || watermark.getLastPurgedAt() == 0 || mvccStore == null) {
            return Optional.empty();
        }
        MvccStore.MvccResult mvcc = mvccStore.get(key, target.getTimestamp(), ReadMode.LINEARIZABLE);
        if (!mvcc.isFound()) {
            return Optional.empty();
        }
        return Optional.of(TemporalQueryResult.ofFound(
                key,
                mvcc.getValue(),
                false,
                target.getVersionId(),
                target.getSequence(),
                mvcc.getTimestamp(),
                target.getBranchId(),
                target.getWriterId(),
                target.getTransactionId(),
                mode,
                System.currentTimeMillis() - startMs
        ));
    }

    private TemporalQueryResult fallbackWhenNoCatalogTarget(String key,
                                                            QueryMode mode,
                                                            String branchId,
                                                            ReadMode readMode,
                                                            long startMs) {
        if (mode == QueryMode.CURRENT && mvccStore != null && DEFAULT_BRANCH.equals(branchId)) {
            MvccStore.MvccResult r = mvccStore.get(key, 0, readMode);
            if (r.isFound()) {
                return TemporalQueryResult.ofFound(
                        key,
                        r.getValue(),
                        r.isSpeculative(),
                        null,
                        0L,
                        r.getTimestamp(),
                        DEFAULT_BRANCH,
                        null,
                        null,
                        mode,
                        System.currentTimeMillis() - startMs
                );
            }
        }
        return TemporalQueryResult.ofNotFound(
                key,
                false,
                null,
                0L,
                0L,
                branchId,
                null,
                null,
                mode,
                System.currentTimeMillis() - startMs
        );
    }

    private DatabaseStateResult fallbackStateWhenNoCatalogTarget(TemporalQueryRequest req,
                                                                 QueryMode mode,
                                                                 String branchId,
                                                                 ReadMode readMode,
                                                                 long startMs) {
        Map<String, String> entries = new LinkedHashMap<>();
        List<DatabaseStateResult.StateEntry> details = new ArrayList<>();
        if (mode == QueryMode.CURRENT && mvccStore != null && DEFAULT_BRANCH.equals(branchId)) {
            String startKey = req.prefix() != null ? req.prefix() : "";
            String endKey = req.prefix() != null ? req.prefix() + "\uffff" : "\uffff";
            int limit = req.limit() > 0 ? req.limit() : 1000;
            for (MvccStore.MvccResult r : mvccStore.scan(startKey, endKey, limit, 0, readMode)) {
                String val = r.getValueAsString();
                entries.put(r.getKey(), val);
                details.add(new DatabaseStateResult.StateEntry(
                        r.getKey(), val, null, 0L, r.getTimestamp(), DEFAULT_BRANCH, null, null));
            }
        }
        return new DatabaseStateResult(
                null,
                0L,
                0L,
                branchId,
                mode,
                entries,
                details,
                entries.size(),
                System.currentTimeMillis() - startMs
        );
    }

    private DatabaseStateResult toDatabaseStateResult(ChronosVersion target,
                                                      QueryMode mode,
                                                      Map<String, DatabaseStateResult.StateEntry> state,
                                                      String prefix,
                                                      int limit,
                                                      long startMs) {
        Map<String, String> entries = new LinkedHashMap<>();
        List<DatabaseStateResult.StateEntry> details = new ArrayList<>();
        String normalizedPrefix = prefix != null ? prefix : "";

        for (Map.Entry<String, DatabaseStateResult.StateEntry> e : state.entrySet()) {
            if (!normalizedPrefix.isEmpty() && !e.getKey().startsWith(normalizedPrefix)) {
                continue;
            }
            if (limit > 0 && entries.size() >= limit) {
                break;
            }
            entries.put(e.getKey(), e.getValue().value());
            details.add(e.getValue());
        }

        return new DatabaseStateResult(
                target.getVersionId(),
                target.getSequence(),
                target.getTimestamp(),
                target.getBranchId(),
                mode,
                entries,
                details,
                entries.size(),
                System.currentTimeMillis() - startMs
        );
    }
}

