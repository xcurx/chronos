package com.chronos.audit;

import com.chronos.gc.ReadWatermarkRegistry;
import com.chronos.model.CatalogWatermark;
import com.chronos.model.ChangeEntry;
import com.chronos.model.ChronosVersion;
import com.chronos.model.VersionStatus;
import com.chronos.query.HistoryUnavailableException;
import com.chronos.query.TemporalQueryService;
import com.chronos.storage.VersionCatalog;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/**
 * Audit and structural diff service over the Chronos version catalog.
 */
@Service
public class AuditService {

    private final VersionCatalog versionCatalog;
    private final TemporalQueryService temporalQueryService;
    private final ReadWatermarkRegistry watermarkRegistry;

    public AuditService(VersionCatalog versionCatalog, TemporalQueryService temporalQueryService) {
        this(versionCatalog, temporalQueryService, new ReadWatermarkRegistry());
    }

    @Autowired
    public AuditService(VersionCatalog versionCatalog,
                        TemporalQueryService temporalQueryService,
                        ReadWatermarkRegistry watermarkRegistry) {
        this.versionCatalog = versionCatalog;
        this.temporalQueryService = temporalQueryService;
        this.watermarkRegistry = watermarkRegistry != null ? watermarkRegistry : new ReadWatermarkRegistry();
    }

    /**
     * Return the chronological mutation trail for a key across all retained versions.
     */
    public List<KeyAuditEntry> getKeyAuditTrail(String key) {
        return getKeyAuditTrail(key, true);
    }

    /**
     * Return the chronological mutation trail for a key across retained versions.
     */
    public List<KeyAuditEntry> getKeyAuditTrail(String key, boolean includeUncommitted) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("key must be non-blank");
        }
        List<ChronosVersion> versions = versionCatalog.listVersions(0);
        List<KeyAuditEntry> trail = new ArrayList<>();

        for (ChronosVersion v : versions) {
            if (!includeUncommitted && v.getStatus() != VersionStatus.COMMITTED) {
                continue;
            }
            for (ChangeEntry change : v.getChangeSet()) {
                if (!key.equals(change.getKey())) {
                    continue;
                }
                trail.add(new KeyAuditEntry(
                        v.getVersionId(),
                        v.getSequence(),
                        v.getTimestamp(),
                        v.getRaftLogIndex(),
                        v.getBranchId(),
                        v.getParentVersionIds(),
                        v.getStatus(),
                        change.getOpType(),
                        key,
                        change.isOldPresent(),
                        decodeUtf8(change.getOldValue()),
                        change.isNewPresent(),
                        decodeUtf8(change.getNewValue()),
                        change.getDeltaAmount(),
                        change.getWriterId() != null ? change.getWriterId() : v.getWriterId(),
                        v.getTransactionId(),
                        v.isMerge(),
                        v.getMergePolicy()
                ));
            }
        }
        return trail;
    }

    /**
     * Retrieve full metadata and change set details for a single version.
     */
    public Optional<ChronosVersion> getVersionAuditDetails(String versionId) {
        if (versionId == null || versionId.isBlank()) {
            return Optional.empty();
        }
        return versionCatalog.getVersion(versionId);
    }

    /**
     * Compute added, modified, and deleted keys between {@code fromVersionId} and {@code toVersionId}.
     */
    public VersionDiffResult diffVersions(String fromVersionId, String toVersionId) {
        try (ReadWatermarkRegistry.ReadLease lease = watermarkRegistry.acquirePin(0L, 0L)) {
            ChronosVersion fromVersion = resolveRetainedVersion(fromVersionId);
            ChronosVersion toVersion = resolveRetainedVersion(toVersionId);
            lease.updateBounds(
                    Math.min(fromVersion.getSequence(), toVersion.getSequence()),
                    Math.min(fromVersion.getTimestamp(), toVersion.getTimestamp())
            );

            Map<String, String> fromState = temporalQueryService.reconstructKeyValuesAtVersion(fromVersion);
            Map<String, String> toState = temporalQueryService.reconstructKeyValuesAtVersion(toVersion);

        Set<String> allKeys = new TreeSet<>();
        allKeys.addAll(fromState.keySet());
        allKeys.addAll(toState.keySet());

        List<VersionDiffEntry> changes = new ArrayList<>();
        int added = 0;
        int modified = 0;
        int deleted = 0;

        for (String key : allKeys) {
            boolean inFrom = fromState.containsKey(key);
            boolean inTo = toState.containsKey(key);
            String fromVal = fromState.get(key);
            String toVal = toState.get(key);

            if (!inFrom && inTo) {
                added++;
                changes.add(new VersionDiffEntry(key, VersionDiffEntry.DiffType.ADDED, null, toVal));
            } else if (inFrom && !inTo) {
                deleted++;
                changes.add(new VersionDiffEntry(key, VersionDiffEntry.DiffType.DELETED, fromVal, null));
            } else if (!Objects.equals(fromVal, toVal)) {
                modified++;
                changes.add(new VersionDiffEntry(key, VersionDiffEntry.DiffType.MODIFIED, fromVal, toVal));
            }
        }

        return new VersionDiffResult(
                fromVersion.getVersionId(),
                fromVersion.getSequence(),
                fromVersion.getTimestamp(),
                toVersion.getVersionId(),
                toVersion.getSequence(),
                toVersion.getTimestamp(),
                added,
                modified,
                deleted,
                changes
        );
        }
    }

    private ChronosVersion resolveRetainedVersion(String versionId) {
        if (versionId == null || versionId.isBlank()) {
            throw new IllegalArgumentException("versionId must be non-blank");
        }
        CatalogWatermark watermark = versionCatalog.getWatermark();
        boolean hasPurged = watermark != null && watermark.getLastPurgedAt() > 0;
        Optional<ChronosVersion> found = versionCatalog.getVersion(versionId);
        if (found.isPresent()) {
            ChronosVersion v = found.get();
            if (hasPurged && v.getSequence() < watermark.getEarliestAvailableSeq()) {
                throw new HistoryUnavailableException(
                        "Version " + versionId + " is below earliest retained sequence "
                                + watermark.getEarliestAvailableSeq(),
                        "DIFF",
                        versionId,
                        watermark.getEarliestAvailableSeq(),
                        watermark.getEarliestAvailableTimestamp()
                );
            }
            return v;
        }
        if (hasPurged) {
            throw new HistoryUnavailableException(
                    "Version " + versionId + " is unavailable; purged before sequence "
                            + watermark.getEarliestAvailableSeq(),
                    "DIFF",
                    versionId,
                    watermark.getEarliestAvailableSeq(),
                    watermark.getEarliestAvailableTimestamp()
            );
        }
        throw new IllegalArgumentException("Unknown version: " + versionId);
    }

    private String decodeUtf8(byte[] bytes) {
        return bytes != null ? new String(bytes, StandardCharsets.UTF_8) : null;
    }
}

