package com.chronos.merge;

import com.chronos.crdt.CrdtMergeResolver;
import com.chronos.crdt.CrdtResolutionResult;
import com.chronos.dag.AggregatedKeyDiff;
import com.chronos.dag.AggregatedPathDiff;
import com.chronos.dag.VersionDagService;
import com.chronos.model.ChronosVersion;
import com.chronos.model.ConflictType;
import com.chronos.model.OperationType;
import com.chronos.query.TemporalQueryService;
import com.chronos.storage.VersionCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * Three-way conflict detector across divergent Chronos branch tips relative to their LCA.
 */
@Service
public class ConflictDetector {

    private static final Logger log = LoggerFactory.getLogger(ConflictDetector.class);

    private final VersionCatalog versionCatalog;
    private final VersionDagService versionDagService;
    private final TemporalQueryService temporalQueryService;
    private final CrdtMergeResolver crdtMergeResolver;

    public ConflictDetector(VersionCatalog versionCatalog,
                            VersionDagService versionDagService,
                            TemporalQueryService temporalQueryService) {
        this(versionCatalog, versionDagService, temporalQueryService, new CrdtMergeResolver());
    }

    @Autowired
    public ConflictDetector(VersionCatalog versionCatalog,
                            VersionDagService versionDagService,
                            TemporalQueryService temporalQueryService,
                            @Autowired(required = false) CrdtMergeResolver crdtMergeResolver) {
        this.versionCatalog = versionCatalog;
        this.versionDagService = versionDagService;
        this.temporalQueryService = temporalQueryService;
        this.crdtMergeResolver = crdtMergeResolver != null ? crdtMergeResolver : new CrdtMergeResolver();
    }

    /**
     * Detect conflicts between two divergent branches relative to their Lowest Common Ancestor (LCA).
     *
     * @param lcaVersionId Common ancestor version id
     * @param branchAId    Target branch id
     * @param branchATipId Target branch tip version id
     * @param branchBId    Source branch id
     * @param branchBTipId Source branch tip version id
     * @return Summary of conflicts, non-conflicting merged state, and delta sums
     */
    public ConflictDetectionResult detectConflicts(
            String lcaVersionId,
            String branchAId,
            String branchATipId,
            String branchBId,
            String branchBTipId) {

        ChronosVersion lcaVersion = versionCatalog.getVersion(lcaVersionId)
                .orElseThrow(() -> new IllegalArgumentException("LCA version not found: " + lcaVersionId));

        Map<String, String> baseState = temporalQueryService.reconstructKeyValuesAtVersion(lcaVersion);
        AggregatedPathDiff diffA = versionDagService.computeAggregatedChangeSet(lcaVersionId, branchATipId);
        AggregatedPathDiff diffB = versionDagService.computeAggregatedChangeSet(lcaVersionId, branchBTipId);

        Map<String, AggregatedKeyDiff> aChanges = diffA.changes();
        Map<String, AggregatedKeyDiff> bChanges = diffB.changes();

        Set<String> allKeys = new LinkedHashSet<>();
        allKeys.addAll(baseState.keySet());
        allKeys.addAll(aChanges.keySet());
        allKeys.addAll(bChanges.keySet());

        List<DetectedConflict> conflicts = new ArrayList<>();
        Map<String, String> tentativeMergedState = new LinkedHashMap<>();
        Map<String, Long> deltaSums = new LinkedHashMap<>();

        boolean hasUnresolvable = false;

        for (String key : allKeys) {
            String baseVal = baseState.get(key);
            boolean modifiedInA = aChanges.containsKey(key);
            boolean modifiedInB = bChanges.containsKey(key);

            if (!modifiedInA && !modifiedInB) {
                // Untouched in both branches: stays as base
                if (baseVal != null) {
                    tentativeMergedState.put(key, baseVal);
                }
            } else if (modifiedInA && !modifiedInB) {
                // Modified only in branch A: take branch A's value (or apply net delta to base)
                AggregatedKeyDiff kdA = aChanges.get(key);
                if (kdA.netDelta() != null && isOnlyDeltas(kdA)) {
                    long baseNum = parseLongOrDefault(baseVal, 0L);
                    long finalSum = baseNum + kdA.netDelta();
                    deltaSums.put(key, finalSum);
                    tentativeMergedState.put(key, String.valueOf(finalSum));
                } else if (!kdA.deleted() && kdA.tipValue() != null) {
                    tentativeMergedState.put(key, kdA.tipValue());
                }
            } else if (!modifiedInA && modifiedInB) {
                // Modified only in branch B: take branch B's value (or apply net delta to base)
                AggregatedKeyDiff kdB = bChanges.get(key);
                if (kdB.netDelta() != null && isOnlyDeltas(kdB)) {
                    long baseNum = parseLongOrDefault(baseVal, 0L);
                    long finalSum = baseNum + kdB.netDelta();
                    deltaSums.put(key, finalSum);
                    tentativeMergedState.put(key, String.valueOf(finalSum));
                } else if (!kdB.deleted() && kdB.tipValue() != null) {
                    tentativeMergedState.put(key, kdB.tipValue());
                }
            } else {
                // Modified in BOTH branches: CONFLICT or COMPATIBLE change!
                AggregatedKeyDiff kdA = aChanges.get(key);
                AggregatedKeyDiff kdB = bChanges.get(key);

                String valA = kdA.tipValue();
                String valB = kdB.tipValue();
                boolean isDeletedA = kdA.deleted();
                boolean isDeletedB = kdB.deleted();

                // Case 1: Both only executed DELTA operations on this key OR numeric changes relative to numeric base
                boolean aIsDelta = (kdA.netDelta() != null && !isDeletedA && isOnlyDeltas(kdA))
                        || (baseVal != null && isNumeric(baseVal) && isNumeric(valA) && !isDeletedA);
                boolean bIsDelta = (kdB.netDelta() != null && !isDeletedB && isOnlyDeltas(kdB))
                        || (baseVal != null && isNumeric(baseVal) && isNumeric(valB) && !isDeletedB);

                if (aIsDelta && bIsDelta && baseVal != null && isNumeric(baseVal) && isNumeric(valA) && isNumeric(valB)) {
                    long baseNum = parseLongOrDefault(baseVal, 0L);
                    long deltaA = kdA.netDelta() != null ? kdA.netDelta() : (parseLongOrDefault(valA, 0L) - baseNum);
                    long deltaB = kdB.netDelta() != null ? kdB.netDelta() : (parseLongOrDefault(valB, 0L) - baseNum);
                    long finalSum = baseNum + deltaA + deltaB;
                    deltaSums.put(key, finalSum);
                    tentativeMergedState.put(key, String.valueOf(finalSum));

                    conflicts.add(new DetectedConflict(
                            key,
                            ConflictType.COMPATIBLE_DELTA,
                            branchAId,
                            branchBId,
                            baseVal,
                            valA,
                            valB,
                            "Compatible commutative deltas: " + deltaA + " on " + branchAId + ", " + deltaB + " on " + branchBId,
                            true
                    ));
                } else if (isDeletedA && isDeletedB) {
                    // Both deleted the key: compatible identical delete
                    // tentativeMergedState does not include the key
                } else if (Objects.equals(valA, valB) && !isDeletedA && !isDeletedB) {
                    // Identical write: both wrote same value
                    tentativeMergedState.put(key, valA);
                    conflicts.add(new DetectedConflict(
                            key,
                            ConflictType.IDENTICAL_WRITE,
                            branchAId,
                            branchBId,
                            baseVal,
                            valA,
                            valB,
                            "Identical write on both branches",
                            true
                    ));
                } else if (!isDeletedA && isDeletedB) {
                    // Branch A updated, Branch B deleted
                    conflicts.add(new DetectedConflict(
                            key,
                            ConflictType.UPDATE_DELETE,
                            branchAId,
                            branchBId,
                            baseVal,
                            valA,
                            null,
                            "Branch " + branchAId + " updated while " + branchBId + " deleted key",
                            false
                    ));
                    hasUnresolvable = true;
                } else if (isDeletedA && !isDeletedB) {
                    // Branch A deleted, Branch B updated
                    conflicts.add(new DetectedConflict(
                            key,
                            ConflictType.DELETE_UPDATE,
                            branchAId,
                            branchBId,
                            baseVal,
                            null,
                            valB,
                            "Branch " + branchAId + " deleted while " + branchBId + " updated key",
                            false
                    ));
                    hasUnresolvable = true;
                } else {
                    // Both wrote different non-null values: check if key or payload qualifies for automatic CRDT resolution
                    if (crdtMergeResolver != null && crdtMergeResolver.canResolveAsCrdt(key, baseVal, valA, valB, null)) {
                        ChronosVersion tipAVer = versionCatalog.getVersion(branchATipId).orElse(null);
                        ChronosVersion tipBVer = versionCatalog.getVersion(branchBTipId).orElse(null);
                        long tsA = tipAVer != null ? tipAVer.getTimestamp() : 0L;
                        long tsB = tipBVer != null ? tipBVer.getTimestamp() : 0L;
                        long seqA = tipAVer != null ? tipAVer.getSequence() : 0L;
                        long seqB = tipBVer != null ? tipBVer.getSequence() : 0L;
                        String writerA = tipAVer != null ? tipAVer.getWriterId() : branchAId;
                        String writerB = tipBVer != null ? tipBVer.getWriterId() : branchBId;

                        CrdtResolutionResult cr = crdtMergeResolver.resolve(
                                key, baseVal, valA, valB,
                                branchAId, branchBId,
                                tsA, tsB, seqA, seqB,
                                writerA, writerB,
                                null
                        );
                        tentativeMergedState.put(key, cr.resolvedValue());
                        conflicts.add(new DetectedConflict(
                                key,
                                ConflictType.COMPATIBLE_CRDT,
                                branchAId,
                                branchBId,
                                baseVal,
                                valA,
                                valB,
                                "Compatible CRDT (" + cr.crdtType() + "): " + cr.explanation(),
                                true
                        ));
                    } else {
                        // WRITE_WRITE: both wrote different non-null values
                        conflicts.add(new DetectedConflict(
                                key,
                                ConflictType.WRITE_WRITE,
                                branchAId,
                                branchBId,
                                baseVal,
                                valA,
                                valB,
                                "Concurrent write on both branches with different values: '" + valA + "' vs '" + valB + "'",
                                false
                        ));
                        hasUnresolvable = true;
                    }
                }
            }
        }

        return new ConflictDetectionResult(
                lcaVersionId,
                branchAId,
                branchATipId,
                branchBId,
                branchBTipId,
                !conflicts.isEmpty(),
                hasUnresolvable,
                conflicts,
                tentativeMergedState,
                deltaSums
        );
    }

    private boolean isOnlyDeltas(AggregatedKeyDiff diff) {
        if (diff.operations() == null || diff.operations().isEmpty()) {
            return false;
        }
        for (OperationType op : diff.operations()) {
            if (op != OperationType.DELTA) {
                return false;
            }
        }
        return true;
    }

    private boolean isNumeric(String s) {
        if (s == null || s.isBlank()) return false;
        try {
            Long.parseLong(s.trim());
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private long parseLongOrDefault(String s, long def) {
        if (s == null || s.isBlank()) return def;
        try {
            return Long.parseLong(s.trim());
        } catch (NumberFormatException e) {
            return def;
        }
    }
}
