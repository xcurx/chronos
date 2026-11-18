package com.chronos.merge;

import com.chronos.branch.BranchInfo;
import com.chronos.branch.BranchManager;
import com.chronos.branch.BranchStatus;
import com.chronos.crdt.CrdtMergeResolver;
import com.chronos.crdt.CrdtResolutionResult;
import com.chronos.model.ChronosVersion;
import com.chronos.storage.VersionCatalog;
import com.chronos.dag.VersionDagService;
import com.chronos.model.ChronosWritePayload;
import com.chronos.model.ConflictRecord;
import com.chronos.model.ConflictType;
import com.chronos.query.TemporalQueryService;
import com.minidb.events.ClusterEvent;
import com.minidb.events.EventBus;
import com.chronos.metrics.ChronosMetrics;
import com.minidb.raft.RaftLog;
import com.minidb.raft.RaftNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * High-performance deterministic branch merge engine for Chronos.
 * Evaluates three-way merge previews, resolves conflicts commutatively, via CRDTs, or via LWW/custom,
 * enforces domain safety invariants, and commits two-parent DAG merge nodes via Raft consensus.
 */
@Service
public class MergeEngine {

    private static final Logger log = LoggerFactory.getLogger(MergeEngine.class);

    private final VersionCatalog versionCatalog;
    private final BranchManager branchManager;
    private final VersionDagService versionDagService;
    private final TemporalQueryService temporalQueryService;
    private final ConflictDetector conflictDetector;
    private final InvariantValidator invariantValidator;
    private final RaftNode raftNode;
    private final EventBus eventBus;
    private final ChronosMetrics chronosMetrics;
    private final CrdtMergeResolver crdtMergeResolver;

    public MergeEngine(
            VersionCatalog versionCatalog,
            BranchManager branchManager,
            VersionDagService versionDagService,
            TemporalQueryService temporalQueryService,
            ConflictDetector conflictDetector,
            InvariantValidator invariantValidator,
            RaftNode raftNode) {
        this(versionCatalog, branchManager, versionDagService, temporalQueryService,
             conflictDetector, invariantValidator, raftNode, null, null, null);
    }

    public MergeEngine(
            VersionCatalog versionCatalog,
            BranchManager branchManager,
            VersionDagService versionDagService,
            TemporalQueryService temporalQueryService,
            ConflictDetector conflictDetector,
            InvariantValidator invariantValidator,
            RaftNode raftNode,
            EventBus eventBus,
            ChronosMetrics chronosMetrics) {
        this(versionCatalog, branchManager, versionDagService, temporalQueryService,
             conflictDetector, invariantValidator, raftNode, eventBus, chronosMetrics, null);
    }

    @Autowired
    public MergeEngine(
            VersionCatalog versionCatalog,
            BranchManager branchManager,
            VersionDagService versionDagService,
            TemporalQueryService temporalQueryService,
            ConflictDetector conflictDetector,
            InvariantValidator invariantValidator,
            RaftNode raftNode,
            @Autowired(required = false) EventBus eventBus,
            @Autowired(required = false) ChronosMetrics chronosMetrics,
            @Autowired(required = false) CrdtMergeResolver crdtMergeResolver) {
        this.versionCatalog = versionCatalog;
        this.branchManager = branchManager;
        this.versionDagService = versionDagService;
        this.temporalQueryService = temporalQueryService;
        this.conflictDetector = conflictDetector;
        this.invariantValidator = invariantValidator;
        this.raftNode = raftNode;
        this.eventBus = eventBus;
        this.chronosMetrics = chronosMetrics;
        this.crdtMergeResolver = crdtMergeResolver != null ? crdtMergeResolver : new CrdtMergeResolver();
    }

    /**
     * Preview a branch merge without applying any mutations.
     */
    public MergePreviewResponse preview(MergeRequest request) {
        long start = System.nanoTime();
        EvaluationResult eval = evaluateMerge(request);
        if (chronosMetrics != null) {
            chronosMetrics.recordConflictDetection(System.nanoTime() - start, eval.conflictRecords().size());
            for (ConflictRecord cr : eval.conflictRecords()) {
                chronosMetrics.recordConflict(cr.getConflictType());
            }
        }
        return eval.preview();
    }

    /**
     * Execute and commit a branch merge through Raft consensus.
     */
    public MergeResult merge(MergeRequest request) {
        long start = System.nanoTime();
        EvaluationResult eval = evaluateMerge(request);
        if (!eval.preview().canMerge()) {
            if (chronosMetrics != null) {
                chronosMetrics.recordMerge(System.nanoTime() - start, false);
            }
            if (eventBus != null) {
                eventBus.publish(ClusterEvent.chronosMergeRejected(
                        raftNode != null ? raftNode.getNodeId() : "local",
                        request.targetBranch(), request.sourceBranch(), eval.preview().reason()));
            }
            throw new IllegalStateException("Merge rejected: " + eval.preview().reason());
        }

        String targetTipId = eval.preview().targetTipId();
        ChronosVersion targetTip = versionCatalog.getVersion(targetTipId)
                .orElseThrow(() -> new IllegalStateException("Target tip version not found: " + targetTipId));
        Map<String, String> targetState = temporalQueryService.reconstructKeyValuesAtVersion(targetTip);
        Map<String, String> tentative = eval.preview().tentativeMergedState();

        List<ChronosWritePayload.WriteOp> ops = new ArrayList<>();
        // All keys in tentative state differing from target tip
        for (Map.Entry<String, String> entry : tentative.entrySet()) {
            String k = entry.getKey();
            String v = entry.getValue();
            String cur = targetState.get(k);
            if (!Objects.equals(v, cur)) {
                ops.add(ChronosWritePayload.WriteOp.put(k, v != null ? v.getBytes(StandardCharsets.UTF_8) : null));
            }
        }
        // Keys deleted relative to target
        for (String k : targetState.keySet()) {
            if (!tentative.containsKey(k)) {
                ops.add(ChronosWritePayload.WriteOp.delete(k));
            }
        }

        List<String> parents = List.of(targetTipId, eval.preview().sourceTipId());
        String writerId = request.writerId() != null ? request.writerId() : "chronos-merge-engine";

        ChronosWritePayload payload = ChronosWritePayload.builder()
                .branchId(request.targetBranch())
                .writerId(writerId)
                .parentVersionIds(parents)
                .isMerge(true)
                .mergePolicy(request.policy().name())
                .conflictMetadata(eval.conflictRecords())
                .operations(ops)
                .build();

        try {
            RaftNode.ChronosWriteResult raftResult = raftNode.submitChronosWrite(RaftLog.CommandType.CHRONOS_MERGE, payload)
                    .get(10, TimeUnit.SECONDS);

            if (raftResult == null || !raftResult.committed()) {
                String errMsg = raftResult != null ? raftResult.error() : "Unknown Raft commit failure";
                throw new IllegalStateException("Failed to commit merge to Raft: " + errMsg);
            }

            // Mark source branch as MERGED
            branchManager.updateBranchStatus(request.sourceBranch(), BranchStatus.MERGED);

            ChronosVersion committedVersion = versionCatalog.getVersion(raftResult.versionId())
                    .orElse(null);

            if (chronosMetrics != null) {
                chronosMetrics.recordMerge(System.nanoTime() - start, true);
            }
            if (eventBus != null) {
                eventBus.publish(ClusterEvent.chronosMergeCommitted(
                        raftNode != null ? raftNode.getNodeId() : "local",
                        raftResult.versionId(), request.targetBranch(), request.sourceBranch(), request.policy().name()));
            }

            log.info("Successfully merged branch '{}' into '{}' with version '{}' (policy={})",
                    request.sourceBranch(), request.targetBranch(), raftResult.versionId(), request.policy());

            return new MergeResult(
                    true,
                    raftResult.versionId(),
                    raftResult.versionSequence(),
                    raftResult.timestamp(),
                    request.targetBranch(),
                    request.sourceBranch(),
                    parents,
                    request.policy(),
                    eval.conflictRecords().size(),
                    eval.conflictRecords(),
                    committedVersion,
                    "Merge successfully committed"
            );
        } catch (Exception e) {
            log.error("Merge execution failed for {} -> {}: {}", request.sourceBranch(), request.targetBranch(), e.getMessage(), e);
            throw new RuntimeException("Merge execution failed: " + e.getMessage(), e);
        }
    }

    private EvaluationResult evaluateMerge(MergeRequest req) {
        BranchInfo sourceBranch = branchManager.getBranch(req.sourceBranch())
                .orElseThrow(() -> new IllegalArgumentException("Source branch not found: " + req.sourceBranch()));
        BranchInfo targetBranch = branchManager.getBranch(req.targetBranch())
                .orElseThrow(() -> new IllegalArgumentException("Target branch not found: " + req.targetBranch()));

        String sourceTipId = sourceBranch.getHeadVersionId();
        if (sourceTipId == null && versionCatalog != null) {
            sourceTipId = versionCatalog.getBranchHead(sourceBranch.getBranchId()).orElse(null);
        }
        String targetTipId = targetBranch.getHeadVersionId();
        if (targetTipId == null && versionCatalog != null) {
            targetTipId = versionCatalog.getBranchHead(targetBranch.getBranchId()).orElse(null);
        }

        if (sourceTipId == null) {
            MergePreviewResponse preview = new MergePreviewResponse(
                    false,
                    req.sourceBranch(),
                    req.targetBranch(),
                    null,
                    targetTipId,
                    null,
                    req.policy(),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    Collections.emptyMap(),
                    Collections.emptyMap(),
                    0,
                    0,
                    "Source branch '" + req.sourceBranch() + "' has no committed versions to merge"
            );
            return new EvaluationResult(preview, Collections.emptyList());
        }

        if (targetTipId == null) {
            MergePreviewResponse preview = new MergePreviewResponse(
                    false,
                    req.sourceBranch(),
                    req.targetBranch(),
                    sourceTipId,
                    null,
                    null,
                    req.policy(),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    Collections.emptyMap(),
                    Collections.emptyMap(),
                    0,
                    0,
                    "Target branch '" + req.targetBranch() + "' has no committed versions to merge into"
            );
            return new EvaluationResult(preview, Collections.emptyList());
        }

        if (sourceTipId.equals(targetTipId)) {
            MergePreviewResponse preview = new MergePreviewResponse(
                    true,
                    req.sourceBranch(),
                    req.targetBranch(),
                    sourceTipId,
                    targetTipId,
                    sourceTipId,
                    req.policy(),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    temporalQueryService.reconstructKeyValuesAtVersion(versionCatalog.getVersion(targetTipId).orElseThrow()),
                    Collections.emptyMap(),
                    0,
                    0,
                    "Branches already synchronized at tip " + sourceTipId
            );
            return new EvaluationResult(preview, Collections.emptyList());
        }

        Optional<ChronosVersion> lcaOpt = versionDagService.findLowestCommonAncestor(targetTipId, sourceTipId);
        if (lcaOpt.isEmpty()) {
            MergePreviewResponse preview = new MergePreviewResponse(
                    false,
                    req.sourceBranch(),
                    req.targetBranch(),
                    sourceTipId,
                    targetTipId,
                    null,
                    req.policy(),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    Collections.emptyMap(),
                    Collections.emptyMap(),
                    0,
                    0,
                    "No lowest common ancestor found between " + targetTipId + " and " + sourceTipId
            );
            return new EvaluationResult(preview, Collections.emptyList());
        }

        ChronosVersion lca = lcaOpt.get();
        if (lca.getVersionId().equals(sourceTipId)) {
            MergePreviewResponse preview = new MergePreviewResponse(
                    true,
                    req.sourceBranch(),
                    req.targetBranch(),
                    sourceTipId,
                    targetTipId,
                    lca.getVersionId(),
                    req.policy(),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    temporalQueryService.reconstructKeyValuesAtVersion(versionCatalog.getVersion(targetTipId).orElseThrow()),
                    Collections.emptyMap(),
                    0,
                    0,
                    "Target branch '" + req.targetBranch() + "' already contains source branch tip " + sourceTipId
            );
            return new EvaluationResult(preview, Collections.emptyList());
        }

        ConflictDetectionResult detection = conflictDetector.detectConflicts(
                lca.getVersionId(),
                req.targetBranch(),
                targetTipId,
                req.sourceBranch(),
                sourceTipId);

        Map<String, String> baseState = temporalQueryService.reconstructKeyValuesAtVersion(lca);
        Map<String, String> tentativeState = new LinkedHashMap<>(detection.nonConflictingMergedState());
        Map<String, String> resolutions = new LinkedHashMap<>();
        List<ConflictRecord> conflictRecords = new ArrayList<>();

        boolean canMerge = true;
        String rejectionReason = null;

        for (DetectedConflict c : detection.conflicts()) {
            String k = c.key();
            String leftVal = c.leftValue(); // target branch value
            String rightVal = c.rightValue(); // source branch value
            byte[] leftBytes = leftVal != null ? leftVal.getBytes(StandardCharsets.UTF_8) : null;
            byte[] rightBytes = rightVal != null ? rightVal.getBytes(StandardCharsets.UTF_8) : null;
            byte[] resolvedBytes = null;
            String strategy = null;
            String winningWriter = null;

            // Check custom override
            if (req.customResolutions() != null && req.customResolutions().containsKey(k)) {
                String customVal = req.customResolutions().get(k);
                if (customVal != null) {
                    tentativeState.put(k, customVal);
                    resolutions.put(k, customVal);
                    resolvedBytes = customVal.getBytes(StandardCharsets.UTF_8);
                } else {
                    tentativeState.remove(k);
                    resolutions.put(k, null);
                }
                strategy = "CUSTOM";
                winningWriter = req.writerId() != null ? req.writerId() : "custom-resolution";
            } else if (c.conflictType() == ConflictType.COMPATIBLE_DELTA) {
                if (req.policy() == MergePolicy.FAIL_ON_CONFLICT) {
                    canMerge = false;
                    rejectionReason = "Conflict detected on key '" + k + "' under FAIL_ON_CONFLICT policy";
                    break;
                }
                Long sum = detection.deltaSums().get(k);
                String sumStr = String.valueOf(sum);
                tentativeState.put(k, sumStr);
                resolutions.put(k, sumStr);
                resolvedBytes = sumStr.getBytes(StandardCharsets.UTF_8);
                strategy = "COMMUTATIVE_DELTA";
                winningWriter = "commutative-engine";
            } else if (c.conflictType() == ConflictType.IDENTICAL_WRITE) {
                tentativeState.put(k, leftVal);
                resolutions.put(k, leftVal);
                resolvedBytes = leftBytes;
                strategy = "IDENTICAL";
                winningWriter = "identical";
            } else if (c.conflictType() == ConflictType.COMPATIBLE_CRDT) {
                if (req.policy() == MergePolicy.FAIL_ON_CONFLICT) {
                    canMerge = false;
                    rejectionReason = "Conflict detected on key '" + k + "' under FAIL_ON_CONFLICT policy";
                    break;
                }
                String crdtVal = tentativeState.get(k);
                if (crdtVal == null) {
                    crdtVal = detection.nonConflictingMergedState().get(k);
                }
                tentativeState.put(k, crdtVal);
                resolutions.put(k, crdtVal);
                resolvedBytes = crdtVal != null ? crdtVal.getBytes(StandardCharsets.UTF_8) : null;
                strategy = "CRDT_AUTO";
                winningWriter = "crdt-engine";
            } else if (isCrdtPolicy(req.policy())
                    || (crdtMergeResolver != null && crdtMergeResolver.canResolveAsCrdt(k, c.baseValue(), leftVal, rightVal, req.policy()))) {
                if (req.policy() == MergePolicy.FAIL_ON_CONFLICT) {
                    canMerge = false;
                    rejectionReason = "Conflict detected on key '" + k + "' under FAIL_ON_CONFLICT policy";
                    break;
                }
                ChronosVersion targetTip = versionCatalog.getVersion(targetTipId).orElse(null);
                ChronosVersion sourceTip = versionCatalog.getVersion(sourceTipId).orElse(null);
                long tsA = targetTip != null ? targetTip.getTimestamp() : 0L;
                long tsB = sourceTip != null ? sourceTip.getTimestamp() : 0L;
                long seqA = targetTip != null ? targetTip.getSequence() : 0L;
                long seqB = sourceTip != null ? sourceTip.getSequence() : 0L;
                String writerA = targetTip != null ? targetTip.getWriterId() : req.targetBranch();
                String writerB = sourceTip != null ? sourceTip.getWriterId() : req.sourceBranch();

                CrdtResolutionResult cr = crdtMergeResolver.resolve(
                        k, c.baseValue(), leftVal, rightVal,
                        req.targetBranch(), req.sourceBranch(),
                        tsA, tsB, seqA, seqB,
                        writerA, writerB,
                        req.policy()
                );
                tentativeState.put(k, cr.resolvedValue());
                resolutions.put(k, cr.resolvedValue());
                resolvedBytes = cr.resolvedValue() != null ? cr.resolvedValue().getBytes(StandardCharsets.UTF_8) : null;
                strategy = cr.strategy();
                winningWriter = cr.winningWriter();
            } else if (req.policy() == MergePolicy.COMMUTATIVE_DELTA
                    && c.baseValue() != null
                    && isNumeric(c.baseValue())
                    && isNumeric(leftVal)
                    && isNumeric(rightVal)) {
                long base = parseLong(c.baseValue(), 0L);
                long leftNum = parseLong(leftVal, 0L);
                long rightNum = parseLong(rightVal, 0L);
                long deltaLeft = leftNum - base;
                long deltaRight = rightNum - base;
                long sum = base + deltaLeft + deltaRight;
                String sumStr = String.valueOf(sum);
                tentativeState.put(k, sumStr);
                resolutions.put(k, sumStr);
                resolvedBytes = sumStr.getBytes(StandardCharsets.UTF_8);
                strategy = "COMMUTATIVE_DELTA";
                winningWriter = "commutative-engine";
            } else if (req.policy() == MergePolicy.LAST_WRITE_WINS) {
                ChronosVersion targetTip = versionCatalog.getVersion(targetTipId).orElse(null);
                ChronosVersion sourceTip = versionCatalog.getVersion(sourceTipId).orElse(null);

                boolean sourceWins = false;
                if (sourceTip != null && targetTip != null) {
                    if (sourceTip.getTimestamp() != targetTip.getTimestamp()) {
                        sourceWins = sourceTip.getTimestamp() > targetTip.getTimestamp();
                    } else {
                        sourceWins = sourceTip.getSequence() > targetTip.getSequence();
                    }
                }

                if (sourceWins) {
                    if (rightVal != null) {
                        tentativeState.put(k, rightVal);
                        resolutions.put(k, rightVal);
                        resolvedBytes = rightBytes;
                    } else {
                        tentativeState.remove(k);
                        resolutions.put(k, null);
                    }
                    strategy = "LAST_WRITE_WINS (source)";
                    winningWriter = sourceTip != null ? sourceTip.getWriterId() : req.sourceBranch();
                } else {
                    if (leftVal != null) {
                        tentativeState.put(k, leftVal);
                        resolutions.put(k, leftVal);
                        resolvedBytes = leftBytes;
                    } else {
                        tentativeState.remove(k);
                        resolutions.put(k, null);
                    }
                    strategy = "LAST_WRITE_WINS (target)";
                    winningWriter = targetTip != null ? targetTip.getWriterId() : req.targetBranch();
                }
            } else {
                canMerge = false;
                rejectionReason = "Unresolvable " + c.conflictType() + " on key '" + k + "' under policy " + req.policy();
                break;
            }

            conflictRecords.add(new ConflictRecord(
                    k,
                    c.conflictType(),
                    req.targetBranch(),
                    req.sourceBranch(),
                    lca.getVersionId(),
                    leftBytes,
                    rightBytes,
                    strategy,
                    resolvedBytes,
                    winningWriter,
                    c.description()
            ));
        }

        // Validate domain invariants on tentative merged state against base state
        List<InvariantViolation> violations = Collections.emptyList();
        if (canMerge) {
            violations = invariantValidator.validate(tentativeState, baseState);
            if (!violations.isEmpty()) {
                canMerge = false;
                rejectionReason = "Domain invariant violations: " + violations.get(0).message();
            }
        }

        MergePreviewResponse preview = new MergePreviewResponse(
                canMerge,
                req.sourceBranch(),
                req.targetBranch(),
                sourceTipId,
                targetTipId,
                lca.getVersionId(),
                req.policy(),
                detection.conflicts(),
                violations,
                tentativeState,
                resolutions,
                detection.conflicts().size(),
                violations.size(),
                rejectionReason
        );

        return new EvaluationResult(preview, conflictRecords);
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

    private long parseLong(String s, long def) {
        if (s == null || s.isBlank()) return def;
        try {
            return Long.parseLong(s.trim());
        } catch (NumberFormatException e) {
            return def;
        }
    }

    private boolean isCrdtPolicy(MergePolicy policy) {
        return policy == MergePolicy.CRDT
                || policy == MergePolicy.PN_COUNTER
                || policy == MergePolicy.OR_SET
                || policy == MergePolicy.LWW_REGISTER;
    }

    private record EvaluationResult(
            MergePreviewResponse preview,
            List<ConflictRecord> conflictRecords
    ) {}
}
