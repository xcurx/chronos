package com.chronos.api;

import com.chronos.audit.AuditService;
import com.chronos.audit.KeyAuditEntry;
import com.chronos.audit.VersionDiffResult;
import com.chronos.branch.BranchInfo;
import com.chronos.branch.BranchManager;
import com.chronos.branch.BranchStatus;
import com.chronos.dag.AggregatedPathDiff;
import com.chronos.dag.VersionDagResponse;
import com.chronos.dag.VersionDagService;
import com.chronos.crdt.CrdtMergeResolver;
import com.chronos.crdt.CrdtResolutionResult;
import com.chronos.gc.ChronosGcService;
import com.chronos.gc.ChronosRetentionConfig;
import com.chronos.gc.ReadWatermarkRegistry;
import com.chronos.gc.RetentionPolicy;
import com.chronos.metrics.ChronosMetrics;
import com.chronos.merge.ConflictDetector;
import com.chronos.merge.InvariantRegistry;
import com.chronos.merge.InvariantRule;
import com.chronos.merge.InvariantType;
import com.chronos.merge.InvariantValidator;
import com.chronos.merge.MergeEngine;
import com.chronos.merge.MergePolicy;
import com.chronos.merge.MergePreviewResponse;
import com.chronos.merge.MergeRequest;
import com.chronos.merge.MergeResult;
import com.chronos.model.ChronosSnapshot;
import com.chronos.model.ChronosVersion;
import com.chronos.model.ChronosWritePayload;
import com.chronos.model.OperationType;
import com.chronos.query.DatabaseStateResult;
import com.chronos.query.HistoryUnavailableException;
import com.chronos.query.QueryMode;
import com.chronos.query.TemporalQueryRequest;
import com.chronos.query.TemporalQueryResult;
import com.chronos.query.TemporalQueryService;
import com.chronos.storage.VersionCatalog;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.minidb.config.RaftConfig;
import com.minidb.events.ClusterEvent;
import com.minidb.events.EventBus;
import com.minidb.mvcc.ReadMode;
import com.minidb.raft.RaftLog;
import com.minidb.raft.RaftNode;
import com.minidb.speculation.SpeculativeManager;
import com.minidb.txn.TxnCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * REST API controller for Chronos writes, temporal queries, state reconstruction,
 * snapshots, diffs, and per-key audit trails.
 */
@RestController
@RequestMapping("/api/chronos")
public class ChronosController {

    private static final Logger log = LoggerFactory.getLogger(ChronosController.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final RaftNode raftNode;
    private final RaftConfig raftConfig;
    private final VersionCatalog versionCatalog;
    private final TemporalQueryService temporalQueryService;
    private final AuditService auditService;
    private final SpeculativeManager speculativeManager;
    private final TxnCoordinator txnCoordinator;
    private final EventBus eventBus;
    private final ChronosRetentionConfig retentionConfig;
    private final ReadWatermarkRegistry watermarkRegistry;
    private final ChronosGcService chronosGcService;
    private final BranchManager branchManager;
    private final VersionDagService versionDagService;
    private final MergeEngine mergeEngine;
    private final InvariantRegistry invariantRegistry;
    private final ChronosMetrics chronosMetrics;
    private final HttpClient httpClient;
    private final CrdtMergeResolver crdtMergeResolver = new CrdtMergeResolver();

    public ChronosController(RaftNode raftNode,
                             RaftConfig raftConfig,
                             VersionCatalog versionCatalog,
                             TemporalQueryService temporalQueryService,
                             AuditService auditService,
                             SpeculativeManager speculativeManager,
                             TxnCoordinator txnCoordinator,
                             EventBus eventBus) {
        this(raftNode, raftConfig, versionCatalog, temporalQueryService, auditService,
                speculativeManager, txnCoordinator, eventBus,
                new ChronosRetentionConfig(), new ReadWatermarkRegistry(), null, null, null, null, null, null);
    }

    public ChronosController(RaftNode raftNode,
                             RaftConfig raftConfig,
                             VersionCatalog versionCatalog,
                             TemporalQueryService temporalQueryService,
                             AuditService auditService,
                             SpeculativeManager speculativeManager,
                             TxnCoordinator txnCoordinator,
                             EventBus eventBus,
                             ChronosRetentionConfig retentionConfig,
                             ReadWatermarkRegistry watermarkRegistry,
                             ChronosGcService chronosGcService) {
        this(raftNode, raftConfig, versionCatalog, temporalQueryService, auditService,
                speculativeManager, txnCoordinator, eventBus,
                retentionConfig, watermarkRegistry, chronosGcService, null, null, null, null, null);
    }

    public ChronosController(RaftNode raftNode,
                             RaftConfig raftConfig,
                             VersionCatalog versionCatalog,
                             TemporalQueryService temporalQueryService,
                             AuditService auditService,
                             SpeculativeManager speculativeManager,
                             TxnCoordinator txnCoordinator,
                             EventBus eventBus,
                             ChronosRetentionConfig retentionConfig,
                             ReadWatermarkRegistry watermarkRegistry,
                             ChronosGcService chronosGcService,
                             BranchManager branchManager,
                             VersionDagService versionDagService) {
        this(raftNode, raftConfig, versionCatalog, temporalQueryService, auditService,
                speculativeManager, txnCoordinator, eventBus,
                retentionConfig, watermarkRegistry, chronosGcService,
                branchManager, versionDagService, null, null, null);
    }

    public ChronosController(RaftNode raftNode,
                             RaftConfig raftConfig,
                             VersionCatalog versionCatalog,
                             TemporalQueryService temporalQueryService,
                             AuditService auditService,
                             SpeculativeManager speculativeManager,
                             TxnCoordinator txnCoordinator,
                             EventBus eventBus,
                             ChronosRetentionConfig retentionConfig,
                             ReadWatermarkRegistry watermarkRegistry,
                             ChronosGcService chronosGcService,
                             BranchManager branchManager,
                             VersionDagService versionDagService,
                             MergeEngine mergeEngine,
                             InvariantRegistry invariantRegistry) {
        this(raftNode, raftConfig, versionCatalog, temporalQueryService, auditService,
                speculativeManager, txnCoordinator, eventBus,
                retentionConfig, watermarkRegistry, chronosGcService,
                branchManager, versionDagService, mergeEngine, invariantRegistry, null);
    }

    @Autowired
    public ChronosController(RaftNode raftNode,
                             RaftConfig raftConfig,
                             VersionCatalog versionCatalog,
                             TemporalQueryService temporalQueryService,
                             AuditService auditService,
                             SpeculativeManager speculativeManager,
                             TxnCoordinator txnCoordinator,
                             EventBus eventBus,
                             ChronosRetentionConfig retentionConfig,
                             ReadWatermarkRegistry watermarkRegistry,
                             ChronosGcService chronosGcService,
                             @Autowired(required = false) BranchManager branchManager,
                             @Autowired(required = false) VersionDagService versionDagService,
                             @Autowired(required = false) MergeEngine mergeEngine,
                             @Autowired(required = false) InvariantRegistry invariantRegistry,
                             @Autowired(required = false) ChronosMetrics chronosMetrics) {
        this.raftNode = raftNode;
        this.raftConfig = raftConfig;
        this.versionCatalog = versionCatalog;
        this.temporalQueryService = temporalQueryService;
        this.auditService = auditService;
        this.speculativeManager = speculativeManager;
        this.txnCoordinator = txnCoordinator;
        this.eventBus = eventBus;
        this.retentionConfig = retentionConfig != null ? retentionConfig : new ChronosRetentionConfig();
        this.watermarkRegistry = watermarkRegistry != null ? watermarkRegistry : new ReadWatermarkRegistry();
        this.chronosGcService = chronosGcService != null
                ? chronosGcService
                : new ChronosGcService(versionCatalog,
                        raftNode != null ? raftNode.getMvccStore() : null,
                        raftNode != null ? raftNode.getSnapshotManager() : null,
                        this.retentionConfig,
                        this.watermarkRegistry);
        this.branchManager = branchManager != null
                ? branchManager
                : (raftNode != null && raftNode.getBranchManager() != null ? raftNode.getBranchManager() : null);
        this.versionDagService = versionDagService != null
                ? versionDagService
                : new VersionDagService(versionCatalog, temporalQueryService);
        if (this.branchManager != null && raftNode != null && raftNode.getBranchManager() == null) {
            raftNode.setBranchManager(this.branchManager);
        }
        this.invariantRegistry = invariantRegistry != null ? invariantRegistry : new InvariantRegistry();
        if (mergeEngine != null) {
            this.mergeEngine = mergeEngine;
        } else {
            InvariantValidator validator = new InvariantValidator(this.invariantRegistry);
            ConflictDetector detector = new ConflictDetector(versionCatalog, this.versionDagService, temporalQueryService);
            this.mergeEngine = new MergeEngine(versionCatalog, this.branchManager, this.versionDagService, temporalQueryService, detector, validator, raftNode);
        }
        this.chronosMetrics = chronosMetrics != null ? chronosMetrics : new ChronosMetrics(null);
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(3))
                .build();
    }

    /**
     * Unified Chronos write supporting single-op or multi-op mutations across branches and ack policies.
     */
    @PostMapping("/write")
    public Map<String, Object> write(@RequestBody Map<String, Object> request) {
        if (!raftNode.isLeader()) {
            return forwardPostToLeader("/api/chronos/write", request);
        }

        long start = System.currentTimeMillis();
        String writerId = asString(request.get("writerId"));
        String branchId = asNonBlankOrDefault(asString(request.get("branchId")), "main");
        String ackPolicyRaw = asNonBlankOrDefault(asString(request.get("ackPolicy")), "COMMITTED").toUpperCase();
        String txnId = asString(request.get("txnId"));
        String expectedParent = asString(request.get("expectedParentVersionId"));
        if (expectedParent == null || expectedParent.isBlank()) {
            expectedParent = asString(request.get("expectedBaseVersionId"));
        }

        boolean autoBranched = false;
        String autoBranchReason = null;
        if ("main".equals(branchId) && expectedParent != null && !expectedParent.isBlank() && branchManager != null) {
            String currentMainHead = versionCatalog.getBranchHead("main").orElse(null);
            if (currentMainHead != null && !expectedParent.equals(currentMainHead)) {
                BranchInfo autoBranch = branchManager.autoCreateStaleBranch(
                        writerId, expectedParent, versionCatalog.nextSequence());
                branchId = autoBranch.getBranchId();
                autoBranched = true;
                autoBranchReason = "Optimistic auto-branch created: main head '" + currentMainHead
                        + "' diverged from expected base '" + expectedParent + "'";
            }
        }

        List<ChronosWritePayload.WriteOp> ops = parseWriteOperations(request, branchId);
        if (ops.isEmpty()) {
            throw new IllegalArgumentException("Write request must specify key or non-empty operations list");
        }

        if ("SPECULATIVE".equals(ackPolicyRaw) && ops.size() == 1
                && ops.get(0).getOpType() != OperationType.DELTA
                && "main".equals(branchId) && speculativeManager != null) {
            return handleSpeculativeSingleOp(ops.get(0), branchId);
        }

        if (ops.size() > 1 && txnCoordinator != null) {
            Map<String, Object> multiResp = handleMultiOpTransaction(ops, branchId, writerId, start);
            if (autoBranched) {
                multiResp.put("autoBranched", true);
                multiResp.put("autoBranchReason", autoBranchReason);
            }
            return multiResp;
        }

        List<String> parents;
        if (expectedParent != null && !expectedParent.isBlank()) {
            parents = List.of(expectedParent);
        } else if (!"main".equals(branchId) && branchManager != null) {
            String branchHead = branchManager.getBranch(branchId)
                    .map(b -> b.getHeadVersionId() != null ? b.getHeadVersionId() : b.getBaseVersionId())
                    .orElse(null);
            parents = branchHead != null ? List.of(branchHead) : Collections.emptyList();
        } else {
            parents = Collections.emptyList();
        }

        Long payloadValidFrom = parseOptionalTimestamp(request.get("validFrom"));
        Long payloadValidTo = parseOptionalTimestamp(request.get("validTo"));

        ChronosWritePayload payload = ChronosWritePayload.builder()
                .txnId(txnId)
                .writerId(writerId)
                .branchId(branchId)
                .parentVersionIds(parents)
                .operations(ops)
                .validFrom(payloadValidFrom)
                .validTo(payloadValidTo)
                .build();

        RaftLog.CommandType cmdType;
        if (ops.size() > 1) {
            cmdType = RaftLog.CommandType.CHRONOS_TXN;
        } else if (!"main".equals(branchId)) {
            cmdType = RaftLog.CommandType.CHRONOS_BRANCH_WRITE;
        } else {
            cmdType = RaftLog.CommandType.CHRONOS_WRITE;
        }

        Map<String, Object> resp = new LinkedHashMap<>();
        try {
            RaftNode.ChronosWriteResult writeResult = raftNode.submitChronosWrite(cmdType, payload)
                    .get(10, TimeUnit.SECONDS);
            long latency = System.currentTimeMillis() - start;
            if (speculativeManager != null && writeResult != null && writeResult.committed()) {
                speculativeManager.recordStandardWrite(TimeUnit.MILLISECONDS.toNanos(latency));
            }

            resp.put("success", writeResult != null && writeResult.committed());
            resp.put("speculative", false);
            resp.put("latencyMs", latency);
            if (autoBranched) {
                resp.put("autoBranched", true);
                resp.put("autoBranchReason", autoBranchReason);
            }
            if (writeResult != null && writeResult.committed()) {
                resp.put("versionId", writeResult.versionId());
                resp.put("versionSequence", writeResult.versionSequence());
                resp.put("timestamp", writeResult.timestamp());
                resp.put("raftLogIndex", writeResult.raftLogIndex());
                resp.put("branchId", writeResult.branchId());
                if (writerId != null) {
                    resp.put("writerId", writerId);
                }
            } else if (writeResult != null && writeResult.error() != null) {
                resp.put("error", writeResult.error());
            }
            if (eventBus != null) {
                eventBus.publish(ClusterEvent.kvOperation("CHRONOS_WRITE", ops.get(0).getKey(),
                        latency, writeResult != null && writeResult.committed()));
            }
        } catch (Exception e) {
            resp.put("success", false);
            resp.put("error", e.getMessage());
            resp.put("latencyMs", System.currentTimeMillis() - start);
        }
        return resp;
    }

    /**
     * Temporal single-key query: CURRENT, AS_OF_VERSION, AS_OF_TIME, AS_OF_SNAPSHOT, AS_OF_VALID_TIME, or BITEMPORAL.
     */
    @GetMapping("/query")
    public Map<String, Object> query(
            @RequestParam String key,
            @RequestParam(required = false) String mode,
            @RequestParam(required = false) String versionId,
            @RequestParam(required = false) String timestamp,
            @RequestParam(required = false) String snapshotId,
            @RequestParam(required = false) String branchId,
            @RequestParam(required = false) String validTime,
            @RequestParam(required = false) String asOfValidTime,
            @RequestParam(required = false) String asOfTxnTime,
            @RequestParam(defaultValue = "LINEARIZABLE") String readMode,
            @RequestParam(defaultValue = "false") boolean consistent) {

        String effectiveVt = (asOfValidTime != null && !asOfValidTime.isBlank()) ? asOfValidTime : validTime;
        String effectiveTt = (asOfTxnTime != null && !asOfTxnTime.isBlank()) ? asOfTxnTime : timestamp;

        TemporalQueryRequest request = new TemporalQueryRequest(
                parseQueryMode(mode),
                key,
                versionId,
                effectiveTt,
                snapshotId,
                branchId,
                parseReadMode(readMode),
                consistent,
                null,
                0,
                effectiveVt,
                null,
                null
        );

        TemporalQueryResult result = temporalQueryService.queryKey(request);
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("found", result.found());
        resp.put("key", result.key());
        resp.put("value", result.value());
        resp.put("tombstone", result.tombstone());
        resp.put("speculative", result.speculative());
        resp.put("versionId", result.versionId());
        resp.put("versionSequence", result.versionSequence());
        resp.put("timestamp", result.timestamp());
        resp.put("branchId", result.branchId());
        resp.put("writerId", result.writerId());
        resp.put("transactionId", result.transactionId());
        resp.put("validFrom", result.validFrom());
        resp.put("validTo", result.validTo());
        resp.put("queryMode", result.queryMode() != null ? result.queryMode().name() : null);
        resp.put("latencyMs", result.latencyMs());
        if (eventBus != null) {
            eventBus.publish(com.minidb.events.ClusterEvent.kvOperation("GET", key, result.latencyMs(), result.found()));
        }
        return resp;
    }

    /**
     * Retrieve the complete 2D bitemporal history matrix for a specific key.
     */
    @GetMapping("/bitemporal/timeline/{key}")
    public Map<String, Object> getKeyBitemporalTimeline(@PathVariable String key) {
        if (temporalQueryService == null) {
            throw new IllegalStateException("TemporalQueryService unavailable");
        }
        List<Map<String, Object>> timeline = temporalQueryService.getKeyBitemporalTimeline(key);
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("key", key);
        resp.put("totalMutations", timeline.size());
        resp.put("timeline", timeline);
        return resp;
    }

    /**
     * Full database state reconstruction at any historical version, timestamp, snapshot, or valid time.
     */
    @GetMapping("/state")
    public Map<String, Object> state(
            @RequestParam(required = false) String mode,
            @RequestParam(required = false) String versionId,
            @RequestParam(required = false) String timestamp,
            @RequestParam(required = false) String snapshotId,
            @RequestParam(required = false) String branchId,
            @RequestParam(required = false) String prefix,
            @RequestParam(required = false) String validTime,
            @RequestParam(required = false) String asOfValidTime,
            @RequestParam(required = false) String asOfTxnTime,
            @RequestParam(defaultValue = "0") int limit,
            @RequestParam(defaultValue = "LINEARIZABLE") String readMode,
            @RequestParam(defaultValue = "false") boolean consistent) {
        String effectiveVt = (asOfValidTime != null && !asOfValidTime.isBlank()) ? asOfValidTime : validTime;
        String effectiveTt = (asOfTxnTime != null && !asOfTxnTime.isBlank()) ? asOfTxnTime : timestamp;
        return handleStateReconstruction(mode, versionId, effectiveTt, snapshotId, branchId, prefix, limit, readMode, consistent, effectiveVt);
    }

    @PostMapping("/state")
    public Map<String, Object> statePost(@RequestBody(required = false) Map<String, Object> body) {
        Map<String, Object> request = body != null ? body : Collections.emptyMap();
        String mode = asString(request.get("queryMode"));
        if (mode == null || mode.isBlank()) mode = asString(request.get("mode"));
        String versionId = asString(request.get("versionId"));
        if (versionId == null || versionId.isBlank()) versionId = asString(request.get("asOfVersion"));
        String timestamp = request.get("timestamp") != null ? String.valueOf(request.get("timestamp")) : asString(request.get("asOfTxnTime"));
        String snapshotId = asString(request.get("snapshotId"));
        String branchId = asString(request.get("branchId"));
        String prefix = asString(request.get("prefix"));
        String validTime = asString(request.get("validTime"));
        if (validTime == null || validTime.isBlank()) validTime = asString(request.get("asOfValidTime"));
        int limit = request.get("limit") instanceof Number n ? n.intValue() : 0;
        String readMode = asNonBlankOrDefault(asString(request.get("readMode")), "LINEARIZABLE");
        boolean consistent = Boolean.TRUE.equals(request.get("consistent"));
        return handleStateReconstruction(mode, versionId, timestamp, snapshotId, branchId, prefix, limit, readMode, consistent, validTime);
    }

    private Map<String, Object> handleStateReconstruction(
            String mode,
            String versionId,
            String timestamp,
            String snapshotId,
            String branchId,
            String prefix,
            int limit,
            String readMode,
            boolean consistent,
            String validTime) {
        if ("AS_OF_SNAPSHOT".equalsIgnoreCase(mode) && snapshotId != null && !raftNode.isLeader()) {
            if (temporalQueryService.getSnapshot(snapshotId).isEmpty()) {
                Map<String, Object> fwdReq = new LinkedHashMap<>();
                fwdReq.put("mode", mode);
                fwdReq.put("snapshotId", snapshotId);
                fwdReq.put("readMode", readMode);
                fwdReq.put("consistent", consistent);
                if (branchId != null) fwdReq.put("branchId", branchId);
                if (prefix != null) fwdReq.put("prefix", prefix);
                if (validTime != null) fwdReq.put("validTime", validTime);
                return forwardPostToLeader("/api/chronos/state", fwdReq);
            }
        }

        TemporalQueryRequest request = new TemporalQueryRequest(
                parseQueryMode(mode),
                null,
                versionId,
                timestamp,
                snapshotId,
                branchId,
                parseReadMode(readMode),
                consistent,
                prefix,
                limit,
                validTime,
                null,
                null
        );

        DatabaseStateResult state = temporalQueryService.reconstructState(request);
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("versionId", state.versionId());
        resp.put("asOfVersionId", state.versionId());
        resp.put("versionSequence", state.versionSequence());
        resp.put("timestamp", state.timestamp());
        resp.put("asOfTimestamp", state.timestamp());
        resp.put("branchId", state.branchId());
        resp.put("queryMode", state.queryMode() != null ? state.queryMode().name() : null);
        resp.put("entries", state.entries());
        resp.put("keys", state.entries());
        resp.put("details", state.details());
        resp.put("count", state.count());
        resp.put("totalKeys", state.count());
        resp.put("latencyMs", state.latencyMs());
        resp.put("executionTimeMs", (double) state.latencyMs());
        if (eventBus != null) {
            eventBus.publish(com.minidb.events.ClusterEvent.kvOperation("GET", prefix != null ? prefix : "state", state.latencyMs(), true));
        }
        return resp;
    }

    /**
     * Create a named snapshot anchored to the current head version of {@code branchId}.
     */
    @PostMapping("/snapshots")
    public ResponseEntity<Map<String, Object>> createSnapshot(@RequestBody(required = false) Map<String, Object> request) {
        Map<String, Object> body = request != null ? request : Collections.emptyMap();
        if (!raftNode.isLeader()) {
            Map<String, Object> forwarded = forwardPostToLeader("/api/chronos/snapshots", body);
            return ResponseEntity.ok(forwarded);
        }

        String snapshotId = asString(body.get("snapshotId"));
        if (snapshotId == null || snapshotId.isBlank()) {
            snapshotId = asString(body.get("name"));
        }
        String description = asString(body.get("description"));
        String branchId = asNonBlankOrDefault(asString(body.get("branchId")), "main");

        ChronosSnapshot snapshot = temporalQueryService.createSnapshot(snapshotId, description, branchId);
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("success", true);
        resp.put("snapshot", snapshot);
        return ResponseEntity.ok(resp);
    }

    /**
     * List all registered Chronos snapshots.
     */
    @GetMapping({"/snapshots", "/snapshot"})
    public Map<String, Object> listSnapshots() {
        if (!raftNode.isLeader()) {
            Map<String, Object> forwarded = forwardGetToLeader("/api/chronos/snapshots");
            if (Boolean.TRUE.equals(forwarded.get("success")) || forwarded.containsKey("snapshots")) {
                return forwarded;
            }
        }
        List<ChronosSnapshot> snapshots = temporalQueryService.listSnapshots();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("snapshots", snapshots);
        resp.put("count", snapshots.size());
        return resp;
    }

    /**
     * List versions ordered descending by sequence, optionally filtered by branch or time window.
     */
    @GetMapping("/versions")
    public Map<String, Object> listVersions(
            @RequestParam(required = false) String branchId,
            @RequestParam(required = false) String fromTs,
            @RequestParam(required = false) String toTs,
            @RequestParam(defaultValue = "50") int limit) {

        List<ChronosVersion> versions;
        if (fromTs != null && !fromTs.isBlank() && toTs != null && !toTs.isBlank()) {
            long from = TemporalQueryService.parseTimestamp(fromTs);
            long to = TemporalQueryService.parseTimestamp(toTs);
            versions = versionCatalog.listVersionsByTimeRange(from, to, limit);
        } else if (branchId != null && !branchId.isBlank()) {
            versions = versionCatalog.listVersionsByBranch(branchId, limit);
        } else {
            versions = versionCatalog.listVersionsDescending(limit);
        }

        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("versions", versions);
        resp.put("count", versions.size());
        resp.put("watermark", versionCatalog.getWatermark());
        resp.put("branchHeads", versionCatalog.listBranchHeads());
        return resp;
    }

    /**
     * Return complete metadata and change set details for a single version.
     */
    @GetMapping("/versions/{versionId}")
    public ResponseEntity<ChronosVersion> getVersion(@PathVariable String versionId) {
        return auditService.getVersionAuditDetails(versionId)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    /**
     * Diff two versions ({@code from} -> {@code to}).
     */
    @GetMapping("/diff")
    public VersionDiffResult diffVersions(
            @RequestParam(required = false) String from,
            @RequestParam(required = false) String to,
            @RequestParam(required = false) String fromVersionId,
            @RequestParam(required = false) String toVersionId,
            @RequestParam(required = false) String v1,
            @RequestParam(required = false) String v2) {
        String resolvedFrom = (from != null && !from.isBlank()) ? from
                : (fromVersionId != null && !fromVersionId.isBlank() ? fromVersionId : v1);
        String resolvedTo = (to != null && !to.isBlank()) ? to
                : (toVersionId != null && !toVersionId.isBlank() ? toVersionId : v2);
        return auditService.diffVersions(resolvedFrom, resolvedTo);
    }

    /**
     * Return complete chronological mutation trail for a key.
     */
    @GetMapping({"/audit/key/{key}", "/audit/{key}"})
    public Map<String, Object> getKeyAuditTrail(
            @PathVariable String key,
            @RequestParam(defaultValue = "true") boolean includeUncommitted) {
        List<KeyAuditEntry> trail = auditService.getKeyAuditTrail(key, includeUncommitted);
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("key", key);
        resp.put("entries", trail);
        resp.put("history", trail);
        resp.put("count", trail.size());
        resp.put("totalModifications", trail.size());
        return resp;
    }

    /**
     * Inspect active retention policy, audit pins, active read watermarks, and purge totals.
     */
    @GetMapping({"/retention", "/retention/watermark"})
    public Map<String, Object> getRetention() {
        return buildRetentionPayload(null);
    }

    /**
     * Adjust retention policy, bounds, or audit pins at runtime and optionally trigger GC.
     */
    @PutMapping("/retention")
    public Map<String, Object> updateRetention(@RequestBody(required = false) Map<String, Object> request) {
        Map<String, Object> body = request != null ? request : Collections.emptyMap();

        if (body.get("policy") != null) {
            retentionConfig.setPolicy(RetentionPolicy.valueOf(String.valueOf(body.get("policy")).trim().toUpperCase()));
        }
        if (body.get("maxVersions") instanceof Number num) {
            retentionConfig.setMaxVersions(num.intValue());
        }
        if (body.get("windowMs") instanceof Number num) {
            retentionConfig.setWindowMs(num.longValue());
        }
        if (body.get("pinVersion") != null) {
            retentionConfig.pinVersion(String.valueOf(body.get("pinVersion")));
        }
        if (body.get("unpinVersion") != null) {
            retentionConfig.unpinVersion(String.valueOf(body.get("unpinVersion")));
        }

        ChronosGcService.GcRunResult gcResult = Boolean.TRUE.equals(body.get("triggerGc"))
                ? chronosGcService.runGc()
                : null;
        return buildRetentionPayload(gcResult);
    }

    /**
     * Manually trigger a Chronos Safe Low-Watermark GC cycle.
     */
    @PostMapping("/gc")
    public Map<String, Object> triggerGc() {
        return buildRetentionPayload(chronosGcService.runGc());
    }

    // ================================================================
    // Multi-Writer Branching & Version DAG Endpoints (Step 5)
    // ================================================================

    /**
     * Create an explicit branch originating from baseVersionId (or current head of main).
     */
    @PostMapping("/branches")
    public ResponseEntity<Map<String, Object>> createBranch(@RequestBody Map<String, Object> request) {
        if (!raftNode.isLeader()) {
            return ResponseEntity.ok(forwardPostToLeader("/api/chronos/branches", request));
        }

        String branchId = asString(request.get("branchId"));
        String baseVersionId = asString(request.get("baseVersionId"));
        String writerId = asString(request.get("writerId"));
        String description = asString(request.get("description"));

        if (branchManager == null) {
            throw new IllegalStateException("BranchManager unavailable");
        }
        BranchInfo branch = branchManager.createBranch(branchId, baseVersionId, writerId, description);
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("success", true);
        resp.put("branch", branch);
        return ResponseEntity.ok(resp);
    }

    /**
     * List all registered branches.
     */
    @GetMapping("/branches")
    public Map<String, Object> listBranches() {
        if (!raftNode.isLeader()) {
            Map<String, Object> forwarded = forwardGetToLeader("/api/chronos/branches");
            if (Boolean.TRUE.equals(forwarded.get("success")) || forwarded.containsKey("branches")) {
                return forwarded;
            }
        }
        List<BranchInfo> branches = branchManager != null ? branchManager.listBranches() : Collections.emptyList();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("branches", branches);
        resp.put("count", branches.size());
        return resp;
    }

    /**
     * Get branch metadata by branch ID.
     */
    @GetMapping("/branches/{branchId}")
    public ResponseEntity<BranchInfo> getBranch(@PathVariable String branchId) {
        if (branchManager == null) {
            return ResponseEntity.notFound().build();
        }
        Optional<BranchInfo> branch = branchManager.getBranch(branchId);
        if (branch.isEmpty() && !raftNode.isLeader()) {
            Map<String, Object> forwarded = forwardGetToLeader("/api/chronos/branches/" + branchId);
            if (forwarded != null && forwarded.containsKey("branchId")) {
                try {
                    BranchInfo fwdBranch = MAPPER.convertValue(forwarded, BranchInfo.class);
                    return ResponseEntity.ok(fwdBranch);
                } catch (Exception ignored) {}
            }
        }
        return branch
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    /**
     * Retrieve Version DAG nodes and directed edges.
     */
    @GetMapping("/dag")
    public VersionDagResponse getDag(
            @RequestParam(defaultValue = "100") int limit,
            @RequestParam(required = false) String branchId) {
        if (versionDagService == null) {
            throw new IllegalStateException("VersionDagService unavailable");
        }
        return versionDagService.getDag(limit, branchId);
    }

    /**
     * Compute Lowest Common Ancestor (LCA) of two versions in the DAG.
     */
    @GetMapping("/dag/lca")
    public ResponseEntity<Map<String, Object>> getLca(
            @RequestParam(required = false) String v1,
            @RequestParam(required = false) String v2,
            @RequestParam(required = false) String versionA,
            @RequestParam(required = false) String versionB) {
        String a = v1 != null ? v1 : versionA;
        String b = v2 != null ? v2 : versionB;
        if (a == null || b == null || versionDagService == null) {
            throw new IllegalArgumentException("v1 and v2 parameters are required");
        }

        Optional<ChronosVersion> lca = versionDagService.findLowestCommonAncestor(a, b);
        if (lca.isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("v1", a);
        resp.put("v2", b);
        resp.put("lcaVersionId", lca.get().getVersionId());
        resp.put("lcaSequence", lca.get().getSequence());
        resp.put("lcaTimestamp", lca.get().getTimestamp());
        resp.put("lcaBranchId", lca.get().getBranchId());
        resp.put("lca", lca.get());
        return ResponseEntity.ok(resp);
    }

    /**
     * Compute aggregated key transitions along the DAG path between ancestor and tip.
     */
    @GetMapping("/dag/diff-path")
    public AggregatedPathDiff getDiffPath(
            @RequestParam String ancestor,
            @RequestParam String tip) {
        if (versionDagService == null) {
            throw new IllegalStateException("VersionDagService unavailable");
        }
        return versionDagService.computeAggregatedChangeSet(ancestor, tip);
    }

    /**
     * Preview a branch merge without applying any mutations.
     */
    @PostMapping("/merge/preview")
    public ResponseEntity<Map<String, Object>> previewMerge(@RequestBody Map<String, Object> request) {
        if (!raftNode.isLeader()) {
            return ResponseEntity.ok(forwardPostToLeader("/api/chronos/merge/preview", request));
        }
        String sourceBranch = asString(request.get("sourceBranch"));
        String targetBranch = asNonBlankOrDefault(asString(request.get("targetBranch")), "main");
        String policyStr = asString(request.get("policy"));
        MergePolicy policy = parseMergePolicy(policyStr);
        @SuppressWarnings("unchecked")
        Map<String, String> customResolutions = (Map<String, String>) request.get("customResolutions");
        String writerId = asString(request.get("writerId"));
        String commitMessage = asString(request.get("commitMessage"));

        MergeRequest req = new MergeRequest(sourceBranch, targetBranch, policy, customResolutions, writerId, commitMessage);
        MergePreviewResponse preview = mergeEngine.preview(req);

        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("canMerge", preview.canMerge());
        resp.put("sourceBranch", preview.sourceBranch());
        resp.put("targetBranch", preview.targetBranch());
        resp.put("sourceTipId", preview.sourceTipId());
        resp.put("targetTipId", preview.targetTipId());
        resp.put("lcaVersionId", preview.lcaVersionId());
        resp.put("policy", preview.policy().name());
        resp.put("conflicts", preview.conflicts());
        resp.put("totalConflicts", preview.totalConflicts());
        resp.put("invariantViolations", preview.invariantViolations());
        resp.put("totalViolations", preview.totalViolations());
        resp.put("tentativeMergedState", preview.tentativeMergedState());
        resp.put("finalResolutions", preview.finalResolutions());
        resp.put("reason", preview.reason());
        return ResponseEntity.ok(resp);
    }

    /**
     * Execute and commit a branch merge through Raft consensus.
     */
    @PostMapping("/merge")
    public ResponseEntity<Map<String, Object>> executeMerge(@RequestBody Map<String, Object> request) {
        if (!raftNode.isLeader()) {
            return ResponseEntity.ok(forwardPostToLeader("/api/chronos/merge", request));
        }

        String sourceBranch = asString(request.get("sourceBranch"));
        String targetBranch = asNonBlankOrDefault(asString(request.get("targetBranch")), "main");
        String policyStr = asString(request.get("policy"));
        MergePolicy policy = parseMergePolicy(policyStr);
        @SuppressWarnings("unchecked")
        Map<String, String> customResolutions = (Map<String, String>) request.get("customResolutions");
        String writerId = asString(request.get("writerId"));
        String commitMessage = asString(request.get("commitMessage"));

        MergeRequest req = new MergeRequest(sourceBranch, targetBranch, policy, customResolutions, writerId, commitMessage);
        MergeResult result = mergeEngine.merge(req);

        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("success", result.success());
        resp.put("versionId", result.mergedVersionId());
        resp.put("sequence", result.sequence());
        resp.put("timestamp", result.timestamp());
        resp.put("targetBranch", result.targetBranch());
        resp.put("sourceBranch", result.sourceBranch());
        resp.put("parentVersionIds", result.parentVersionIds());
        resp.put("policy", result.policy().name());
        resp.put("resolvedConflictsCount", result.resolvedConflictsCount());
        resp.put("conflictRecords", result.conflictRecords());
        resp.put("message", result.message());
        return ResponseEntity.ok(resp);
    }

    /**
     * List all registered domain safety invariant rules.
     */
    @GetMapping("/invariants")
    public Map<String, Object> getInvariants() {
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("rules", invariantRegistry.getRules());
        resp.put("count", invariantRegistry.getRules().size());
        return resp;
    }

    /**
     * Register a new domain safety invariant rule.
     */
    @PostMapping("/invariants")
    public ResponseEntity<Map<String, Object>> addInvariant(@RequestBody Map<String, Object> request) {
        String name = asString(request.get("name"));
        String typeStr = asString(request.get("type"));
        String prefix = asString(request.get("prefix"));
        Long min = request.get("min") != null ? Long.valueOf(String.valueOf(request.get("min"))) : null;
        Long max = request.get("max") != null ? Long.valueOf(String.valueOf(request.get("max"))) : null;

        InvariantType type = typeStr != null ? InvariantType.valueOf(typeStr.trim().toUpperCase()) : InvariantType.NON_NEGATIVE_NUMERIC;
        InvariantRule rule = new InvariantRule(name, type, prefix, min, max);
        invariantRegistry.registerRule(rule);

        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("success", true);
        resp.put("rule", rule);
        return ResponseEntity.ok(resp);
    }

    /**
     * Unregister a domain safety invariant rule by name.
     */
    @DeleteMapping("/invariants/{name}")
    public ResponseEntity<Map<String, Object>> deleteInvariant(@PathVariable String name) {
        boolean removed = invariantRegistry.unregisterRule(name);
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("success", removed);
        resp.put("name", name);
        return ResponseEntity.ok(resp);
    }

    /**
     * Retrieve supported CRDT types and policies.
     */
    @GetMapping("/crdt/types")
    public Map<String, Object> getCrdtTypes() {
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("types", List.of("PN_COUNTER", "LWW_REGISTER", "OR_SET", "OR_MAP"));
        resp.put("policies", List.of("CRDT", "PN_COUNTER", "OR_SET", "LWW_REGISTER", "COMMUTATIVE_DELTA", "LAST_WRITE_WINS", "FAIL_ON_CONFLICT"));
        return resp;
    }

    /**
     * Standalone CRDT resolution endpoint for inspecting and testing conflict-free resolution.
     */
    @PostMapping("/crdt/resolve")
    public ResponseEntity<Map<String, Object>> resolveCrdt(@RequestBody Map<String, Object> req) {
        String key = asString(req.get("key"));
        String baseVal = asString(req.get("baseValue"));
        String leftVal = asString(req.get("leftValue"));
        String rightVal = asString(req.get("rightValue"));
        String leftBranch = asNonBlankOrDefault(asString(req.get("leftBranch")), "feature");
        String rightBranch = asNonBlankOrDefault(asString(req.get("rightBranch")), "main");
        String policyStr = asString(req.get("policy"));
        MergePolicy policy = parseMergePolicy(policyStr);

        CrdtResolutionResult result = crdtMergeResolver.resolve(
                key, baseVal, leftVal, rightVal,
                leftBranch, rightBranch,
                System.currentTimeMillis(), System.currentTimeMillis(),
                1, 2,
                "writer-left", "writer-right",
                policy
        );

        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("key", key);
        resp.put("resolvedValue", result.resolvedValue());
        resp.put("crdtType", result.crdtType().name());
        resp.put("strategy", result.strategy());
        resp.put("winningWriter", result.winningWriter());
        resp.put("explanation", result.explanation());
        return ResponseEntity.ok(resp);
    }

    private MergePolicy parseMergePolicy(String raw) {
        if (raw == null || raw.isBlank()) return MergePolicy.CRDT;
        try {
            return MergePolicy.valueOf(raw.trim().toUpperCase().replace("-", "_"));
        } catch (IllegalArgumentException e) {
            return MergePolicy.COMMUTATIVE_DELTA;
        }
    }

    /**
     * Retrieve Prometheus metrics and latency stats for Chronos.
     */
    @GetMapping("/metrics")
    public Map<String, Object> getMetrics() {
        return chronosMetrics.getMetricsSnapshot();
    }

    private Map<String, Object> buildRetentionPayload(ChronosGcService.GcRunResult lastGc) {
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("policy", retentionConfig.getPolicy().name());
        resp.put("maxVersions", retentionConfig.getMaxVersions());
        resp.put("windowMs", retentionConfig.getWindowMs());
        resp.put("pinnedVersions", retentionConfig.getPinnedVersions());
        resp.put("activeReadPins", watermarkRegistry.getActivePinCount());
        resp.put("activeLowWatermarkSeq", watermarkRegistry.getActiveLowWatermarkSeq());
        resp.put("activeLowWatermarkTimestamp", watermarkRegistry.getActiveLowWatermarkTimestamp());
        resp.put("watermark", versionCatalog.getWatermark());
        resp.put("totalVersionsPurged", chronosGcService.getTotalVersionsPurged());
        resp.put("totalMvccPurged", chronosGcService.getTotalMvccPurged());
        if (lastGc != null) {
            resp.put("lastGc", lastGc);
        }
        return resp;
    }

    @ExceptionHandler(HistoryUnavailableException.class)
    public ResponseEntity<Map<String, Object>> handleHistoryUnavailable(HistoryUnavailableException ex) {
        Map<String, Object> err = new LinkedHashMap<>();
        err.put("success", false);
        err.put("errorCode", HistoryUnavailableException.ERROR_CODE);
        err.put("error", ex.getMessage());
        err.put("requestedSelector", ex.getRequestedSelector());
        err.put("requestedValue", ex.getRequestedValue());
        err.put("earliestAvailableSeq", ex.getEarliestAvailableSeq());
        err.put("earliestAvailableTimestamp", ex.getEarliestAvailableTimestamp());
        return ResponseEntity.status(HttpStatus.GONE).body(err);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, Object>> handleBadRequest(IllegalArgumentException ex) {
        Map<String, Object> err = new LinkedHashMap<>();
        err.put("success", false);
        err.put("error", ex.getMessage());
        HttpStatus status = ex.getMessage() != null
                && (ex.getMessage().startsWith("Unknown version:") || ex.getMessage().startsWith("Unknown snapshot:"))
                ? HttpStatus.NOT_FOUND
                : HttpStatus.BAD_REQUEST;
        return ResponseEntity.status(status).body(err);
    }

    @ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<Map<String, Object>> handleIllegalState(IllegalStateException ex) {
        Map<String, Object> err = new LinkedHashMap<>();
        err.put("success", false);
        err.put("error", ex.getMessage());
        return ResponseEntity.status(HttpStatus.CONFLICT).body(err);
    }

    @SuppressWarnings("unchecked")
    private List<ChronosWritePayload.WriteOp> parseWriteOperations(Map<String, Object> request, String branchId) {
        Object rawOps = request.get("operations");
        if (rawOps == null) {
            rawOps = request.get("ops");
        }
        if (rawOps instanceof List<?> list && !list.isEmpty()) {
            List<ChronosWritePayload.WriteOp> parsed = new ArrayList<>(list.size());
            for (Object item : list) {
                if (item instanceof Map<?, ?> opMap) {
                    parsed.add(parseSingleOp((Map<String, Object>) opMap, branchId));
                }
            }
            return parsed;
        }
        String key = asString(request.get("key"));
        if (key == null || key.isBlank()) {
            return Collections.emptyList();
        }
        return List.of(parseSingleOp(request, branchId));
    }

    private ChronosWritePayload.WriteOp parseSingleOp(Map<String, Object> opMap, String branchId) {
        String key = asString(opMap.get("key"));
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("Operation key must be non-blank");
        }
        String rawType = asString(opMap.get("op"));
        if (rawType == null || rawType.isBlank()) {
            rawType = asString(opMap.get("type"));
        }
        if (rawType == null || rawType.isBlank()) {
            rawType = asString(opMap.get("opType"));
        }
        OperationType opType = switch (asNonBlankOrDefault(rawType, "PUT").toUpperCase()) {
            case "DELETE" -> OperationType.DELETE;
            case "DELTA" -> OperationType.DELTA;
            default -> OperationType.PUT;
        };
        Long validFrom = parseOptionalTimestamp(opMap.get("validFrom"));
        Long validTo = parseOptionalTimestamp(opMap.get("validTo"));

        if (opType == OperationType.DELETE) {
            return ChronosWritePayload.WriteOp.delete(key, validFrom, validTo);
        }
        if (opType == OperationType.DELTA) {
            Object rawDelta = opMap.get("deltaAmount");
            if (rawDelta == null) {
                rawDelta = opMap.get("delta");
            }
            if (!(rawDelta instanceof Number num)) {
                throw new IllegalArgumentException("DELTA operation requires numeric deltaAmount");
            }
            long delta = num.longValue();
            String explicitVal = asString(opMap.get("value"));
            byte[] resolvedBytes;
            if (explicitVal != null) {
                resolvedBytes = explicitVal.getBytes(StandardCharsets.UTF_8);
            } else {
                long currentNumeric = resolveCurrentNumericValue(key, branchId);
                resolvedBytes = String.valueOf(currentNumeric + delta).getBytes(StandardCharsets.UTF_8);
            }
            return ChronosWritePayload.WriteOp.delta(key, resolvedBytes, delta, validFrom, validTo);
        }
        String val = asString(opMap.get("value"));
        byte[] bytes = val != null ? val.getBytes(StandardCharsets.UTF_8) : new byte[0];
        return ChronosWritePayload.WriteOp.put(key, bytes, validFrom, validTo);
    }

    private Long parseOptionalTimestamp(Object obj) {
        if (obj == null) return null;
        if (obj instanceof Number num) return num.longValue();
        String str = obj.toString().trim();
        if (str.isBlank()) return null;
        try {
            return TemporalQueryService.parseTimestamp(str);
        } catch (Exception ignored) {
            return null;
        }
    }

    private long resolveCurrentNumericValue(String key, String branchId) {
        TemporalQueryResult current = temporalQueryService.queryKey(
                TemporalQueryRequest.current(key, branchId, ReadMode.LINEARIZABLE, false));
        if (!current.found() && !"main".equals(branchId) && branchManager != null) {
            Optional<BranchInfo> bInfo = branchManager.getBranch(branchId);
            if (bInfo.isPresent() && bInfo.get().getBaseVersionId() != null) {
                current = temporalQueryService.queryKey(
                        TemporalQueryRequest.asOfVersion(key, bInfo.get().getBaseVersionId(), false));
            }
        }
        if (!current.found() || current.value() == null || current.value().isBlank()) {
            return 0L;
        }
        try {
            return Long.parseLong(current.value().trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Cannot apply DELTA to non-numeric value for key '" + key + "': " + current.value());
        }
    }

    private Map<String, Object> handleSpeculativeSingleOp(ChronosWritePayload.WriteOp op, String branchId) {
        SpeculativeManager.SpeculativeResult specResult = op.getOpType() == OperationType.DELETE
                ? speculativeManager.submitSpeculativeDelete(raftNode, op.getKey())
                : speculativeManager.submitSpeculativePut(raftNode, op.getKey(), op.getValue(),
                SpeculativeManager.AckPolicy.SPECULATIVE);

        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("success", specResult.success());
        resp.put("speculative", specResult.speculative());
        resp.put("ackPolicy", "SPECULATIVE");
        resp.put("writeLatencyMs", specResult.writeLatencyMs());
        if (specResult.success()) {
            resp.put("versionId", specResult.versionId());
            resp.put("timestamp", specResult.timestamp());
            resp.put("raftLogIndex", specResult.raftLogIndex());
            resp.put("branchId", branchId);
        } else {
            resp.put("error", specResult.error());
        }
        return resp;
    }

    private Map<String, Object> handleMultiOpTransaction(List<ChronosWritePayload.WriteOp> ops,
                                                         String branchId,
                                                         String writerId,
                                                         long startMs) {
        List<TxnCoordinator.TxnOperation> txnOps = new ArrayList<>(ops.size());
        for (ChronosWritePayload.WriteOp op : ops) {
            TxnCoordinator.TxnOperation.OpType type = switch (op.getOpType()) {
                case DELETE -> TxnCoordinator.TxnOperation.OpType.DELETE;
                case DELTA -> TxnCoordinator.TxnOperation.OpType.DELTA;
                case PUT -> TxnCoordinator.TxnOperation.OpType.PUT;
            };
            txnOps.add(new TxnCoordinator.TxnOperation(op.getKey(), op.getValue(), type, op.getDeltaAmount()));
        }

        Map<String, Object> resp = new LinkedHashMap<>();
        try {
            TxnCoordinator.TxnCommitResult commit = txnCoordinator
                    .executeWithResult(txnOps, branchId, writerId)
                    .get(10, TimeUnit.SECONDS);
            resp.put("success", commit.committed());
            resp.put("speculative", false);
            resp.put("txnId", commit.txnId());
            resp.put("operationCount", ops.size());
            resp.put("latencyMs", System.currentTimeMillis() - startMs);
            if (commit.committed()) {
                resp.put("versionId", commit.versionId());
                resp.put("versionSequence", commit.versionSequence());
                resp.put("timestamp", commit.timestamp());
                resp.put("branchId", commit.branchId());
                if (writerId != null) {
                    resp.put("writerId", writerId);
                }
            } else if (commit.error() != null) {
                resp.put("error", commit.error());
            }
        } catch (Exception e) {
            resp.put("success", false);
            resp.put("error", e.getMessage());
            resp.put("latencyMs", System.currentTimeMillis() - startMs);
        }
        return resp;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> forwardPostToLeader(String path, Map<String, Object> requestBody) {
        String leaderUrl = resolveLeaderHttpUrl();
        if (leaderUrl == null) {
            Map<String, Object> err = new LinkedHashMap<>();
            err.put("success", false);
            err.put("error", "No leader available");
            return err;
        }
        try {
            String json = MAPPER.writeValueAsString(requestBody);
            HttpRequest httpRequest = HttpRequest.newBuilder()
                    .uri(URI.create(leaderUrl + path))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(json))
                    .timeout(Duration.ofSeconds(30))
                    .build();
            HttpResponse<String> response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());
            return MAPPER.readValue(response.body(), LinkedHashMap.class);
        } catch (Exception e) {
            log.warn("Failed to forward POST {} to leader at {}: {}", path, leaderUrl, e.getMessage());
            Map<String, Object> err = new LinkedHashMap<>();
            err.put("success", false);
            err.put("error", "Failed to forward to leader: " + e.getMessage());
            return err;
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> forwardGetToLeader(String path) {
        String leaderUrl = resolveLeaderHttpUrl();
        if (leaderUrl == null) {
            Map<String, Object> err = new LinkedHashMap<>();
            err.put("success", false);
            err.put("error", "No leader available");
            return err;
        }
        try {
            HttpRequest httpRequest = HttpRequest.newBuilder()
                    .uri(URI.create(leaderUrl + path))
                    .GET()
                    .timeout(Duration.ofSeconds(30))
                    .build();
            HttpResponse<String> response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());
            return MAPPER.readValue(response.body(), LinkedHashMap.class);
        } catch (Exception e) {
            log.warn("Failed to forward GET {} to leader at {}: {}", path, leaderUrl, e.getMessage());
            Map<String, Object> err = new LinkedHashMap<>();
            err.put("success", false);
            err.put("error", "Failed to forward to leader: " + e.getMessage());
            return err;
        }
    }

    private String resolveLeaderHttpUrl() {
        String leaderId = raftNode.getLeaderId();
        if (leaderId == null || raftConfig == null) {
            return null;
        }
        for (String peer : raftConfig.getPeers()) {
            String peerNodeId = peer.contains("@") ? peer.split("@")[0] : peer;
            if (peerNodeId.equals(leaderId)) {
                String address = peer.contains("@") ? peer.split("@")[1] : peer;
                String[] parts = address.split(":");
                String host = parts[0];
                int grpcPort = parts.length > 1 ? Integer.parseInt(parts[1]) : 9090;
                int httpPort = grpcPort - 1010;
                return "http://" + host + ":" + httpPort;
            }
        }
        return null;
    }

    private QueryMode parseQueryMode(String mode) {
        if (mode == null || mode.isBlank()) {
            return null;
        }
        return QueryMode.valueOf(mode.trim().toUpperCase());
    }

    private ReadMode parseReadMode(String readMode) {
        if ("SPECULATIVE".equalsIgnoreCase(readMode)) {
            return ReadMode.SPECULATIVE;
        }
        return ReadMode.LINEARIZABLE;
    }

    private String asString(Object obj) {
        return obj != null ? String.valueOf(obj) : null;
    }

    private String asNonBlankOrDefault(String value, String fallback) {
        return (value != null && !value.isBlank()) ? value : fallback;
    }
}
