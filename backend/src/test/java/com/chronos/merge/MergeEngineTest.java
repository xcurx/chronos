package com.chronos.merge;

import com.chronos.api.ChronosController;
import com.chronos.audit.AuditService;
import com.chronos.branch.BranchInfo;
import com.chronos.branch.BranchManager;
import com.chronos.branch.BranchStatus;
import com.chronos.dag.DagEdge;
import com.chronos.dag.VersionDagResponse;
import com.chronos.dag.VersionDagService;
import com.chronos.gc.ChronosGcService;
import com.chronos.gc.ChronosRetentionConfig;
import com.chronos.gc.ReadWatermarkRegistry;
import com.chronos.gc.RetentionPolicy;
import com.chronos.model.ChronosVersion;
import com.chronos.model.ChronosWritePayload;
import com.chronos.model.ConflictRecord;
import com.chronos.model.ConflictType;
import com.chronos.model.OperationType;
import com.chronos.query.DatabaseStateResult;
import com.chronos.query.TemporalQueryRequest;
import com.chronos.query.TemporalQueryResult;
import com.chronos.query.TemporalQueryService;
import com.chronos.storage.VersionCatalog;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.minidb.config.RaftConfig;
import com.minidb.events.EventBus;
import com.minidb.mvcc.MvccStore;
import com.minidb.mvcc.ReadMode;
import com.minidb.raft.RaftLog;
import com.minidb.raft.RaftNode;
import com.minidb.speculation.SpeculativeManager;
import com.minidb.storage.RocksDbEngine;
import com.minidb.txn.TxnCoordinator;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Step 6 tests: Three-way conflict detection, commutative delta resolution, domain safety
 * invariant validation, Last-Write-Wins, custom resolution overrides, FAIL_ON_CONFLICT policies,
 * two-parent DAG diamond merges, and REST merge preview/execution endpoints.
 */
class MergeEngineTest {

    @TempDir
    Path tempDir;

    private RocksDbEngine storage;
    private VersionCatalog catalog;
    private RaftNode raftNode;
    private MvccStore mvccStore;
    private BranchManager branchManager;
    private VersionDagService dagService;
    private TemporalQueryService temporalQueryService;
    private AuditService auditService;
    private ChronosGcService gcService;
    private ChronosRetentionConfig retentionConfig;
    private ReadWatermarkRegistry watermarkRegistry;
    private ConflictDetector conflictDetector;
    private InvariantRegistry invariantRegistry;
    private InvariantValidator invariantValidator;
    private MergeEngine mergeEngine;
    private MockMvc mockMvc;
    private final ObjectMapper mapper = new ObjectMapper();

    @BeforeEach
    void setUp() throws Exception {
        storage = new RocksDbEngine(tempDir.resolve("db").toString());
        catalog = new VersionCatalog(storage);
        branchManager = new BranchManager(storage, catalog);

        RaftConfig config = new RaftConfig() {
            @Override public String getNodeId() { return "node-1"; }
            @Override public List<String> getPeers() { return List.of(); }
            @Override public int getElectionTimeoutMin() { return 80; }
            @Override public int getElectionTimeoutMax() { return 120; }
            @Override public int getHeartbeatInterval() { return 30; }
        };

        EventBus eventBus = new EventBus();
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        raftNode = new RaftNode(config, storage, eventBus, meterRegistry, catalog);
        raftNode.setBranchManager(branchManager);
        mvccStore = raftNode.getMvccStore();
        raftNode.setBatchingEnabled(false);
        raftNode.start();

        long deadline = System.currentTimeMillis() + 3000;
        while (!raftNode.isLeader() && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        assertTrue(raftNode.isLeader(), "Node must elect itself leader");

        watermarkRegistry = new ReadWatermarkRegistry();
        temporalQueryService = new TemporalQueryService(raftNode, catalog, mvccStore, watermarkRegistry);
        dagService = new VersionDagService(catalog, temporalQueryService);
        auditService = new AuditService(catalog, temporalQueryService, watermarkRegistry);
        retentionConfig = new ChronosRetentionConfig(RetentionPolicy.LAST_N_VERSIONS, 100, 3_600_000L, List.of());
        gcService = new ChronosGcService(catalog, mvccStore, raftNode.getSnapshotManager(),
                retentionConfig, watermarkRegistry);

        conflictDetector = new ConflictDetector(catalog, dagService, temporalQueryService);
        invariantRegistry = new InvariantRegistry();
        invariantValidator = new InvariantValidator(invariantRegistry);
        mergeEngine = new MergeEngine(catalog, branchManager, dagService, temporalQueryService,
                conflictDetector, invariantValidator, raftNode);

        ChronosController controller = new ChronosController(
                raftNode, config, catalog, temporalQueryService, auditService,
                new SpeculativeManager(eventBus, meterRegistry),
                new TxnCoordinator(raftNode, eventBus),
                eventBus, retentionConfig, watermarkRegistry, gcService,
                branchManager, dagService, mergeEngine, invariantRegistry);
        mockMvc = MockMvcBuilders.standaloneSetup(controller).build();
    }

    @AfterEach
    void tearDown() {
        if (raftNode != null) {
            raftNode.stop();
        }
        if (storage != null) {
            storage.close();
        }
    }

    private String commitPut(String branchId, String key, String value, String writer) throws Exception {
        ChronosWritePayload payload = ChronosWritePayload.builder()
                .branchId(branchId)
                .writerId(writer)
                .operations(List.of(ChronosWritePayload.WriteOp.put(key, value.getBytes(StandardCharsets.UTF_8))))
                .build();
        RaftNode.ChronosWriteResult res = raftNode.submitChronosWrite(
                "main".equals(branchId) ? RaftLog.CommandType.CHRONOS_WRITE : RaftLog.CommandType.CHRONOS_BRANCH_WRITE,
                payload).get(5, TimeUnit.SECONDS);
        assertTrue(res.committed());
        return res.versionId();
    }

    private String commitDelta(String branchId, String key, long delta, String writer) throws Exception {
        ChronosVersion head = catalog.getVersion(branchManager.getBranch(branchId).orElseThrow().getHeadVersionId()).orElseThrow();
        Map<String, String> current = temporalQueryService.reconstructKeyValuesAtVersion(head);
        long curVal = 0L;
        if (current.containsKey(key)) {
            curVal = Long.parseLong(current.get(key));
        }
        long resolved = curVal + delta;

        ChronosWritePayload payload = ChronosWritePayload.builder()
                .branchId(branchId)
                .writerId(writer)
                .operations(List.of(ChronosWritePayload.WriteOp.delta(key, String.valueOf(resolved).getBytes(StandardCharsets.UTF_8), delta)))
                .build();
        RaftNode.ChronosWriteResult res = raftNode.submitChronosWrite(
                "main".equals(branchId) ? RaftLog.CommandType.CHRONOS_WRITE : RaftLog.CommandType.CHRONOS_BRANCH_WRITE,
                payload).get(5, TimeUnit.SECONDS);
        assertTrue(res.committed());
        return res.versionId();
    }

    @Test
    void testCommutativeDeltaMergeReconcilesSumsDeterministically() throws Exception {
        // Base commit on main: counter = 100
        String vBase = commitPut("main", "counter", "100", "init-writer");

        // Fork branch-a and branch-b from base
        BranchInfo branchA = branchManager.createBranch("branch-a", vBase, "writer-a", "Branch A delta +20");
        BranchInfo branchB = branchManager.createBranch("branch-b", vBase, "writer-b", "Branch B delta -30");

        // Branch A adds 20 (now 120 on A)
        commitDelta("branch-a", "counter", 20L, "writer-a");
        // Branch B subtracts 30 (now 70 on B)
        commitDelta("branch-b", "counter", -30L, "writer-b");

        // Preview merge of branch-b into branch-a
        MergePreviewResponse preview = mergeEngine.preview(MergeRequest.builder()
                .sourceBranch("branch-b")
                .targetBranch("branch-a")
                .policy(MergePolicy.COMMUTATIVE_DELTA)
                .build());

        assertTrue(preview.canMerge(), "Preview must approve commutative delta merge");
        assertEquals(1, preview.totalConflicts());
        assertEquals(ConflictType.COMPATIBLE_DELTA, preview.conflicts().get(0).conflictType());
        assertTrue(preview.conflicts().get(0).resolvable());
        assertEquals("90", preview.tentativeMergedState().get("counter"), "100 + 20 - 30 = 90");

        // Commit the merge
        MergeResult result = mergeEngine.merge(MergeRequest.builder()
                .sourceBranch("branch-b")
                .targetBranch("branch-a")
                .policy(MergePolicy.COMMUTATIVE_DELTA)
                .writerId("merge-robot")
                .build());

        assertTrue(result.success());
        assertEquals(2, result.parentVersionIds().size());
        assertEquals(branchManager.getBranch("branch-b").orElseThrow().getHeadVersionId(), result.parentVersionIds().get(1));
        assertEquals(1, result.resolvedConflictsCount());

        // Source branch must now be MERGED
        assertEquals(BranchStatus.MERGED, branchManager.getBranch("branch-b").orElseThrow().getStatus());

        // Query branch-a tip: reconstructed value must be exactly 90
        ChronosVersion mergedVersion = catalog.getVersion(result.mergedVersionId()).orElseThrow();
        assertTrue(mergedVersion.isMerge());
        assertEquals("COMMUTATIVE_DELTA", mergedVersion.getMergePolicy());
        assertFalse(mergedVersion.getConflictMetadata().isEmpty());

        Map<String, String> stateOnA = temporalQueryService.reconstructKeyValuesAtVersion(mergedVersion);
        assertEquals("90", stateOnA.get("counter"));
    }

    @Test
    void testDomainInvariantRejectsNegativeBalanceMerge() throws Exception {
        // Base commit on main: balance:acc1 = 50
        String vBase = commitPut("main", "balance:acc1", "50", "bank-admin");

        // Branch A withdraws 40 (remaining 10 on A)
        branchManager.createBranch("atm-1", vBase, "atm-user-1", "ATM 1");
        commitDelta("atm-1", "balance:acc1", -40L, "atm-user-1");

        // Branch B withdraws 30 (remaining 20 on B)
        branchManager.createBranch("atm-2", vBase, "atm-user-2", "ATM 2");
        commitDelta("atm-2", "balance:acc1", -30L, "atm-user-2");

        // Merging atm-2 into atm-1 evaluates to: 50 - 40 - 30 = -20 < 0 (violates NonNegativeBalance)
        MergePreviewResponse preview = mergeEngine.preview(MergeRequest.builder()
                .sourceBranch("atm-2")
                .targetBranch("atm-1")
                .policy(MergePolicy.COMMUTATIVE_DELTA)
                .build());

        assertFalse(preview.canMerge(), "Preview must reject invariant violation");
        assertEquals(1, preview.totalViolations());
        assertEquals("NonNegativeBalance", preview.invariantViolations().get(0).ruleName());
        assertEquals("-20", preview.invariantViolations().get(0).actualValue());
        assertTrue(preview.reason().contains("Domain invariant violations"));

        // Attempting to commit the merge must throw IllegalStateException
        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                mergeEngine.merge(MergeRequest.builder()
                        .sourceBranch("atm-2")
                        .targetBranch("atm-1")
                        .policy(MergePolicy.COMMUTATIVE_DELTA)
                        .build())
        );
        assertTrue(ex.getMessage().contains("Domain invariant violations"));

        // Source branch atm-2 remains ACTIVE because merge failed
        assertEquals(BranchStatus.ACTIVE, branchManager.getBranch("atm-2").orElseThrow().getStatus());
    }

    @Test
    void testLastWriteWinsAndFailOnConflictPolicies() throws Exception {
        // Base commit: theme = "default"
        String vBase = commitPut("main", "theme", "default", "designer");

        // Branch dark-mode sets theme = "dark"
        branchManager.createBranch("feat-dark", vBase, "dev-1", "Dark theme");
        commitPut("feat-dark", "theme", "dark", "dev-1");

        Thread.sleep(10); // ensure timestamp separation

        // Branch light-mode sets theme = "solarized-light"
        branchManager.createBranch("feat-light", vBase, "dev-2", "Light theme");
        commitPut("feat-light", "theme", "solarized-light", "dev-2");

        // FAIL_ON_CONFLICT must reject WRITE_WRITE conflict
        MergePreviewResponse failPreview = mergeEngine.preview(MergeRequest.builder()
                .sourceBranch("feat-light")
                .targetBranch("feat-dark")
                .policy(MergePolicy.FAIL_ON_CONFLICT)
                .build());

        assertFalse(failPreview.canMerge());
        assertTrue(failPreview.reason().contains("FAIL_ON_CONFLICT"));

        // LAST_WRITE_WINS must resolve in favor of the later commit (feat-light)
        MergePreviewResponse lwwPreview = mergeEngine.preview(MergeRequest.builder()
                .sourceBranch("feat-light")
                .targetBranch("feat-dark")
                .policy(MergePolicy.LAST_WRITE_WINS)
                .build());

        assertTrue(lwwPreview.canMerge());
        assertEquals("solarized-light", lwwPreview.tentativeMergedState().get("theme"));

        // Execute merge with LAST_WRITE_WINS
        MergeResult result = mergeEngine.merge(MergeRequest.builder()
                .sourceBranch("feat-light")
                .targetBranch("feat-dark")
                .policy(MergePolicy.LAST_WRITE_WINS)
                .build());

        assertTrue(result.success());
        ChronosVersion mergedVersion = catalog.getVersion(result.mergedVersionId()).orElseThrow();
        Map<String, String> state = temporalQueryService.reconstructKeyValuesAtVersion(mergedVersion);
        assertEquals("solarized-light", state.get("theme"));
    }

    @Test
    void testCustomResolutionOverridesConflictingKeys() throws Exception {
        String vBase = commitPut("main", "email", "orig@domain.com", "admin");

        branchManager.createBranch("b1", vBase, "w1", "Branch 1");
        commitPut("b1", "email", "alice@domain.com", "w1");

        branchManager.createBranch("b2", vBase, "w2", "Branch 2");
        commitPut("b2", "email", "bob@domain.com", "w2");

        // Supply explicit custom resolution
        MergeRequest req = MergeRequest.builder()
                .sourceBranch("b2")
                .targetBranch("b1")
                .policy(MergePolicy.CUSTOM_RESOLUTIONS)
                .customResolutions(Map.of("email", "contact@domain.com"))
                .writerId("manual-resolver")
                .build();

        MergePreviewResponse preview = mergeEngine.preview(req);
        assertTrue(preview.canMerge());
        assertEquals("contact@domain.com", preview.tentativeMergedState().get("email"));

        MergeResult result = mergeEngine.merge(req);
        assertTrue(result.success());

        ChronosVersion v = catalog.getVersion(result.mergedVersionId()).orElseThrow();
        ConflictRecord cr = v.getConflictMetadata().get(0);
        assertEquals("email", cr.getKey());
        assertEquals("CUSTOM", cr.getResolutionStrategy());
        assertEquals("contact@domain.com", new String(cr.getResolvedValue(), StandardCharsets.UTF_8));
    }

    @Test
    void testDiamondMergeCreatesTwoParentVersionDagNode() throws Exception {
        // V1
        String v1 = commitPut("main", "k1", "v1", "root");

        // V2 on left, V3 on right
        branchManager.createBranch("left", v1, "dev-l", "Left");
        String v2 = commitPut("left", "k_left", "val_l", "dev-l");

        branchManager.createBranch("right", v1, "dev-r", "Right");
        String v3 = commitPut("right", "k_right", "val_r", "dev-r");

        // Merge right into left -> creates diamond merge commit M1
        MergeResult mergeRes = mergeEngine.merge(MergeRequest.builder()
                .sourceBranch("right")
                .targetBranch("left")
                .policy(MergePolicy.COMMUTATIVE_DELTA)
                .build());

        String m1 = mergeRes.mergedVersionId();

        // Verify DAG structure
        VersionDagResponse dag = dagService.getDag(100, null);
        assertNotNull(dag);

        // M1 must have two directed edges: M1 -> V2 and M1 -> V3
        long parentEdgesFromM1 = dag.edges().stream()
                .filter(e -> e.childVersionId().equals(m1))
                .count();
        assertEquals(2, parentEdgesFromM1, "Merge commit must have exactly 2 parent edges in DAG");

        // LCA of V2 and V3 is V1
        Optional<ChronosVersion> lca = dagService.findLowestCommonAncestor(v2, v3);
        assertTrue(lca.isPresent());
        assertEquals(v1, lca.get().getVersionId());

        // Merged state has both k_left and k_right
        ChronosVersion mergedV = catalog.getVersion(m1).orElseThrow();
        Map<String, String> state = temporalQueryService.reconstructKeyValuesAtVersion(mergedV);
        assertEquals("v1", state.get("k1"));
        assertEquals("val_l", state.get("k_left"));
        assertEquals("val_r", state.get("k_right"));
    }

    @Test
    void testRestMergePreviewAndExecutionEndpoints() throws Exception {
        String vBase = commitPut("main", "inventory:apples", "100", "inv-mgr");

        branchManager.createBranch("store-a", vBase, "mgr-a", "Store A");
        commitDelta("store-a", "inventory:apples", -20L, "mgr-a");

        branchManager.createBranch("store-b", vBase, "mgr-b", "Store B");
        commitDelta("store-b", "inventory:apples", -15L, "mgr-b");

        // REST Preview
        Map<String, Object> previewReq = Map.of(
                "sourceBranch", "store-b",
                "targetBranch", "store-a",
                "policy", "COMMUTATIVE_DELTA"
        );

        mockMvc.perform(post("/api/chronos/merge/preview")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(previewReq)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.canMerge").value(true))
                .andExpect(jsonPath("$.tentativeMergedState['inventory:apples']").value("65"));

        // REST Invariants List
        mockMvc.perform(get("/api/chronos/invariants"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.count").isNumber());

        // REST Register new Invariant
        Map<String, Object> ruleReq = Map.of(
                "name", "MaxAppleStock",
                "type", "CUSTOM_BOUNDS",
                "prefix", "inventory:apples",
                "min", 0,
                "max", 500
        );
        mockMvc.perform(post("/api/chronos/invariants")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(ruleReq)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true));

        // REST Execute Merge
        Map<String, Object> mergeReq = Map.of(
                "sourceBranch", "store-b",
                "targetBranch", "store-a",
                "policy", "COMMUTATIVE_DELTA",
                "writerId", "rest-tester"
        );

        mockMvc.perform(post("/api/chronos/merge")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(mergeReq)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.versionId").isString())
                .andExpect(jsonPath("$.parentVersionIds.length()").value(2));

        // REST Delete Invariant
        mockMvc.perform(delete("/api/chronos/invariants/MaxAppleStock"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true));
    }
}

