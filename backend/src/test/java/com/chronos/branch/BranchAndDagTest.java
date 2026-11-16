package com.chronos.branch;

import com.chronos.api.ChronosController;
import com.chronos.audit.AuditService;
import com.chronos.dag.AggregatedKeyDiff;
import com.chronos.dag.AggregatedPathDiff;
import com.chronos.dag.VersionDagResponse;
import com.chronos.dag.VersionDagService;
import com.chronos.gc.ChronosGcService;
import com.chronos.gc.ChronosRetentionConfig;
import com.chronos.gc.ReadWatermarkRegistry;
import com.chronos.gc.RetentionPolicy;
import com.chronos.model.ChangeEntry;
import com.chronos.model.ChronosVersion;
import com.chronos.model.ChronosWritePayload;
import com.chronos.model.OperationType;
import com.chronos.model.VersionStatus;
import com.chronos.query.DatabaseStateResult;
import com.chronos.query.HistoryUnavailableException;
import com.chronos.query.TemporalQueryRequest;
import com.chronos.query.TemporalQueryResult;
import com.chronos.query.TemporalQueryService;
import com.chronos.storage.VersionCatalog;
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
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Step 5 tests: Multi-writer branch isolation, Version DAG building, exact LCA resolution
 * (forks, diamonds, self, ancestors), aggregated path change-sets, optimistic stale-base
 * auto-branching, REST branch/dag APIs, and GC protection of branch roots.
 */
class BranchAndDagTest {

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
    private MockMvc mockMvc;

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
        assertTrue(raftNode.isLeader());

        watermarkRegistry = new ReadWatermarkRegistry();
        temporalQueryService = new TemporalQueryService(raftNode, catalog, mvccStore, watermarkRegistry);
        dagService = new VersionDagService(catalog, temporalQueryService);
        auditService = new AuditService(catalog, temporalQueryService, watermarkRegistry);
        retentionConfig = new ChronosRetentionConfig(RetentionPolicy.LAST_N_VERSIONS, 100, 3_600_000L, List.of());
        gcService = new ChronosGcService(catalog, mvccStore, raftNode.getSnapshotManager(),
                retentionConfig, watermarkRegistry);

        ChronosController controller = new ChronosController(
                raftNode, config, catalog, temporalQueryService, auditService,
                new SpeculativeManager(eventBus, meterRegistry),
                new TxnCoordinator(raftNode, eventBus),
                eventBus, retentionConfig, watermarkRegistry, gcService,
                branchManager, dagService);
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

    @Test
    void testConcurrentBranchIsolationAndUnpollutedMain() throws Exception {
        // Step 1: Base write on main
        RaftNode.ChronosWriteResult v1 = writeOp("main", "alice",
                ChronosWritePayload.WriteOp.put("shared", utf8("v0")),
                ChronosWritePayload.WriteOp.put("baseOnly", utf8("base-data")));

        // Step 2: Create branches exp-a and exp-b off v1
        BranchInfo branchA = branchManager.createBranch("exp-a", v1.versionId(), "alice", "Experiment A");
        BranchInfo branchB = branchManager.createBranch("exp-b", v1.versionId(), "bob", "Experiment B");
        assertEquals("exp-a", branchA.getBranchId());
        assertEquals(v1.versionId(), branchA.getBaseVersionId());

        // Step 3: Writer A commits on exp-a
        RaftNode.ChronosWriteResult v2A = writeOp("exp-a", "writer-a",
                ChronosWritePayload.WriteOp.put("shared", utf8("from-a")),
                ChronosWritePayload.WriteOp.put("onlyA", utf8("A1")));

        // Step 4: Writer B commits on exp-b
        RaftNode.ChronosWriteResult v2B = writeOp("exp-b", "writer-b",
                ChronosWritePayload.WriteOp.put("shared", utf8("from-b")),
                ChronosWritePayload.WriteOp.put("onlyB", utf8("B1")));

        // Step 5: Verify main is 100% unpolluted by exp-a or exp-b!
        TemporalQueryResult mainShared = temporalQueryService.queryKey(
                TemporalQueryRequest.current("shared", "main", ReadMode.LINEARIZABLE, false));
        assertTrue(mainShared.found());
        assertEquals("v0", mainShared.value());

        TemporalQueryResult mainOnlyA = temporalQueryService.queryKey(
                TemporalQueryRequest.current("onlyA", "main", ReadMode.LINEARIZABLE, false));
        assertFalse(mainOnlyA.found(), "onlyA must not leak into main");

        TemporalQueryResult mainOnlyB = temporalQueryService.queryKey(
                TemporalQueryRequest.current("onlyB", "main", ReadMode.LINEARIZABLE, false));
        assertFalse(mainOnlyB.found(), "onlyB must not leak into main");

        // Step 6: Verify branch exp-a sees only its own mutations + base
        DatabaseStateResult stateA = temporalQueryService.reconstructState(
                TemporalQueryRequest.current(null, "exp-a", ReadMode.LINEARIZABLE, false));
        assertEquals(Map.of("baseOnly", "base-data", "onlyA", "A1", "shared", "from-a"), stateA.entries());

        // Step 7: Verify branch exp-b sees only its own mutations + base
        DatabaseStateResult stateB = temporalQueryService.reconstructState(
                TemporalQueryRequest.current(null, "exp-b", ReadMode.LINEARIZABLE, false));
        assertEquals(Map.of("baseOnly", "base-data", "onlyB", "B1", "shared", "from-b"), stateB.entries());

        // Step 8: Verify AS OF VERSION reads directly map to exact branch tips
        assertEquals("from-a", temporalQueryService.queryKey(
                TemporalQueryRequest.asOfVersion("shared", v2A.versionId(), false)).value());
        assertEquals("from-b", temporalQueryService.queryKey(
                TemporalQueryRequest.asOfVersion("shared", v2B.versionId(), false)).value());
    }

    @Test
    void testLowestCommonAncestorAndPathDiff() throws Exception {
        RaftNode.ChronosWriteResult v1 = writeOp("main", "init",
                ChronosWritePayload.WriteOp.put("counter", utf8("10")));

        branchManager.createBranch("feature-1", v1.versionId(), "dev1", "Feature 1");
        branchManager.createBranch("feature-2", v1.versionId(), "dev2", "Feature 2");

        RaftNode.ChronosWriteResult v2A = writeOp("feature-1", "dev1",
                ChronosWritePayload.WriteOp.delta("counter", utf8("15"), 5L),
                ChronosWritePayload.WriteOp.put("f1_only", utf8("on")));
        RaftNode.ChronosWriteResult v3A = writeOp("feature-1", "dev1",
                ChronosWritePayload.WriteOp.delta("counter", utf8("25"), 10L));

        RaftNode.ChronosWriteResult v2B = writeOp("feature-2", "dev2",
                ChronosWritePayload.WriteOp.delta("counter", utf8("12"), 2L),
                ChronosWritePayload.WriteOp.put("f2_only", utf8("yes")));

        // 1. LCA between forks resolves to V1
        Optional<ChronosVersion> lcaFork = dagService.findLowestCommonAncestor(v3A.versionId(), v2B.versionId());
        assertTrue(lcaFork.isPresent());
        assertEquals(v1.versionId(), lcaFork.get().getVersionId());

        // 2. LCA on same branch resolves to ancestor
        Optional<ChronosVersion> lcaLinear = dagService.findLowestCommonAncestor(v2A.versionId(), v3A.versionId());
        assertTrue(lcaLinear.isPresent());
        assertEquals(v2A.versionId(), lcaLinear.get().getVersionId());

        // 3. LCA of self resolves to self
        Optional<ChronosVersion> lcaSelf = dagService.findLowestCommonAncestor(v3A.versionId(), v3A.versionId());
        assertTrue(lcaSelf.isPresent());
        assertEquals(v3A.versionId(), lcaSelf.get().getVersionId());

        // 4. Diamond Merge Simulation: Merge v3A and v2B into v4
        ChronosVersion v4Merge = ChronosVersion.builder()
                .versionId("V4-MERGE")
                .sequence(catalog.nextSequence())
                .parentVersionIds(List.of(v3A.versionId(), v2B.versionId()))
                .branchId("main")
                .timestamp(System.currentTimeMillis())
                .status(VersionStatus.COMMITTED)
                .isMerge(true)
                .mergePolicy("COMMUTATIVE_DELTA")
                .changeSet(List.of(ChangeEntry.delta("counter", utf8("10"), utf8("27"), 17L, "merge-engine")))
                .build();
        catalog.saveCommittedFromRaft(v4Merge);

        // LCA of v4Merge and v3A -> v3A
        Optional<ChronosVersion> lcaDiamondA = dagService.findLowestCommonAncestor(v4Merge.getVersionId(), v3A.versionId());
        assertTrue(lcaDiamondA.isPresent());
        assertEquals(v3A.versionId(), lcaDiamondA.get().getVersionId());

        // LCA of v4Merge and v2B -> v2B
        Optional<ChronosVersion> lcaDiamondB = dagService.findLowestCommonAncestor(v4Merge.getVersionId(), v2B.versionId());
        assertTrue(lcaDiamondB.isPresent());
        assertEquals(v2B.versionId(), lcaDiamondB.get().getVersionId());

        // 5. Test Aggregated Path Diff along feature-1: V1 -> V3A
        AggregatedPathDiff pathDiff = dagService.computeAggregatedChangeSet(v1.versionId(), v3A.versionId());
        assertEquals(2, pathDiff.totalKeysChanged());

        AggregatedKeyDiff counterDiff = pathDiff.changes().get("counter");
        assertNotNull(counterDiff);
        assertEquals("10", counterDiff.baseValue());
        assertEquals("25", counterDiff.tipValue());
        assertEquals(15L, counterDiff.netDelta());
        assertEquals(List.of(OperationType.DELTA, OperationType.DELTA), counterDiff.operations());

        AggregatedKeyDiff f1Diff = pathDiff.changes().get("f1_only");
        assertNotNull(f1Diff);
        assertNull(f1Diff.baseValue());
        assertEquals("on", f1Diff.tipValue());
    }

    @Test
    void testOptimisticStaleBaseAutoBranchingAndRestApis() throws Exception {
        RaftNode.ChronosWriteResult v1 = writeOp("main", "alice",
                ChronosWritePayload.WriteOp.put("doc", utf8("version 1")));

        // Writer 1 advances main to V2
        RaftNode.ChronosWriteResult v2M = writeOp("main", "bob",
                ChronosWritePayload.WriteOp.put("doc", utf8("version 2 by bob")));

        // Writer 2 sends a write expecting main to still be at v1!
        mockMvc.perform(post("/api/chronos/write")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "key": "doc",
                                  "value": "concurrent edit by charlie",
                                  "op": "PUT",
                                  "writerId": "charlie",
                                  "expectedParentVersionId": "%s"
                                }
                                """.formatted(v1.versionId())))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.autoBranched").value(true))
                .andExpect(jsonPath("$.branchId").value(org.hamcrest.Matchers.startsWith("auto-charlie-")));

        // Main is still untouched by charlie!
        assertEquals("version 2 by bob", temporalQueryService.queryKey(
                TemporalQueryRequest.current("doc", "main", ReadMode.LINEARIZABLE, false)).value());

        // REST: test DAG endpoint
        mockMvc.perform(get("/api/chronos/dag"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.nodeCount").isNumber())
                .andExpect(jsonPath("$.edgeCount").isNumber());

        // REST: test LCA endpoint
        mockMvc.perform(get("/api/chronos/dag/lca")
                        .param("v1", v1.versionId())
                        .param("v2", v2M.versionId()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.lcaVersionId").value(v1.versionId()));

        // REST: test list and get branches
        mockMvc.perform(get("/api/chronos/branches"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.count").isNumber());

        // REST: create explicit branch via HTTP
        mockMvc.perform(post("/api/chronos/branches")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "branchId": "experiment-http",
                                  "baseVersionId": "%s",
                                  "writerId": "http-tester",
                                  "description": "Created via REST"
                                }
                                """.formatted(v2M.versionId())))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.branch.branchId").value("experiment-http"));

        mockMvc.perform(get("/api/chronos/branches/experiment-http"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.baseVersionId").value(v2M.versionId()));
    }

    @Test
    void testGcProtectsBranchBasesFromPrematurePurge() throws Exception {
        RaftNode.ChronosWriteResult v1 = writeOp("main", "alice",
                ChronosWritePayload.WriteOp.put("k", utf8("v1")));
        RaftNode.ChronosWriteResult v2 = writeOp("main", "alice",
                ChronosWritePayload.WriteOp.put("k", utf8("v2")));

        // Create active long-lived branch from v1
        branchManager.createBranch("long-lived-branch", v1.versionId(), "researcher", "Keep V1");

        // Advance main through v3, v4, v5
        writeOp("main", "bob", ChronosWritePayload.WriteOp.put("k", utf8("v3")));
        writeOp("main", "bob", ChronosWritePayload.WriteOp.put("k", utf8("v4")));
        RaftNode.ChronosWriteResult v5 = writeOp("main", "bob", ChronosWritePayload.WriteOp.put("k", utf8("v5")));

        // Set maxVersions = 2 (which would normally purge v1, v2, v3)
        retentionConfig.setPolicy(RetentionPolicy.LAST_N_VERSIONS);
        retentionConfig.setMaxVersions(2);

        // Run Safe GC
        gcService.runGc();

        // V1 MUST BE PRESERVED because it is the baseVersion of 'long-lived-branch'!
        Optional<ChronosVersion> retainedV1 = catalog.getVersion(v1.versionId());
        assertTrue(retainedV1.isPresent(), "Branch base version V1 must be protected from GC");

        // Now abandon the branch and run GC again
        branchManager.updateBranchStatus("long-lived-branch", BranchStatus.ABANDONED);
        gcService.runGc();

        // Once abandoned, V1 can be safely purged
        assertThrows(HistoryUnavailableException.class, () -> temporalQueryService.queryKey(
                TemporalQueryRequest.asOfVersion("k", v1.versionId(), false)));
    }

    private RaftNode.ChronosWriteResult writeOp(String branchId, String writerId, ChronosWritePayload.WriteOp... ops)
            throws Exception {
        ChronosWritePayload payload = ChronosWritePayload.builder()
                .branchId(branchId)
                .writerId(writerId)
                .operations(List.of(ops))
                .build();
        RaftLog.CommandType cmdType = "main".equals(branchId)
                ? (ops.length > 1 ? RaftLog.CommandType.CHRONOS_TXN : RaftLog.CommandType.CHRONOS_WRITE)
                : RaftLog.CommandType.CHRONOS_BRANCH_WRITE;

        RaftNode.ChronosWriteResult res = raftNode.submitChronosWrite(cmdType, payload)
                .get(5, TimeUnit.SECONDS);
        assertTrue(res.committed(), "Write on branch '" + branchId + "' must commit cleanly");
        return res;
    }

    private byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}

