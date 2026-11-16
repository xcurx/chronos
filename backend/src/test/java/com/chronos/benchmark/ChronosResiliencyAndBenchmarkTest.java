package com.chronos.benchmark;

import com.chronos.api.ChronosBenchmarkController;
import com.chronos.branch.BranchManager;
import com.chronos.dag.VersionDagService;
import com.chronos.gc.ChronosGcService;
import com.chronos.gc.ChronosRetentionConfig;
import com.chronos.gc.ReadWatermarkRegistry;
import com.chronos.gc.RetentionPolicy;
import com.chronos.merge.ConflictDetector;
import com.chronos.merge.InvariantRegistry;
import com.chronos.merge.InvariantValidator;
import com.chronos.merge.MergeEngine;
import com.chronos.metrics.ChronosMetrics;
import com.chronos.query.TemporalQueryService;
import com.chronos.storage.VersionCatalog;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.minidb.config.RaftConfig;
import com.minidb.events.EventBus;
import com.minidb.mvcc.MvccStore;
import com.minidb.raft.RaftNode;
import com.minidb.speculation.SpeculativeManager;
import com.minidb.storage.RocksDbEngine;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Step 8 evaluation suite: Research evaluation dimensions (chronos.pdf slide 19)
 * and multi-writer conflict resolution Workloads A-D.
 */
class ChronosResiliencyAndBenchmarkTest {

    @TempDir
    Path tempDir;

    private RocksDbEngine storage;
    private VersionCatalog catalog;
    private RaftNode raftNode;
    private MvccStore mvccStore;
    private BranchManager branchManager;
    private VersionDagService dagService;
    private TemporalQueryService temporalQueryService;
    private ChronosGcService gcService;
    private ChronosRetentionConfig retentionConfig;
    private ReadWatermarkRegistry watermarkRegistry;
    private ConflictDetector conflictDetector;
    private InvariantRegistry invariantRegistry;
    private InvariantValidator invariantValidator;
    private MergeEngine mergeEngine;
    private SpeculativeManager speculativeManager;
    private ChronosMetrics chronosMetrics;
    private ChronosBenchmarkService benchmarkService;
    private MockMvc mockMvc;
    private final ObjectMapper mapper = new ObjectMapper();

    @BeforeEach
    void setUp() throws Exception {
        storage = new RocksDbEngine(tempDir.resolve("bench-db").toString());
        catalog = new VersionCatalog(storage);
        branchManager = new BranchManager(storage, catalog);

        RaftConfig config = new RaftConfig() {
            @Override public String getNodeId() { return "bench-node-1"; }
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
        assertTrue(raftNode.isLeader(), "Benchmark Raft node must become leader");

        watermarkRegistry = new ReadWatermarkRegistry();
        temporalQueryService = new TemporalQueryService(raftNode, catalog, mvccStore, watermarkRegistry);
        dagService = new VersionDagService(catalog, temporalQueryService);
        retentionConfig = new ChronosRetentionConfig(RetentionPolicy.LAST_N_VERSIONS, 1, 3_600_000L, List.of());
        gcService = new ChronosGcService(catalog, mvccStore, raftNode.getSnapshotManager(),
                retentionConfig, watermarkRegistry);

        conflictDetector = new ConflictDetector(catalog, dagService, temporalQueryService);
        invariantRegistry = new InvariantRegistry();
        invariantValidator = new InvariantValidator(invariantRegistry);
        chronosMetrics = new ChronosMetrics(meterRegistry);
        mergeEngine = new MergeEngine(catalog, branchManager, dagService, temporalQueryService,
                conflictDetector, invariantValidator, raftNode, eventBus, chronosMetrics);
        speculativeManager = new SpeculativeManager(eventBus, meterRegistry);

        benchmarkService = new ChronosBenchmarkService(
                catalog,
                temporalQueryService,
                branchManager,
                dagService,
                conflictDetector,
                mergeEngine,
                invariantValidator,
                watermarkRegistry,
                gcService,
                speculativeManager,
                raftNode,
                chronosMetrics
        );

        ChronosBenchmarkController controller = new ChronosBenchmarkController(benchmarkService);
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
    void testReadLatencyVsHistoryDepth() {
        Map<String, Object> result = benchmarkService.benchmarkHistoryDepth(25, 10, 25);
        assertNotNull(result);
        assertEquals(25, result.get("historyDepth"));
        assertEquals(10, result.get("distinctKeys"));

        LatencyStats currentStats = (LatencyStats) result.get("currentReadStatsMs");
        LatencyStats asOfVersionStats = (LatencyStats) result.get("asOfVersionStatsMs");
        LatencyStats asOfTimeStats = (LatencyStats) result.get("asOfTimeStatsMs");
        LatencyStats reconstructStats = (LatencyStats) result.get("reconstructStateStatsMs");

        assertNotNull(currentStats);
        assertEquals(25, currentStats.count());
        assertTrue(currentStats.avgMs() >= 0.0);

        assertNotNull(asOfVersionStats);
        assertEquals(25, asOfVersionStats.count());
        assertTrue(asOfVersionStats.avgMs() >= 0.0);

        assertNotNull(asOfTimeStats);
        assertEquals(25, asOfTimeStats.count());

        assertNotNull(reconstructStats);
        assertTrue(reconstructStats.count() > 0);
        assertTrue(result.get("summary").toString().contains("Evaluated 25 versions"));
    }

    @Test
    void testStorageOverheadAndDeltaCompression() {
        Map<String, Object> result = benchmarkService.benchmarkStorageOverhead(100, 50, 0.10);
        assertNotNull(result);

        double savingsPercent = (double) result.get("savingsPercent");
        double compressionRatio = (double) result.get("compressionRatio");
        long bytesSaved = (long) result.get("bytesSaved");

        assertTrue(savingsPercent >= 80.0, "Delta changeset storage savings should be >= 80%, was: " + savingsPercent);
        assertTrue(compressionRatio >= 5.0, "Compression ratio should be >= 5x, was: " + compressionRatio);
        assertTrue(bytesSaved > 0);
    }

    @Test
    void testWorkloadAIndependentWritesAutoMerge() {
        Map<String, Object> result = benchmarkService.runWorkloadA(8);
        assertNotNull(result);
        assertEquals("SUCCESS", result.get("status"));
        assertEquals(0, result.get("totalConflicts"));
        assertEquals(100.0, result.get("autoMergeRatePercent"));
        assertTrue((boolean) result.get("canMerge"));
        assertTrue((boolean) result.get("mergeCommitted"));
        assertTrue((boolean) result.get("allKeysVerifiedInState"));
        assertNotNull(result.get("mergedVersionId"));
    }

    @Test
    void testWorkloadBHighContentionCommutativeAndLww() {
        Map<String, Object> result = benchmarkService.runWorkloadB(5);
        assertNotNull(result);
        assertEquals("SUCCESS", result.get("status"));

        @SuppressWarnings("unchecked")
        Map<String, Object> failOnConflict = (Map<String, Object>) result.get("failOnConflictPreview");
        assertFalse((boolean) failOnConflict.get("canMerge"), "FAIL_ON_CONFLICT must reject write-write collision");
        assertEquals(1, failOnConflict.get("totalConflicts"));

        @SuppressWarnings("unchecked")
        Map<String, Object> lww = (Map<String, Object>) result.get("lastWriteWinsPreview");
        assertTrue((boolean) lww.get("canMerge"), "LAST_WRITE_WINS must permit merge");

        @SuppressWarnings("unchecked")
        Map<String, Object> commutative = (Map<String, Object>) result.get("commutativeDeltaMerge");
        assertTrue((boolean) commutative.get("canMerge"));
        assertTrue((boolean) commutative.get("mergeCommitted"));
        assertEquals("45", commutative.get("actualCounter"), "100 - 20 - 35 must equal 45");
    }

    @Test
    void testWorkloadCMixed8020CommutativeMerge() {
        Map<String, Object> result = benchmarkService.runWorkloadC(10);
        assertNotNull(result);
        assertEquals("SUCCESS", result.get("status"));
        assertTrue((boolean) result.get("canMerge"));
        assertTrue((boolean) result.get("mergeCommitted"));
        assertTrue((boolean) result.get("commutativeDeltasReconciled"), "Mixed commutative deltas must reconcile correctly");
    }

    @Test
    void testWorkloadDMultiCommitPartitionConvergence() {
        Map<String, Object> result = benchmarkService.runWorkloadD();
        assertNotNull(result);
        assertEquals("SUCCESS", result.get("status"));
        assertTrue((boolean) result.get("lcaMatchesExpectedBase"), "LCA must accurately match the partition base version");
        assertTrue((boolean) result.get("allMutationsConvergedDeterministically"),
                "Multi-commit East and West partitions must converge deterministically");
    }

    @Test
    void testChaosSpeculativeRollbackIsolation() {
        Map<String, Object> result = benchmarkService.runChaosScenarioSpeculativeRollback();
        assertNotNull(result);
        assertEquals("SUCCESS", result.get("status"));
        assertEquals("ROLLED_BACK", result.get("speculativeStatusAfterRollback"));
        assertTrue((boolean) result.get("uncommittedDataIsolated"));
        assertEquals("committed-value", result.get("linearizableReadValue"));
    }

    @Test
    void testChaosStaleWriterAutoBranchProtection() {
        Map<String, Object> result = benchmarkService.runChaosScenarioStaleWriter();
        assertNotNull(result);
        assertEquals("SUCCESS", result.get("status"));
        assertTrue((boolean) result.get("mainHistoryProtectedFromOverwrite"));
        assertNotNull(result.get("staleBranchCreated"));
    }

    @Test
    void testChaosWatermarkGcConcurrencySafety() {
        Map<String, Object> result = benchmarkService.runChaosScenarioWatermarkGcProtection();
        assertNotNull(result);
        assertEquals("SUCCESS", result.get("status"));
        assertTrue((boolean) result.get("versionProtectedDuringActivePin"));
    }

    @Test
    void testEvaluationSummaryDimensionAggregation() {
        Map<String, Object> summary = benchmarkService.generateEvaluationSummary();
        assertNotNull(summary);
        assertEquals("Chronos (Distributed Time-Travel Database)", summary.get("database"));

        @SuppressWarnings("unchecked")
        Map<String, Object> dims = (Map<String, Object>) summary.get("dimensions");
        assertNotNull(dims);
        assertTrue(dims.containsKey("1_Correctness"));
        assertTrue(dims.containsKey("2_Latency"));
        assertTrue(dims.containsKey("3_ThroughputAndAutoMerge"));
        assertTrue(dims.containsKey("4_RecoveryAndResilience"));
        assertTrue(dims.containsKey("5_StorageScalability"));
    }

    @Test
    void testBenchmarkControllerRestEndpoints() throws Exception {
        // 1. History Depth
        mockMvc.perform(post("/api/chronos/benchmark/history-depth")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(Map.of("numVersions", 10, "numKeys", 5, "querySamples", 10))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.benchmark").value("Read Latency vs History Depth"))
                .andExpect(jsonPath("$.historyDepth").value(10));

        // 2. Storage Overhead
        mockMvc.perform(post("/api/chronos/benchmark/storage-overhead")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(Map.of("numVersions", 50, "numKeys", 20, "modifiedRatio", 0.10))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.benchmark").value("Storage Overhead & Delta Compression"))
                .andExpect(jsonPath("$.savingsPercent").exists());

        // 3. Workload A
        mockMvc.perform(post("/api/chronos/benchmark/workload-a")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(Map.of("operationsPerBranch", 4))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("SUCCESS"))
                .andExpect(jsonPath("$.autoMergeRatePercent").value(100.0));

        // 4. Workload B
        mockMvc.perform(post("/api/chronos/benchmark/workload-b")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(Map.of("operationsPerBranch", 2))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("SUCCESS"));

        // 5. Workload C
        mockMvc.perform(post("/api/chronos/benchmark/workload-c")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(Map.of("operationsPerBranch", 5))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("SUCCESS"));

        // 6. Workload D
        mockMvc.perform(post("/api/chronos/benchmark/workload-d"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("SUCCESS"));

        // 7. Chaos endpoints
        mockMvc.perform(post("/api/chronos/benchmark/chaos/speculative-rollback"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("SUCCESS"));

        mockMvc.perform(post("/api/chronos/benchmark/chaos/stale-writer"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("SUCCESS"));

        mockMvc.perform(post("/api/chronos/benchmark/chaos/watermark-gc"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("SUCCESS"));

        // 8. Evaluation Summary
        mockMvc.perform(get("/api/chronos/benchmark/summary"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.database").value("Chronos (Distributed Time-Travel Database)"))
                .andExpect(jsonPath("$.dimensions.1_Correctness").exists())
                .andExpect(jsonPath("$.dimensions.5_StorageScalability").exists());
    }
}
