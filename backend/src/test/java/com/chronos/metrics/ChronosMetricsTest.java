package com.chronos.metrics;

import com.chronos.api.ChronosController;
import com.chronos.audit.AuditService;
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
import com.chronos.model.ConflictType;
import com.chronos.query.TemporalQueryService;
import com.chronos.storage.VersionCatalog;
import com.minidb.config.RaftConfig;
import com.minidb.events.ClusterEvent;
import com.minidb.events.EventBus;
import com.minidb.mvcc.MvccStore;
import com.minidb.raft.RaftNode;
import com.minidb.speculation.SpeculativeManager;
import com.minidb.storage.RocksDbEngine;
import com.minidb.txn.TxnCoordinator;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Step 7 tests: Prometheus Chronos metrics recording, latency timers,
 * REST /api/chronos/metrics observability endpoint, and WebSocket ClusterEvent publishing.
 */
class ChronosMetricsTest {

    @TempDir
    Path tempDir;

    private RocksDbEngine storage;
    private VersionCatalog catalog;
    private RaftNode raftNode;
    private ChronosMetrics metrics;
    private MockMvc mockMvc;
    private EventBus eventBus;

    @BeforeEach
    void setUp() throws Exception {
        storage = new RocksDbEngine(tempDir.resolve("db").toString());
        catalog = new VersionCatalog(storage);
        BranchManager branchManager = new BranchManager(storage, catalog);

        RaftConfig config = new RaftConfig() {
            @Override public String getNodeId() { return "node-1"; }
            @Override public List<String> getPeers() { return List.of(); }
            @Override public int getElectionTimeoutMin() { return 80; }
            @Override public int getElectionTimeoutMax() { return 120; }
            @Override public int getHeartbeatInterval() { return 30; }
        };

        eventBus = new EventBus();
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        raftNode = new RaftNode(config, storage, eventBus, meterRegistry, catalog);
        raftNode.setBranchManager(branchManager);
        raftNode.setBatchingEnabled(false);
        raftNode.start();

        long deadline = System.currentTimeMillis() + 3000;
        while (!raftNode.isLeader() && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        assertTrue(raftNode.isLeader());

        metrics = new ChronosMetrics(meterRegistry);

        ReadWatermarkRegistry watermarkRegistry = new ReadWatermarkRegistry();
        MvccStore mvccStore = raftNode.getMvccStore();
        TemporalQueryService temporalQueryService = new TemporalQueryService(raftNode, catalog, mvccStore, watermarkRegistry);
        VersionDagService dagService = new VersionDagService(catalog, temporalQueryService);
        AuditService auditService = new AuditService(catalog, temporalQueryService, watermarkRegistry);
        ChronosRetentionConfig retentionConfig = new ChronosRetentionConfig(RetentionPolicy.LAST_N_VERSIONS, 100, 3_600_000L, List.of());
        ChronosGcService gcService = new ChronosGcService(catalog, mvccStore, raftNode.getSnapshotManager(), retentionConfig, watermarkRegistry, eventBus);

        ConflictDetector conflictDetector = new ConflictDetector(catalog, dagService, temporalQueryService);
        InvariantRegistry invariantRegistry = new InvariantRegistry();
        InvariantValidator invariantValidator = new InvariantValidator(invariantRegistry);
        MergeEngine mergeEngine = new MergeEngine(catalog, branchManager, dagService, temporalQueryService,
                conflictDetector, invariantValidator, raftNode, eventBus, metrics);

        ChronosController controller = new ChronosController(
                raftNode, config, catalog, temporalQueryService, auditService,
                new SpeculativeManager(eventBus, meterRegistry),
                new TxnCoordinator(raftNode, eventBus),
                eventBus, retentionConfig, watermarkRegistry, gcService,
                branchManager, dagService, mergeEngine, invariantRegistry, metrics);
        mockMvc = MockMvcBuilders.standaloneSetup(controller).build();
    }

    @AfterEach
    void tearDown() {
        if (raftNode != null) raftNode.stop();
        if (storage != null) storage.close();
    }

    @Test
    void testMetricsRecordingAndSnapshot() {
        metrics.recordVersionCreation(15_000_000L); // 15 ms
        metrics.recordHistoricalQuery(2_500_000L);  // 2.5 ms
        metrics.recordReconstruction(5_000_000L);   // 5 ms
        metrics.recordConflictDetection(3_000_000L, 2);
        metrics.recordMerge(20_000_000L, true);
        metrics.recordMerge(8_000_000L, false);
        metrics.recordConflict(ConflictType.COMPATIBLE_DELTA);
        metrics.setActiveBranches(3);
        metrics.setHighestSequence(42);

        Map<String, Object> snapshot = metrics.getMetricsSnapshot();
        assertNotNull(snapshot);
        assertEquals(1L, snapshot.get("versionsTotal"));
        assertEquals(2L, snapshot.get("conflictsTotal"));
        assertEquals(1L, snapshot.get("mergesCommitted"));
        assertEquals(1L, snapshot.get("mergesRejected"));
        assertEquals(3, snapshot.get("activeBranches"));
        assertEquals(42L, snapshot.get("highestSequence"));

        @SuppressWarnings("unchecked")
        Map<String, Object> latencies = (Map<String, Object>) snapshot.get("latencies");
        assertNotNull(latencies);
        assertTrue((Double) latencies.get("versionCreationAvgMs") > 0);
        assertTrue((Double) latencies.get("historicalQueryAvgMs") > 0);
        assertTrue((Double) latencies.get("reconstructionAvgMs") > 0);
        assertTrue((Double) latencies.get("conflictDetectionAvgMs") > 0);
        assertTrue((Double) latencies.get("mergeAvgMs") > 0);
    }

    @Test
    void testRestMetricsEndpointReturnsExpectedJsonStructure() throws Exception {
        metrics.recordVersionCreation(10_000_000L);
        metrics.recordMerge(12_000_000L, true);

        mockMvc.perform(get("/api/chronos/metrics"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.versionsTotal").isNumber())
                .andExpect(jsonPath("$.mergesCommitted").isNumber())
                .andExpect(jsonPath("$.activeBranches").isNumber())
                .andExpect(jsonPath("$.latencies").isMap());
    }

    @Test
    void testClusterEventChronosCategoryAndLifecycleEvents() {
        List<ClusterEvent> received = new ArrayList<>();
        eventBus.subscribe(received::add);

        // 1. Version Created event
        ClusterEvent vEvent = ClusterEvent.chronosVersionCreated("node-1", "V10", 10L, "main", 3);
        eventBus.publish(vEvent);
        assertEquals(ClusterEvent.Category.CHRONOS, vEvent.getCategory());
        assertEquals("CHRONOS_VERSION_CREATED", vEvent.getType());

        // 2. Branch Created event
        ClusterEvent bEvent = ClusterEvent.chronosBranchCreated("node-1", "feature-x", "V1", "alice");
        eventBus.publish(bEvent);
        assertEquals(ClusterEvent.Category.CHRONOS, bEvent.getCategory());
        assertEquals("CHRONOS_BRANCH_CREATED", bEvent.getType());

        // 3. Conflict Detected event
        ClusterEvent cEvent = ClusterEvent.chronosConflictDetected("node-1", "main", "feature-x", 2);
        eventBus.publish(cEvent);
        assertEquals(ClusterEvent.Category.CHRONOS, cEvent.getCategory());
        assertEquals("CHRONOS_CONFLICT_DETECTED", cEvent.getType());

        // 4. Merge Committed event
        ClusterEvent mEvent = ClusterEvent.chronosMergeCommitted("node-1", "V11", "main", "feature-x", "COMMUTATIVE_DELTA");
        eventBus.publish(mEvent);
        assertEquals(ClusterEvent.Category.CHRONOS, mEvent.getCategory());
        assertEquals("CHRONOS_MERGE_COMMITTED", mEvent.getType());

        // 5. Merge Rejected event
        ClusterEvent rEvent = ClusterEvent.chronosMergeRejected("node-1", "main", "feature-x", "Invariant violation");
        eventBus.publish(rEvent);
        assertEquals(ClusterEvent.Category.CHRONOS, rEvent.getCategory());
        assertEquals("CHRONOS_MERGE_REJECTED", rEvent.getType());

        // 6. Watermark GC event
        ClusterEvent gEvent = ClusterEvent.chronosWatermarkGc("node-1", 5L, 12L, 8L);
        eventBus.publish(gEvent);
        assertEquals(ClusterEvent.Category.CHRONOS, gEvent.getCategory());
        assertEquals("CHRONOS_WATERMARK_GC", gEvent.getType());

        assertEquals(6, received.size());
    }
}

