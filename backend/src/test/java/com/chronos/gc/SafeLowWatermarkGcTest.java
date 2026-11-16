package com.chronos.gc;

import com.chronos.api.ChronosController;
import com.chronos.audit.AuditService;
import com.chronos.model.CatalogWatermark;
import com.chronos.model.ChronosWritePayload;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Step 4 tests: ROLLED_BACK index-0 MVCC bug fix, ReadWatermarkRegistry active query
 * pin protection, boundary value preservation, retention policies, and REST /retention + /gc.
 */
class SafeLowWatermarkGcTest {

    @TempDir
    Path tempDir;

    private RocksDbEngine storage;
    private VersionCatalog catalog;
    private RaftNode raftNode;
    private MvccStore mvccStore;
    private EventBus eventBus;
    private ChronosRetentionConfig retentionConfig;
    private ReadWatermarkRegistry watermarkRegistry;
    private ChronosGcService gcService;
    private TemporalQueryService temporalQueryService;
    private MockMvc mockMvc;

    @BeforeEach
    void setUp() throws Exception {
        storage = new RocksDbEngine(tempDir.resolve("db").toString());
        catalog = new VersionCatalog(storage);

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
        mvccStore = raftNode.getMvccStore();
        raftNode.setBatchingEnabled(false);
        raftNode.start();

        long deadline = System.currentTimeMillis() + 3000;
        while (!raftNode.isLeader() && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        assertTrue(raftNode.isLeader());

        retentionConfig = new ChronosRetentionConfig(RetentionPolicy.LAST_N_VERSIONS, 100, 3_600_000L, List.of());
        watermarkRegistry = new ReadWatermarkRegistry();
        gcService = new ChronosGcService(catalog, mvccStore, raftNode.getSnapshotManager(),
                retentionConfig, watermarkRegistry);
        temporalQueryService = new TemporalQueryService(raftNode, catalog, mvccStore, watermarkRegistry);
        AuditService auditService = new AuditService(catalog, temporalQueryService, watermarkRegistry);

        ChronosController controller = new ChronosController(
                raftNode, config, catalog, temporalQueryService, auditService,
                new SpeculativeManager(eventBus, meterRegistry),
                new TxnCoordinator(raftNode, eventBus),
                eventBus, retentionConfig, watermarkRegistry, gcService);
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
    void testRolledBackAtIndexZeroIsPurgedCleanly() {
        long t1 = mvccStore.put("k-solo", utf8("committed"));
        assertEquals(1, mvccStore.getVersionHistory("k-solo").size());

        // Write speculative overwrite at index 0 (newest timestamp) and roll it back
        long tSpec = mvccStore.putSpeculative("k-solo", utf8("bad-spec"), 1L);
        assertEquals(2, mvccStore.getVersionHistory("k-solo").size());
        mvccStore.rollbackVersion("k-solo", tSpec);

        // Also test a key whose ONLY version at index 0 was ROLLED_BACK
        long tOnly = mvccStore.putSpeculative("k-orphan", utf8("orphan-spec"), 2L);
        mvccStore.rollbackVersion("k-orphan", tOnly);

        int purged = mvccStore.garbageCollectAtCutoff(0L, false);
        assertEquals(2, purged, "Both index-0 ROLLED_BACK entries must be purged");
        assertEquals(1, mvccStore.getVersionHistory("k-solo").size());
        assertEquals(t1, mvccStore.getVersionHistory("k-solo").get(0).getTimestamp());
        assertEquals("committed", mvccStore.get("k-solo", ReadMode.LINEARIZABLE).getValueAsString());
        assertTrue(mvccStore.getVersionHistory("k-orphan").isEmpty());
    }

    @Test
    void testActiveReadPinBlocksPrematureGcAndPostGcThrowsHistoryUnavailable() throws Exception {
        RaftNode.ChronosWriteResult v1 = writeOp("k", "v1");
        RaftNode.ChronosWriteResult v2 = writeOp("k", "v2");
        RaftNode.ChronosWriteResult v3 = writeOp("k", "v3");
        RaftNode.ChronosWriteResult v4 = writeOp("k", "v4");
        RaftNode.ChronosWriteResult v5 = writeOp("k", "v5");

        retentionConfig.setPolicy(RetentionPolicy.LAST_N_VERSIONS);
        retentionConfig.setMaxVersions(2); // Normally keeps V4..V5

        // Acquire active read pin protecting V2
        try (ReadWatermarkRegistry.ReadLease pin = watermarkRegistry.acquirePin(v2.versionSequence(), v2.timestamp())) {
            ChronosGcService.GcRunResult blockedRun = gcService.runGc();
            assertTrue(blockedRun.blockedByActiveReads(), "Active read pin on V2 must hold watermark at V2");
            assertEquals(1, blockedRun.versionsPurged(), "Only V1 before pinned V2 may be purged");
            assertEquals(v2.versionSequence(), blockedRun.earliestAvailableSeq());

            // V2 and V3 are still readable while pin is held
            TemporalQueryResult r2 = temporalQueryService.queryKey(
                    TemporalQueryRequest.asOfVersion("k", v2.versionId(), false));
            assertTrue(r2.found());
            assertEquals("v2", r2.value());
        }

        // Pin released -> V2 and V3 can now be safely purged
        ChronosGcService.GcRunResult unblockedRun = gcService.runGc();
        assertFalse(unblockedRun.blockedByActiveReads());
        assertEquals(2, unblockedRun.versionsPurged(), "V2 and V3 should purge once pin is released");
        assertEquals(v4.versionSequence(), unblockedRun.earliestAvailableSeq());

        assertThrows(HistoryUnavailableException.class, () -> temporalQueryService.queryKey(
                TemporalQueryRequest.asOfVersion("k", v1.versionId(), false)));
        assertThrows(HistoryUnavailableException.class, () -> temporalQueryService.queryKey(
                TemporalQueryRequest.asOfVersion("k", v2.versionId(), false)));
        assertThrows(HistoryUnavailableException.class, () -> temporalQueryService.queryKey(
                TemporalQueryRequest.asOfTime("k", String.valueOf(v3.timestamp()), "main", false)));

        // V4 and V5 remain completely intact
        assertEquals("v4", temporalQueryService.queryKey(
                TemporalQueryRequest.asOfVersion("k", v4.versionId(), false)).value());
        assertEquals("v5", temporalQueryService.queryKey(
                TemporalQueryRequest.asOfVersion("k", v5.versionId(), false)).value());
    }

    @Test
    void testWatermarkBoundaryPreservesUntouchedKeysAndAuditPins() throws Exception {
        RaftNode.ChronosWriteResult v1 = writeOp("stable-key", "init-value");
        writeOp("hot-key", "h1");
        RaftNode.ChronosWriteResult v3 = writeOp("hot-key", "h2");
        writeOp("hot-key", "h3");

        retentionConfig.setPolicy(RetentionPolicy.LAST_N_VERSIONS);
        retentionConfig.setMaxVersions(2); // Keep V3..V4, purge V1..V2

        ChronosGcService.GcRunResult res = gcService.runGc();
        assertEquals(2, res.versionsPurged());
        assertEquals(v3.versionSequence(), res.earliestAvailableSeq());

        // Querying V1 throws HISTORY_UNAVAILABLE
        assertThrows(HistoryUnavailableException.class, () -> temporalQueryService.queryKey(
                TemporalQueryRequest.asOfVersion("stable-key", v1.versionId(), false)));

        // Querying retained V3 still reconstructs untouched stable-key from preserved boundary!
        TemporalQueryResult stableAtV3 = temporalQueryService.queryKey(
                TemporalQueryRequest.asOfVersion("stable-key", v3.versionId(), false));
        assertTrue(stableAtV3.found());
        assertEquals("init-value", stableAtV3.value());

        DatabaseStateResult fullAtV3 = temporalQueryService.reconstructState(
                TemporalQueryRequest.asOfVersion(null, v3.versionId(), false));
        assertEquals("init-value", fullAtV3.entries().get("stable-key"));
        assertEquals("h2", fullAtV3.entries().get("hot-key"));

        // Verify audit-pinning V3 protects it even when maxVersions shrinks to 1 via REST API
        mockMvc.perform(put("/api/chronos/retention")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"policy":"LAST_N_VERSIONS","maxVersions":1,"pinVersion":"%s","triggerGc":true}
                                """.formatted(v3.versionId())))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.watermark.earliestAvailableSeq").value(v3.versionSequence()));

        mockMvc.perform(get("/api/chronos/retention"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.policy").value("LAST_N_VERSIONS"))
                .andExpect(jsonPath("$.maxVersions").value(1));
    }

    @Test
    void testConcurrentReaderPreventsTornHistoryDuringGcAndTimeWindowPolicy() throws Exception {
        RaftNode.ChronosWriteResult v1 = writeOp("counter", "10");
        long t1 = v1.timestamp();
        Thread.sleep(25);
        writeOp("counter", "20");
        writeOp("counter", "30");

        // Switch to UNBOUNDED first via REST: 0 versions purged
        retentionConfig.setPolicy(RetentionPolicy.UNBOUNDED);
        assertEquals(0, gcService.runGc().versionsPurged());

        // Now run concurrent AS_OF V1 reads while TIME_WINDOW GC attempts to purge old versions
        AtomicBoolean readerRunning = new AtomicBoolean(true);
        CountDownLatch readerStarted = new CountDownLatch(1);
        AtomicBoolean badDataObserved = new AtomicBoolean(false);

        Thread readerThread = new Thread(() -> {
            readerStarted.countDown();
            while (readerRunning.get()) {
                try {
                    TemporalQueryResult r = temporalQueryService.queryKey(
                            TemporalQueryRequest.asOfVersion("counter", v1.versionId(), false));
                    if (!r.found() || !"10".equals(r.value())) {
                        badDataObserved.set(true);
                    }
                } catch (HistoryUnavailableException expectedAfterGc) {
                    // Acceptable once GC cleanly advances watermark; never silent wrong data!
                }
            }
        });
        readerThread.start();
        assertTrue(readerStarted.await(2, TimeUnit.SECONDS));

        retentionConfig.setPolicy(RetentionPolicy.TIME_WINDOW);
        retentionConfig.setWindowMs(10L);
        for (int i = 0; i < 5; i++) {
            gcService.runGcAt(t1 + 5000L);
            Thread.sleep(2);
        }

        readerRunning.set(false);
        readerThread.join(2000);
        assertFalse(badDataObserved.get(), "Concurrent GC must never cause torn or wrong historical reads");

        // Final sweep after active readers finish guarantees watermark advances cleanly
        gcService.runGcAt(t1 + 5000L);
        CatalogWatermark wm = catalog.getWatermark();
        assertTrue(wm.getLastPurgedAt() > 0);
        assertTrue(wm.getEarliestAvailableSeq() > v1.versionSequence());

        mockMvc.perform(post("/api/chronos/gc"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.policy").value("TIME_WINDOW"));
    }

    private RaftNode.ChronosWriteResult writeOp(String key, String value) throws Exception {
        ChronosWritePayload payload = ChronosWritePayload.builder()
                .branchId("main")
                .writerId("test-writer")
                .operations(List.of(ChronosWritePayload.WriteOp.put(key, utf8(value))))
                .build();
        RaftNode.ChronosWriteResult res = raftNode.submitChronosWrite(RaftLog.CommandType.CHRONOS_WRITE, payload)
                .get(5, TimeUnit.SECONDS);
        assertTrue(res.committed());
        return res;
    }

    private byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
