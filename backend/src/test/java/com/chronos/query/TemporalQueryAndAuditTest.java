package com.chronos.query;

import com.chronos.api.ChronosController;
import com.chronos.audit.AuditService;
import com.chronos.audit.KeyAuditEntry;
import com.chronos.audit.VersionDiffEntry;
import com.chronos.audit.VersionDiffResult;
import com.chronos.model.CatalogWatermark;
import com.chronos.model.ChangeEntry;
import com.chronos.model.ChronosSnapshot;
import com.chronos.model.ChronosVersion;
import com.chronos.model.ChronosWritePayload;
import com.chronos.model.OperationType;
import com.chronos.model.VersionStatus;
import com.chronos.storage.VersionCatalog;
import com.minidb.config.RaftConfig;
import com.minidb.events.EventBus;
import com.minidb.grpc.KvGrpcService;
import com.minidb.mvcc.MvccStore;
import com.minidb.mvcc.ReadMode;
import com.minidb.proto.AuditHistoryRequest;
import com.minidb.proto.AuditHistoryResponse;
import com.minidb.proto.ReconstructStateRequest;
import com.minidb.proto.ReconstructStateResponse;
import com.minidb.proto.TemporalKeyRequest;
import com.minidb.proto.TemporalKeyResponse;
import com.minidb.raft.RaftLog;
import com.minidb.raft.RaftNode;
import com.minidb.speculation.SpeculativeManager;
import com.minidb.storage.RocksDbEngine;
import com.minidb.txn.TxnCoordinator;
import io.grpc.stub.StreamObserver;
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
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Step 3 tests: TemporalQueryService (CURRENT, AS OF VERSION/TIME/SNAPSHOT),
 * branch lineage isolation, GC watermark HISTORY_UNAVAILABLE bounds, AuditService,
 * ChronosController REST endpoints, and KvGrpcService historical RPCs.
 */
class TemporalQueryAndAuditTest {

    @TempDir
    Path tempDir;

    private RocksDbEngine storage;
    private MvccStore mvccStore;
    private VersionCatalog catalog;
    private RaftNode raftNode;
    private SpeculativeManager speculativeManager;
    private TxnCoordinator txnCoordinator;
    private EventBus eventBus;
    private TemporalQueryService temporalQueryService;
    private AuditService auditService;
    private ChronosController controller;
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
        assertTrue(raftNode.isLeader(), "Single-node cluster must become leader");

        speculativeManager = new SpeculativeManager(eventBus, meterRegistry);
        txnCoordinator = new TxnCoordinator(raftNode, eventBus);
        temporalQueryService = new TemporalQueryService(raftNode, catalog, mvccStore);
        auditService = new AuditService(catalog, temporalQueryService);
        controller = new ChronosController(
                raftNode, config, catalog, temporalQueryService, auditService,
                speculativeManager, txnCoordinator, eventBus);
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
    void testTemporalQueriesAcrossVersionsTimestampsSnapshotsAndDeletes() throws Exception {
        RaftNode.ChronosWriteResult r1 = writeOp("main", "alice",
                ChronosWritePayload.WriteOp.put("user:1:tier", utf8("free")),
                ChronosWritePayload.WriteOp.put("user:1:credits", utf8("100")));
        Thread.sleep(10);

        RaftNode.ChronosWriteResult r2 = writeOp("main", "billing",
                ChronosWritePayload.WriteOp.put("user:1:tier", utf8("pro")),
                ChronosWritePayload.WriteOp.delta("user:1:credits", utf8("75"), -25L));
        Thread.sleep(10);

        ChronosSnapshot snapAfterUpgrade = temporalQueryService.createSnapshot(
                "after-upgrade", "Before account deletion", "main");

        RaftNode.ChronosWriteResult r3 = writeOp("main", "cleanup-job",
                ChronosWritePayload.WriteOp.delete("user:1:tier"));

        // CURRENT shows deleted tier and updated credits
        TemporalQueryResult currentTier = temporalQueryService.queryKey(
                TemporalQueryRequest.current("user:1:tier", "main", ReadMode.LINEARIZABLE, true));
        assertFalse(currentTier.found());
        assertTrue(currentTier.tombstone());

        TemporalQueryResult currentCredits = temporalQueryService.queryKey(
                TemporalQueryRequest.current("user:1:credits", "main", ReadMode.LINEARIZABLE, false));
        assertTrue(currentCredits.found());
        assertEquals("75", currentCredits.value());
        assertEquals("billing", currentCredits.writerId());

        // AS OF VERSION v1 still sees tier=free and credits=100
        TemporalQueryResult v1Tier = temporalQueryService.queryKey(
                TemporalQueryRequest.asOfVersion("user:1:tier", r1.versionId(), false));
        assertTrue(v1Tier.found());
        assertEquals("free", v1Tier.value());
        assertEquals(r1.versionId(), v1Tier.versionId());

        // AS OF TIME (epoch ms and ISO-8601) sees state between r2 and r3
        TemporalQueryResult asOfTimeEpoch = temporalQueryService.queryKey(
                TemporalQueryRequest.asOfTime("user:1:tier", String.valueOf(r2.timestamp()), "main", false));
        assertTrue(asOfTimeEpoch.found());
        assertEquals("pro", asOfTimeEpoch.value());

        String isoBetween = Instant.ofEpochMilli(r2.timestamp()).toString();
        TemporalQueryResult asOfTimeIso = temporalQueryService.queryKey(
                TemporalQueryRequest.asOfTime("user:1:credits", isoBetween, "main", false));
        assertTrue(asOfTimeIso.found());
        assertEquals("75", asOfTimeIso.value());

        // Timestamp strictly before first write on unpurged DB returns empty/notFound (no exception)
        TemporalQueryResult beforeFirstWrite = temporalQueryService.queryKey(
                TemporalQueryRequest.asOfTime("user:1:tier", String.valueOf(r1.timestamp() - 1000L), "main", false));
        assertFalse(beforeFirstWrite.found());

        // AS OF SNAPSHOT restores state pinned at snapshot
        DatabaseStateResult snapState = temporalQueryService.reconstructState(
                TemporalQueryRequest.asOfSnapshot(null, snapAfterUpgrade.getSnapshotId(), false));
        assertEquals(r2.versionId(), snapState.versionId());
        assertEquals(Map.of("user:1:credits", "75", "user:1:tier", "pro"), snapState.entries());

        // Reconstruct CURRENT excludes tombstone
        DatabaseStateResult currentState = temporalQueryService.reconstructState(
                new TemporalQueryRequest(QueryMode.CURRENT, null, null, null, null,
                        "main", ReadMode.LINEARIZABLE, false, "user:1:", 10));
        assertEquals(r3.versionId(), currentState.versionId());
        assertEquals(Map.of("user:1:credits", "75"), currentState.entries());
    }

    @Test
    void testLineageIsolationAcrossDivergentBranches() {
        long t0 = 1_700_000_001_000L;
        ChronosVersion base = new ChronosVersion(
                "v-1", 1L, List.of(), "main", t0, 1L, "init", null,
                VersionStatus.COMMITTED, false, null, List.of(),
                List.of(ChangeEntry.put("shared", null, utf8("v0"), "init"))
        );
        ChronosVersion branchA = new ChronosVersion(
                "v-2", 2L, List.of("v-1"), "exp-a", t0 + 100, 2L, "writer-a", null,
                VersionStatus.COMMITTED, false, null, List.of(),
                List.of(
                        ChangeEntry.put("shared", utf8("v0"), utf8("from-a"), "writer-a"),
                        ChangeEntry.put("onlyA", null, utf8("A1"), "writer-a")
                )
        );
        ChronosVersion branchB = new ChronosVersion(
                "v-3", 3L, List.of("v-1"), "exp-b", t0 + 200, 3L, "writer-b", null,
                VersionStatus.COMMITTED, false, null, List.of(),
                List.of(
                        ChangeEntry.put("shared", utf8("v0"), utf8("from-b"), "writer-b"),
                        ChangeEntry.put("onlyB", null, utf8("B1"), "writer-b")
                )
        );
        catalog.saveVersion(base);
        catalog.saveVersion(branchA);
        catalog.saveVersion(branchB);

        // Even though v-3 has higher seq/ts than v-2, AS OF v-2 must strictly follow Ancestors(v-2)
        DatabaseStateResult stateA = temporalQueryService.reconstructState(
                TemporalQueryRequest.asOfVersion(null, "v-2", false));
        assertEquals(Map.of("onlyA", "A1", "shared", "from-a"), stateA.entries());

        TemporalQueryResult bLeakCheck = temporalQueryService.queryKey(
                TemporalQueryRequest.asOfVersion("onlyB", "v-2", false));
        assertFalse(bLeakCheck.found());

        DatabaseStateResult stateB = temporalQueryService.reconstructState(
                TemporalQueryRequest.asOfVersion(null, "v-3", false));
        assertEquals(Map.of("onlyB", "B1", "shared", "from-b"), stateB.entries());
    }

    @Test
    void testHistoryUnavailablePastGcWatermarkAndRestEndpoints() throws Exception {
        RaftNode.ChronosWriteResult r1 = writeOp("main", "w1",
                ChronosWritePayload.WriteOp.put("k", utf8("v1")));
        Thread.sleep(5);
        RaftNode.ChronosWriteResult r2 = writeOp("main", "w2",
                ChronosWritePayload.WriteOp.put("k", utf8("v2")));

        // REST write + query + snapshot + versions + diff + audit happy paths
        mockMvc.perform(post("/api/chronos/write")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"key":"order:42","value":"paid","op":"PUT","writerId":"checkout-svc"}
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.versionId").exists());

        mockMvc.perform(get("/api/chronos/query")
                        .param("key", "order:42")
                        .param("mode", "CURRENT")
                        .param("consistent", "true"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.found").value(true))
                .andExpect(jsonPath("$.value").value("paid"))
                .andExpect(jsonPath("$.writerId").value("checkout-svc"));

        mockMvc.perform(get("/api/chronos/diff")
                        .param("from", r1.versionId())
                        .param("to", r2.versionId()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.modifiedCount").value(1));

        // Verify REST DELTA auto-resolution + audit endpoint
        mockMvc.perform(post("/api/chronos/write")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"key":"balance","value":"100","op":"PUT","writerId":"init"}
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true));
        mockMvc.perform(post("/api/chronos/write")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"key":"balance","op":"DELTA","deltaAmount":-30,"writerId":"payment"}
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true));
        mockMvc.perform(get("/api/chronos/audit/key/balance"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.count").value(2))
                .andExpect(jsonPath("$.entries[1].newValue").value("70"))
                .andExpect(jsonPath("$.entries[1].operationType").value("DELTA"));

        // Simulate GC advancing low-watermark past r1
        catalog.updateWatermark(new CatalogWatermark(r2.versionSequence(), r2.timestamp(), 2L, System.currentTimeMillis()));

        assertThrows(HistoryUnavailableException.class, () -> temporalQueryService.queryKey(
                TemporalQueryRequest.asOfVersion("k", r1.versionId(), false)));
        assertThrows(HistoryUnavailableException.class, () -> temporalQueryService.queryKey(
                TemporalQueryRequest.asOfTime("k", String.valueOf(r1.timestamp()), "main", false)));

        mockMvc.perform(get("/api/chronos/query")
                        .param("key", "k")
                        .param("mode", "AS_OF_VERSION")
                        .param("versionId", r1.versionId()))
                .andExpect(status().isGone())
                .andExpect(jsonPath("$.errorCode").value("HISTORY_UNAVAILABLE"));

        mockMvc.perform(get("/api/chronos/state")
                        .param("mode", "AS_OF_TIME")
                        .param("timestamp", String.valueOf(r1.timestamp())))
                .andExpect(status().isGone())
                .andExpect(jsonPath("$.errorCode").value("HISTORY_UNAVAILABLE"));
    }

    @Test
    void testAuditServiceAndGrpcHistoricalEndpoints() throws Exception {
        RaftNode.ChronosWriteResult r1 = writeOp("main", "alice",
                ChronosWritePayload.WriteOp.put("acct:1", utf8("100")),
                ChronosWritePayload.WriteOp.put("temp:flag", utf8("true")));
        RaftNode.ChronosWriteResult r2 = writeOp("main", "bob",
                ChronosWritePayload.WriteOp.delta("acct:1", utf8("150"), 50L),
                ChronosWritePayload.WriteOp.delete("temp:flag"),
                ChronosWritePayload.WriteOp.put("acct:2", utf8("500")));

        List<KeyAuditEntry> acctTrail = auditService.getKeyAuditTrail("acct:1");
        assertEquals(2, acctTrail.size());
        assertEquals(OperationType.PUT, acctTrail.get(0).operationType());
        assertEquals("100", acctTrail.get(0).newValue());
        assertEquals("alice", acctTrail.get(0).writerId());

        assertEquals(OperationType.DELTA, acctTrail.get(1).operationType());
        assertEquals("100", acctTrail.get(1).oldValue());
        assertEquals("150", acctTrail.get(1).newValue());
        assertEquals(50L, acctTrail.get(1).deltaAmount());
        assertEquals("bob", acctTrail.get(1).writerId());

        VersionDiffResult diff = auditService.diffVersions(r1.versionId(), r2.versionId());
        assertEquals(1, diff.addedCount());
        assertEquals(1, diff.modifiedCount());
        assertEquals(1, diff.deletedCount());
        assertTrue(diff.changes().contains(
                new VersionDiffEntry("acct:2", VersionDiffEntry.DiffType.ADDED, null, "500")));
        assertTrue(diff.changes().contains(
                new VersionDiffEntry("acct:1", VersionDiffEntry.DiffType.MODIFIED, "100", "150")));
        assertTrue(diff.changes().contains(
                new VersionDiffEntry("temp:flag", VersionDiffEntry.DiffType.DELETED, "true", null)));

        // Verify KvGrpcService Step 3 RPCs
        KvGrpcService grpcService = new KvGrpcService(
                raftNode, eventBus, speculativeManager, temporalQueryService, auditService, new SimpleMeterRegistry());

        TemporalKeyResponse v1Resp = invokeGrpc(obs -> grpcService.getAsOfVersion(
                TemporalKeyRequest.newBuilder().setKey("acct:1").setVersionId(r1.versionId()).build(), obs));
        assertTrue(v1Resp.getFound());
        assertEquals("100", v1Resp.getValue().toStringUtf8());

        ReconstructStateResponse stateResp = invokeGrpc(obs -> grpcService.reconstructState(
                ReconstructStateRequest.newBuilder().setMode("AS_OF_VERSION").setVersionId(r2.versionId()).build(), obs));
        assertEquals(2, stateResp.getPairsCount());

        AuditHistoryResponse auditResp = invokeGrpc(obs -> grpcService.getAuditHistory(
                AuditHistoryRequest.newBuilder().setKey("acct:1").setIncludeUncommitted(true).build(), obs));
        assertEquals(2, auditResp.getEntriesCount());
        assertEquals("bob", auditResp.getEntries(1).getWriterId());
    }

    private RaftNode.ChronosWriteResult writeOp(String branchId, String writerId, ChronosWritePayload.WriteOp... ops)
            throws Exception {
        ChronosWritePayload payload = ChronosWritePayload.builder()
                .branchId(branchId)
                .writerId(writerId)
                .operations(List.of(ops))
                .build();
        RaftNode.ChronosWriteResult res = raftNode.submitChronosWrite(RaftLog.CommandType.CHRONOS_WRITE, payload)
                .get(5, TimeUnit.SECONDS);
        assertTrue(res.committed(), "Expected write to commit cleanly");
        return res;
    }

    private byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private <T> T invokeGrpc(java.util.function.Consumer<StreamObserver<T>> call) {
        AtomicReference<T> ref = new AtomicReference<>();
        AtomicReference<Throwable> err = new AtomicReference<>();
        call.accept(new StreamObserver<>() {
            @Override
            public void onNext(T value) {
                ref.set(value);
            }

            @Override
            public void onError(Throwable t) {
                err.set(t);
            }

            @Override
            public void onCompleted() {
            }
        });
        assertNull(err.get());
        return ref.get();
    }
}
