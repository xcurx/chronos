package com.chronos.replication;

import com.chronos.model.ChronosVersion;
import com.chronos.model.ChronosWritePayload;
import com.chronos.model.OperationType;
import com.chronos.model.VersionStatus;
import com.chronos.storage.VersionCatalog;
import com.minidb.config.RaftConfig;
import com.minidb.events.EventBus;
import com.minidb.grpc.RaftProtoMapper;
import com.minidb.mvcc.MvccStore;
import com.minidb.mvcc.ReadMode;
import com.minidb.proto.LogEntry;
import com.minidb.raft.RaftLog;
import com.minidb.raft.RaftNode;
import com.minidb.raft.RaftPeerClient;
import com.minidb.speculation.SpeculativeManager;
import com.minidb.storage.RocksDbEngine;
import com.minidb.txn.TxnCoordinator;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Step 2 tests: deterministic Raft commit anchoring, protobuf/log serialization,
 * atomic multi-key 2PC ChronosVersions, Speculative MVCC + VersionCatalog lifecycle,
 * and 3-node failover rollback convergence.
 */
class ChronosReplicationTest {

    @TempDir
    Path tempDir;

    private final List<AutoCloseable> cleanupCallbacks = new ArrayList<>();

    @AfterEach
    void tearDown() {
        for (int i = cleanupCallbacks.size() - 1; i >= 0; i--) {
            try {
                cleanupCallbacks.get(i).close();
            } catch (Exception ignored) {
            }
        }
        cleanupCallbacks.clear();
    }

    @Test
    void testProtoAndLogEntrySerializationRoundTrip() {
        ChronosWritePayload payload = ChronosWritePayload.builder()
                .txnId("txn-42")
                .writerId("writer-a")
                .branchId("main")
                .parentVersionIds(List.of("V1"))
                .operations(List.of(
                        ChronosWritePayload.WriteOp.put("Alice", bytes("1500")),
                        ChronosWritePayload.WriteOp.delta("Bob", bytes("2500"), 500L)))
                .build();

        RaftLog.Entry entry = new RaftLog.Entry(
                7L, 3L, RaftLog.CommandType.CHRONOS_TXN, "txn-42", payload.toJsonBytes(),
                987654321L, "V2", 2L, "main", "writer-a", List.of("V1"));

        // Binary RaftLog persistence round-trip
        RaftLog.Entry fromBinary = RaftLog.Entry.deserialize(entry.serialize());
        assertEquals(entry.index(), fromBinary.index());
        assertEquals(entry.term(), fromBinary.term());
        assertEquals(entry.type(), fromBinary.type());
        assertEquals(entry.timestamp(), fromBinary.timestamp());
        assertEquals(entry.versionId(), fromBinary.versionId());
        assertEquals(entry.versionSequence(), fromBinary.versionSequence());
        assertEquals(entry.branchId(), fromBinary.branchId());
        assertEquals(entry.writerId(), fromBinary.writerId());
        assertEquals(entry.parentVersionIds(), fromBinary.parentVersionIds());

        // Protobuf wire RPC round-trip
        LogEntry proto = RaftProtoMapper.toProtoEntry(entry);
        RaftLog.Entry fromProto = RaftProtoMapper.fromProtoEntry(proto);
        assertEquals(entry.timestamp(), fromProto.timestamp());
        assertEquals(entry.versionId(), fromProto.versionId());
        assertEquals(entry.versionSequence(), fromProto.versionSequence());
        assertEquals(entry.parentVersionIds(), fromProto.parentVersionIds());

        ChronosWritePayload decodedPayload = ChronosWritePayload.fromJsonBytes(fromProto.value());
        assertEquals(2, decodedPayload.getOperations().size());
        assertEquals(OperationType.DELTA, decodedPayload.getOperations().get(1).getOpType());
        assertEquals(500L, decodedPayload.getOperations().get(1).getDeltaAmount());
    }

    @Test
    void testAtomicMultiKeyTransactionAndReplicaDeterminism() throws Exception {
        TestCluster cluster = createCluster();
        RaftNode leader = cluster.awaitLeader(5000);
        TxnCoordinator coordinator = cluster.txnCoordinator(leader.getNodeId());

        // Multi-key 2PC transaction #1: Alice = 1000, Bob = 2000
        TxnCoordinator.TxnCommitResult res1 = coordinator.executeWithResult(List.of(
                        new TxnCoordinator.TxnOperation("Alice", bytes("1000"), TxnCoordinator.TxnOperation.OpType.PUT),
                        new TxnCoordinator.TxnOperation("Bob", bytes("2000"), TxnCoordinator.TxnOperation.OpType.PUT)),
                "main", "writer-1").get(5, TimeUnit.SECONDS);

        assertTrue(res1.committed());
        assertEquals("V1", res1.versionId());
        assertEquals(1L, res1.versionSequence());

        // Multi-key 2PC transaction #2: Alice = 1500 (+500 delta), Charlie = 3000
        TxnCoordinator.TxnCommitResult res2 = coordinator.executeWithResult(List.of(
                        new TxnCoordinator.TxnOperation("Alice", bytes("1500"), TxnCoordinator.TxnOperation.OpType.DELTA, 500L),
                        new TxnCoordinator.TxnOperation("Charlie", bytes("3000"), TxnCoordinator.TxnOperation.OpType.PUT)),
                "main", "writer-1").get(5, TimeUnit.SECONDS);

        assertTrue(res2.committed());
        assertEquals("V2", res2.versionId());
        assertEquals(2L, res2.versionSequence());
        assertTrue(res2.timestamp() > res1.timestamp());

        // Wait for all 3 replicas to apply V1 and V2
        cluster.awaitCondition(5000, () -> cluster.nodes.values().stream()
                .allMatch(n -> n.catalog.getVersion("V2").isPresent()));

        // Verify exact byte-for-byte timestamp, versionId, parent lineage, and MVCC values on ALL 3 nodes
        for (NodeHarness node : cluster.nodes.values()) {
            ChronosVersion v1 = node.catalog.getVersion("V1").orElseThrow();
            ChronosVersion v2 = node.catalog.getVersion("V2").orElseThrow();

            assertEquals(VersionStatus.COMMITTED, v1.getStatus());
            assertEquals(res1.timestamp(), v1.getTimestamp());
            assertEquals(2, v1.getChangeSet().size());
            assertFalse(v1.getChangeSet().get(0).isOldPresent());

            assertEquals(VersionStatus.COMMITTED, v2.getStatus());
            assertEquals(res2.timestamp(), v2.getTimestamp());
            assertEquals(List.of("V1"), v2.getParentVersionIds());
            assertEquals(2, v2.getChangeSet().size());
            assertEquals("1000", v2.getChangeSet().get(0).getOldValueAsString());
            assertEquals("1500", v2.getChangeSet().get(0).getNewValueAsString());
            assertEquals(OperationType.DELTA, v2.getChangeSet().get(0).getOpType());
            assertEquals(500L, v2.getChangeSet().get(0).getDeltaAmount());

            // Atomic timestamp in MVCC for both keys in txn #1
            MvccStore mvcc = node.raftNode.getMvccStore();
            MvccStore.MvccResult aliceV1 = mvcc.get("Alice", res1.timestamp());
            MvccStore.MvccResult bobV1 = mvcc.get("Bob", res1.timestamp());
            assertTrue(aliceV1.isFound());
            assertTrue(bobV1.isFound());
            assertEquals("1000", aliceV1.getValueAsString());
            assertEquals("2000", bobV1.getValueAsString());
            assertEquals(res1.timestamp(), aliceV1.getTimestamp());
            assertEquals(res1.timestamp(), bobV1.getTimestamp());

            assertEquals("1500", mvcc.get("Alice").getValueAsString());
            assertEquals("3000", mvcc.get("Charlie").getValueAsString());
            assertEquals("V2", node.catalog.getBranchHead("main").orElse(null));
        }
    }

    @Test
    void testSpeculativePutPromotesDeterministicallyAcrossReplicas() throws Exception {
        TestCluster cluster = createCluster();
        RaftNode leader = cluster.awaitLeader(5000);
        NodeHarness leaderHarness = cluster.nodes.get(leader.getNodeId());
        SpeculativeManager specMgr = new SpeculativeManager(leaderHarness.eventBus, new SimpleMeterRegistry());

        // Temporarily isolate followers so the pre-commit SPECULATIVE state is observed deterministically
        for (String id : cluster.nodes.keySet()) {
            if (!id.equals(leader.getNodeId())) {
                cluster.isolateNode(id);
            }
        }

        SpeculativeManager.SpeculativeResult specRes = specMgr.submitSpeculativePut(
                leader, "AccountA", bytes("500"), SpeculativeManager.AckPolicy.SPECULATIVE);

        assertTrue(specRes.success());
        assertTrue(specRes.speculative());
        assertEquals("V1", specRes.versionId());

        // Immediately visible under SPECULATIVE read, hidden under LINEARIZABLE until quorum commit
        MvccStore leaderMvcc = leader.getMvccStore();
        assertEquals("500", leaderMvcc.get("AccountA", ReadMode.SPECULATIVE).getValueAsString());
        assertFalse(leaderMvcc.get("AccountA", ReadMode.LINEARIZABLE).isFound());
        assertEquals(VersionStatus.SPECULATIVE, leaderHarness.catalog.getVersion("V1").orElseThrow().getStatus());

        // Heal network and wait for majority commit promotion
        cluster.healNetwork();
        assertTrue(specRes.commitFuture().get(5, TimeUnit.SECONDS));

        cluster.awaitCondition(5000, () -> cluster.nodes.values().stream()
                .allMatch(n -> "V1".equals(n.catalog.getBranchHead("main").orElse(null))));

        for (NodeHarness node : cluster.nodes.values()) {
            ChronosVersion v1 = node.catalog.getVersion("V1").orElseThrow();
            assertEquals(VersionStatus.COMMITTED, v1.getStatus());
            assertEquals(specRes.timestamp(), v1.getTimestamp());
            MvccStore.MvccResult lin = node.raftNode.getMvccStore().get("AccountA", ReadMode.LINEARIZABLE);
            assertTrue(lin.isFound());
            assertEquals("500", lin.getValueAsString());
            assertEquals(specRes.timestamp(), lin.getTimestamp());
            assertFalse(lin.isSpeculative());
        }
    }

    @Test
    void testSpeculativeRollbackAndFailoverConvergence() throws Exception {
        TestCluster cluster = createCluster();
        RaftNode oldLeader = cluster.awaitLeader(5000);
        String oldLeaderId = oldLeader.getNodeId();
        NodeHarness oldLeaderHarness = cluster.nodes.get(oldLeaderId);

        // Commit V1 across all 3 nodes
        RaftNode.ChronosWriteResult v1Res = oldLeader.submitPutWithResult("Alice", bytes("1000")).get(5, TimeUnit.SECONDS);
        assertTrue(v1Res.committed());
        cluster.awaitCondition(5000, () -> cluster.nodes.values().stream()
                .allMatch(n -> n.catalog.getVersion("V1").isPresent()));

        // Isolate old leader from both followers
        cluster.isolateNode(oldLeaderId);

        // Speculative write on isolated old leader cannot commit to quorum
        SpeculativeManager specMgr = new SpeculativeManager(oldLeaderHarness.eventBus, new SimpleMeterRegistry());
        SpeculativeManager.SpeculativeResult orphanedSpec = specMgr.submitSpeculativePut(
                oldLeader, "Alice", bytes("9999"), SpeculativeManager.AckPolicy.SPECULATIVE);
        assertTrue(orphanedSpec.success());
        assertTrue(orphanedSpec.speculative());
        assertEquals("9999", oldLeader.getMvccStore().get("Alice", ReadMode.SPECULATIVE).getValueAsString());

        // Remaining majority elects a new leader in a higher term
        RaftNode newLeader = cluster.awaitNewLeaderExcluding(oldLeaderId, 5000);
        assertNotEquals(oldLeaderId, newLeader.getNodeId());

        // New leader commits V2 (Alice = 2000)
        RaftNode.ChronosWriteResult v2Res = newLeader.submitPutWithResult("Alice", bytes("2000")).get(5, TimeUnit.SECONDS);
        assertTrue(v2Res.committed());
        assertEquals("V2", v2Res.versionId());

        // Heal partition: old leader steps down, rolls back uncommitted 9999, truncates log, and converges on V2
        cluster.healNetwork();
        assertFalse(orphanedSpec.commitFuture().get(5, TimeUnit.SECONDS));

        cluster.awaitCondition(5000, () -> cluster.nodes.values().stream()
                .allMatch(n -> {
                    Optional<ChronosVersion> v2 = n.catalog.getVersion("V2");
                    return v2.isPresent()
                            && v2.get().isCommitted()
                            && "2000".equals(n.raftNode.getMvccStore().get("Alice", ReadMode.SPECULATIVE).getValueAsString());
                }));

        for (NodeHarness node : cluster.nodes.values()) {
            ChronosVersion v2 = node.catalog.getVersion("V2").orElseThrow();
            assertEquals(v2Res.timestamp(), v2.getTimestamp());
            assertEquals(List.of("V1"), v2.getParentVersionIds());
            assertEquals("2000", node.raftNode.getMvccStore().get("Alice", ReadMode.LINEARIZABLE).getValueAsString());
            assertEquals("2000", node.raftNode.getMvccStore().get("Alice", ReadMode.SPECULATIVE).getValueAsString());
            assertEquals("V2", node.catalog.getBranchHead("main").orElse(null));
        }
    }

    private TestCluster createCluster() {
        List<String> nodeIds = List.of("node-1", "node-2", "node-3");
        TestCluster cluster = new TestCluster(nodeIds);
        for (int i = 0; i < nodeIds.size(); i++) {
            String nodeId = nodeIds.get(i);
            List<String> peers = nodeIds.stream().filter(id -> !id.equals(nodeId)).toList();
            // Stagger timeouts so node-1 wins initial election quickly and node-2 wins on failover
            int minTimeout = (i == 0) ? 100 : (i == 1 ? 350 : 650);
            int maxTimeout = minTimeout + 80;

            RaftConfig cfg = new RaftConfig() {
                @Override public String getNodeId() { return nodeId; }
                @Override public List<String> getPeers() { return peers; }
                @Override public int getElectionTimeoutMin() { return minTimeout; }
                @Override public int getElectionTimeoutMax() { return maxTimeout; }
                @Override public int getHeartbeatInterval() { return 40; }
                @Override public int getSnapshotInterval() { return 1000; }
            };

            RocksDbEngine storage = new RocksDbEngine(tempDir.resolve(nodeId).toString());
            VersionCatalog catalog = new VersionCatalog(storage);
            EventBus eventBus = new EventBus();
            RaftNode raftNode = new RaftNode(cfg, storage, eventBus, new SimpleMeterRegistry(), catalog);
            raftNode.setBatchingEnabled(false);
            raftNode.setPeerClient(cluster.clientFor(nodeId));
            cluster.nodes.put(nodeId, new NodeHarness(raftNode, catalog, storage, eventBus));
        }

        cluster.nodes.values().forEach(n -> n.raftNode.start());
        cleanupCallbacks.add(cluster::shutdown);
        return cluster;
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private record NodeHarness(RaftNode raftNode, VersionCatalog catalog, RocksDbEngine storage, EventBus eventBus) {}

    private static final class TestCluster {
        final Map<String, NodeHarness> nodes = new LinkedHashMap<>();
        final Set<String> isolatedNodes = ConcurrentHashMap.newKeySet();

        TestCluster(List<String> nodeIds) {}

        void isolateNode(String nodeId) {
            isolatedNodes.add(nodeId);
        }

        void healNetwork() {
            isolatedNodes.clear();
        }

        TxnCoordinator txnCoordinator(String nodeId) {
            NodeHarness h = nodes.get(nodeId);
            return new TxnCoordinator(h.raftNode, h.eventBus);
        }

        RaftPeerClient clientFor(String fromNodeId) {
            return new RaftPeerClient() {
                @Override
                public CompletableFuture<RaftNode.RequestVoteResult> requestVote(
                        String peerId, long term, String candidateId, long lastLogIndex, long lastLogTerm) {
                    if (isolatedNodes.contains(fromNodeId) || isolatedNodes.contains(peerId)) {
                        return CompletableFuture.completedFuture(null);
                    }
                    NodeHarness target = nodes.get(peerId);
                    return CompletableFuture.completedFuture(
                            target.raftNode.handleRequestVote(term, candidateId, lastLogIndex, lastLogTerm));
                }

                @Override
                public CompletableFuture<RaftNode.AppendEntriesResult> appendEntries(
                        String peerId, long term, String leaderId, long prevLogIndex, long prevLogTerm,
                        List<RaftLog.Entry> entries, long leaderCommit) {
                    if (isolatedNodes.contains(fromNodeId) || isolatedNodes.contains(peerId)) {
                        return CompletableFuture.completedFuture(null);
                    }
                    // Preserve wire fidelity by round-tripping through protobuf LogEntry
                    List<RaftLog.Entry> wired = entries.stream()
                            .map(RaftProtoMapper::toProtoEntry)
                            .map(RaftProtoMapper::fromProtoEntry)
                            .toList();
                    NodeHarness target = nodes.get(peerId);
                    return CompletableFuture.completedFuture(
                            target.raftNode.handleAppendEntries(term, leaderId, prevLogIndex, prevLogTerm, wired, leaderCommit));
                }

                @Override
                public CompletableFuture<Long> installSnapshot(
                        String peerId, long term, String leaderId, long lastIncludedIndex, long lastIncludedTerm,
                        byte[] data, long offset, boolean done) {
                    if (isolatedNodes.contains(fromNodeId) || isolatedNodes.contains(peerId)) {
                        return CompletableFuture.completedFuture(-1L);
                    }
                    NodeHarness target = nodes.get(peerId);
                    return CompletableFuture.completedFuture(
                            target.raftNode.handleInstallSnapshot(term, leaderId, lastIncludedIndex, lastIncludedTerm, data, offset, done));
                }
            };
        }

        RaftNode awaitLeader(long timeoutMs) throws InterruptedException {
            return awaitLeaderMatching(n -> n.isLeader(), timeoutMs);
        }

        RaftNode awaitNewLeaderExcluding(String excludedId, long timeoutMs) throws InterruptedException {
            return awaitLeaderMatching(n -> n.isLeader() && !n.getNodeId().equals(excludedId), timeoutMs);
        }

        private RaftNode awaitLeaderMatching(java.util.function.Predicate<RaftNode> predicate, long timeoutMs)
                throws InterruptedException {
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (System.currentTimeMillis() < deadline) {
                for (NodeHarness h : nodes.values()) {
                    if (predicate.test(h.raftNode)) {
                        return h.raftNode;
                    }
                }
                Thread.sleep(20);
            }
            fail("Timed out waiting for Raft leader election");
            return null;
        }

        void awaitCondition(long timeoutMs, BooleanSupplier condition) throws InterruptedException {
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (System.currentTimeMillis() < deadline) {
                if (condition.getAsBoolean()) {
                    return;
                }
                Thread.sleep(20);
            }
            fail("Condition not satisfied within " + timeoutMs + "ms");
        }

        void shutdown() {
            nodes.values().forEach(h -> h.raftNode.stop());
            nodes.values().forEach(h -> h.storage.close());
        }
    }
}

