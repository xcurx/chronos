package com.minidb.raft;

import com.chronos.branch.BranchManager;
import com.chronos.model.ChangeEntry;
import com.chronos.model.ChronosVersion;
import com.chronos.model.ChronosWritePayload;
import com.chronos.model.OperationType;
import com.chronos.model.VersionStatus;
import com.chronos.storage.VersionCatalog;
import com.minidb.config.RaftConfig;
import com.minidb.events.ClusterEvent;
import com.minidb.events.EventBus;
import com.minidb.mvcc.MvccStore;
import com.minidb.mvcc.VersionState;
import com.minidb.storage.StorageEngine;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Core Raft state machine implementing leader election, log replication,
 * and commit tracking. Implements the Raft protocol from the paper.
 */
@Component
public class RaftNode {

    private static final Logger log = LoggerFactory.getLogger(RaftNode.class);

    public enum Role { FOLLOWER, CANDIDATE, LEADER }

    // Persistent state (saved to RocksDB)
    private volatile long currentTerm = 0;
    private volatile String votedFor = null;

    // Volatile state
    private volatile Role role = Role.FOLLOWER;
    private volatile String leaderId = null;
    private volatile long commitIndex = 0;
    private volatile long lastApplied = 0;

    // Leader state
    private final Map<String, Long> nextIndex = new ConcurrentHashMap<>();
    private final Map<String, Long> matchIndex = new ConcurrentHashMap<>();

    // Election state
    private volatile long lastHeartbeatTime = System.currentTimeMillis();
    private volatile int currentElectionTimeout;

    // Components
    private final RaftConfig config;
    private final RaftLog raftLog;
    private final MvccStore mvccStore;
    private final VersionCatalog versionCatalog;
    private final StorageEngine storage;
    private final EventBus eventBus;
    private final ElectionTimer electionTimer;
    private final SnapshotManager snapshotManager;
    private final ReentrantLock lock = new ReentrantLock();
    // Separate lightweight lock for log appends — avoids contention with main Raft lock
    private final ReentrantLock appendLock = new ReentrantLock();
    // Serializes committed log entry application across scheduler and commit callbacks
    private final Object applyLock = new Object();

    // Client request tracking: logIndex -> CompletableFuture
    private final Map<Long, CompletableFuture<Boolean>> pendingRequests = new ConcurrentHashMap<>();

    // Speculative commit & rollback tracking: logIndex -> callbacks
    private final Map<Long, Runnable> speculativeCommitCallbacks = new ConcurrentHashMap<>();
    private final Map<Long, Runnable> speculativeRollbackCallbacks = new ConcurrentHashMap<>();

    // Deterministic leader-side timestamp and version sequence allocation (protected by appendLock)
    private final AtomicLong lastAssignedTimestamp = new AtomicLong(0);
    private final AtomicLong inFlightSequence = new AtomicLong(0);
    private final Map<String, String> inFlightBranchHeads = new ConcurrentHashMap<>();

    // Peer communication (set by gRPC service)
    private volatile RaftPeerClient peerClient;
    private volatile BranchManager branchManager;

    // Chaos simulation
    private volatile boolean killed = false;
    private final Set<String> partitionedPeers = ConcurrentHashMap.newKeySet();

    // Peer health tracking: peer address -> last successful contact timestamp
    private final Map<String, Long> peerLastSeen = new ConcurrentHashMap<>();
    private final Map<String, Boolean> peerAlive = new ConcurrentHashMap<>();
    private static final long PEER_DEAD_THRESHOLD_MS = 5000; // 5 seconds without contact = dead

    // ====================================================================
    // Write Batching — Optimized Raft Baseline (Reviewer Challenge §4)
    // ====================================================================
    // Instead of calling sendHeartbeats() after every single submitCommand(),
    // we coalesce replication RPCs within a short window. Multiple concurrent
    // writes that arrive within BATCH_WINDOW_MICROS are combined into a single
    // AppendEntries RPC per peer — matching production Raft implementations
    // (etcd, CockroachDB) for a fair baseline comparison.
    //
    // When batchingEnabled=true:  submitCommand() schedules a delayed flush
    // When batchingEnabled=false: submitCommand() flushes immediately (original behavior)
    // ====================================================================
    private volatile boolean batchingEnabled = true;
    private static final long BATCH_WINDOW_MICROS = 500; // 0.5ms batch window (etcd uses ~1ms)
    private volatile ScheduledFuture<?> pendingBatchFlush = null;
    private final Object batchFlushLock = new Object();

    // Metrics
    private final Counter electionsTotal;
    private final Counter logEntriesTotal;

    // Scheduled executor for heartbeats and election timer
    private ScheduledExecutorService scheduler;

    // Startup time
    private final long startTime = System.currentTimeMillis();

    public RaftNode(RaftConfig config, StorageEngine storage, EventBus eventBus,
                    MeterRegistry meterRegistry) {
        this(config, storage, eventBus, meterRegistry, new VersionCatalog(storage));
    }

    @Autowired
    public RaftNode(RaftConfig config, StorageEngine storage, EventBus eventBus,
                    MeterRegistry meterRegistry, VersionCatalog versionCatalog) {
        this.config = config;
        this.storage = storage;
        this.eventBus = eventBus;
        this.raftLog = new RaftLog(storage);
        this.mvccStore = new MvccStore(storage, eventBus);
        this.versionCatalog = versionCatalog != null ? versionCatalog : new VersionCatalog(storage);
        this.inFlightSequence.set(this.versionCatalog.getHighestSequence());
        this.lastAssignedTimestamp.set(Math.max(System.currentTimeMillis(), this.versionCatalog.getLatestTimestamp()));
        this.mvccStore.observeTimestamp(this.lastAssignedTimestamp.get());
        this.electionTimer = new ElectionTimer(config.getElectionTimeoutMin(),
                config.getElectionTimeoutMax());
        this.snapshotManager = new SnapshotManager(storage, raftLog, config);

        // Register metrics
        this.electionsTotal = Counter.builder("raft_elections_total")
                .description("Total number of elections")
                .register(meterRegistry);
        this.logEntriesTotal = Counter.builder("raft_log_entries_total")
                .description("Total log entries appended")
                .register(meterRegistry);
        Gauge.builder("raft_term", () -> currentTerm)
                .description("Current Raft term")
                .register(meterRegistry);
        Gauge.builder("raft_commit_index", () -> commitIndex)
                .description("Current commit index")
                .register(meterRegistry);
        Gauge.builder("raft_last_applied", () -> lastApplied)
                .description("Last applied index")
                .register(meterRegistry);
    }

    @PostConstruct
    public void start() {
        // Load persisted state
        loadPersistedState();

        // Reset election timeout
        currentElectionTimeout = electionTimer.randomTimeout();
        lastHeartbeatTime = System.currentTimeMillis();

        // Start scheduler
        scheduler = Executors.newScheduledThreadPool(2, r -> {
            Thread t = Thread.ofVirtual().unstarted(r);
            t.setName("raft-scheduler-" + config.getNodeId());
            return t;
        });

        // Election timer check
        scheduler.scheduleAtFixedRate(this::checkElectionTimeout,
                100, 50, TimeUnit.MILLISECONDS);

        // Apply committed entries
        scheduler.scheduleAtFixedRate(this::applyCommittedEntries,
                100, 10, TimeUnit.MILLISECONDS);

        log.info("RaftNode started: id={}, peers={}", config.getNodeId(), config.getPeers());
    }

    @PreDestroy
    public void stop() {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
        log.info("RaftNode stopped: id={}", config.getNodeId());
    }

    public void setPeerClient(RaftPeerClient peerClient) {
        this.peerClient = peerClient;
    }

    // ============================================================
    // Leader Election
    // ============================================================

    private void checkElectionTimeout() {
        if (killed || role == Role.LEADER) return;

        long elapsed = System.currentTimeMillis() - lastHeartbeatTime;
        if (elapsed >= currentElectionTimeout) {
            startElection();
        }
    }

    private void startElection() {
        lock.lock();
        try {
            if (killed) return;

            currentTerm++;
            Role oldRole = role;
            role = Role.CANDIDATE;
            votedFor = config.getNodeId();
            persistState();

            log.info("[{}] Starting election for term {}", config.getNodeId(), currentTerm);
            eventBus.publish(ClusterEvent.electionStarted(config.getNodeId(), currentTerm));
            if (oldRole != Role.CANDIDATE) {
                eventBus.publish(ClusterEvent.raftStateChange(config.getNodeId(),
                        oldRole.name(), Role.CANDIDATE.name(), currentTerm));
            }
            electionsTotal.increment();

            currentElectionTimeout = electionTimer.randomTimeout();
            lastHeartbeatTime = System.currentTimeMillis();

            // Vote for self
            int votesReceived = 1;
            int peersCount = config.getPeers().size();
            int majority = (peersCount + 1) / 2 + 1;

            if (peersCount == 0) {
                // Single node cluster - become leader immediately
                becomeLeader();
                return;
            }

            long savedTerm = currentTerm;
            long lastLogIndex = raftLog.getLastIndex();
            long lastLogTerm = raftLog.getLastTerm();

            // Request votes from peers in parallel
            if (peerClient != null) {
                List<CompletableFuture<RequestVoteResult>> futures = new ArrayList<>();
                for (String peer : config.getPeers()) {
                    if (partitionedPeers.contains(peer)) continue;
                    futures.add(peerClient.requestVote(peer, savedTerm,
                            config.getNodeId(), lastLogIndex, lastLogTerm));
                }

                // Count votes asynchronously
                CompletableFuture.runAsync(() -> {
                    int votes = 1; // self-vote
                    for (CompletableFuture<RequestVoteResult> f : futures) {
                        try {
                            RequestVoteResult result = f.get(
                                    config.getElectionTimeoutMax(), TimeUnit.MILLISECONDS);
                            if (result != null) {
                                if (result.term() > savedTerm) {
                                    stepDown(result.term());
                                    return;
                                }
                                if (result.voteGranted()) {
                                    votes++;
                                }
                            }
                        } catch (Exception e) {
                            log.debug("[{}] Vote request failed: {}", config.getNodeId(), e.getMessage());
                        }
                    }

                    lock.lock();
                    try {
                        if (role == Role.CANDIDATE && currentTerm == savedTerm && votes >= majority) {
                            becomeLeader();
                        }
                    } finally {
                        lock.unlock();
                    }
                });
            }
        } finally {
            lock.unlock();
        }
    }

    private void becomeLeader() {
        Role oldRole = role;
        role = Role.LEADER;
        leaderId = config.getNodeId();

        log.info("[{}] Became LEADER for term {}", config.getNodeId(), currentTerm);
        eventBus.publish(ClusterEvent.leaderElected(config.getNodeId(), currentTerm));
        eventBus.publish(ClusterEvent.raftStateChange(config.getNodeId(),
                oldRole.name(), Role.LEADER.name(), currentTerm));

        // Initialize leader state
        long lastLogIdx = raftLog.getLastIndex();
        for (String peer : config.getPeers()) {
            nextIndex.put(peer, lastLogIdx + 1);
            matchIndex.put(peer, 0L);
        }

        // Append a no-op entry to commit entries from previous terms
        appendLock.lock();
        try {
            syncChronosLeaderStateLocked();
            RaftLog.Entry noop = new RaftLog.Entry(
                    raftLog.getLastIndex() + 1, currentTerm,
                    RaftLog.CommandType.NOOP, "", null);
            raftLog.append(noop);
        } finally {
            appendLock.unlock();
        }

        // Start heartbeat timer
        startHeartbeats();
    }

    private void startHeartbeats() {
        scheduler.scheduleAtFixedRate(() -> {
            if (role == Role.LEADER && !killed) {
                sendHeartbeats();
            }
        }, 0, config.getHeartbeatInterval(), TimeUnit.MILLISECONDS);
    }

    private void sendHeartbeats() {
        if (config.getPeers().isEmpty()) {
            tryAdvanceCommitIndex();
            return;
        }
        if (peerClient == null) return;

        for (String peer : config.getPeers()) {
            if (partitionedPeers.contains(peer)) continue;

            long prevLogIndex = nextIndex.getOrDefault(peer, 1L) - 1;
            long prevLogTerm = prevLogIndex > 0 ? raftLog.getTermAt(prevLogIndex) : 0;

            // Gather entries to replicate
            List<RaftLog.Entry> entries = raftLog.getEntriesFrom(
                    nextIndex.getOrDefault(peer, 1L), 100);

            long startTime = System.currentTimeMillis();
            peerClient.appendEntries(peer, currentTerm, config.getNodeId(),
                    prevLogIndex, prevLogTerm, entries, commitIndex)
                    .thenAccept(result -> {
                        long latency = System.currentTimeMillis() - startTime;
                        if (result == null) {
                            // gRPC call failed (peer returned error or is unreachable)
                            peerAlive.put(peer, false);
                            log.debug("[{}] AppendEntries to {} returned null (unreachable)",
                                    config.getNodeId(), peer);
                            return;
                        }

                        if (result.term() > currentTerm) {
                            stepDown(result.term());
                            return;
                        }

                        if (result.success()) {
                            // Mark peer as alive
                            peerLastSeen.put(peer, System.currentTimeMillis());
                            peerAlive.put(peer, true);

                            if (!entries.isEmpty()) {
                                long newMatchIndex = entries.get(entries.size() - 1).index();
                                matchIndex.put(peer, newMatchIndex);
                                nextIndex.put(peer, newMatchIndex + 1);
                                eventBus.publish(ClusterEvent.appendEntriesAcked(peer, newMatchIndex));
                                tryAdvanceCommitIndex();
                            }
                            eventBus.publish(ClusterEvent.heartbeatSent(
                                    config.getNodeId(), peer, latency));
                        } else {
                            // Peer responded but rejected — still alive
                            peerLastSeen.put(peer, System.currentTimeMillis());
                            peerAlive.put(peer, true);
                            // Decrement nextIndex and retry
                            long newNext = Math.max(1, nextIndex.getOrDefault(peer, 1L) - 1);
                            nextIndex.put(peer, newNext);
                            eventBus.publish(ClusterEvent.appendEntriesRejected(peer, result.term()));
                        }
                    })
                    .exceptionally(e -> {
                        // Peer unreachable — mark as down
                        peerAlive.put(peer, false);
                        log.debug("[{}] AppendEntries to {} failed: {}",
                                config.getNodeId(), peer, e.getMessage());
                        return null;
                    });
        }
    }

    /**
     * Try to advance commitIndex based on matchIndex values.
     */
    private void tryAdvanceCommitIndex() {
        long lastLogIndex = raftLog.getLastIndex();
        for (long n = lastLogIndex; n > commitIndex; n--) {
            if (raftLog.getTermAt(n) != currentTerm) continue;

            int replicatedCount = 1; // self
            for (String peer : config.getPeers()) {
                if (matchIndex.getOrDefault(peer, 0L) >= n) {
                    replicatedCount++;
                }
            }
            int majority = (config.getPeers().size() + 1) / 2 + 1;
            if (replicatedCount >= majority) {
                long oldCommitIndex = commitIndex;
                commitIndex = n;
                log.debug("[{}] Advanced commitIndex to {}", config.getNodeId(), commitIndex);
                // Event-driven commit application: process newly committed entries
                // immediately instead of waiting for the 10ms polling interval.
                // This is the key fix for promotion latency.
                if (commitIndex > oldCommitIndex) {
                    applyCommittedEntries();
                }
                break;
            }
        }
    }

    // ============================================================
    // RPC Handlers
    // ============================================================

    /**
     * Handle RequestVote RPC.
     */
    public RequestVoteResult handleRequestVote(long term, String candidateId,
                                                long lastLogIndex, long lastLogTerm) {
        lock.lock();
        try {
            if (killed) return new RequestVoteResult(currentTerm, false);

            if (term > currentTerm) {
                stepDown(term);
            }

            if (term < currentTerm) {
                eventBus.publish(ClusterEvent.voteRejected(config.getNodeId(), candidateId, term));
                return new RequestVoteResult(currentTerm, false);
            }

            boolean logUpToDate = isLogUpToDate(lastLogIndex, lastLogTerm);
            boolean canVote = (votedFor == null || votedFor.equals(candidateId)) && logUpToDate;

            if (canVote) {
                votedFor = candidateId;
                persistState();
                lastHeartbeatTime = System.currentTimeMillis();
                log.info("[{}] Voted for {} in term {}", config.getNodeId(), candidateId, currentTerm);
                eventBus.publish(ClusterEvent.voteGranted(config.getNodeId(), candidateId, term));
                return new RequestVoteResult(currentTerm, true);
            }

            eventBus.publish(ClusterEvent.voteRejected(config.getNodeId(), candidateId, term));
            return new RequestVoteResult(currentTerm, false);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Handle AppendEntries RPC.
     */
    public AppendEntriesResult handleAppendEntries(long term, String leaderIdIn,
                                                     long prevLogIndex, long prevLogTerm,
                                                     List<RaftLog.Entry> entries,
                                                     long leaderCommit) {
        lock.lock();
        try {
            if (killed) return new AppendEntriesResult(currentTerm, false, 0);

            if (term > currentTerm) {
                stepDown(term);
            }

            if (term < currentTerm) {
                return new AppendEntriesResult(currentTerm, false, 0);
            }

            // Valid leader heartbeat
            lastHeartbeatTime = System.currentTimeMillis();
            leaderId = leaderIdIn;

            // Track that we successfully heard from the leader
            // Find peer identifier that matches the leader
            for (String peer : config.getPeers()) {
                String peerId = peer.contains("@") ? peer.split("@")[0] : peer;
                if (peerId.equals(leaderIdIn)) {
                    peerLastSeen.put(peer, System.currentTimeMillis());
                    peerAlive.put(peer, true);
                    break;
                }
            }

            if (role != Role.FOLLOWER) {
                Role oldRole = role;
                role = Role.FOLLOWER;
                eventBus.publish(ClusterEvent.raftStateChange(config.getNodeId(),
                        oldRole.name(), Role.FOLLOWER.name(), currentTerm));
            }

            eventBus.publish(ClusterEvent.heartbeatReceived(config.getNodeId(), leaderIdIn));

            // Check log consistency
            if (prevLogIndex > 0) {
                long termAtPrev = raftLog.getTermAt(prevLogIndex);
                if (termAtPrev == -1 || termAtPrev != prevLogTerm) {
                    return new AppendEntriesResult(currentTerm, false, 0);
                }
            }

            // Append new entries
            if (entries != null && !entries.isEmpty()) {
                for (RaftLog.Entry entry : entries) {
                    long existingTerm = raftLog.getTermAt(entry.index());
                    if (existingTerm != -1 && existingTerm != entry.term()) {
                        // Conflicting entry - delete it and all after
                        raftLog.truncateFrom(entry.index());
                        mvccStore.cascadeRollback(entry.index());
                        versionCatalog.rollbackSpeculativeByRaftIndex(entry.index());
                        inFlightBranchHeads.clear();
                    }
                    if (raftLog.getTermAt(entry.index()) == -1) {
                        raftLog.append(entry);
                        logEntriesTotal.increment();
                    }
                }
            }

            // Update commit index
            if (leaderCommit > commitIndex) {
                commitIndex = Math.min(leaderCommit, raftLog.getLastIndex());
            }

            return new AppendEntriesResult(currentTerm, true, raftLog.getLastIndex());
        } finally {
            lock.unlock();
        }
    }

    /**
     * Handle InstallSnapshot RPC.
     */
    public long handleInstallSnapshot(long term, String leaderIdIn,
                                       long lastIncludedIndex, long lastIncludedTerm,
                                       byte[] data, long offset, boolean done) {
        lock.lock();
        try {
            if (killed) return currentTerm;

            if (term > currentTerm) {
                stepDown(term);
            }

            if (term < currentTerm) {
                return currentTerm;
            }

            lastHeartbeatTime = System.currentTimeMillis();
            leaderId = leaderIdIn;

            eventBus.publish(ClusterEvent.snapshotInstallProgress(
                    config.getNodeId(), offset, offset + data.length));

            snapshotManager.receiveSnapshotChunk(data, offset, done);

            if (done) {
                snapshotManager.applyReceivedSnapshot(lastIncludedIndex, lastIncludedTerm);
                versionCatalog.reloadFromStorage();
                inFlightSequence.set(versionCatalog.getHighestSequence());
                inFlightBranchHeads.clear();
                lastAssignedTimestamp.accumulateAndGet(versionCatalog.getLatestTimestamp(), Math::max);
                commitIndex = lastIncludedIndex;
                lastApplied = lastIncludedIndex;
                log.info("[{}] Snapshot installed up to index {}", config.getNodeId(), lastIncludedIndex);
            }

            return currentTerm;
        } finally {
            lock.unlock();
        }
    }

    // ============================================================
    // Client Request Handling
    // ============================================================

    /**
     * Submit a Put command. Returns a future that completes when committed.
     */
    public CompletableFuture<Boolean> submitPut(String key, byte[] value) {
        return submitPutWithResult(key, value).thenApply(ChronosWriteResult::committed);
    }

    /**
     * Submit a Put command and return the committed Chronos version metadata.
     */
    public CompletableFuture<ChronosWriteResult> submitPutWithResult(String key, byte[] value) {
        return submitCommandWithResult(RaftLog.CommandType.PUT, key, value, "main", null, null, null);
    }

    /**
     * Submit a Delete command. Returns a future that completes when committed.
     */
    public CompletableFuture<Boolean> submitDelete(String key) {
        return submitDeleteWithResult(key).thenApply(ChronosWriteResult::committed);
    }

    /**
     * Submit a Delete command and return the committed Chronos version metadata.
     */
    public CompletableFuture<ChronosWriteResult> submitDeleteWithResult(String key) {
        return submitCommandWithResult(RaftLog.CommandType.DELETE, key, null, "main", null, null, null);
    }

    /**
     * Submit a structured multi-key or branched Chronos write payload through Raft consensus.
     */
    public CompletableFuture<ChronosWriteResult> submitChronosWrite(
            RaftLog.CommandType type, ChronosWritePayload payload) {
        return submitCommandWithResult(
                type,
                payload.getTxnId(),
                payload.toJsonBytes(),
                payload.getBranchId(),
                payload.getWriterId(),
                payload.getParentVersionIds(),
                null);
    }

    private CompletableFuture<ChronosWriteResult> submitCommandWithResult(
            RaftLog.CommandType type,
            String key,
            byte[] value,
            String branchId,
            String writerId,
            List<String> explicitParents,
            String explicitVersionId) {
        if (role != Role.LEADER || killed) {
            return CompletableFuture.completedFuture(
                    ChronosWriteResult.failed("Not the leader or node is killed"));
        }

        RaftLog.Entry entry;
        CompletableFuture<Boolean> commitFuture;
        appendLock.lock();
        try {
            if (role != Role.LEADER || killed) {
                return CompletableFuture.completedFuture(
                        ChronosWriteResult.failed("Not the leader or node is killed"));
            }
            entry = buildAndAppendEntryLocked(
                    type, key, value, branchId, writerId, explicitParents, explicitVersionId);
            commitFuture = new CompletableFuture<>();
            pendingRequests.put(entry.index(), commitFuture);
        } finally {
            appendLock.unlock();
        }

        if (batchingEnabled) {
            scheduleBatchFlush();
        } else {
            sendHeartbeats();
        }

        CompletableFuture.delayedExecutor(5, TimeUnit.SECONDS)
                .execute(() -> commitFuture.completeExceptionally(
                        new TimeoutException("Command not committed within timeout")));

        return commitFuture.thenApply(committed -> committed != null && committed
                ? ChronosWriteResult.committed(entry.versionId(), entry.versionSequence(),
                        entry.timestamp(), entry.index(), entry.branchId())
                : ChronosWriteResult.failed("Raft commit rejected or lost leadership"));
    }

    /**
     * Schedule a coalesced replication flush within BATCH_WINDOW_MICROS.
     * If a flush is already scheduled, skip — the pending flush will pick up
     * all entries accumulated since the last flush.
     *
     * <p>This models production Raft implementations (etcd batches at ~1ms)
     * and ensures baseline benchmark fairness (Reviewer Challenge §4).</p>
     */
    private void scheduleBatchFlush() {
        synchronized (batchFlushLock) {
            if (pendingBatchFlush == null || pendingBatchFlush.isDone()) {
                pendingBatchFlush = scheduler.schedule(() -> {
                    if (role == Role.LEADER && !killed) {
                        sendHeartbeats();
                    }
                }, BATCH_WINDOW_MICROS, TimeUnit.MICROSECONDS);
            }
        }
    }

    /**
     * Confirm Raft leadership and block until the local state machine has applied all
     * entries up to the ReadIndex captured at read start.
     */
    public CompletableFuture<Void> awaitLinearizableReadBarrier() {
        if (role != Role.LEADER) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Not the leader"));
        }

        long readIndex = commitIndex;
        return confirmLeadership().thenAccept(confirmed -> {
            if (!confirmed || role != Role.LEADER) {
                throw new CompletionException(
                        new IllegalStateException("Lost leadership during read"));
            }
            while (lastApplied < readIndex) {
                if (killed || role != Role.LEADER) {
                    throw new CompletionException(
                            new IllegalStateException("Node stepped down before applying ReadIndex " + readIndex));
                }
                Thread.onSpinWait();
            }
        });
    }

    /**
     * Linearizable read using ReadIndex protocol.
     */
    public CompletableFuture<MvccStore.MvccResult> consistentGet(String key) {
        return awaitLinearizableReadBarrier().thenApply(v -> mvccStore.get(key));
    }

    /**
     * Confirm leadership by getting acknowledgment from majority.
     */
    public CompletableFuture<Boolean> confirmLeadership() {
        if (config.getPeers().isEmpty()) {
            return CompletableFuture.completedFuture(true);
        }

        if (peerClient == null) {
            return CompletableFuture.completedFuture(role == Role.LEADER);
        }

        List<CompletableFuture<Boolean>> heartbeatFutures = new ArrayList<>();
        for (String peer : config.getPeers()) {
            if (partitionedPeers.contains(peer)) continue;

            long prevLogIndex = nextIndex.getOrDefault(peer, 1L) - 1;
            long prevLogTerm = prevLogIndex > 0 ? raftLog.getTermAt(prevLogIndex) : 0;

            heartbeatFutures.add(
                    peerClient.appendEntries(peer, currentTerm, config.getNodeId(),
                                    prevLogIndex, prevLogTerm, List.of(), commitIndex)
                            .thenApply(result -> result != null && result.success())
                            .exceptionally(e -> false)
            );
        }

        return CompletableFuture.allOf(heartbeatFutures.toArray(new CompletableFuture[0]))
                .thenApply(v -> {
                    int acks = 1; // self
                    for (CompletableFuture<Boolean> f : heartbeatFutures) {
                        try {
                            if (f.get()) acks++;
                        } catch (Exception e) {
                            // ignore
                        }
                    }
                    int majority = (config.getPeers().size() + 1) / 2 + 1;
                    return acks >= majority;
                });
    }

    // ============================================================
    // Speculative Write Infrastructure
    // ============================================================

    /**
     * Append a command to the Raft log WITHOUT blocking for replication.
     * Used by SpeculativeManager to get the log index before writing to MVCC.
     *
     * <p>Uses a dedicated {@code appendLock} instead of the main Raft lock to
     * minimize contention on the speculative hot path. The main lock is used
     * for state transitions (elections, stepDown) which are rare; appends are
     * the high-frequency operation.</p>
     *
     * @return the log index, or -1 if not leader/killed
     */
    public long appendToLog(RaftLog.CommandType type, String key, byte[] value) {
        RaftLog.Entry entry = appendChronosEntry(type, key, value, "main", null, null, null);
        return entry != null ? entry.index() : -1;
    }

    /**
     * Append a command to the Raft log with deterministic leader-assigned timestamp
     * and Chronos version metadata WITHOUT blocking for replication.
     *
     * @return the appended {@link RaftLog.Entry}, or {@code null} if not leader/killed
     */
    public RaftLog.Entry appendChronosEntry(
            RaftLog.CommandType type,
            String key,
            byte[] value,
            String branchId,
            String writerId,
            List<String> explicitParents,
            String explicitVersionId) {
        if (role != Role.LEADER || killed) return null;

        appendLock.lock();
        try {
            if (role != Role.LEADER || killed) return null;
            return buildAndAppendEntryLocked(
                    type, key, value, branchId, writerId, explicitParents, explicitVersionId);
        } finally {
            appendLock.unlock();
        }
    }

    private RaftLog.Entry buildAndAppendEntryLocked(
            RaftLog.CommandType type,
            String key,
            byte[] value,
            String branchId,
            String writerId,
            List<String> explicitParents,
            String explicitVersionId) {
        String effectiveBranch = (branchId != null && !branchId.isBlank()) ? branchId : "main";
        long seq = Math.max(inFlightSequence.get(), versionCatalog.getHighestSequence()) + 1;
        inFlightSequence.set(seq);

        String versionId = (explicitVersionId != null && !explicitVersionId.isBlank())
                ? explicitVersionId
                : ("main".equals(effectiveBranch) ? "V" + seq : effectiveBranch + "-V" + seq);

        List<String> parents;
        if (explicitParents != null && !explicitParents.isEmpty()) {
            parents = List.copyOf(explicitParents);
        } else {
            String head = inFlightBranchHeads.get(effectiveBranch);
            if (head == null) {
                head = versionCatalog.getBranchHead(effectiveBranch).orElse(null);
            }
            if (head == null && branchManager != null) {
                head = branchManager.getBranch(effectiveBranch)
                        .map(b -> b.getHeadVersionId() != null ? b.getHeadVersionId() : b.getBaseVersionId())
                        .orElse(null);
            }
            parents = (head != null && !head.isBlank()) ? List.of(head) : List.of();
        }

        long now = System.currentTimeMillis();
        long prevTs = Math.max(lastAssignedTimestamp.get(), versionCatalog.getLatestTimestamp());
        long ts = Math.max(now, prevTs + 1);
        lastAssignedTimestamp.set(ts);
        mvccStore.observeTimestamp(ts);

        long index = raftLog.getLastIndex() + 1;
        RaftLog.Entry entry = new RaftLog.Entry(
                index, currentTerm, type, key, value, ts,
                versionId, seq, effectiveBranch, writerId, parents);
        raftLog.append(entry);
        logEntriesTotal.increment();
        inFlightBranchHeads.put(effectiveBranch, versionId);
        return entry;
    }

    private void syncChronosLeaderStateLocked() {
        inFlightBranchHeads.clear();
        inFlightSequence.set(versionCatalog.getHighestSequence());
        long maxTs = Math.max(System.currentTimeMillis(), versionCatalog.getLatestTimestamp());
        lastAssignedTimestamp.accumulateAndGet(maxTs, Math::max);
        mvccStore.observeTimestamp(lastAssignedTimestamp.get());
    }

    /**
     * Register a callback to be invoked when a speculative write at the given
     * log index is committed by Raft consensus (majority ACK).
     */
    public void trackSpeculativeCommit(long logIndex, Runnable onCommit) {
        trackSpeculativeCommit(logIndex, onCommit, null);
    }

    /**
     * Register commit and optional rollback callbacks for a speculative write.
     */
    public void trackSpeculativeCommit(long logIndex, Runnable onCommit, Runnable onRollback) {
        speculativeCommitCallbacks.put(logIndex, onCommit);
        if (onRollback != null) {
            speculativeRollbackCallbacks.put(logIndex, onRollback);
        }

        // Fix race: entry may already be committed + applied before we registered.
        if (logIndex <= lastApplied) {
            Runnable cb = speculativeCommitCallbacks.remove(logIndex);
            speculativeRollbackCallbacks.remove(logIndex);
            if (cb != null) {
                try {
                    cb.run();
                } catch (Exception e) {
                    log.error("[{}] Late speculative callback fired at index {}: {}",
                            config.getNodeId(), logIndex, e.getMessage());
                }
            }
        }
    }

    /**
     * Trigger coalesced replication to all peers (non-blocking).
     * Called by SpeculativeManager after appending a speculative entry.
     */
    public void triggerReplication() {
        if (role == Role.LEADER && !killed) {
            scheduleBatchFlush();
        }
    }

    // ============================================================
    // State Machine Application
    // ============================================================

    private void applyCommittedEntries() {
        if (killed) return;

        synchronized (applyLock) {
            while (!killed && lastApplied < commitIndex) {
                lastApplied++;
                RaftLog.Entry entry = raftLog.getEntry(lastApplied);
                if (entry == null) {
                    lastApplied--;
                    break;
                }

                // Fire speculative commit callback INSTEAD of applying to MVCC
                // (speculative path already wrote the value — just promote it)
                Runnable specCallback = speculativeCommitCallbacks.remove(entry.index());
                speculativeRollbackCallbacks.remove(entry.index());
                if (specCallback != null) {
                    try {
                        specCallback.run();
                    } catch (Exception e) {
                        log.error("[{}] Speculative callback failed at index {}: {}",
                                config.getNodeId(), entry.index(), e.getMessage());
                        // Callback failed but entry IS committed — apply normally as fallback
                        applyEntry(entry);
                    }
                } else {
                    // Standard path: apply deterministically to MVCC + VersionCatalog
                    applyEntry(entry);
                }

                // Complete pending client request
                CompletableFuture<Boolean> future = pendingRequests.remove(entry.index());
                if (future != null) {
                    future.complete(true);
                }
            }
        }
    }

    private void applyEntry(RaftLog.Entry entry) {
        if (entry.type() == RaftLog.CommandType.NOOP) {
            log.debug("[{}] Applied NOOP at index {}", config.getNodeId(), entry.index());
            return;
        }

        long ts = entry.timestamp() > 0 ? entry.timestamp() : mvccStore.nextTimestamp();
        boolean hasVersionMetadata = entry.versionId() != null
                && !entry.versionId().isBlank()
                && entry.versionSequence() >= 1;

        if (hasVersionMetadata) {
            Optional<ChronosVersion> existing = versionCatalog.getVersion(entry.versionId());
            if (existing.isPresent()
                    && existing.get().isCommitted()
                    && existing.get().getRaftLogIndex() == entry.index()) {
                mvccStore.observeTimestamp(ts);
                return;
            }
        }

        switch (entry.type()) {
            case PUT -> {
                if (entry.key() != null && entry.value() != null) {
                    byte[] oldVal = hasVersionMetadata ? mvccStore.getLatestLinearizableBytes(entry.key()) : null;
                    mvccStore.put(entry.key(), entry.value(), ts);
                    if (hasVersionMetadata) {
                        persistSingleChangeVersion(entry, ts,
                                ChangeEntry.put(entry.key(), oldVal, entry.value(), entry.writerId()));
                    }
                    log.debug("[{}] Applied PUT: key={}, ts={}", config.getNodeId(), entry.key(), ts);
                }
            }
            case DELETE -> {
                if (entry.key() != null) {
                    byte[] oldVal = hasVersionMetadata ? mvccStore.getLatestLinearizableBytes(entry.key()) : null;
                    mvccStore.delete(entry.key(), ts);
                    if (hasVersionMetadata) {
                        persistSingleChangeVersion(entry, ts,
                                ChangeEntry.delete(entry.key(), oldVal, entry.writerId()));
                    }
                    log.debug("[{}] Applied DELETE: key={}, ts={}", config.getNodeId(), entry.key(), ts);
                }
            }
            case CHRONOS_WRITE, CHRONOS_TXN, CHRONOS_BRANCH_WRITE, CHRONOS_MERGE -> {
                if (entry.value() == null) break;
                ChronosWritePayload payload = ChronosWritePayload.fromJsonBytes(entry.value());
                String branch = !entry.branchId().isBlank() ? entry.branchId() : payload.getBranchId();
                List<String> parents = !entry.parentVersionIds().isEmpty()
                        ? entry.parentVersionIds()
                        : payload.getParentVersionIds();
                List<ChangeEntry> changes = captureChangeEntries(payload, branch, parents);
                if ("main".equals(branch)) {
                    mvccStore.writeBatchAt(payload.getOperations(), ts, VersionState.COMMITTED);
                }

                if (hasVersionMetadata) {
                    String writer = payload.getWriterId() != null ? payload.getWriterId() : entry.writerId();
                    ChronosVersion version = ChronosVersion.builder()
                            .versionId(entry.versionId())
                            .sequence(entry.versionSequence())
                            .parentVersionIds(parents)
                            .branchId(branch)
                            .timestamp(ts)
                            .raftLogIndex(entry.index())
                            .writerId(writer)
                            .transactionId(payload.getTxnId())
                            .status(VersionStatus.COMMITTED)
                            .isMerge(payload.isMerge())
                            .mergePolicy(payload.getMergePolicy())
                            .conflictMetadata(payload.getConflictMetadata())
                            .changeSet(changes)
                            .build();
                    versionCatalog.saveCommittedFromRaft(version);
                    if (branchManager != null) {
                        branchManager.recordCommitToBranch(branch, entry.versionId(), writer);
                    }
                    if (eventBus != null) {
                        eventBus.publish(ClusterEvent.chronosVersionCreated(
                                config.getNodeId(), entry.versionId(), entry.versionSequence(), branch, changes.size()));
                    }
                }
                log.debug("[{}] Applied {}: versionId={}, ops={}, ts={}",
                        config.getNodeId(), entry.type(), entry.versionId(),
                        payload.getOperations().size(), ts);
            }
            default -> {
                log.debug("[{}] Skipping entry type: {}", config.getNodeId(), entry.type());
            }
        }
    }

    private void persistSingleChangeVersion(RaftLog.Entry entry, long ts, ChangeEntry change) {
        ChronosVersion version = ChronosVersion.builder()
                .versionId(entry.versionId())
                .sequence(entry.versionSequence())
                .parentVersionIds(entry.parentVersionIds())
                .branchId(entry.branchId())
                .timestamp(ts)
                .raftLogIndex(entry.index())
                .writerId(entry.writerId())
                .status(VersionStatus.COMMITTED)
                .changeSet(List.of(change))
                .build();
        versionCatalog.saveCommittedFromRaft(version);
        if (branchManager != null) {
            branchManager.recordCommitToBranch(entry.branchId(), entry.versionId(), entry.writerId());
        }
        if (eventBus != null) {
            eventBus.publish(ClusterEvent.chronosVersionCreated(
                    config.getNodeId(), entry.versionId(), entry.versionSequence(), entry.branchId(), 1));
        }
    }

    private List<ChangeEntry> captureChangeEntries(ChronosWritePayload payload, String branch, List<String> parents) {
        List<ChangeEntry> changes = new ArrayList<>(payload.getOperations().size());
        Map<String, Optional<byte[]>> priorValues = new HashMap<>();
        for (ChronosWritePayload.WriteOp op : payload.getOperations()) {
            byte[] oldVal = priorValues.computeIfAbsent(
                            op.getKey(),
                            k -> Optional.ofNullable(resolvePriorValueForBranch(k, branch, parents)))
                    .orElse(null);
            switch (op.getOpType()) {
                case PUT -> {
                    changes.add(ChangeEntry.put(op.getKey(), oldVal, op.getValue(), payload.getWriterId()));
                    priorValues.put(op.getKey(), Optional.ofNullable(op.getValue()));
                }
                case DELETE -> {
                    changes.add(ChangeEntry.delete(op.getKey(), oldVal, payload.getWriterId()));
                    priorValues.put(op.getKey(), Optional.empty());
                }
                case DELTA -> {
                    long delta = op.getDeltaAmount() != null ? op.getDeltaAmount() : 0L;
                    changes.add(ChangeEntry.delta(op.getKey(), oldVal, op.getValue(), delta, payload.getWriterId()));
                    priorValues.put(op.getKey(), Optional.ofNullable(op.getValue()));
                }
            }
        }
        return changes;
    }

    private byte[] resolvePriorValueForBranch(String key, String branch, List<String> parents) {
        if ("main".equals(branch)) {
            return mvccStore.getLatestLinearizableBytes(key);
        }
        if (parents != null && !parents.isEmpty()) {
            return resolveKeyAtVersion(key, parents.get(0));
        }
        if (branchManager != null) {
            String headOrBase = branchManager.getBranch(branch)
                    .map(b -> b.getHeadVersionId() != null ? b.getHeadVersionId() : b.getBaseVersionId())
                    .orElse(null);
            if (headOrBase != null) {
                return resolveKeyAtVersion(key, headOrBase);
            }
        }
        return mvccStore.getLatestLinearizableBytes(key);
    }

    private byte[] resolveKeyAtVersion(String key, String versionId) {
        Deque<String> queue = new ArrayDeque<>();
        Set<String> visited = new HashSet<>();
        queue.add(versionId);
        visited.add(versionId);

        while (!queue.isEmpty()) {
            String currId = queue.poll();
            Optional<ChronosVersion> verOpt = versionCatalog.getVersion(currId);
            if (verOpt.isEmpty()) continue;
            ChronosVersion v = verOpt.get();
            for (int i = v.getChangeSet().size() - 1; i >= 0; i--) {
                ChangeEntry change = v.getChangeSet().get(i);
                if (key.equals(change.getKey())) {
                    if (change.getOpType() == OperationType.DELETE || !change.isNewPresent()) {
                        return null;
                    }
                    return change.getNewValue();
                }
            }
            for (String p : v.getParentVersionIds()) {
                if (p != null && !p.isBlank() && visited.add(p)) {
                    queue.add(p);
                }
            }
        }
        return mvccStore.getLatestLinearizableBytes(key);
    }

    // ============================================================
    // State Transitions
    // ============================================================

    private void stepDown(long newTerm) {
        Role oldRole = role;
        currentTerm = newTerm;
        role = Role.FOLLOWER;
        votedFor = null;
        leaderId = null;
        persistState();

        inFlightBranchHeads.clear();
        inFlightSequence.set(versionCatalog.getHighestSequence());

        // Cascade rollback: all speculative entries above commitIndex are invalid
        if (oldRole == Role.LEADER) {
            mvccStore.cascadeRollback(commitIndex + 1);
            versionCatalog.rollbackSpeculativeByRaftIndex(commitIndex + 1);

            if (!speculativeRollbackCallbacks.isEmpty()) {
                log.info("[{}] Leadership lost — running rollback callbacks above commitIndex {}",
                        config.getNodeId(), commitIndex);
                speculativeRollbackCallbacks.forEach((idx, rb) -> {
                    if (idx > commitIndex && rb != null) {
                        try {
                            rb.run();
                        } catch (Exception ignored) {
                            // Best effort
                        }
                    }
                });
            }
            speculativeRollbackCallbacks.clear();
            speculativeCommitCallbacks.clear();

            pendingRequests.forEach((idx, future) -> future.complete(false));
            pendingRequests.clear();
        }

        if (oldRole != Role.FOLLOWER) {
            log.info("[{}] Stepping down to FOLLOWER (term {})", config.getNodeId(), newTerm);
            eventBus.publish(ClusterEvent.raftStateChange(config.getNodeId(),
                    oldRole.name(), Role.FOLLOWER.name(), currentTerm));
        }
    }

    private boolean isLogUpToDate(long lastLogIndex, long lastLogTerm) {
        long myLastTerm = raftLog.getLastTerm();
        long myLastIndex = raftLog.getLastIndex();

        if (lastLogTerm != myLastTerm) {
            return lastLogTerm > myLastTerm;
        }
        return lastLogIndex >= myLastIndex;
    }

    // ============================================================
    // Persistence
    // ============================================================

    private void persistState() {
        try {
            storage.put("__raft_term__".getBytes(StandardCharsets.UTF_8),
                    ByteBuffer.allocate(8).putLong(currentTerm).array());
            if (votedFor != null) {
                storage.put("__raft_voted_for__".getBytes(StandardCharsets.UTF_8),
                        votedFor.getBytes(StandardCharsets.UTF_8));
            } else {
                storage.delete("__raft_voted_for__".getBytes(StandardCharsets.UTF_8));
            }
        } catch (Exception e) {
            log.error("Failed to persist Raft state", e);
        }
    }

    private void loadPersistedState() {
        try {
            byte[] termBytes = storage.get("__raft_term__".getBytes(StandardCharsets.UTF_8));
            if (termBytes != null) {
                currentTerm = ByteBuffer.wrap(termBytes).getLong();
            }
            byte[] votedForBytes = storage.get("__raft_voted_for__".getBytes(StandardCharsets.UTF_8));
            if (votedForBytes != null) {
                votedFor = new String(votedForBytes, StandardCharsets.UTF_8);
            }
            log.info("[{}] Loaded persisted state: term={}, votedFor={}",
                    config.getNodeId(), currentTerm, votedFor);
        } catch (Exception e) {
            log.warn("Failed to load persisted state, starting fresh", e);
        }
    }

    // ============================================================
    // Chaos Engineering
    // ============================================================

    public void kill() {
        killed = true;
        eventBus.publish(ClusterEvent.nodeDown(config.getNodeId()));
        log.info("[{}] Node KILLED (simulated)", config.getNodeId());
    }

    public void recover() {
        killed = false;
        lastHeartbeatTime = System.currentTimeMillis();
        currentElectionTimeout = electionTimer.randomTimeout();
        eventBus.publish(ClusterEvent.nodeRecovered(config.getNodeId()));
        log.info("[{}] Node RECOVERED", config.getNodeId());
    }

    public void addPartition(String peerId) {
        partitionedPeers.add(peerId);
        eventBus.publish(ClusterEvent.chaosAction("PARTITION", peerId));
    }

    public void removePartition(String peerId) {
        partitionedPeers.remove(peerId);
        eventBus.publish(ClusterEvent.chaosAction("HEAL_PARTITION", peerId));
    }

    public boolean isPartitioned(String peerId) {
        return partitionedPeers.contains(peerId);
    }

    // ============================================================
    // Getters
    // ============================================================

    public String getNodeId() { return config.getNodeId(); }
    public long getCurrentTerm() { return currentTerm; }
    public Role getRole() { return role; }
    public String getLeaderId() { return leaderId; }
    public long getCommitIndex() { return commitIndex; }
    public long getLastApplied() { return lastApplied; }
    public String getVotedFor() { return votedFor; }
    public boolean isKilled() { return killed; }
    public boolean isLeader() { return role == Role.LEADER && !killed; }
    public RaftLog getRaftLog() { return raftLog; }
    public MvccStore getMvccStore() { return mvccStore; }
    public VersionCatalog getVersionCatalog() { return versionCatalog; }
    public SnapshotManager getSnapshotManager() { return snapshotManager; }
    public BranchManager getBranchManager() { return branchManager; }

    @Autowired(required = false)
    public void setBranchManager(BranchManager branchManager) {
        this.branchManager = branchManager;
    }
    public Map<String, Long> getNextIndex() { return Collections.unmodifiableMap(nextIndex); }
    public Map<String, Long> getMatchIndex() { return Collections.unmodifiableMap(matchIndex); }
    public Set<String> getPartitionedPeers() { return Collections.unmodifiableSet(partitionedPeers); }
    public long getUptime() { return System.currentTimeMillis() - startTime; }
    public List<String> getPeers() { return config.getPeers(); }

    /**
     * Check if a peer is alive based on heartbeat tracking and chaos notifications.
     * All roles first check the explicit peerAlive map (set by chaos kill/recover
     * broadcasts). Leaders additionally use heartbeat response tracking.
     */
    public boolean isPeerAlive(String peer) {
        // First check explicit alive/dead marking (from chaos operations — works for ALL roles)
        Boolean alive = peerAlive.get(peer);
        if (alive != null) return alive;

        // If we're the leader, also use heartbeat response tracking
        if (role == Role.LEADER) {
            Long lastSeen = peerLastSeen.get(peer);
            if (lastSeen == null) return true; // assume alive until proven otherwise
            return (System.currentTimeMillis() - lastSeen) < PEER_DEAD_THRESHOLD_MS;
        }

        // For followers with no explicit info, assume alive unless partitioned
        return !partitionedPeers.contains(peer);
    }

    public Map<String, Long> getPeerLastSeen() { return Collections.unmodifiableMap(peerLastSeen); }
    public Map<String, Boolean> getPeerAliveMap() { return Collections.unmodifiableMap(peerAlive); }

    // Write batching control (Reviewer Challenge §4: Baseline Fairness)
    public boolean isBatchingEnabled() { return batchingEnabled; }
    public void setBatchingEnabled(boolean enabled) {
        this.batchingEnabled = enabled;
        log.info("[{}] Write batching: {}", config.getNodeId(), enabled ? "ENABLED" : "DISABLED");
    }

    /**
     * Immediately mark a peer as dead — used after a chaos kill is forwarded so the
     * topology reflects the change without waiting for the next heartbeat cycle.
     */
    public void markPeerDead(String peer) {
        peerAlive.put(peer, false);
        log.info("[{}] Peer {} marked as dead (chaos kill)", config.getNodeId(), peer);
    }

    /**
     * Mark a peer as alive — used after a chaos recover is forwarded so the
     * topology reflects the change immediately.
     */
    public void markPeerAlive(String peer) {
        peerAlive.put(peer, true);
        peerLastSeen.put(peer, System.currentTimeMillis());
        log.info("[{}] Peer {} marked as alive (chaos recover)", config.getNodeId(), peer);
    }

    // Record types for RPC results
    public record RequestVoteResult(long term, boolean voteGranted) {}
    public record AppendEntriesResult(long term, boolean success, long matchIndex) {}

    public record ChronosWriteResult(
            boolean committed,
            String versionId,
            long versionSequence,
            long timestamp,
            long raftLogIndex,
            String branchId,
            String error) {

        public static ChronosWriteResult committed(
                String versionId, long versionSequence, long timestamp, long raftLogIndex, String branchId) {
            return new ChronosWriteResult(true, versionId, versionSequence, timestamp, raftLogIndex, branchId, null);
        }

        public static ChronosWriteResult failed(String error) {
            return new ChronosWriteResult(false, null, 0L, 0L, -1L, "main", error);
        }
    }
}
