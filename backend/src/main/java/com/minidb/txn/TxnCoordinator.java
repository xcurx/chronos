package com.minidb.txn;

import com.chronos.model.ChronosWritePayload;
import com.minidb.events.ClusterEvent;
import com.minidb.events.EventBus;
import com.minidb.raft.RaftLog;
import com.minidb.raft.RaftNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.*;

/**
 * Two-Phase Commit (2PC) transaction coordinator.
 * Orchestrates atomic multi-key transactions as a single deterministic ChronosVersion.
 */
@Component
public class TxnCoordinator {

    private static final Logger log = LoggerFactory.getLogger(TxnCoordinator.class);

    private final RaftNode raftNode;
    private final EventBus eventBus;

    // Active transactions
    private final Map<String, TxnState> transactions = new ConcurrentHashMap<>();

    public enum TxnStatus {
        PENDING, PREPARED, COMMITTED, ABORTED
    }

    public record TxnOperation(String key, byte[] value, OpType type, Long deltaAmount) {
        public enum OpType { PUT, DELETE, DELTA }

        public TxnOperation(String key, byte[] value, OpType type) {
            this(key, value, type, null);
        }
    }

    public record TxnCommitResult(
            boolean committed,
            String txnId,
            String versionId,
            long versionSequence,
            long timestamp,
            long raftLogIndex,
            String branchId,
            String error) {}

    private static class TxnState {
        final String txnId;
        final List<TxnOperation> operations;
        final String branchId;
        final String writerId;
        volatile TxnStatus status;
        volatile String versionId;
        volatile long versionSequence;
        volatile long timestamp;
        volatile long raftLogIndex;
        final long createdAt;

        TxnState(String txnId, List<TxnOperation> operations, String branchId, String writerId) {
            this.txnId = txnId;
            this.operations = operations;
            this.branchId = (branchId != null && !branchId.isBlank()) ? branchId : "main";
            this.writerId = writerId;
            this.status = TxnStatus.PENDING;
            this.createdAt = System.currentTimeMillis();
        }
    }

    public TxnCoordinator(RaftNode raftNode, EventBus eventBus) {
        this.raftNode = raftNode;
        this.eventBus = eventBus;
    }

    /**
     * Begin a new transaction with the given operations on the canonical branch.
     * @return transaction ID
     */
    public String begin(List<TxnOperation> operations) {
        return begin(operations, "main", null);
    }

    public String begin(List<TxnOperation> operations, String branchId, String writerId) {
        String txnId = UUID.randomUUID().toString();
        transactions.put(txnId, new TxnState(txnId, operations, branchId, writerId));
        log.info("Transaction {} begun with {} operations on branch={}", txnId, operations.size(), branchId);
        return txnId;
    }

    /**
     * Execute a complete 2PC transaction.
     * Phase 1: Prepare all operations
     * Phase 2: Commit or abort atomically
     */
    public CompletableFuture<Boolean> execute(List<TxnOperation> operations) {
        return executeWithResult(operations, "main", null).thenApply(TxnCommitResult::committed);
    }

    /**
     * Execute a complete 2PC transaction and return full Chronos commit metadata.
     */
    public CompletableFuture<TxnCommitResult> executeWithResult(
            List<TxnOperation> operations, String branchId, String writerId) {
        String txnId = begin(operations, branchId, writerId);
        return prepare(txnId).thenCompose(prepared -> {
            if (prepared) {
                return commitWithResult(txnId);
            } else {
                return abort(txnId).thenApply(v -> new TxnCommitResult(
                        false, txnId, null, 0L, 0L, -1L, branchId, "Transaction prepare failed"));
            }
        });
    }

    /**
     * Phase 1: Prepare. Validate all operations can be applied.
     */
    public CompletableFuture<Boolean> prepare(String txnId) {
        TxnState txn = transactions.get(txnId);
        if (txn == null) {
            return CompletableFuture.completedFuture(false);
        }

        if (!raftNode.isLeader()) {
            log.warn("Cannot prepare transaction: not the leader");
            return CompletableFuture.completedFuture(false);
        }

        log.info("Preparing transaction {} ({} operations)", txnId, txn.operations.size());

        // In a single-shard setup, prepare means validating and locking
        // all keys can be written
        try {
            for (TxnOperation op : txn.operations) {
                eventBus.publish(ClusterEvent.txnPhase(txnId, "PREPARE", op.key(), true));
            }
            txn.status = TxnStatus.PREPARED;
            return CompletableFuture.completedFuture(true);
        } catch (Exception e) {
            log.error("Transaction {} prepare failed", txnId, e);
            txn.status = TxnStatus.ABORTED;
            return CompletableFuture.completedFuture(false);
        }
    }

    /**
     * Phase 2: Commit. Submit all operations atomically as one CHRONOS_TXN Raft entry.
     */
    public CompletableFuture<Boolean> commit(String txnId) {
        return commitWithResult(txnId).thenApply(TxnCommitResult::committed);
    }

    /**
     * Phase 2: Commit and return the created ChronosVersion metadata.
     */
    public CompletableFuture<TxnCommitResult> commitWithResult(String txnId) {
        TxnState txn = transactions.get(txnId);
        if (txn == null || txn.status != TxnStatus.PREPARED) {
            return CompletableFuture.completedFuture(
                    new TxnCommitResult(false, txnId, null, 0L, 0L, -1L, "main", "Transaction not prepared"));
        }

        log.info("Committing transaction {} ({} operations) atomically", txnId, txn.operations.size());

        List<ChronosWritePayload.WriteOp> writeOps = new ArrayList<>(txn.operations.size());
        for (TxnOperation op : txn.operations) {
            switch (op.type()) {
                case PUT -> writeOps.add(ChronosWritePayload.WriteOp.put(op.key(), op.value()));
                case DELETE -> writeOps.add(ChronosWritePayload.WriteOp.delete(op.key()));
                case DELTA -> writeOps.add(ChronosWritePayload.WriteOp.delta(
                        op.key(), op.value(), op.deltaAmount() != null ? op.deltaAmount() : 0L));
            }
        }

        ChronosWritePayload payload = ChronosWritePayload.builder()
                .txnId(txnId)
                .branchId(txn.branchId)
                .writerId(txn.writerId)
                .operations(writeOps)
                .build();

        return raftNode.submitChronosWrite(RaftLog.CommandType.CHRONOS_TXN, payload)
                .thenApply(writeResult -> {
                    boolean committed = writeResult != null && writeResult.committed();
                    if (committed) {
                        txn.status = TxnStatus.COMMITTED;
                        txn.versionId = writeResult.versionId();
                        txn.versionSequence = writeResult.versionSequence();
                        txn.timestamp = writeResult.timestamp();
                        txn.raftLogIndex = writeResult.raftLogIndex();
                        for (TxnOperation op : txn.operations) {
                            eventBus.publish(ClusterEvent.txnPhase(txnId, "COMMIT", op.key(), true));
                        }
                        log.info("Transaction {} committed atomically as version {} (ts={})",
                                txnId, writeResult.versionId(), writeResult.timestamp());
                        return new TxnCommitResult(true, txnId, writeResult.versionId(),
                                writeResult.versionSequence(), writeResult.timestamp(),
                                writeResult.raftLogIndex(), writeResult.branchId(), null);
                    } else {
                        txn.status = TxnStatus.ABORTED;
                        for (TxnOperation op : txn.operations) {
                            eventBus.publish(ClusterEvent.txnPhase(txnId, "ABORT", op.key(), false));
                        }
                        String err = writeResult != null ? writeResult.error() : "Raft commit rejected";
                        log.warn("Transaction {} commit failed: {}", txnId, err);
                        return new TxnCommitResult(false, txnId, null, 0L, 0L, -1L, txn.branchId, err);
                    }
                });
    }

    /**
     * Abort a transaction.
     */
    public CompletableFuture<Boolean> abort(String txnId) {
        TxnState txn = transactions.get(txnId);
        if (txn == null) {
            return CompletableFuture.completedFuture(false);
        }

        txn.status = TxnStatus.ABORTED;
        for (TxnOperation op : txn.operations) {
            eventBus.publish(ClusterEvent.txnPhase(txnId, "ABORT", op.key(), true));
        }
        log.info("Transaction {} aborted", txnId);
        return CompletableFuture.completedFuture(true);
    }

    /**
     * Get the status of a transaction.
     */
    public TxnStatus getStatus(String txnId) {
        TxnState txn = transactions.get(txnId);
        return txn != null ? txn.status : null;
    }

    /**
     * Get all active transactions.
     */
    public Map<String, TxnStatus> getActiveTransactions() {
        Map<String, TxnStatus> result = new LinkedHashMap<>();
        for (Map.Entry<String, TxnState> entry : transactions.entrySet()) {
            result.put(entry.getKey(), entry.getValue().status);
        }
        return result;
    }
}
