package com.minidb.grpc;

import com.chronos.audit.AuditService;
import com.chronos.audit.KeyAuditEntry;
import com.chronos.query.DatabaseStateResult;
import com.chronos.query.HistoryUnavailableException;
import com.chronos.query.QueryMode;
import com.chronos.query.TemporalQueryRequest;
import com.chronos.query.TemporalQueryResult;
import com.chronos.query.TemporalQueryService;
import com.google.protobuf.ByteString;
import com.minidb.events.ClusterEvent;
import com.minidb.events.EventBus;
import com.minidb.mvcc.MvccStore;
import com.minidb.mvcc.ReadMode;
import com.minidb.raft.RaftNode;
import com.minidb.speculation.SpeculativeManager;
import com.minidb.proto.*;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import net.devh.boot.grpc.server.service.GrpcService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * gRPC service implementation for the Key-Value API.
 * Routes Put/Delete through Raft consensus, serves reads from MVCC store.
 */
@GrpcService
public class KvGrpcService extends KvServiceGrpc.KvServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(KvGrpcService.class);

    private final RaftNode raftNode;
    private final EventBus eventBus;
    private final SpeculativeManager speculativeManager;
    private final TemporalQueryService temporalQueryService;
    private final AuditService auditService;
    private final Counter kvOpsCounter;
    private final Timer kvLatencyTimer;

    public KvGrpcService(RaftNode raftNode, EventBus eventBus,
                         SpeculativeManager speculativeManager,
                         MeterRegistry meterRegistry) {
        this(raftNode, eventBus, speculativeManager, null, null, meterRegistry);
    }

    @Autowired
    public KvGrpcService(RaftNode raftNode, EventBus eventBus,
                         SpeculativeManager speculativeManager,
                         TemporalQueryService temporalQueryService,
                         AuditService auditService,
                         MeterRegistry meterRegistry) {
        this.raftNode = raftNode;
        this.eventBus = eventBus;
        this.speculativeManager = speculativeManager;
        this.temporalQueryService = temporalQueryService;
        this.auditService = auditService;
        this.kvOpsCounter = Counter.builder("kv_operations_total")
                .description("Total KV operations")
                .register(meterRegistry);
        this.kvLatencyTimer = Timer.builder("kv_operation_latency_seconds")
                .description("KV operation latency")
                .register(meterRegistry);
    }

    @Override
    public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
        long start = System.currentTimeMillis();
        kvOpsCounter.increment();

        if (!raftNode.isLeader()) {
            responseObserver.onNext(PutResponse.newBuilder()
                    .setSuccess(false)
                    .setError("Not the leader. Leader: " + raftNode.getLeaderId())
                    .build());
            responseObserver.onCompleted();
            return;
        }

        raftNode.submitPutWithResult(request.getKey(), request.getValue().toByteArray())
                .thenAccept(writeResult -> {
                    boolean success = writeResult != null && writeResult.committed();
                    long latency = System.currentTimeMillis() - start;
                    eventBus.publish(ClusterEvent.kvOperation("PUT", request.getKey(), latency, success));

                    responseObserver.onNext(PutResponse.newBuilder()
                            .setSuccess(success)
                            .setTimestamp(success ? writeResult.timestamp() : 0L)
                            .build());
                    responseObserver.onCompleted();
                })
                .exceptionally(e -> {
                    long latency = System.currentTimeMillis() - start;
                    eventBus.publish(ClusterEvent.kvOperation("PUT", request.getKey(), latency, false));

                    responseObserver.onNext(PutResponse.newBuilder()
                            .setSuccess(false)
                            .setError(e.getMessage())
                            .build());
                    responseObserver.onCompleted();
                    return null;
                });
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
        long start = System.currentTimeMillis();
        kvOpsCounter.increment();

        try {
            MvccStore mvccStore = raftNode.getMvccStore();
            MvccStore.MvccResult result;

            if (request.getTimestamp() > 0) {
                result = mvccStore.get(request.getKey(), request.getTimestamp());
            } else {
                result = mvccStore.get(request.getKey());
            }

            long latency = System.currentTimeMillis() - start;
            eventBus.publish(ClusterEvent.kvOperation("GET", request.getKey(), latency, result.isFound()));

            GetResponse.Builder builder = GetResponse.newBuilder()
                    .setFound(result.isFound());
            if (result.isFound()) {
                builder.setValue(com.google.protobuf.ByteString.copyFrom(result.getValue()))
                       .setTimestamp(result.getTimestamp());
            }
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onNext(GetResponse.newBuilder()
                    .setFound(false)
                    .setError(e.getMessage())
                    .build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void consistentGet(GetRequest request, StreamObserver<GetResponse> responseObserver) {
        long start = System.currentTimeMillis();
        kvOpsCounter.increment();

        raftNode.consistentGet(request.getKey())
                .thenAccept(result -> {
                    long latency = System.currentTimeMillis() - start;
                    eventBus.publish(ClusterEvent.kvOperation("CONSISTENT_GET",
                            request.getKey(), latency, result.isFound()));

                    GetResponse.Builder builder = GetResponse.newBuilder()
                            .setFound(result.isFound());
                    if (result.isFound()) {
                        builder.setValue(com.google.protobuf.ByteString.copyFrom(result.getValue()))
                               .setTimestamp(result.getTimestamp());
                    }
                    responseObserver.onNext(builder.build());
                    responseObserver.onCompleted();
                })
                .exceptionally(e -> {
                    responseObserver.onNext(GetResponse.newBuilder()
                            .setFound(false)
                            .setError(e.getMessage())
                            .build());
                    responseObserver.onCompleted();
                    return null;
                });
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
        long start = System.currentTimeMillis();
        kvOpsCounter.increment();

        if (!raftNode.isLeader()) {
            responseObserver.onNext(DeleteResponse.newBuilder()
                    .setSuccess(false)
                    .setError("Not the leader. Leader: " + raftNode.getLeaderId())
                    .build());
            responseObserver.onCompleted();
            return;
        }

        raftNode.submitDelete(request.getKey())
                .thenAccept(success -> {
                    long latency = System.currentTimeMillis() - start;
                    eventBus.publish(ClusterEvent.kvOperation("DELETE", request.getKey(), latency, success));

                    responseObserver.onNext(DeleteResponse.newBuilder()
                            .setSuccess(success)
                            .build());
                    responseObserver.onCompleted();
                })
                .exceptionally(e -> {
                    responseObserver.onNext(DeleteResponse.newBuilder()
                            .setSuccess(false)
                            .setError(e.getMessage())
                            .build());
                    responseObserver.onCompleted();
                    return null;
                });
    }

    @Override
    public void scan(ScanRequest request, StreamObserver<ScanResponse> responseObserver) {
        long start = System.currentTimeMillis();
        kvOpsCounter.increment();

        try {
            MvccStore mvccStore = raftNode.getMvccStore();
            long ts = request.getTimestamp() > 0 ? request.getTimestamp() : Long.MAX_VALUE;
            List<MvccStore.MvccResult> results = mvccStore.scan(
                    request.getStartKey(), request.getEndKey(),
                    request.getLimit(), ts);

            ScanResponse.Builder builder = ScanResponse.newBuilder();
            for (MvccStore.MvccResult result : results) {
                builder.addPairs(KvPair.newBuilder()
                        .setKey(result.getKey())
                        .setValue(com.google.protobuf.ByteString.copyFrom(result.getValue()))
                        .setTimestamp(result.getTimestamp())
                        .build());
            }

            long latency = System.currentTimeMillis() - start;
            eventBus.publish(ClusterEvent.kvOperation("SCAN",
                    request.getStartKey() + "→" + request.getEndKey(), latency, true));

            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onNext(ScanResponse.newBuilder()
                    .setError(e.getMessage())
                    .build());
            responseObserver.onCompleted();
        }
    }

    // ================================================================
    // Speculative MVCC Consensus gRPC Handlers
    // ================================================================

    @Override
    public void speculativePut(SpeculativePutRequest request,
                                StreamObserver<SpeculativePutResponse> responseObserver) {
        kvOpsCounter.increment();

        if (!raftNode.isLeader()) {
            responseObserver.onNext(SpeculativePutResponse.newBuilder()
                    .setSuccess(false)
                    .setError("Not the leader. Leader: " + raftNode.getLeaderId())
                    .build());
            responseObserver.onCompleted();
            return;
        }

        SpeculativeManager.SpeculativeResult result =
                speculativeManager.submitSpeculativePut(raftNode,
                        request.getKey(), request.getValue().toByteArray());

        SpeculativePutResponse.Builder builder = SpeculativePutResponse.newBuilder()
                .setSuccess(result.success())
                .setSpeculative(result.speculative())
                .setWriteLatencyMs(result.writeLatencyMs());

        if (result.success()) {
            builder.setTimestamp(result.timestamp())
                   .setRaftLogIndex(result.raftLogIndex());
        } else {
            builder.setError(result.error());
        }

        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void speculativeDelete(SpeculativeDeleteRequest request,
                                   StreamObserver<SpeculativePutResponse> responseObserver) {
        kvOpsCounter.increment();

        if (!raftNode.isLeader()) {
            responseObserver.onNext(SpeculativePutResponse.newBuilder()
                    .setSuccess(false)
                    .setError("Not the leader. Leader: " + raftNode.getLeaderId())
                    .build());
            responseObserver.onCompleted();
            return;
        }

        SpeculativeManager.SpeculativeResult result =
                speculativeManager.submitSpeculativeDelete(raftNode, request.getKey());

        SpeculativePutResponse.Builder builder = SpeculativePutResponse.newBuilder()
                .setSuccess(result.success())
                .setSpeculative(result.speculative())
                .setWriteLatencyMs(result.writeLatencyMs());

        if (result.success()) {
            builder.setTimestamp(result.timestamp())
                   .setRaftLogIndex(result.raftLogIndex());
        } else {
            builder.setError(result.error());
        }

        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    // ---- Chronos Time-Travel & Audit RPCs ----

    @Override
    public void getAsOfVersion(TemporalKeyRequest request,
                               StreamObserver<TemporalKeyResponse> responseObserver) {
        handleTemporalKeyLookup(
                TemporalQueryRequest.asOfVersion(request.getKey(), request.getVersionId(), request.getConsistent()),
                responseObserver);
    }

    @Override
    public void getAsOfTime(TemporalKeyRequest request,
                            StreamObserver<TemporalKeyResponse> responseObserver) {
        handleTemporalKeyLookup(
                TemporalQueryRequest.asOfTime(request.getKey(), request.getTimestamp(),
                        request.getBranchId(), request.getConsistent()),
                responseObserver);
    }

    private void handleTemporalKeyLookup(TemporalQueryRequest queryRequest,
                                         StreamObserver<TemporalKeyResponse> responseObserver) {
        kvOpsCounter.increment();
        TemporalKeyResponse.Builder resp = TemporalKeyResponse.newBuilder();
        try {
            if (temporalQueryService == null) {
                throw new IllegalStateException("TemporalQueryService unavailable");
            }
            TemporalQueryResult result = temporalQueryService.queryKey(queryRequest);
            resp.setFound(result.found())
                    .setTombstone(result.tombstone())
                    .setSpeculative(result.speculative())
                    .setVersionSequence(result.versionSequence())
                    .setTimestamp(result.timestamp());
            if (result.valueBytes() != null) {
                resp.setValue(ByteString.copyFrom(result.valueBytes()));
            }
            if (result.versionId() != null) resp.setVersionId(result.versionId());
            if (result.branchId() != null) resp.setBranchId(result.branchId());
            if (result.writerId() != null) resp.setWriterId(result.writerId());
        } catch (HistoryUnavailableException e) {
            resp.setFound(false)
                    .setErrorCode(HistoryUnavailableException.ERROR_CODE)
                    .setError(e.getMessage());
        } catch (Exception e) {
            resp.setFound(false).setError(e.getMessage());
        }
        responseObserver.onNext(resp.build());
        responseObserver.onCompleted();
    }

    @Override
    public void reconstructState(ReconstructStateRequest request,
                                 StreamObserver<ReconstructStateResponse> responseObserver) {
        kvOpsCounter.increment();
        ReconstructStateResponse.Builder resp = ReconstructStateResponse.newBuilder();
        try {
            if (temporalQueryService == null) {
                throw new IllegalStateException("TemporalQueryService unavailable");
            }
            QueryMode mode = request.getMode().isBlank() ? null
                    : QueryMode.valueOf(request.getMode().trim().toUpperCase());
            TemporalQueryRequest queryRequest = new TemporalQueryRequest(
                    mode,
                    null,
                    request.getVersionId().isBlank() ? null : request.getVersionId(),
                    request.getTimestamp().isBlank() ? null : request.getTimestamp(),
                    request.getSnapshotId().isBlank() ? null : request.getSnapshotId(),
                    request.getBranchId().isBlank() ? null : request.getBranchId(),
                    ReadMode.LINEARIZABLE,
                    request.getConsistent(),
                    request.getPrefix().isBlank() ? null : request.getPrefix(),
                    request.getLimit()
            );
            DatabaseStateResult state = temporalQueryService.reconstructState(queryRequest);
            if (state.versionId() != null) resp.setVersionId(state.versionId());
            resp.setVersionSequence(state.versionSequence())
                    .setTimestamp(state.timestamp());
            if (state.branchId() != null) resp.setBranchId(state.branchId());
            for (DatabaseStateResult.StateEntry entry : state.details()) {
                resp.addPairs(KvPair.newBuilder()
                        .setKey(entry.key())
                        .setValue(ByteString.copyFromUtf8(entry.value()))
                        .setTimestamp(entry.timestamp())
                        .build());
            }
        } catch (HistoryUnavailableException e) {
            resp.setErrorCode(HistoryUnavailableException.ERROR_CODE)
                    .setError(e.getMessage());
        } catch (Exception e) {
            resp.setError(e.getMessage());
        }
        responseObserver.onNext(resp.build());
        responseObserver.onCompleted();
    }

    @Override
    public void getAuditHistory(AuditHistoryRequest request,
                                StreamObserver<AuditHistoryResponse> responseObserver) {
        kvOpsCounter.increment();
        AuditHistoryResponse.Builder resp = AuditHistoryResponse.newBuilder()
                .setKey(request.getKey());
        try {
            if (auditService == null) {
                throw new IllegalStateException("AuditService unavailable");
            }
            List<KeyAuditEntry> trail = auditService.getKeyAuditTrail(
                    request.getKey(), request.getIncludeUncommitted());
            for (KeyAuditEntry entry : trail) {
                AuditEntryProto.Builder proto = AuditEntryProto.newBuilder()
                        .setVersionId(entry.versionId())
                        .setVersionSequence(entry.versionSequence())
                        .setTimestamp(entry.timestamp())
                        .setStatus(entry.status() != null ? entry.status().name() : "")
                        .setOperationType(entry.operationType() != null ? entry.operationType().name() : "");
                if (entry.branchId() != null) proto.setBranchId(entry.branchId());
                if (entry.oldValue() != null) proto.setOldValue(entry.oldValue());
                if (entry.newValue() != null) proto.setNewValue(entry.newValue());
                if (entry.writerId() != null) proto.setWriterId(entry.writerId());
                if (entry.transactionId() != null) proto.setTransactionId(entry.transactionId());
                resp.addEntries(proto.build());
            }
        } catch (Exception e) {
            resp.setError(e.getMessage());
        }
        responseObserver.onNext(resp.build());
        responseObserver.onCompleted();
    }
}
