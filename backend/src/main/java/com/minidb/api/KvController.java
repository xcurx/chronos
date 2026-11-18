package com.minidb.api;

import com.minidb.config.RaftConfig;
import com.minidb.events.ClusterEvent;
import com.minidb.events.EventBus;
import com.minidb.mvcc.MvccStore;
import com.minidb.mvcc.ReadMode;
import com.minidb.raft.RaftNode;
import com.minidb.speculation.SpeculativeManager;
import com.minidb.txn.TxnCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;

/**
 * REST controller for KV operations from the dashboard UI.
 * Automatically forwards writes to the Raft leader.
 */
@RestController
@RequestMapping("/api/kv")
public class KvController {

    private static final Logger log = LoggerFactory.getLogger(KvController.class);

    private final RaftNode raftNode;
    private final RaftConfig raftConfig;
    private final TxnCoordinator txnCoordinator;
    private final SpeculativeManager speculativeManager;
    private final EventBus eventBus;
    private final HttpClient httpClient;

    public KvController(RaftNode raftNode, RaftConfig raftConfig,
                        TxnCoordinator txnCoordinator, SpeculativeManager speculativeManager,
                        EventBus eventBus) {
        this.raftNode = raftNode;
        this.raftConfig = raftConfig;
        this.txnCoordinator = txnCoordinator;
        this.speculativeManager = speculativeManager;
        this.eventBus = eventBus;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(3))
                .build();
    }

    /**
     * Resolve the HTTP base URL for the leader node by matching leader ID
     * to the peer config format "node-id@host:grpcPort".
     * Convention: HTTP port = gRPC port - 1010 (9090→8080, 9092→8082, 9094→8084).
     */
    private String getLeaderHttpUrl() {
        String leaderId = raftNode.getLeaderId();
        if (leaderId == null) return null;
        for (String peer : raftConfig.getPeers()) {
            String peerNodeId = peer.contains("@") ? peer.split("@")[0] : peer;
            if (peerNodeId.equals(leaderId)) {
                String address = peer.contains("@") ? peer.split("@")[1] : peer;
                String[] parts = address.split(":");
                String host = parts[0];
                int grpcPort = parts.length > 1 ? Integer.parseInt(parts[1]) : 9090;
                int httpPort = grpcPort - 1010;
                return "http://" + host + ":" + httpPort;
            }
        }
        return null;
    }

    /**
     * Forward a POST request body to the leader's endpoint.
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> forwardToLeader(String path, String jsonBody) {
        String leaderUrl = getLeaderHttpUrl();
        if (leaderUrl == null) {
            Map<String, Object> err = new LinkedHashMap<>();
            err.put("success", false);
            err.put("error", "No leader available");
            return err;
        }
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(leaderUrl + path))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                    .timeout(Duration.ofSeconds(10))
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            // Parse JSON response manually (avoid extra dependency)
            return parseJson(response.body());
        } catch (Exception e) {
            log.warn("Failed to forward to leader at {}: {}", leaderUrl, e.getMessage());
            Map<String, Object> err = new LinkedHashMap<>();
            err.put("success", false);
            err.put("error", "Failed to forward to leader: " + e.getMessage());
            return err;
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> parseJson(String json) {
        try {
            var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
            return mapper.readValue(json, LinkedHashMap.class);
        } catch (Exception e) {
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("raw", json);
            return result;
        }
    }

    @PostMapping("/put")
    public Map<String, Object> put(@RequestBody Map<String, String> request) {
        if (!raftNode.isLeader()) {
            try {
                var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
                return forwardToLeader("/api/kv/put", mapper.writeValueAsString(request));
            } catch (Exception e) {
                Map<String, Object> err = new LinkedHashMap<>();
                err.put("success", false);
                err.put("error", "Forward failed: " + e.getMessage());
                return err;
            }
        }

        long start = System.currentTimeMillis();
        String key = request.get("key");
        String value = request.get("value");

        Map<String, Object> result = new LinkedHashMap<>();
        try {
            RaftNode.ChronosWriteResult writeResult = raftNode.submitPutWithResult(
                            key, value.getBytes(StandardCharsets.UTF_8))
                    .get(5, java.util.concurrent.TimeUnit.SECONDS);

            boolean committed = writeResult != null && writeResult.committed();
            long latency = System.currentTimeMillis() - start;
            result.put("success", committed);
            result.put("latencyMs", latency);
            if (committed) {
                result.put("versionId", writeResult.versionId());
                result.put("versionSequence", writeResult.versionSequence());
                result.put("timestamp", writeResult.timestamp());
                result.put("branchId", writeResult.branchId());
            } else if (writeResult != null && writeResult.error() != null) {
                result.put("error", writeResult.error());
            }
            eventBus.publish(ClusterEvent.kvOperation("PUT", key, latency, committed));
        } catch (Exception e) {
            long latency = System.currentTimeMillis() - start;
            result.put("success", false);
            result.put("error", e.getMessage());
            result.put("latencyMs", latency);
            eventBus.publish(ClusterEvent.kvOperation("PUT", key, latency, false));
        }
        return result;
    }

    // ================================================================
    // Speculative Write Endpoints (Novel Contribution)
    // ================================================================

    /**
     * Speculative PUT — writes immediately to local MVCC store with SPECULATIVE state,
     * returns to client before Raft consensus completes.
     *
     * <p>Response contains:</p>
     * <ul>
     *   <li>{@code speculative: true/false} — indicates whether write is tentative</li>
     *   <li>{@code writeLatencyMs} — write latency (local for SPECULATIVE, total for WAIT_FOR_COMMIT)</li>
     *   <li>{@code timestamp} — MVCC timestamp of the version</li>
     *   <li>{@code raftLogIndex} — Raft log index for tracking commit status</li>
     *   <li>{@code ackPolicy} — which ack policy was used</li>
     * </ul>
     *
     * <p><b>Reviewer Challenge §1 — Latency Paradox:</b> The client MAY set
     * {@code "waitForCommit": true} to get full durability guarantee while
     * still benefiting from immediate reader visibility via SPECULATIVE read mode.
     * Without it, the response explicitly includes {@code speculative: true}.</p>
     */
    @PostMapping("/speculative-put")
    public Map<String, Object> speculativePut(@RequestBody Map<String, Object> request) {
        if (!raftNode.isLeader()) {
            try {
                var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
                return forwardToLeader("/api/kv/speculative-put", mapper.writeValueAsString(request));
            } catch (Exception e) {
                Map<String, Object> err = new LinkedHashMap<>();
                err.put("success", false);
                err.put("error", "Forward failed: " + e.getMessage());
                return err;
            }
        }

        String key = (String) request.get("key");
        String value = (String) request.get("value");
        boolean waitForCommit = Boolean.TRUE.equals(request.get("waitForCommit"));

        SpeculativeManager.AckPolicy ackPolicy = waitForCommit
                ? SpeculativeManager.AckPolicy.WAIT_FOR_COMMIT
                : SpeculativeManager.AckPolicy.SPECULATIVE;

        Map<String, Object> result = new LinkedHashMap<>();
        SpeculativeManager.SpeculativeResult specResult =
                speculativeManager.submitSpeculativePut(raftNode, key,
                        value.getBytes(StandardCharsets.UTF_8), ackPolicy);

        result.put("success", specResult.success());
        result.put("speculative", specResult.speculative());
        result.put("ackPolicy", ackPolicy.name());
        result.put("writeLatencyMs", specResult.writeLatencyMs());
        if (specResult.success()) {
            result.put("timestamp", specResult.timestamp());
            result.put("raftLogIndex", specResult.raftLogIndex());
            if (specResult.versionId() != null) {
                result.put("versionId", specResult.versionId());
            }
        } else {
            result.put("error", specResult.error());
        }
        eventBus.publish(ClusterEvent.kvOperation("PUT", key, (long) specResult.writeLatencyMs(), specResult.success()));
        return result;
    }

    /**
     * Speculative DELETE — same as speculative-put but for deletions.
     */
    @PostMapping("/speculative-delete")
    public Map<String, Object> speculativeDelete(@RequestBody Map<String, String> request) {
        if (!raftNode.isLeader()) {
            try {
                var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
                return forwardToLeader("/api/kv/speculative-delete", mapper.writeValueAsString(request));
            } catch (Exception e) {
                Map<String, Object> err = new LinkedHashMap<>();
                err.put("success", false);
                err.put("error", "Forward failed: " + e.getMessage());
                return err;
            }
        }

        String key = request.get("key");
        Map<String, Object> result = new LinkedHashMap<>();
        SpeculativeManager.SpeculativeResult specResult =
                speculativeManager.submitSpeculativeDelete(raftNode, key);

        result.put("success", specResult.success());
        result.put("speculative", specResult.speculative());
        result.put("writeLatencyMs", specResult.writeLatencyMs());
        if (specResult.success()) {
            result.put("timestamp", specResult.timestamp());
            result.put("raftLogIndex", specResult.raftLogIndex());
            if (specResult.versionId() != null) {
                result.put("versionId", specResult.versionId());
            }
        } else {
            result.put("error", specResult.error());
        }
        eventBus.publish(ClusterEvent.kvOperation("DELETE", key, (long) specResult.writeLatencyMs(), specResult.success()));
        return result;
    }

    /**
     * Toggle speculation mode — for A/B benchmarking between standard and speculative paths.
     */
    @PostMapping("/speculation/toggle")
    public Map<String, Object> toggleSpeculation(@RequestBody Map<String, Object> request) {
        boolean enabled = Boolean.TRUE.equals(request.get("enabled"));
        speculativeManager.setSpeculationEnabled(enabled);

        if (!raftNode.isLeader()) {
            try {
                var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
                Map<String, Object> forwarded = forwardToLeader("/api/kv/speculation/toggle", mapper.writeValueAsString(request));
                if (forwarded != null && !forwarded.isEmpty()) {
                    return forwarded;
                }
            } catch (Exception e) {
                log.warn("Forwarding /api/kv/speculation/toggle failed: {}", e.getMessage());
            }
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("speculationEnabled", speculativeManager.isSpeculationEnabled());
        return result;
    }

    /**
     * Get speculation metrics — for paper evaluation.
     */
    @GetMapping("/speculation/metrics")
    public Map<String, Object> speculationMetrics() {
        if (!raftNode.isLeader()) {
            String leaderUrl = getLeaderHttpUrl();
            if (leaderUrl != null) {
                try {
                    HttpRequest request = HttpRequest.newBuilder()
                            .uri(URI.create(leaderUrl + "/api/kv/speculation/metrics"))
                            .GET()
                            .timeout(Duration.ofSeconds(3))
                            .build();
                    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                    if (response.statusCode() == 200) {
                        return parseJson(response.body());
                    }
                } catch (Exception e) {
                    log.debug("Leader speculation metrics fetch failed, fallback to local: {}", e.getMessage());
                }
            }
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("speculationEnabled", speculativeManager.isSpeculationEnabled());
        result.put("avgSpeculativeLatencyMs", speculativeManager.getAverageSpeculativeLatencyMs());
        result.put("avgStandardLatencyMs", speculativeManager.getAverageStandardLatencyMs());
        result.put("avgPromotionLatencyMs", speculativeManager.getAveragePromotionLatencyMs());
        result.put("speculativeWriteCount", speculativeManager.getSpeculativeWriteCount());
        result.put("standardWriteCount", speculativeManager.getStandardWriteCount());
        result.put("pendingSpeculations", speculativeManager.getPendingSpeculations());

        // MVCC-level stats
        MvccStore mvccStore = raftNode.getMvccStore();
        result.put("mvccSpeculativeWrites", mvccStore.getSpeculativeWriteCount());
        result.put("mvccCommitPromotions", mvccStore.getCommitPromotionCount());
        result.put("mvccRollbacks", mvccStore.getRollbackCount());
        result.put("mvccSpeculationSuccessRate", mvccStore.getSpeculationSuccessRate());
        return result;
    }

    @PostMapping("/get")
    public Map<String, Object> get(@RequestBody Map<String, Object> request) {
        long start = System.currentTimeMillis();
        String key = (String) request.get("key");
        Long timestamp = request.containsKey("timestamp") ?
                ((Number) request.get("timestamp")).longValue() : 0L;
        // Support speculative reads via "readMode" parameter
        ReadMode readMode = "SPECULATIVE".equalsIgnoreCase(
                (String) request.getOrDefault("readMode", "LINEARIZABLE"))
                ? ReadMode.SPECULATIVE : ReadMode.LINEARIZABLE;

        Map<String, Object> result = new LinkedHashMap<>();
        try {
            MvccStore.MvccResult mvccResult;
            if (timestamp > 0) {
                mvccResult = raftNode.getMvccStore().get(key, timestamp);
            } else {
                mvccResult = raftNode.getMvccStore().get(key, readMode);
            }

            long latency = System.currentTimeMillis() - start;
            result.put("found", mvccResult.isFound());
            if (mvccResult.isFound()) {
                result.put("value", mvccResult.getValueAsString());
                result.put("timestamp", mvccResult.getTimestamp());
                result.put("speculative", mvccResult.isSpeculative());
                if (mvccResult.getVersionState() != null) {
                    result.put("versionState", mvccResult.getVersionState().name());
                }
            }
            result.put("readMode", readMode.name());
            result.put("latencyMs", latency);
            eventBus.publish(ClusterEvent.kvOperation("GET", key, latency, mvccResult.isFound()));
        } catch (Exception e) {
            result.put("found", false);
            result.put("error", e.getMessage());
        }
        return result;
    }

    @PostMapping("/delete")
    public Map<String, Object> delete(@RequestBody Map<String, String> request) {
        if (!raftNode.isLeader()) {
            try {
                var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
                return forwardToLeader("/api/kv/delete", mapper.writeValueAsString(request));
            } catch (Exception e) {
                Map<String, Object> err = new LinkedHashMap<>();
                err.put("success", false);
                err.put("error", "Forward failed: " + e.getMessage());
                return err;
            }
        }

        long start = System.currentTimeMillis();
        String key = request.get("key");

        Map<String, Object> result = new LinkedHashMap<>();
        try {
            RaftNode.ChronosWriteResult writeResult = raftNode.submitDeleteWithResult(key)
                    .get(5, java.util.concurrent.TimeUnit.SECONDS);

            boolean committed = writeResult != null && writeResult.committed();
            long latency = System.currentTimeMillis() - start;
            result.put("success", committed);
            result.put("latencyMs", latency);
            if (committed) {
                result.put("versionId", writeResult.versionId());
                result.put("versionSequence", writeResult.versionSequence());
                result.put("timestamp", writeResult.timestamp());
                result.put("branchId", writeResult.branchId());
            } else if (writeResult != null && writeResult.error() != null) {
                result.put("error", writeResult.error());
            }
            eventBus.publish(ClusterEvent.kvOperation("DELETE", key, latency, committed));
        } catch (Exception e) {
            long latency = System.currentTimeMillis() - start;
            result.put("success", false);
            result.put("error", e.getMessage());
            eventBus.publish(ClusterEvent.kvOperation("DELETE", key, latency, false));
        }
        return result;
    }

    @PostMapping("/scan")
    public Map<String, Object> scan(@RequestBody Map<String, Object> request) {
        long start = System.currentTimeMillis();
        // Accept both "prefix"/"startKey" from frontend
        String startKey = (String) request.getOrDefault("prefix",
                request.getOrDefault("startKey", ""));
        String endKey = (String) request.getOrDefault("endKey", null);
        int limit = request.containsKey("limit") ?
                ((Number) request.get("limit")).intValue() : 100;
        long timestamp = request.containsKey("timestamp") ?
                ((Number) request.get("timestamp")).longValue() : 0L;
        String readModeStr = String.valueOf(request.getOrDefault("readMode", "LINEARIZABLE"));
        ReadMode readMode = "SPECULATIVE".equalsIgnoreCase(readModeStr)
                ? ReadMode.SPECULATIVE : ReadMode.LINEARIZABLE;

        Map<String, Object> result = new LinkedHashMap<>();
        try {
            List<MvccStore.MvccResult> results = raftNode.getMvccStore()
                    .scan(startKey, endKey, limit, timestamp, readMode);

            List<Map<String, Object>> pairs = new ArrayList<>();
            for (MvccStore.MvccResult r : results) {
                Map<String, Object> pair = new LinkedHashMap<>();
                pair.put("key", r.getKey());
                pair.put("value", r.getValueAsString());
                pair.put("timestamp", r.getTimestamp());
                pairs.add(pair);
            }

            long latency = System.currentTimeMillis() - start;
            result.put("pairs", pairs);
            result.put("count", pairs.size());
            result.put("latencyMs", latency);
            eventBus.publish(ClusterEvent.kvOperation("SCAN", startKey, latency, true));
        } catch (Exception e) {
            result.put("error", e.getMessage());
        }
        return result;
    }

    @GetMapping("/versions/{key}")
    public Map<String, Object> getVersionHistory(@PathVariable String key) {
        Map<String, Object> result = new LinkedHashMap<>();
        try {
            List<MvccStore.MvccResult> versions = raftNode.getMvccStore().getVersionHistory(key);
            List<Map<String, Object>> versionList = new ArrayList<>();
            int versionNum = versions.size();
            for (MvccStore.MvccResult v : versions) {
                Map<String, Object> ver = new LinkedHashMap<>();
                ver.put("version", versionNum--);
                ver.put("timestamp", v.getTimestamp());
                ver.put("value", v.getValueAsString());
                ver.put("tombstone", v.isTombstone());
                ver.put("speculative", v.isSpeculative());
                if (v.getVersionState() != null) {
                    ver.put("versionState", v.getVersionState().name());
                }
                versionList.add(ver);
            }
            result.put("key", key);
            result.put("versions", versionList);
        } catch (Exception e) {
            result.put("error", e.getMessage());
        }
        return result;
    }

    @PostMapping("/txn")
    public Map<String, Object> executeTransaction(@RequestBody Map<String, Object> request) {
        if (!raftNode.isLeader()) {
            try {
                var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
                return forwardToLeader("/api/kv/txn", mapper.writeValueAsString(request));
            } catch (Exception e) {
                Map<String, Object> err = new LinkedHashMap<>();
                err.put("success", false);
                err.put("error", "Forward failed: " + e.getMessage());
                return err;
            }
        }

        long start = System.currentTimeMillis();
        Map<String, Object> result = new LinkedHashMap<>();

        try {
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> ops = (List<Map<String, Object>>) request.get("operations");
            String branchId = request.containsKey("branchId") ? String.valueOf(request.get("branchId")) : "main";
            String writerId = request.containsKey("writerId") ? String.valueOf(request.get("writerId")) : null;
            List<TxnCoordinator.TxnOperation> operations = new ArrayList<>();

            for (Map<String, Object> op : ops) {
                String opType = op.get("type") != null ? String.valueOf(op.get("type")) : "PUT";
                String key = String.valueOf(op.get("key"));
                byte[] value = op.get("value") != null ?
                        String.valueOf(op.get("value")).getBytes(StandardCharsets.UTF_8) : null;
                Long deltaAmount = op.get("deltaAmount") instanceof Number n ? n.longValue() : null;

                TxnCoordinator.TxnOperation.OpType type = switch (opType.toUpperCase()) {
                    case "DELETE" -> TxnCoordinator.TxnOperation.OpType.DELETE;
                    case "DELTA" -> TxnCoordinator.TxnOperation.OpType.DELTA;
                    default -> TxnCoordinator.TxnOperation.OpType.PUT;
                };

                operations.add(new TxnCoordinator.TxnOperation(key, value, type, deltaAmount));
            }

            TxnCoordinator.TxnCommitResult commitResult = txnCoordinator
                    .executeWithResult(operations, branchId, writerId)
                    .get(10, java.util.concurrent.TimeUnit.SECONDS);

            long latency = System.currentTimeMillis() - start;
            result.put("success", commitResult.committed());
            result.put("txnId", commitResult.txnId());
            result.put("latencyMs", latency);
            result.put("operationCount", operations.size());
            if (commitResult.committed()) {
                result.put("versionId", commitResult.versionId());
                result.put("versionSequence", commitResult.versionSequence());
                result.put("timestamp", commitResult.timestamp());
                result.put("branchId", commitResult.branchId());
            } else if (commitResult.error() != null) {
                result.put("error", commitResult.error());
            }
        } catch (Exception e) {
            result.put("success", false);
            result.put("error", e.getMessage());
        }
        return result;
    }
}
