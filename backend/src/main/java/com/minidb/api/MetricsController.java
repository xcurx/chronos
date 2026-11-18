package com.minidb.api;

import com.minidb.events.ClusterEvent;
import com.minidb.events.EventBus;
import com.minidb.mvcc.GarbageCollector;
import com.minidb.mvcc.MvccStore;
import com.minidb.raft.RaftNode;
import com.minidb.raft.SnapshotManager;
import com.minidb.speculation.SpeculativeManager;
import com.minidb.storage.StorageEngine;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * REST controller for metrics and observability data.
 */
@RestController
@RequestMapping("/api/metrics")
public class MetricsController {

    private final RaftNode raftNode;
    private final StorageEngine storage;
    private final GarbageCollector gc;
    private final SpeculativeManager speculativeManager;
    private final MeterRegistry meterRegistry;
    private final EventBus eventBus;

    private final AtomicLong cumulativePuts = new AtomicLong(0);
    private final AtomicLong cumulativeGets = new AtomicLong(0);
    private final AtomicLong cumulativeDeletes = new AtomicLong(0);

    private final AtomicLong lastTotalPuts = new AtomicLong(0);
    private final AtomicLong lastTotalGets = new AtomicLong(0);
    private final AtomicLong lastTotalDeletes = new AtomicLong(0);
    private final AtomicLong lastCheckTime = new AtomicLong(System.currentTimeMillis());

    private volatile double smoothedPutOps = 0.0;
    private volatile double smoothedGetOps = 0.0;
    private volatile double smoothedDeleteOps = 0.0;

    private final Deque<Long> recentLatenciesMs = new ConcurrentLinkedDeque<>();
    private static final int MAX_RECENT_LATENCIES = 200;

    public MetricsController(RaftNode raftNode, StorageEngine storage, GarbageCollector gc,
                             @Autowired(required = false) SpeculativeManager speculativeManager,
                             @Autowired(required = false) MeterRegistry meterRegistry,
                             @Autowired(required = false) EventBus eventBus) {
        this.raftNode = raftNode;
        this.storage = storage;
        this.gc = gc;
        this.speculativeManager = speculativeManager;
        this.meterRegistry = meterRegistry;
        this.eventBus = eventBus;
        this.lastTotalPuts.set(raftNode.getCommitIndex());

        if (eventBus != null) {
            for (ClusterEvent ev : eventBus.getRecentEvents(1000)) {
                recordClusterEvent(ev);
            }
            eventBus.subscribe(this::recordClusterEvent);
        }
    }

    private void recordClusterEvent(ClusterEvent event) {
        if (event != null && event.getCategory() == ClusterEvent.Category.KV) {
            Map<String, Object> meta = event.getData();
            if (meta != null) {
                String op = String.valueOf(meta.get("operation"));
                if (op != null) {
                    switch (op.toUpperCase()) {
                        case "GET", "CONSISTENT_GET", "QUERY", "SCAN" -> cumulativeGets.incrementAndGet();
                        case "PUT", "CHRONOS_WRITE", "BATCH_PUT" -> cumulativePuts.incrementAndGet();
                        case "DELETE" -> cumulativeDeletes.incrementAndGet();
                    }
                }
                Object latObj = meta.get("latencyMs");
                if (latObj instanceof Number n) {
                    long lat = n.longValue();
                    if (lat > 0) {
                        recentLatenciesMs.addFirst(lat);
                        while (recentLatenciesMs.size() > MAX_RECENT_LATENCIES) {
                            recentLatenciesMs.removeLast();
                        }
                    }
                }
            }
        }
    }

    @GetMapping("/overview")
    public Map<String, Object> getOverview() {
        Map<String, Object> overview = new LinkedHashMap<>();
        overview.put("nodeId", raftNode.getNodeId());
        overview.put("role", raftNode.getRole().name());
        overview.put("term", raftNode.getCurrentTerm());
        overview.put("commitIndex", raftNode.getCommitIndex());
        overview.put("lastApplied", raftNode.getLastApplied());
        long logSize = raftNode.getRaftLog().getLastIndex();
        overview.put("logSize", logSize);
        overview.put("logEntries", logSize);
        overview.put("uptime", raftNode.getUptime());
        overview.put("killed", raftNode.isKilled());

        // Elections count
        long elections = 0;
        if (meterRegistry != null) {
            Counter eCounter = meterRegistry.find("raft_elections_total").counter();
            if (eCounter != null) {
                elections = (long) eCounter.count();
            }
        }
        overview.put("elections", elections);

        long now = System.currentTimeMillis();
        long prevTime = lastCheckTime.getAndSet(now);
        long elapsedMs = Math.max(1, now - prevTime);

        MvccStore mvccStore = raftNode.getMvccStore();

        // 1. PUT throughput calculation: measure delta operations / second
        long currentTotalPuts = Math.max(cumulativePuts.get(), raftNode.getCommitIndex());
        if (speculativeManager != null) {
            currentTotalPuts = Math.max(currentTotalPuts,
                    speculativeManager.getSpeculativeWriteCount() + speculativeManager.getStandardWriteCount());
        }
        if (mvccStore != null) {
            currentTotalPuts = Math.max(currentTotalPuts,
                    mvccStore.getSpeculativeWriteCount() + mvccStore.getTotalStandardWrites());
        }

        long prevPuts = lastTotalPuts.getAndSet(currentTotalPuts);
        long deltaPuts = Math.max(0, currentTotalPuts - prevPuts);

        if (deltaPuts > 0) {
            smoothedPutOps = (deltaPuts * 1000.0) / elapsedMs;
        } else {
            smoothedPutOps = Math.max(0, smoothedPutOps * 0.65);
            if (smoothedPutOps < 0.2) smoothedPutOps = 0;
        }

        // 2. GET throughput calculation
        long currentTotalGets = cumulativeGets.get();
        if (mvccStore != null) {
            currentTotalGets = Math.max(currentTotalGets, mvccStore.getTotalGetOperations());
        }
        long prevGets = lastTotalGets.getAndSet(currentTotalGets);
        long deltaGets = Math.max(0, currentTotalGets - prevGets);

        if (deltaGets > 0) {
            smoothedGetOps = (deltaGets * 1000.0) / elapsedMs;
        } else {
            smoothedGetOps = Math.max(0, smoothedGetOps * 0.65);
            if (smoothedGetOps < 0.2) smoothedGetOps = 0;
        }

        // 3. DELETE throughput calculation
        long currentTotalDeletes = cumulativeDeletes.get();
        if (mvccStore != null) {
            currentTotalDeletes = Math.max(currentTotalDeletes, mvccStore.getTotalDeleteOperations());
        }
        long prevDeletes = lastTotalDeletes.getAndSet(currentTotalDeletes);
        long deltaDeletes = Math.max(0, currentTotalDeletes - prevDeletes);

        if (deltaDeletes > 0) {
            smoothedDeleteOps = (deltaDeletes * 1000.0) / elapsedMs;
        } else {
            smoothedDeleteOps = Math.max(0, smoothedDeleteOps * 0.65);
            if (smoothedDeleteOps < 0.2) smoothedDeleteOps = 0;
        }

        long putRate = Math.round(smoothedPutOps);
        long getRate = Math.round(smoothedGetOps);
        long deleteRate = Math.round(smoothedDeleteOps);
        long totalOpsPerSec = putRate + getRate + deleteRate;

        // Latency calculation
        double p50 = 0.0, p95 = 0.0, p99 = 0.0;
        if (!recentLatenciesMs.isEmpty()) {
            List<Long> latList = new ArrayList<>(recentLatenciesMs);
            Collections.sort(latList);
            int size = latList.size();
            p50 = latList.get((int) (size * 0.50));
            p95 = latList.get(Math.min(size - 1, (int) (size * 0.95)));
            p99 = latList.get(Math.min(size - 1, (int) (size * 0.99)));
        } else {
            if (speculativeManager != null) {
                double specLat = speculativeManager.getAverageSpeculativeLatencyMs();
                double stdLat = speculativeManager.getAverageStandardLatencyMs();
                if (specLat > 0 || stdLat > 0) {
                    p50 = specLat > 0 ? specLat : stdLat;
                    p95 = Math.max(specLat, stdLat) * 1.35;
                    p99 = Math.max(specLat, stdLat) * 1.9;
                }
            }

            if (meterRegistry != null) {
                Timer kvTimer = meterRegistry.find("kv_operation_latency_seconds").timer();
                if (kvTimer != null && kvTimer.count() > 0) {
                    double meanMs = kvTimer.mean(TimeUnit.MILLISECONDS);
                    double maxMs = kvTimer.max(TimeUnit.MILLISECONDS);
                    if (meanMs > 0) {
                        p50 = meanMs;
                        p95 = Math.max(p95, meanMs * 1.4);
                        p99 = Math.max(p99, maxMs);
                    }
                }
            }
        }

        overview.put("opsPerSec", totalOpsPerSec);
        overview.put("putOps", putRate);
        overview.put("getOps", getRate);
        overview.put("deleteOps", deleteRate);
        overview.put("latencyP50", Math.round(p50 * 100.0) / 100.0);
        overview.put("latencyP95", Math.round(p95 * 100.0) / 100.0);
        overview.put("latencyP99", Math.round(p99 * 100.0) / 100.0);

        return overview;
    }

    @GetMapping("/storage")
    public Map<String, String> getStorageStats() {
        return storage.getStats();
    }

    @GetMapping("/snapshot")
    public Map<String, Object> getSnapshotInfo() {
        SnapshotManager sm = raftNode.getSnapshotManager();
        Map<String, Object> info = new LinkedHashMap<>();
        info.put("lastSnapshotIndex", sm.getLastSnapshotIndex());
        info.put("lastSnapshotTerm", sm.getLastSnapshotTerm());
        info.put("lastSnapshotSize", sm.getLastSnapshotSize());
        info.put("lastSnapshotTime", sm.getLastSnapshotTime());
        return info;
    }

    @GetMapping("/gc")
    public Map<String, Object> getGcInfo() {
        Map<String, Object> info = new LinkedHashMap<>();
        info.put("totalPurged", gc.getTotalPurged());
        return info;
    }
}
