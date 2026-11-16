package com.chronos.metrics;

import com.chronos.model.ConflictType;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Micrometer Prometheus metrics registry and snapshot provider for Chronos.
 */
@Component
public class ChronosMetrics {

    private final MeterRegistry registry;

    private final Timer versionCreationLatency;
    private final Timer historicalQueryLatency;
    private final Timer reconstructionLatency;
    private final Timer conflictDetectionLatency;
    private final Timer mergeLatency;

    private final Counter versionsTotal;
    private final Counter conflictsTotal;
    private final Counter mergesCommittedTotal;
    private final Counter mergesRejectedTotal;

    private final AtomicInteger activeBranches = new AtomicInteger(1);
    private final AtomicLong highestSequence = new AtomicLong(0);

    @Autowired
    public ChronosMetrics(@Autowired(required = false) MeterRegistry registry) {
        this.registry = registry != null ? registry : new SimpleMeterRegistry();

        this.versionCreationLatency = Timer.builder("chronos_version_creation_latency")
                .description("Latency of creating and persisting committed Chronos versions")
                .register(this.registry);

        this.historicalQueryLatency = Timer.builder("chronos_historical_query_latency")
                .description("Latency of temporal historical queries (AS OF VERSION / TIME)")
                .register(this.registry);

        this.reconstructionLatency = Timer.builder("chronos_reconstruction_latency")
                .description("Latency of reconstructing full database state at a version")
                .register(this.registry);

        this.conflictDetectionLatency = Timer.builder("chronos_conflict_detection_latency")
                .description("Latency of three-way branch conflict detection")
                .register(this.registry);

        this.mergeLatency = Timer.builder("chronos_merge_latency")
                .description("Latency of evaluating and committing branch merges")
                .register(this.registry);

        this.versionsTotal = Counter.builder("chronos_versions_total")
                .description("Total number of Chronos versions committed")
                .register(this.registry);

        this.conflictsTotal = Counter.builder("chronos_conflicts_total")
                .description("Total number of conflicts detected during branch merges")
                .register(this.registry);

        this.mergesCommittedTotal = Counter.builder("chronos_merges_total")
                .tag("outcome", "committed")
                .description("Total branch merges successfully committed")
                .register(this.registry);

        this.mergesRejectedTotal = Counter.builder("chronos_merges_total")
                .tag("outcome", "rejected")
                .description("Total branch merges rejected due to conflicts or invariant violations")
                .register(this.registry);

        this.registry.gauge("chronos_active_branches", activeBranches);
    }

    public void recordVersionCreation(long durationNanos) {
        versionCreationLatency.record(durationNanos, TimeUnit.NANOSECONDS);
        versionsTotal.increment();
    }

    public void recordHistoricalQuery(long durationNanos) {
        historicalQueryLatency.record(durationNanos, TimeUnit.NANOSECONDS);
    }

    public void recordReconstruction(long durationNanos) {
        reconstructionLatency.record(durationNanos, TimeUnit.NANOSECONDS);
    }

    public void recordConflictDetection(long durationNanos, int conflictCount) {
        conflictDetectionLatency.record(durationNanos, TimeUnit.NANOSECONDS);
        if (conflictCount > 0) {
            conflictsTotal.increment(conflictCount);
        }
    }

    public void recordMerge(long durationNanos, boolean committed) {
        mergeLatency.record(durationNanos, TimeUnit.NANOSECONDS);
        if (committed) {
            mergesCommittedTotal.increment();
        } else {
            mergesRejectedTotal.increment();
        }
    }

    public void recordConflict(ConflictType type) {
        Counter.builder("chronos_conflicts_by_type")
                .tag("type", type != null ? type.name() : "unknown")
                .register(registry)
                .increment();
    }

    public void setActiveBranches(int count) {
        activeBranches.set(count);
    }

    public void setHighestSequence(long seq) {
        highestSequence.set(seq);
    }

    public Map<String, Object> getMetricsSnapshot() {
        Map<String, Object> snapshot = new LinkedHashMap<>();
        snapshot.put("versionsTotal", (long) versionsTotal.count());
        snapshot.put("conflictsTotal", (long) conflictsTotal.count());
        snapshot.put("mergesCommitted", (long) mergesCommittedTotal.count());
        snapshot.put("mergesRejected", (long) mergesRejectedTotal.count());
        snapshot.put("activeBranches", activeBranches.get());
        snapshot.put("highestSequence", highestSequence.get());

        Map<String, Object> latencies = new LinkedHashMap<>();
        latencies.put("versionCreationAvgMs", versionCreationLatency.mean(TimeUnit.MILLISECONDS));
        latencies.put("historicalQueryAvgMs", historicalQueryLatency.mean(TimeUnit.MILLISECONDS));
        latencies.put("reconstructionAvgMs", reconstructionLatency.mean(TimeUnit.MILLISECONDS));
        latencies.put("conflictDetectionAvgMs", conflictDetectionLatency.mean(TimeUnit.MILLISECONDS));
        latencies.put("mergeAvgMs", mergeLatency.mean(TimeUnit.MILLISECONDS));
        snapshot.put("latencies", latencies);

        return snapshot;
    }

    public MeterRegistry getRegistry() {
        return registry;
    }
}

