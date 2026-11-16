package com.chronos.api;

import com.chronos.benchmark.ChronosBenchmarkService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * REST controller for executing Chronos evaluation benchmarks, multi-writer conflict workloads,
 * and failure/chaos resiliency experiments.
 */
@RestController
@RequestMapping("/api/chronos/benchmark")
public class ChronosBenchmarkController {

    private final ChronosBenchmarkService benchmarkService;

    @Autowired
    public ChronosBenchmarkController(ChronosBenchmarkService benchmarkService) {
        this.benchmarkService = benchmarkService;
    }

    /**
     * Measure read latency vs version history depth across query modes.
     */
    @PostMapping("/history-depth")
    public ResponseEntity<Map<String, Object>> benchmarkHistoryDepth(
            @RequestBody(required = false) Map<String, Object> body) {
        int numVersions = getInt(body, "numVersions", 50);
        int numKeys = getInt(body, "numKeys", 20);
        int querySamples = getInt(body, "querySamples", 50);
        Map<String, Object> result = benchmarkService.benchmarkHistoryDepth(numVersions, numKeys, querySamples);
        return ResponseEntity.ok(result);
    }

    /**
     * Measure delta change set storage vs naive full snapshots across versions.
     */
    @PostMapping("/storage-overhead")
    public ResponseEntity<Map<String, Object>> benchmarkStorageOverhead(
            @RequestBody(required = false) Map<String, Object> body) {
        int numVersions = getInt(body, "numVersions", 100);
        int numKeys = getInt(body, "numKeys", 50);
        double modifiedRatio = getDouble(body, "modifiedRatio", 0.10);
        Map<String, Object> result = benchmarkService.benchmarkStorageOverhead(numVersions, numKeys, modifiedRatio);
        return ResponseEntity.ok(result);
    }

    /**
     * Run Workload A: Independent writes across disjoint branches (verifies 100% auto-merge).
     */
    @PostMapping("/workload-a")
    public ResponseEntity<Map<String, Object>> runWorkloadA(
            @RequestBody(required = false) Map<String, Object> body) {
        int ops = getInt(body, "operationsPerBranch", 10);
        Map<String, Object> result = benchmarkService.runWorkloadA(ops);
        return ResponseEntity.ok(result);
    }

    /**
     * Run Workload B: High contention writes (verifies commutative delta and LWW resolutions vs rejection).
     */
    @PostMapping("/workload-b")
    public ResponseEntity<Map<String, Object>> runWorkloadB(
            @RequestBody(required = false) Map<String, Object> body) {
        int ops = getInt(body, "operationsPerBranch", 5);
        Map<String, Object> result = benchmarkService.runWorkloadB(ops);
        return ResponseEntity.ok(result);
    }

    /**
     * Run Workload C: Mixed 80% disjoint / 20% contended delta writes.
     */
    @PostMapping("/workload-c")
    public ResponseEntity<Map<String, Object>> runWorkloadC(
            @RequestBody(required = false) Map<String, Object> body) {
        int ops = getInt(body, "operationsPerBranch", 10);
        Map<String, Object> result = benchmarkService.runWorkloadC(ops);
        return ResponseEntity.ok(result);
    }

    /**
     * Run Workload D: Multi-commit partition divergence and 3-way DAG reconciliation.
     */
    @PostMapping("/workload-d")
    public ResponseEntity<Map<String, Object>> runWorkloadD() {
        Map<String, Object> result = benchmarkService.runWorkloadD();
        return ResponseEntity.ok(result);
    }

    /**
     * Chaos Scenario 1: Speculative rollback isolation.
     */
    @PostMapping("/chaos/speculative-rollback")
    public ResponseEntity<Map<String, Object>> runChaosSpeculativeRollback() {
        Map<String, Object> result = benchmarkService.runChaosScenarioSpeculativeRollback();
        return ResponseEntity.ok(result);
    }

    /**
     * Chaos Scenario 2: Stale writer auto-branching protection.
     */
    @PostMapping("/chaos/stale-writer")
    public ResponseEntity<Map<String, Object>> runChaosStaleWriter() {
        Map<String, Object> result = benchmarkService.runChaosScenarioStaleWriter();
        return ResponseEntity.ok(result);
    }

    /**
     * Chaos Scenario 3: Watermark GC concurrency safety.
     */
    @PostMapping("/chaos/watermark-gc")
    public ResponseEntity<Map<String, Object>> runChaosWatermarkGc() {
        Map<String, Object> result = benchmarkService.runChaosScenarioWatermarkGcProtection();
        return ResponseEntity.ok(result);
    }

    /**
     * High-level evaluation summary across all 5 research dimensions.
     */
    @GetMapping("/summary")
    public ResponseEntity<Map<String, Object>> getEvaluationSummary() {
        Map<String, Object> summary = benchmarkService.generateEvaluationSummary();
        return ResponseEntity.ok(summary);
    }

    private int getInt(Map<String, Object> body, String key, int def) {
        if (body == null || !body.containsKey(key) || body.get(key) == null) return def;
        try {
            return Integer.parseInt(String.valueOf(body.get(key)));
        } catch (Exception e) {
            return def;
        }
    }

    private double getDouble(Map<String, Object> body, String key, double def) {
        if (body == null || !body.containsKey(key) || body.get(key) == null) return def;
        try {
            return Double.parseDouble(String.valueOf(body.get(key)));
        } catch (Exception e) {
            return def;
        }
    }
}

