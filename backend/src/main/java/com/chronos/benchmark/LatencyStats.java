package com.chronos.benchmark;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Summary latency distribution statistics in milliseconds.
 */
public record LatencyStats(
        int count,
        double minMs,
        double maxMs,
        double avgMs,
        double p50Ms,
        double p95Ms,
        double p99Ms
) {
    public static LatencyStats compute(List<Long> nanosList) {
        if (nanosList == null || nanosList.isEmpty()) {
            return new LatencyStats(0, 0, 0, 0, 0, 0, 0);
        }
        List<Long> sorted = new ArrayList<>(nanosList);
        Collections.sort(sorted);
        int n = sorted.size();
        double min = sorted.get(0) / 1_000_000.0;
        double max = sorted.get(n - 1) / 1_000_000.0;
        double sum = 0;
        for (long val : sorted) sum += val;
        double avg = (sum / n) / 1_000_000.0;
        double p50 = sorted.get((int) (n * 0.50)) / 1_000_000.0;
        double p95 = sorted.get(Math.min(n - 1, (int) (n * 0.95))) / 1_000_000.0;
        double p99 = sorted.get(Math.min(n - 1, (int) (n * 0.99))) / 1_000_000.0;
        return new LatencyStats(n, min, max, avg, p50, p95, p99);
    }
}

