#!/usr/bin/env bash
# ==============================================================================
# Chronos Scalability & Throughput Benchmark Suite
# ==============================================================================
# Evaluates multi-threaded scalability, write payload scaling,
# and sub-millisecond write-path latency breakdown.
# Outputs publication/report-ready comparative tables.
# ==============================================================================

set -euo pipefail

# Auto-detect active Raft leader among cluster nodes if LEADER_HOST is not set
LEADER_HOST="${LEADER_HOST:-}"
if [[ -z "$LEADER_HOST" ]]; then
    for port in 8080 8082 8084 8086 8088 8081 8083; do
        STATUS=$(curl -sf "http://localhost:${port}/api/cluster/status" 2>/dev/null || echo '{}')
        ROLE=$(echo "$STATUS" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('role',''))" 2>/dev/null || echo "")
        if [[ "$ROLE" == "LEADER" ]]; then
            LEADER_HOST="http://localhost:${port}"
            LEADER_NODE=$(echo "$STATUS" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('nodeId',''))" 2>/dev/null)
            echo ">> Active Raft Leader detected at ${LEADER_HOST} (${LEADER_NODE})"
            break
        fi
    done
fi
LEADER_HOST="${LEADER_HOST:-http://localhost:8080}"

echo "=============================================================================="
echo " Chronos Multi-Threaded Scalability & Latency Breakdown Suite"
echo " Target Leader: ${LEADER_HOST}"
echo "=============================================================================="

# Check connectivity
if ! curl -sf "${LEADER_HOST}/api/cluster/status" >/dev/null 2>&1; then
    echo "ERROR: Could not connect to cluster at ${LEADER_HOST}. Please start the cluster first."
    exit 1
fi

RESULTS_DIR="$(dirname "$0")/../logs/benchmarks"
mkdir -p "$RESULTS_DIR"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULT_FILE="$RESULTS_DIR/scalability_results_${TIMESTAMP}.json"

echo ""
echo ">> [Phase 1/3] Concurrency & Load Scalability Matrix..."
echo "------------------------------------------------------------------------------"

CONFIGS=(
    "300:5:256"
    "500:10:256"
    "500:20:256"
    "500:30:256"
    "300:10:1024"
)

python3 - <<EOF
import json, requests, sys, time

leader = "${LEADER_HOST}"
configs = [
    {"ops": 300, "threads": 5, "bytes": 256, "label": "Low Load (5 threads, 256B)"},
    {"ops": 500, "threads": 10, "bytes": 256, "label": "Medium Load (10 threads, 256B)"},
    {"ops": 500, "threads": 20, "bytes": 256, "label": "High Concurrency (20 threads, 256B)"},
    {"ops": 500, "threads": 30, "bytes": 256, "label": "Stress Concurrency (30 threads, 256B)"},
    {"ops": 300, "threads": 10, "bytes": 1024, "label": "Large Payload (10 threads, 1KB)"},
]

results = []

print(f"{'Configuration':<35} | {'Std p50':<8} | {'Std Thr':<8} | {'Spec p50':<8} | {'Spec Thr':<9} | {'Speedup':<8}")
print("-" * 92)

for cfg in configs:
    payload = {
        "numOps": cfg["ops"],
        "concurrency": cfg["threads"],
        "valueSizeBytes": cfg["bytes"],
        "mode": "BOTH"
    }
    try:
        res = requests.post(f"{leader}/api/benchmark/run", json=payload, timeout=60).json()
        std = res.get("standard", {})
        spec = res.get("speculative", {})

        std_p50 = std.get("writeLatency", {}).get("p50Ms", 0.0)
        std_p99 = std.get("writeLatency", {}).get("p99Ms", 0.0)
        std_thr = std.get("throughputOpsPerSec", 0.0)

        spec_p50 = spec.get("writeLatency", {}).get("p50Ms", 0.0)
        spec_p99 = spec.get("writeLatency", {}).get("p99Ms", 0.0)
        spec_thr = spec.get("throughputOpsPerSec", 0.0)
        promo_rate = spec.get("promotionSuccessRate", 1.0) * 100

        speedup = (std_p50 / spec_p50) if spec_p50 > 0 else 1.0

        results.append({
            "label": cfg["label"],
            "threads": cfg["threads"],
            "bytes": cfg["bytes"],
            "std_p50": std_p50,
            "std_p99": std_p99,
            "std_thr": std_thr,
            "spec_p50": spec_p50,
            "spec_p99": spec_p99,
            "spec_thr": spec_thr,
            "speedup": speedup,
            "promo_rate": promo_rate
        })

        print(f"{cfg['label']:<35} | {std_p50:>6.2f}ms | {std_thr:>6.0f}/s | {spec_p50:>6.2f}ms | {spec_thr:>7.0f}/s | {speedup:>6.2f}x")
    except Exception as e:
        print(f"{cfg['label']:<35} | ERROR: {str(e)[:45]}")

with open("${RESULT_FILE}", "w") as f:
    json.dump(results, f, indent=2)
EOF

echo ""
echo ">> [Phase 2/3] Sub-Millisecond Write-Path Latency Breakdown (500 iterations)..."
echo "------------------------------------------------------------------------------"
python3 - <<EOF
import json, requests, sys

leader = "${LEADER_HOST}"
try:
    res = requests.post(f"{leader}/api/benchmark/latency-breakdown", json={"iterations": 500}, timeout=60).json()
    if "error" in res:
        print("Error:", res["error"])
        sys.exit(0)

    print(f"{'Execution Stage':<25} | {'p50 (µs)':<10} | {'p95 (µs)':<10} | {'p99 (µs)':<10} | {'Avg (µs)':<10}")
    print("-" * 75)

    stages = [
        ("Log Append (Raft)", res.get("logAppend", {})),
        ("MVCC Local Write", res.get("mvccWrite", {})),
        ("Callback Setup", res.get("callbackSetup", {})),
        ("Total Write Path", res.get("totalWritePath", {})),
    ]

    for name, s in stages:
        p50 = s.get("p50Us", 0.0)
        p95 = s.get("p95Us", 0.0)
        p99 = s.get("p99Us", 0.0)
        avg = s.get("avgUs", 0.0)
        print(f"{name:<25} | {p50:>8.1f}µs | {p95:>8.1f}µs | {p99:>8.1f}µs | {avg:>8.1f}µs")

    promo = res.get("promotionLatency", {})
    print("-" * 75)
    print(f"Background Quorum Promotion: p50={promo.get('p50Ms', 0.0):.2f}ms, Success Rate={res.get('promotionRate', 1.0)*100:.1f}%")
except Exception as e:
    print(f"Failed to fetch latency breakdown: {e}")
EOF

echo ""
echo ">> [Phase 3/3] Research Report Markdown Table..."
echo "------------------------------------------------------------------------------"
python3 - <<EOF
import json
with open("${RESULT_FILE}") as f:
    results = json.load(f)

print("| Workload / Concurrency | Baseline p50 | Speculative p50 | Latency Speedup | Baseline Thr | Speculative Thr | Thr Gain |")
print("| :--------------------- | -----------: | --------------: | --------------: | -----------: | --------------: | -------: |")
for r in results:
    thr_gain = ((r['spec_thr'] / r['std_thr']) - 1.0) * 100 if r['std_thr'] > 0 else 0
    print(f"| {r['label']} | {r['std_p50']:.2f} ms | {r['spec_p50']:.2f} ms | **{r['speedup']:.2f}x** | {r['std_thr']:.0f} ops/s | {r['spec_thr']:.0f} ops/s | **+{thr_gain:.1f}%** |")
EOF

echo ""
echo "=============================================================================="
echo " Scalability Benchmark Complete! Results saved to: ${RESULT_FILE}"
echo "=============================================================================="
