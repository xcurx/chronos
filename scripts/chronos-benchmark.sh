#!/usr/bin/env bash
# ==============================================================================
# Chronos Evaluation Suite & Research Benchmarks Runner
# ==============================================================================
# Evaluates the 5 research dimensions from chronos.pdf (Slide 19):
# 1. Correctness (Linearizability, DAG LCA, Safe Watermark GC, Invariants)
# 2. Latency (Read latency vs history depth percentiles)
# 3. Throughput & Auto-Merge (Workload A-D multi-writer conflict benchmarks)
# 4. Recovery & Resilience (Chaos speculative rollback, stale-writer protection)
# 5. Storage Scalability (Delta change-set compression vs naive full snapshots)
# ==============================================================================

set -euo pipefail

# Ensure Java 21+ and local Maven are configured and in PATH
NEED_JDK_OVERRIDE=false
if [[ -z "${JAVA_HOME:-}" ]]; then
    NEED_JDK_OVERRIDE=true
else
    if [[ -x "${JAVA_HOME}/bin/java" ]]; then
        CURRENT_JAVA_VER=$("${JAVA_HOME}/bin/java" -version 2>&1 | awk -F '"' '/version/ {print $2}' | awk -F. '{print $1}')
        if [[ -n "$CURRENT_JAVA_VER" && "$CURRENT_JAVA_VER" -lt 21 ]]; then
            NEED_JDK_OVERRIDE=true
        fi
    else
        NEED_JDK_OVERRIDE=true
    fi
fi

if [[ "$NEED_JDK_OVERRIDE" == true ]]; then
    if [[ -d "/usr/lib/jvm/java-27-openjdk" ]]; then
        export JAVA_HOME="/usr/lib/jvm/java-27-openjdk"
    elif [[ -d "/usr/lib/jvm/default" ]]; then
        export JAVA_HOME="/usr/lib/jvm/default"
    fi
fi

if [[ -n "${JAVA_HOME:-}" ]]; then
    export PATH="${JAVA_HOME}/bin:$HOME/.local/bin:$PATH"
else
    export PATH="$HOME/.local/bin:$PATH"
fi

MVN_BIN="mvn"
if ! command -v mvn &>/dev/null && [[ -x "$HOME/.local/bin/mvn" ]]; then
    MVN_BIN="$HOME/.local/bin/mvn"
fi

CHRONOS_HOST="${CHRONOS_HOST:-http://localhost:8080}"

echo "======================================================================"
echo " Chronos Research Evaluation & Benchmark Suite"
echo " Target Host: ${CHRONOS_HOST}"
echo "======================================================================"

# Health check
if ! curl -sf "${CHRONOS_HOST}/actuator/health" >/dev/null 2>&1 && ! curl -sf "${CHRONOS_HOST}/api/cluster/status" >/dev/null 2>&1; then
    echo "Warning: Chronos server not reachable at ${CHRONOS_HOST}."
    echo "Starting evaluation benchmark suite via Maven standalone test runner..."
    cd "$(dirname "$0")/../backend"
    "$MVN_BIN" test -Dtest=ChronosResiliencyAndBenchmarkTest
    exit 0
fi

echo ""
echo "=== [1/5] Read Latency vs History Depth ==="
curl -s -X POST "${CHRONOS_HOST}/api/chronos/benchmark/history-depth" \
    -H "Content-Type: application/json" \
    -d '{"numVersions": 50, "numKeys": 20, "querySamples": 50}' | jq .

echo ""
echo "=== [2/5] Storage Overhead & Delta Compression Scalability ==="
curl -s -X POST "${CHRONOS_HOST}/api/chronos/benchmark/storage-overhead" \
    -H "Content-Type: application/json" \
    -d '{"numVersions": 100, "numKeys": 50, "modifiedRatio": 0.10}' | jq .

echo ""
echo "=== [3/5] Multi-Writer Conflict Resolution Workloads (A-D) ==="
echo "--> Workload A: Independent Writes Disjoint Branches"
curl -s -X POST "${CHRONOS_HOST}/api/chronos/benchmark/workload-a" \
    -H "Content-Type: application/json" \
    -d '{"operationsPerBranch": 10}' | jq .

echo "--> Workload B: High Contention & Commutative Merges"
curl -s -X POST "${CHRONOS_HOST}/api/chronos/benchmark/workload-b" \
    -H "Content-Type: application/json" \
    -d '{"operationsPerBranch": 5}' | jq .

echo "--> Workload C: Mixed 80/20 Workload"
curl -s -X POST "${CHRONOS_HOST}/api/chronos/benchmark/workload-c" \
    -H "Content-Type: application/json" \
    -d '{"operationsPerBranch": 10}' | jq .

echo "--> Workload D: Multi-Hop Partition Convergence"
curl -s -X POST "${CHRONOS_HOST}/api/chronos/benchmark/workload-d" | jq .

echo ""
echo "=== [4/5] Chaos & Resiliency Scenarios ==="
echo "--> Chaos 1: Speculative Rollback Isolation"
curl -s -X POST "${CHRONOS_HOST}/api/chronos/benchmark/chaos/speculative-rollback" | jq .

echo "--> Chaos 2: Stale Writer Auto-Branching Protection"
curl -s -X POST "${CHRONOS_HOST}/api/chronos/benchmark/chaos/stale-writer" | jq .

echo "--> Chaos 3: Watermark GC Concurrency Safety"
curl -s -X POST "${CHRONOS_HOST}/api/chronos/benchmark/chaos/watermark-gc" | jq .

echo ""
echo "=== [5/5] Comprehensive Evaluation Summary ==="
curl -s -X GET "${CHRONOS_HOST}/api/chronos/benchmark/summary" | jq .

echo ""
echo "======================================================================"
echo " Chronos Benchmark Suite Finished Successfully"
echo "======================================================================"

