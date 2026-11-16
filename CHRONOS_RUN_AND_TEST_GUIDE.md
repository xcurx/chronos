# Chronos: Run & Test Guide

This document provides step-by-step instructions for launching, testing, and verifying every capability of the **Chronos Distributed Time-Travel Database**.

---

## Architecture & Port Mapping

When the cluster starts, it deploys a 3-node Raft consensus group alongside an interactive React frontend:

| Component | HTTP API | gRPC Port | Description |
|---|---|---|---|
| **Node 1** | `http://localhost:8080` | `9090` | Raft Node 1 (Default Leader) |
| **Node 2** | `http://localhost:8082` | `9092` | Raft Node 2 (Follower) |
| **Node 3** | `http://localhost:8084` | `9094` | Raft Node 3 (Follower) |
| **Dashboard** | `http://localhost:3000` | — | Interactive React + Vite Dashboard |

---

## Quick Start: One-Command Launch

To start the entire cluster and dashboard:
```bash
./start-cluster.sh
```
*(The launcher automatically detects and configures JDK 27, runs the Maven build, starts all 3 Raft nodes in the background, and launches the Vite dashboard on port 3000).*

To cleanly stop the cluster at any time:
```bash
./stop-cluster.sh
```

---

## Method 1: Interactive Visual Testing via the Dashboard (Recommended for Demos)

Open **`http://localhost:3000`** in your browser.

### 1. Time Travel & Point-in-Time Historical Queries
1. Click **Time Travel** in the left sidebar.
2. In **KV Explorer**, create and update a key to build history:
   - Put `balance:alice` = `1000`
   - Put `balance:alice` = `1500`
   - Put `balance:alice` = `2200`
3. Return to the **Time Travel** tab:
   - **Query Mode: `CURRENT`** $\to$ Displays `balance:alice = 2200`.
   - **Query Mode: `AS OF VERSION`** $\to$ Select past version dropdown (`V1`, `V2`, `V3`). Notice how the table updates in real time to show Alice's balance as it existed in that version ($1000 \to 1500 \to 2200$).
   - **Query Mode: `AS OF TIME`** $\to$ Pick intermediate timestamps.
   - **Key Provenance Audit Trail:** In the audit box, type `balance:alice`. View the full chronological timeline of mutations with diffs, operation types, timestamps, and writer IDs.
   - **Named Snapshot:** Click **Create Snapshot** with description `"Q3 Audit"`. Switch query mode to `AS OF SNAPSHOT` to verify exact state restoration.

### 2. Version DAG & Multi-Writer Branching
1. Click **Version DAG** in the left sidebar.
2. Observe the interactive SVG DAG showing commits, branch lanes, and directed parent links.
3. Click **New Branch**:
   - Branch ID: `feature-ml`
   - Base Version: `V1`
   - Writer: `ml-team`
   - Click **Create Branch**. A new branch lane immediately appears branching off from `V1`.

### 3. One-Click Guided Scenarios & Deterministic Merge Studio
At the bottom of the **Version DAG** tab, you have two guided scenarios ready to run with a single click:

#### Scenario A: Commutative Inventory Reconciliation
- **Concept:** Reconciles concurrent arithmetic delta updates across two divergent branches without lost updates ($100 - \$20 - \$15 = \$65$).
- **Click:** **"Scenario 1: Commutative Inventory Reconciliation"** button.
- **What happens:**
  1. Base commit on `main` initializes `inventory:sku-99` = `100`.
  2. Branch A decrements inventory by `20`.
  3. Branch B concurrently decrements inventory by `15`.
  4. The 3-way conflict detector flags `COMPATIBLE_DELTA`.
  5. The merge engine computes $100 - 20 - 15 = 65$ and commits through Raft.
  6. A **two-parent diamond merge node** appears in the SVG graph.

#### Scenario B: Financial Invariant Violation Rejection
- **Concept:** Enforces domain safety invariants across concurrent branches (e.g., account balance must never drop below 0).
- **Click:** **"Scenario 2: Financial Account Invariant Violation"** button.
- **What happens:**
  1. Base commit has `account:bob` = `50`.
  2. A domain safety invariant (`account:* >= 0`) is registered.
  3. Branch 1 withdraws `40`. Branch 2 concurrently withdraws `30`.
  4. The merge engine detects that $50 - 40 - 30 = -20 < 0$.
  5. **Result:** The merge is safely **rejected**, and an alert displays the exact invariant violation preventing database corruption.

---

## Method 2: Automated Research Benchmark & Chaos Suite

To run the research benchmark suite covering the 5 evaluation dimensions from `chronos.pdf` (Slide 19) and Workloads A–D:

```bash
./scripts/chronos-benchmark.sh
```

*(If the cluster is running, it tests against live HTTP endpoints; if offline, it executes the standalone integration test harness).*

### What It Evaluates:
1. **Dimension 1 (Correctness):**
   - Linearizability via Raft consensus.
   - Lowest Common Ancestor (LCA) graph traversal.
   - Domain safety invariant validation.
2. **Dimension 2 (Latency vs History Depth):**
   - Percentile latency distribution (`p50`, `p95`, `p99`) comparing `CURRENT` ($O(1)$) vs `AS_OF_VERSION` vs `AS_OF_TIME` vs `RECONSTRUCT_STATE`.
3. **Dimension 3 (Throughput & Multi-Writer Workloads A–D):**
   - **Workload A (Independent Writes):** Disjoint branches achieve **100% auto-merge** rate with 0 conflicts.
   - **Workload B (High Contention):** Evaluates `FAIL_ON_CONFLICT` rejection, `LAST_WRITE_WINS` deterministic resolution, and `COMMUTATIVE_DELTA` arithmetic summation ($100 - 20 - 35 = 45$).
   - **Workload C (Mixed 80/20):** 80% disjoint keys + 20% commutative deltas.
   - **Workload D (Partition & Multi-Hop Convergence):** Isolated East ($E_1 \to E_2 \to E_3$) and West ($W_1 \to W_2$) branches converge deterministically into the base branch without lost updates.
4. **Dimension 4 (Chaos & Recovery):**
   - **Speculative Rollback:** Aborted uncommitted speculative writes are safely purged while linearizable reads remain untainted.
   - **Stale Writer Protection:** Writes against outdated parents are diverted to isolated auto-branches instead of silently corrupting history.
   - **Watermark GC Protection:** Active historical read pins block aggressive GC from prematurely deleting versions.
5. **Dimension 5 (Storage Scalability):**
   - Quantifies space savings of delta `ChangeEntry` changesets versus naive full snapshots (>80% storage savings, >5x compression).

---

## Method 3: Full Backend JUnit Test Suite

To run all 49 automated unit and integration tests:

```bash
cd backend
JAVA_HOME=/usr/lib/jvm/java-27-openjdk ~/.local/bin/mvn test
```

### Test Suite Breakdown:
- `ChronosResiliencyAndBenchmarkTest` (11 tests): Workloads A–D, Latency percentiles, Storage compression, Chaos recovery.
- `MergeEngineTest` (7 tests): Commutative deltas, LWW, Domain Invariant rejection, Custom overrides, REST endpoints.
- `ChronosReplicationTest` (6 tests): Raft deterministic commit anchoring, log replication, speculative rollbacks.
- `VersionCatalogTest` (5 tests): RocksDB sequential indexing, monotonic sequence & timestamp validation.
- `SafeLowWatermarkGcTest` (4 tests): Low-watermark GC retention, ReadWatermarkRegistry lease protection.
- `TemporalQueryAndAuditTest` (4 tests): Point-in-time historical querying, Key provenance audit, Version diffs.
- `BranchAndDagTest` (3 tests): Branch forks, LCA graph calculation, topological DAG path traversal.
- `ChronosMetricsTest` (3 tests): Prometheus latency timers, counters, WebSocket event publishing.
- `MiniDbApplicationTests` (6 tests): Baseline distributed KV tests.

---

## Method 4: Direct REST API / `curl` Testing

When the cluster is running (`./start-cluster.sh`), you can test the REST APIs directly against Node 1 (`http://localhost:8080`):

### 1. Write a Chronos Mutation
```bash
curl -s -X POST http://localhost:8080/api/chronos/write \
  -H "Content-Type: application/json" \
  -d '{
    "branchId": "main",
    "writerId": "alice",
    "operations": [
      {"key": "account:101", "value": "1000", "opType": "PUT"}
    ]
  }' | jq .
```

### 2. Point-in-Time Temporal Reads
```bash
# CURRENT read:
curl -s "http://localhost:8080/api/chronos/query?key=account:101&mode=CURRENT" | jq .

# AS_OF_VERSION read:
curl -s "http://localhost:8080/api/chronos/query?key=account:101&mode=AS_OF_VERSION&versionId=V1" | jq .

# Reconstruct entire database state at a version:
curl -s "http://localhost:8080/api/chronos/state?versionId=V1" | jq .
```

### 3. Key Provenance Audit Trail
```bash
curl -s "http://localhost:8080/api/chronos/audit/key/account:101" | jq .
```

### 4. Create and List Branches
```bash
# Create branch:
curl -s -X POST http://localhost:8080/api/chronos/branches \
  -H "Content-Type: application/json" \
  -d '{
    "branchId": "feature-experiment",
    "baseVersionId": "V1",
    "writerId": "data-team",
    "description": "Experiment branch"
  }' | jq .

# List branches:
curl -s http://localhost:8080/api/chronos/branches | jq .
```

### 5. Preview & Commit a Branch Merge
```bash
# Preview 3-way merge:
curl -s -X POST http://localhost:8080/api/chronos/merge/preview \
  -H "Content-Type: application/json" \
  -d '{
    "sourceBranch": "feature-experiment",
    "targetBranch": "main",
    "policy": "COMMUTATIVE_DELTA"
  }' | jq .

# Commit merge:
curl -s -X POST http://localhost:8080/api/chronos/merge \
  -H "Content-Type: application/json" \
  -d '{
    "sourceBranch": "feature-experiment",
    "targetBranch": "main",
    "policy": "COMMUTATIVE_DELTA",
    "writerId": "merge-coordinator"
  }' | jq .
```

### 6. Query Chronos Evaluation Benchmarks Directly
```bash
# Run Workload A:
curl -s -X POST http://localhost:8080/api/chronos/benchmark/workload-a \
  -H "Content-Type: application/json" \
  -d '{"operationsPerBranch": 10}' | jq .

# Get full 5-dimension research evaluation summary:
curl -s http://localhost:8080/api/chronos/benchmark/summary | jq .
```

