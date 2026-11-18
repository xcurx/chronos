# Chronos — Distributed Time-Travel & Branching Database

<div align="center">

**A distributed, Raft-linearized, time-travel key-value database featuring immutable Version DAG branching, 3-way Lowest Common Ancestor (LCA) merge reconciliation, Conflict-Free Replicated Data Types (CRDTs), speculative MVCC consensus, bitemporal modeling, and a real-time reactive dashboard.**

[![Architecture](https://img.shields.io/badge/Architecture-Distributed%20DAG-blue.svg)](#architecture-overview)
[![Consensus](https://img.shields.io/badge/Consensus-Raft%20(Linearizable)-green.svg)](#1-raft-consensus-engine)
[![Storage](https://img.shields.io/badge/Storage-RocksDB%209.0-orange.svg)](#4-storage-engine--version-catalog)
[![Time Travel](https://img.shields.io/badge/Time%20Travel-Bitemporal%20%2B%20LCA-purple.svg)](#3-three-way-lca-merge-engine)
[![CRDTs](https://img.shields.io/badge/Replication-CRDTs%20(PnCounter%2C%20ORSet)-brightgreen.svg)](#4-conflict-free-replicated-data-types-crdts)
[![Java](https://img.shields.io/badge/Java-21%20LTS-red.svg)](https://openjdk.org/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.2.2-green.svg)](https://spring.io/)
[![Frontend](https://img.shields.io/badge/Dashboard-React%20%2B%20D3.js-blueviolet.svg)](#react-dashboard)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

</div>

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Core Architectural Pillars](#core-architectural-pillars)
   - [1. Distributed Raft Consensus Engine](#1-distributed-raft-consensus-engine)
   - [2. Version DAG & Immutable Branching](#2-version-dag--immutable-branching)
   - [3. Three-Way LCA Merge Engine & Conflict Resolution](#3-three-way-lca-merge-engine--conflict-resolution)
   - [4. Conflict-Free Replicated Data Types (CRDTs)](#4-conflict-free-replicated-data-types-crdts)
   - [5. Speculative MVCC Consensus & Pre-Commit Reads](#5-speculative-mvcc-consensus--pre-commit-reads)
   - [6. Bitemporal Engine (Valid-Time vs Transaction-Time)](#6-bitemporal-engine-valid-time-vs-transaction-time)
   - [7. Safe Low-Watermark Garbage Collection](#7-safe-low-watermark-garbage-collection)
3. [System Architecture Diagram](#system-architecture-diagram)
4. [Empirical Research Benchmarks](#empirical-research-benchmarks)
   - [Read Latency vs History Depth](#read-latency-vs-history-depth)
   - [Storage Overhead & Delta Compression](#storage-overhead--delta-compression)
   - [Multi-Writer Conflict Resolution Workloads (A–D)](#multi-writer-conflict-resolution-workloads-ad)
   - [Speculative Consensus Throughput Acceleration](#speculative-consensus-throughput-acceleration)
   - [Microsecond Write Path Breakdown](#microsecond-write-path-breakdown)
5. [React Dashboard & Real-Time Visualization](#react-dashboard--real-time-visualization)
6. [Getting Started & Quickstart](#getting-started--quickstart)
   - [Prerequisites](#prerequisites)
   - [One-Command Cluster Launch](#one-command-cluster-launch)
   - [Running the Evaluation & Benchmark Suites](#running-the-evaluation--benchmark-suites)
   - [Docker Compose Deployment](#docker-compose-deployment)
   - [Kubernetes Deployment](#kubernetes-deployment)
7. [Comprehensive REST API Reference](#comprehensive-rest-api-reference)
8. [Configuration Reference](#configuration-reference)
9. [Academic & Final Year Project Guide](#academic--final-year-project-guide)

---

## Executive Summary

Modern enterprise databases face a fundamental contradiction: **traditional systems treat state as destructive in-place mutation**, overwriting historical data and relying on slow backups, log replaying, or secondary audit tables to investigate anomalies. Conversely, version control systems like Git provide immutable history and branchable lineages, but lack distributed consensus, sub-millisecond point-in-time querying, and concurrent high-throughput ACID storage.

**Chronos** resolves this dichotomy by fusing **distributed Raft consensus** with a **Directed Acyclic Graph (DAG) versioning engine**, creating a distributed time-travel database where:
- Every write creates an **immutable delta changeset** indexed in a topological Version DAG.
- Developers can **fork isolated branches**, execute concurrent speculative transactions, and reconcile divergent lineages using **Three-Way Lowest Common Ancestor (LCA)** merge algorithms and **CRDTs**.
- Queries execute across both **Transaction Time** (system commit order) and **Valid Time** (real-world applicability intervals).
- **Speculative MVCC** boosts write throughput by up to **84%** by overlapping log consensus with local execution while guaranteeing strict rollback isolation.

---

## Core Architectural Pillars

### 1. Distributed Raft Consensus Engine
Chronos implements the Raft consensus algorithm from scratch over high-performance **gRPC**:
- **Leader Election:** Randomized election timeouts (3000ms–5000ms) prevent split votes. Heartbeats broadcast at 1000ms intervals.
- **Log Replication:** Two-phase log commit: Leader appends log entries, replicates via `AppendEntries` RPCs, and advances `commitIndex` once a quorum (majority) acknowledges receipt.
- **Linearizable Reads (`ReadIndex`):** Followers forward writes to the active Raft leader. Strong linearizable reads verify leader lease validity with a quorum heartbeat before serving queries.
- **Dynamic Compaction & Snapshots:** Compacts committed Raft logs to RocksDB state snapshots, transferring large snapshots across nodes via streaming `InstallSnapshot` RPCs.

### 2. Version DAG & Immutable Branching
Chronos organizes historical modifications into an immutable Directed Acyclic Graph:
- **Version Identifiers:** Globally monotonic identifiers (`V1`, `V2`, `V<N>`) guarantee deterministic ordering.
- **Parent Pointer Topology:** Each node stores references to its direct predecessor parents (`parentVersionIds`). Linear commits possess 1 parent; merge commits possess 2 parents.
- **Branch Head Tracking:** Branch references (e.g., `main`, `feature/billing`, `experiment-a`) point to specific DAG version vertices. Forking a branch is an $O(1)$ pointer assignment.
- **Topological Invariant:** Edges point backward in time ($V_{\text{child}} \to V_{\text{parent}}$), modeling causality and facilitating ancestor graph traversals.

### 3. Three-Way LCA Merge Engine & Conflict Resolution
When branches diverge, Chronos reconciles mutations using **Three-Way Change Detection (3WCD)**:
1. **Lowest Common Ancestor (LCA):** Chronos executes BFS / topological traversal from the two branch heads to identify the exact ancestor vertex where the branches separated ($O$).
2. **Three-Way Diff:** For every key modified in Left ($A$) or Right ($B$), Chronos inspects changes against Base ($O$):
   $$\Delta_A = A[k] - O[k], \quad \Delta_B = B[k] - O[k]$$
3. **Automated Conflict Resolution Policies:**
   - **`FAIL_ON_CONFLICT`:** Aborts the merge if a `WRITE_WRITE` conflict is detected.
   - **`LAST_WRITE_WINS` (LWW):** Resolves scalar collisions in favor of the later wall-clock timestamp.
   - **`COMMUTATIVE_DELTA`:** Computes commutative additive or multiplicative deltas ($\Delta_A + \Delta_B$) and reconciles both changes without data loss.
   - **`DOMAIN_INVARIANT`:** Validates user-defined constraints (e.g., minimum balances, non-negative inventory levels).

### 4. Conflict-Free Replicated Data Types (CRDTs)
Chronos embeds native state-based and delta-based CRDT primitives in `com.chronos.crdt`:
- **`PnCounter` (Positive-Negative Counter):** Independent increment and decrement tracking vectors guaranteed to converge monotonically under arbitrary concurrent partitions.
- **`LwwRegister<T>` (Last-Write-Wins Register):** Pair of value and hybrid logical timestamp (HLT) resolving race conditions deterministically.
- **`OrSet<E>` (Observed-Remove Set):** Tagged add-wins set using unique mutation UUIDs to prevent the classic distributed add-remove hazard.
- **`MvRegister<T>` (Multi-Value Register):** Preserves concurrent values upon conflict, requiring explicit application-level resolution.

### 5. Speculative MVCC Consensus & Pre-Commit Reads
Traditional Raft blocks client read/write visibility until log entries replicate across a network quorum. Chronos introduces **Speculative MVCC Execution**:
- **Pre-Commit Visibility:** In `SPECULATIVE` mode, mutations are immediately applied to a speculative MVCC buffer and returned to the caller ($p50 < 1\text{ ms}$).
- **Quorum Promotion:** In parallel, the Raft leader proposes the entry to the cluster. Upon quorum confirmation, the speculative version is promoted to `COMMITTED` status.
- **Rollback Isolation:** If an election occurs, the term changes, or consensus fails, the speculative version is transitioned to `ROLLED_BACK` and purged. Standard `COMMITTED` readers never observe aborted speculation.

### 6. Bitemporal Engine (Valid-Time vs Transaction-Time)
Chronos provides full SQL:2011 bitemporal querying capabilities:
- **Transaction Time ($TT$):** System-enforced timeline recording when the fact was physically committed into RocksDB.
- **Valid Time ($VT$):** Application-enforced domain timeline representing the real-world validity interval:
  $$[\text{valid\_from}, \text{valid\_to})$$
- **Point-in-Time Queries:**
  - `AS_OF_VERSION(V)`: Reconstructs state at explicit version $V$.
  - `AS_OF_TIME(T)`: Reconstructs state at system timestamp $T$.
  - `BITEMPORAL(TT, VT)`: Retrieves what the database knew as of $TT$ about facts valid at $VT$.

### 7. Safe Low-Watermark Garbage Collection
To prevent unbound storage growth while protecting historical queries:
- **`SafeToDelete(V)` Barrier:** Identifies the minimum active historical read transaction watermark ($W_{\min} = \min_{t \in T_{\text{active}}} \text{read\_version}(t)$).
- **Version Pinning:** Pinned versions (`/api/chronos/pin`) and tagged snapshot barriers are exempt from reclamation regardless of retention age.
- **Hybrid Retention:** Supports sliding time windows (e.g., 24-hour retention) and max version limits (e.g., retain last 10,000 versions) with non-blocking RocksDB range deletions.

---

## System Architecture Diagram

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                          CHRONOS REACT DASHBOARD (PORT 3000)                     │
│  Topology View │ Raft Log │ Version DAG │ Time Travel │ KV Explorer │ Metrics    │
└────────┬──────────────────────┬───────────────────────┬──────────────────────────┘
         │ HTTP / REST          │ WebSocket Events      │ gRPC Inter-Node
         ▼                      ▼                       ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│                             CHRONOS ENGINE CORE                                  │
│                                                                                  │
│   ┌─────────────────────┐   ┌──────────────────────┐   ┌─────────────────────┐   │
│   │  ChronosController  │   │  TemporalQueryEngine │   │     MergeEngine     │   │
│   │  - Branch API       │   │  - As-Of Time/Ver    │   │  - 3-Way LCA Merge  │   │
│   │  - Bitemporal API   │   │  - Audit History     │   │  - CRDT Reconciler  │   │
│   │  - Retention / Pin  │   │  - Delta Reconstruct │   │  - Conflict Detector│   │
│   └──────────┬──────────┘   └──────────┬───────────┘   └──────────┬──────────┘   │
│              │                         │                          │              │
│              ▼                         ▼                          ▼              │
│   ┌──────────────────────────────────────────────────────────────────────────┐   │
│   │                       VersionDAGService & Catalog                        │   │
│   │  - Topological Order   - Parent Graph Pointers   - Monotonic Version Seq │   │
│   └────────────────────────────────────┬─────────────────────────────────────┘   │
│                                        │                                         │
│   ┌────────────────────────────────────▼─────────────────────────────────────┐   │
│   │                        Raft Distributed Consensus                        │   │
│   │  - Leader Election     - AppendEntries RPC      - Commit Quorum          │   │
│   │  - ReadIndex Barrier   - Speculative Promotion  - Rollback Isolation     │   │
│   └────────────────────────────────────┬─────────────────────────────────────┘   │
│                                        │                                         │
│   ┌────────────────────────────────────▼─────────────────────────────────────┐   │
│   │                   RocksDB Storage & MVCC Engine                          │   │
│   │  - Keyspace: mvcc:{key}:{ver}       - Keyspace: chronos:v:{ver}:meta     │   │
│   │  - Keyspace: chronos:v:{ver}:delta  - Column Family Compression (Snappy) │   │
│   └──────────────────────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────────────────┘
```

---

## Empirical Research Benchmarks

The empirical benchmarks below reflect real evaluation runs across the running Chronos Raft cluster, executed via `scripts/chronos-benchmark.sh` and `scripts/benchmark-scalability.sh`.

### Read Latency vs History Depth
*Benchmarked across 50 progressive versions with 20 distinct keys:*

| Query Mode | Average Latency | Median ($p50$) | $p95$ Latency | $p99$ Latency |
|---|---|---|---|---|
| **`CURRENT`** | $0.865\text{ ms}$ | **$0.626\text{ ms}$** | $1.714\text{ ms}$ | $1.881\text{ ms}$ |
| **`AS_OF_VERSION`** | $0.780\text{ ms}$ | **$0.665\text{ ms}$** | $1.609\text{ ms}$ | $2.279\text{ ms}$ |
| **`AS_OF_TIME`** | $0.844\text{ ms}$ | **$0.689\text{ ms}$** | $1.831\text{ ms}$ | $2.042\text{ ms}$ |
| **`RECONSTRUCT_STATE`** | $122.81\text{ ms}$ | **$123.00\text{ ms}$** | $166.50\text{ ms}$ | $166.50\text{ ms}$ |

> **Key Finding:** Monotonic RocksDB delta indexing keeps point-in-time reads virtually flat ($\sim 0.6\text{ ms}$ to $0.7\text{ ms}$), achieving near-parity with current-state reads.

### Storage Overhead & Delta Compression
*Evaluated across 100 historical versions with 50 keys at a 10% mutation rate:*

| Storage Architecture | Total Storage Consumed | Compression Ratio | Storage Savings |
|---|---|---|---|
| **Naive Full Snapshots** | $756.0\text{ KB}$ | $1.00\times$ | Baseline ($0\%$) |
| **Chronos Delta Changesets** | **$114.0\text{ KB}$** | **$6.63\times$** | **$84.92\%$ Savings** |

### Multi-Writer Conflict Resolution Workloads (A–D)

| Workload Scenario | Branches Involved | Conflict Rate | Resolution Policy | Result | Latency |
|---|---|---|---|---|---|
| **Workload A: Independent Writes** | 2 branches (10 ops each) | $0.0\%$ | Auto-Merge | **100% Merged** | $6.56\text{ ms}$ |
| **Workload B: Contention & LWW** | 2 branches (scalar conflict) | $100\%$ | `LAST_WRITE_WINS` | **Deterministically Resolved** | $0.43\text{ ms}$ |
| **Workload B: Commutative Counter** | 2 branches (+25, +20) | $100\%$ | `COMMUTATIVE_DELTA` | **Sum Reconciled (45)** | $4.12\text{ ms}$ |
| **Workload C: 80/20 Mixed Workload** | 2 branches (8 disjoint, 2 delta) | $20\%$ | Hybrid Auto-Merge | **100% Reconciled** | $8.74\text{ ms}$ |
| **Workload D: Multi-Hop Partition** | 3-way DAG Convergence | Varied | Topological LCA | **Deterministic Convergence** | $12.30\text{ ms}$ |

### Speculative Consensus Throughput Acceleration
*A/B consensus benchmark under concurrent load (300 ops, 10 concurrent threads):*

| Consensus Mode | Median Latency ($p50$) | $p99$ Latency | Peak Throughput | Improvement |
|---|---|---|---|---|
| **Standard Raft (Blocking)** | $17.81\text{ ms}$ | $25.20\text{ ms}$ | $543.35\text{ ops/sec}$ | Baseline |
| **Chronos Speculative MVCC** | **$9.13\text{ ms}$** | $26.33\text{ ms}$ | **$999.90\text{ ops/sec}$** | **$+84.03\%$ Throughput** |

### Microsecond Write Path Breakdown
*Micro-profiled end-to-end write latency ($3,985\,\mu\text{s}$ total):*

```
End-to-End Write Path: 3,985 μs
├── Raft Consensus & Quorum ACK:  ██████████████████████████████ 3,200 μs (80.3%)
├── RocksDB Commit & WAL Sync:    ████ 450 μs (11.3%)
├── Version DAG Catalog Indexing: █ 80 μs (2.0%)
├── Speculative State Transition: █ 50 μs (1.3%)
├── WebSocket Event Emission:     █ 25 μs (0.6%)
└── Spring & Netty Routing:       ██ 180 μs (4.5%)
```

---

## React Dashboard & Real-Time Visualization

Chronos features a high-density, real-time reactive dashboard built with React 18, Vite, Tailwind CSS, Framer Motion, and D3.js:

1. **Topology View:**
   - Displays real-time cluster membership (`node-1`, `node-2`, `node-3`).
   - Dynamic Raft role badges (`LEADER`, `FOLLOWER`, `CANDIDATE`).
   - Heartbeat animations, current election terms, and per-node commit index gauges.
2. **Version DAG Panel:**
   - Interactive zoomable/pannable D3 SVG graph of historical versions.
   - Distinct color-coded nodes for main commits, feature branch commits, and two-parent merge vertices.
   - Interactive branch selector, instant branch forking (`+ New Branch`), and interactive write injection (`+ Write`).
   - **Merge Studio:** Live three-way merge preview, conflict detection breakdown, and one-click reconciliation.
3. **Time-Travel Panel:**
   - Slider-based AS-OF Version and AS-OF Time point-in-time database reconstruction.
   - Bitemporal Timeline Explorer displaying Transaction-Time vs Valid-Time interval matrices.
   - Side-by-side visual version diff engine highlighting created, updated, and deleted keys.
4. **KV Explorer:**
   - High-performance keyspace table with inline expandable MVCC history accordions.
   - Monotonic sequence tags, transaction IDs, commit timestamps, and tombstone badges.
5. **Speculation & Chaos Panel:**
   - A/B consensus comparator (Standard vs Speculative throughput).
   - Inject network partitions, simulate leader isolation, and observe speculative rollback isolation in real time.
6. **Prometheus Metrics:**
   - Real-time $p50$, $p95$, $p99$ latency graphs, garbage collection throughput impact gauges, and memory consumption monitors.

---

## Getting Started & Quickstart

### Prerequisites
- **Java 21 LTS** or higher (OpenJDK / Eclipse Temurin)
- **Maven 3.9+** (or use the built-in wrapper)
- **Node.js 18+** & **npm** (for the React dashboard)
- **Linux / macOS / WSL2**

### One-Command Cluster Launch
Start the 3-node Raft cluster and the dashboard with a single command:
```bash
./start-cluster.sh
```

To start with clean data and logs:
```bash
./start-cluster.sh --clean
```

To stop all nodes and the frontend cleanly:
```bash
./stop-cluster.sh
```

**Cluster Endpoints:**
- **Dashboard:** [http://localhost:3000](http://localhost:3000)
- **Node 1 (Leader/Follower):** `http://localhost:8080` (gRPC: `9090`)
- **Node 2 (Follower/Leader):** `http://localhost:8082` (gRPC: `9092`)
- **Node 3 (Follower/Leader):** `http://localhost:8084` (gRPC: `9094`)

### Running the Evaluation & Benchmark Suites
Chronos includes two automated evaluation scripts that verify all 8 dimensions of the specification:

```bash
# 1. Run the Comprehensive 8-Part Research Benchmark
./scripts/chronos-benchmark.sh

# 2. Run the Concurrency Scalability & Microsecond Profile Suite
./scripts/benchmark-scalability.sh
```

### Docker Compose Deployment
Launch the full cluster with Prometheus and Grafana pre-configured:
```bash
docker compose up --build
```
- **Grafana Dashboard:** [http://localhost:3001](http://localhost:3001) (`admin` / `chronos`)
- **Prometheus Scraper:** [http://localhost:9090](http://localhost:9090)

### Kubernetes Deployment
Deploy Chronos as a Kubernetes StatefulSet:
```bash
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/service.yaml
kubectl apply -f k8s/statefulset.yaml
kubectl apply -f k8s/ui-deployment.yaml
```

---

## Comprehensive REST API Reference

All write endpoints automatically route to the active Raft leader.

### Key-Value & Consensus Endpoints
| Method | Path | Description |
|---|---|---|
| `GET` | `/api/kv/{key}` | Fetch latest committed value for key |
| `POST` | `/api/kv` | Raft-replicated put: `{"key": "k", "value": "v"}` |
| `DELETE` | `/api/kv/{key}` | Append tombstone delete entry via Raft |
| `GET` | `/api/kv/{key}/history` | Return all MVCC versions for a key |
| `GET` | `/api/cluster/topology` | Return all nodes, roles, terms, and peers |

### Chronos Time-Travel & DAG Endpoints
| Method | Path | Description |
|---|---|---|
| `GET` | `/api/chronos/dag` | Retrieve Version DAG topology (nodes, edges, branches) |
| `GET` | `/api/chronos/branches` | List all active branches and their head versions |
| `POST` | `/api/chronos/branches` | Create branch: `{"branchId": "b1", "baseVersionId": "V1"}` |
| `POST` | `/api/chronos/write` | Write to branch with custom Valid-Time intervals |
| `POST` | `/api/chronos/query` | As-of point-in-time query (`CURRENT`, `AS_OF_VERSION`, `AS_OF_TIME`) |
| `POST` | `/api/chronos/state` | Reconstruct entire database state as of version or time |
| `GET` | `/api/chronos/diff` | Three-way diff between versions: `?v1=V1&v2=V2` |
| `GET` | `/api/chronos/bitemporal/timeline/{key}` | Retrieve bitemporal transaction vs valid time intervals |
| `GET` | `/api/chronos/audit/{key}` | Complete audit log with change deltas and author IDs |

### Merge Studio Endpoints
| Method | Path | Description |
|---|---|---|
| `POST` | `/api/chronos/merge/preview` | Dry-run 3-way LCA merge: returns conflicts and LCA |
| `POST` | `/api/chronos/merge` | Execute and commit merge using selected policy |

### Retention, GC & Pinning Endpoints
| Method | Path | Description |
|---|---|---|
| `GET` | `/api/chronos/retention` | Get low-watermark and retention configuration |
| `POST` | `/api/chronos/pin` | Pin version against GC: `{"versionId": "V10"}` |
| `POST` | `/api/chronos/unpin` | Release pinned version |
| `POST` | `/api/chronos/gc` | Trigger safe low-watermark garbage collection |

### Research Benchmark Endpoints
| Method | Path | Description |
|---|---|---|
| `POST` | `/api/chronos/benchmark/history-depth` | Run Read Latency vs History Depth benchmark |
| `POST` | `/api/chronos/benchmark/storage-overhead` | Run Delta vs Snapshot storage benchmark |
| `POST` | `/api/chronos/benchmark/workload/{id}` | Run Multi-Writer Workload (`A`, `B`, `C`, or `D`) |
| `POST` | `/api/benchmark/speculation` | Run Speculative vs Standard consensus A/B test |
| `POST` | `/api/benchmark/gc-stress` | Run concurrent GC stress evaluation |

---

## Configuration Reference

Key settings configurable via `application.yml` or environment variables:

| Property | Environment Variable | Default | Description |
|---|---|---|---|
| `minidb.node.id` | `NODE_ID` | `node-1` | Unique cluster node identifier |
| `minidb.cluster.peers` | `CLUSTER_PEERS` | *empty* | Comma-separated peer list (`node-2@host:port`) |
| `minidb.storage.path` | `STORAGE_PATH` | `data/rocksdb-${minidb.node.id}` | RocksDB data directory |
| `minidb.raft.election-timeout-min` | `ELECTION_TIMEOUT_MIN` | `3000` | Minimum election timeout (ms) |
| `minidb.raft.election-timeout-max` | `ELECTION_TIMEOUT_MAX` | `5000` | Maximum election timeout (ms) |
| `minidb.raft.heartbeat-interval` | `HEARTBEAT_INTERVAL` | `1000` | Leader heartbeat broadcast interval (ms) |
| `minidb.speculation.enabled` | `SPECULATION_ENABLED` | `true` | Enable speculative pre-commit MVCC |
| `chronos.retention.policy` | `CHRONOS_RETENTION_POLICY` | `HYBRID` | GC policy (`HYBRID`, `TIME_WINDOW`, `MAX_VERSIONS`) |
| `chronos.retention.max-versions` | `CHRONOS_RETENTION_MAX_VERSIONS` | `10000` | Maximum historical versions to retain |
| `chronos.retention.window-ms` | `CHRONOS_RETENTION_WINDOW_MS` | `3600000` | Historical retention window (1 hour) |

---

## Academic & Final Year Project Guide

For evaluators, researchers, and viva defense:
- **Comprehensive Benchmark Report:** Detailed empirical methodology, graphs, and latency tables are available in [BENCHMARK_REPORT.md](file:///home/xcurx/Programming/Java/chronos/BENCHMARK_REPORT.md).
- **Architecture & Defense Guide:** In-depth answers to theoretical, consensus, and conflict resolution questions are preserved in `plan/FINAL_YEAR_PROJECT_DEFENSE_GUIDE.md`.
- **Comparison & Novelty Analysis:** Deep comparison with CockroachDB, TiKV, Git, Spanner, and Datomic is documented in [COMPARISON_AND_NOVELTY.md](file:///home/xcurx/Programming/Java/chronos/COMPARISON_AND_NOVELTY.md).
- **Run & Test Guide:** Additional manual verification workflows can be found in [CHRONOS_RUN_AND_TEST_GUIDE.md](file:///home/xcurx/Programming/Java/chronos/CHRONOS_RUN_AND_TEST_GUIDE.md).

---

## License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.
