import { useState, useEffect, useCallback } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { useClusterStore } from '../stores/clusterStore';
import type {
  QueryMode,
  ReadMode,
  ChronosVersionSummary,
  DatabaseStateResult,
  KeyAuditTrail,
  VersionDiffResult,
  RetentionWatermark,
} from '../types/chronos';
import {
  Clock, History, Search, GitCompare, Camera, ShieldAlert,
  Calendar, Layers, CheckCircle2, AlertTriangle, ArrowRight,
  Database, RefreshCw, Loader2, Sparkles, Sliders, ShieldCheck, X
} from 'lucide-react';

function getApiBase() {
  return useClusterStore.getState().apiBaseUrl || '/api';
}

export default function TimeTravelPanel() {
  const [subTab, setSubTab] = useState<'reconstruction' | 'audit' | 'diff' | 'retention'>('reconstruction');

  // Query parameters
  const [queryMode, setQueryMode] = useState<QueryMode>('CURRENT');
  const [readMode, setReadMode] = useState<ReadMode>('LINEARIZABLE');
  const [selectedVersion, setSelectedVersion] = useState('');
  const [asOfTimestamp, setAsOfTimestamp] = useState<string>('');
  const [selectedSnapshot, setSelectedSnapshot] = useState('');
  const [queryKey, setQueryKey] = useState('');

  // Loaded metadata
  const [versions, setVersions] = useState<ChronosVersionSummary[]>([]);
  const [snapshots, setSnapshots] = useState<string[]>([]);
  const [watermark, setWatermark] = useState<RetentionWatermark | null>(null);

  // Results
  const [dbState, setDbState] = useState<DatabaseStateResult | null>(null);
  const [auditTrail, setAuditTrail] = useState<KeyAuditTrail | null>(null);
  const [diffResult, setDiffResult] = useState<VersionDiffResult | null>(null);
  const [diffBaseVer, setDiffBaseVer] = useState('');
  const [diffTargetVer, setDiffTargetVer] = useState('');

  // UI state
  const [loading, setLoading] = useState(false);
  const [errorMsg, setErrorMsg] = useState<string | null>(null);
  const [historyPruned, setHistoryPruned] = useState(false);
  const [snapshotName, setSnapshotName] = useState('');
  const [snapshotSuccess, setSnapshotSuccess] = useState<string | null>(null);

  // Fetch version list and retention stats on mount
  const refreshMetadata = useCallback(async () => {
    try {
      const [versRes, snapsRes, wmRes] = await Promise.all([
        fetch(`${getApiBase()}/chronos/versions?limit=50`).then((r) => r.ok ? r.json() : null),
        fetch(`${getApiBase()}/chronos/snapshots`).then((r) => r.ok ? r.json() : null),
        fetch(`${getApiBase()}/chronos/retention`).then((r) => r.ok ? r.json() : null),
      ]);

      if (versRes?.versions) {
        setVersions(versRes.versions);
        if (!selectedVersion && versRes.versions.length > 0) {
          setSelectedVersion(versRes.versions[0].versionId);
        }
        if (!diffTargetVer && versRes.versions.length > 0) {
          setDiffTargetVer(versRes.versions[0].versionId);
        }
        if (!diffBaseVer && versRes.versions.length > 1) {
          setDiffBaseVer(versRes.versions[1].versionId);
        }
      }
      if (snapsRes?.snapshots) {
        const snapIds = snapsRes.snapshots.map((s: any) =>
          typeof s === 'string' ? s : (s.snapshotId || s.name || String(s))
        );
        setSnapshots(snapIds);
        if (!selectedSnapshot && snapIds.length > 0) {
          setSelectedSnapshot(snapIds[0]);
        }
      }
      if (wmRes) {
        setWatermark(wmRes);
      }
    } catch (err) {
      console.warn('Failed to fetch Chronos metadata:', err);
    }
  }, [selectedVersion, diffTargetVer, diffBaseVer, selectedSnapshot]);

  useEffect(() => {
    refreshMetadata();
  }, [refreshMetadata]);

  // Execute State Reconstruction Query
  const executeReconstruction = useCallback(async () => {
    setLoading(true);
    setErrorMsg(null);
    setHistoryPruned(false);
    try {
      const payload: Record<string, unknown> = {
        queryMode,
        readMode,
      };

      if (queryMode === 'AS_OF_VERSION') payload.versionId = selectedVersion;
      if (queryMode === 'AS_OF_TIME') {
        const parsed = Date.parse(asOfTimestamp);
        payload.timestamp = !isNaN(parsed) ? parsed : Date.now();
      }
      if (queryMode === 'AS_OF_SNAPSHOT') payload.snapshotId = selectedSnapshot;

      const res = await fetch(`${getApiBase()}/chronos/state`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });

      if (res.status === 410) {
        const data = await res.json().catch(() => ({}));
        setHistoryPruned(true);
        setErrorMsg(data.error || 'Requested timestamp has been pruned by Low-Watermark GC');
        setDbState(null);
      } else if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        setErrorMsg(data.error || `Query failed with status ${res.status}`);
        setDbState(null);
      } else {
        const data = await res.json();
        const keysMap = data.keys || data.entries || {};
        const total = data.totalKeys ?? data.count ?? Object.keys(keysMap).length;
        setDbState({
          ...data,
          keys: keysMap,
          totalKeys: total,
          asOfVersionId: data.asOfVersionId || data.versionId || 'V_HEAD',
          executionTimeMs: data.executionTimeMs ?? data.latencyMs ?? 0,
        });
      }
    } catch (e: any) {
      setErrorMsg(e.message || 'Network error');
    } finally {
      setLoading(false);
    }
  }, [queryMode, readMode, selectedVersion, asOfTimestamp, selectedSnapshot]);

  // Auto-run reconstruction when query parameters change or on mount
  useEffect(() => {
    executeReconstruction();
  }, [executeReconstruction]);

  // Execute Key Audit Query
  const executeAudit = async (targetKey?: string) => {
    const k = targetKey || queryKey;
    if (!k.trim()) return;
    setLoading(true);
    setErrorMsg(null);
    try {
      const res = await fetch(`${getApiBase()}/chronos/audit/${encodeURIComponent(k.trim())}`);
      if (res.status === 410) {
        setHistoryPruned(true);
        setErrorMsg('Some audit history for this key was pruned beyond the low watermark');
      } else if (!res.ok) {
        setErrorMsg(`Audit lookup failed (${res.status})`);
      } else {
        const data = await res.json();
        setAuditTrail(data);
      }
    } catch (e: any) {
      setErrorMsg(e.message || 'Audit lookup error');
    } finally {
      setLoading(false);
    }
  };

  // Execute Version Diff Query
  const executeDiff = async () => {
    if (!diffBaseVer || !diffTargetVer) return;
    setLoading(true);
    setErrorMsg(null);
    try {
      const res = await fetch(
        `${getApiBase()}/chronos/diff?v1=${encodeURIComponent(diffBaseVer)}&v2=${encodeURIComponent(diffTargetVer)}`
      );
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        setErrorMsg(data.error || `Diff failed with status ${res.status}`);
      } else {
        const data = await res.json();
        setDiffResult(data);
      }
    } catch (e: any) {
      setErrorMsg(e.message || 'Diff error');
    } finally {
      setLoading(false);
    }
  };

  // Create Snapshot
  const handleCreateSnapshot = async () => {
    if (!snapshotName.trim()) return;
    try {
      const res = await fetch(`${getApiBase()}/chronos/snapshots`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ snapshotId: snapshotName.trim() }),
      });
      if (res.ok) {
        setSnapshotSuccess(`Snapshot '${snapshotName}' created successfully`);
        setSnapshotName('');
        refreshMetadata();
        setTimeout(() => setSnapshotSuccess(null), 4000);
      }
    } catch (err: any) {
      setErrorMsg('Failed to create snapshot: ' + err.message);
    }
  };

  return (
    <div className="flex-1 flex flex-col h-full overflow-hidden bg-surface-950 p-6 space-y-5">
      {/* Top Banner / Watermark Stats */}
      <div className="flex items-center justify-between bg-surface-900/70 border border-white/[0.08] rounded-2xl px-6 py-4 backdrop-blur-xl shadow-lg">
        <div className="flex items-center gap-4">
          <div className="w-12 h-12 rounded-xl bg-gradient-to-br from-indigo-500/20 to-purple-500/20 border border-indigo-500/30 flex items-center justify-center text-indigo-400">
            <History className="w-6 h-6" />
          </div>
          <div>
            <div className="flex items-center gap-2">
              <h2 className="text-base font-bold text-white tracking-tight">Chronos Time-Travel & Provenance Engine</h2>
              <span className="px-2 py-0.5 text-[10px] font-semibold tracking-wider uppercase rounded-full bg-indigo-500/20 text-indigo-300 border border-indigo-500/30">
                MVCC DAG
              </span>
            </div>
            <p className="text-xs text-white/50 mt-0.5">
              Reconstruct exact point-in-time database states, inspect key mutation lineages, and audit version diffs.
            </p>
          </div>
        </div>

        {/* Retention Watermark Pill */}
        {watermark && (
          <div className="flex items-center gap-3 bg-white/[0.03] border border-white/[0.06] rounded-xl px-4 py-2 text-xs">
            <ShieldCheck className="w-4 h-4 text-emerald-400" />
            <div>
              <span className="text-white/40 block text-[10px]">Safe Low-Watermark</span>
              <span className="font-mono text-emerald-300 font-semibold">
                Seq ≥ {watermark.watermark?.earliestAvailableSeq ?? 0}
              </span>
            </div>
            <div className="h-6 w-px bg-white/10 mx-1" />
            <div>
              <span className="text-white/40 block text-[10px]">Active Read Leases</span>
              <span className="font-mono text-cyan-300 font-semibold">{watermark.activeReadPins ?? 0}</span>
            </div>
          </div>
        )}
      </div>

      {/* Navigation Sub-tabs */}
      <div className="flex items-center justify-between border-b border-white/[0.08] pb-3">
        <div className="flex items-center gap-1.5 bg-surface-900/60 p-1 rounded-xl border border-white/[0.06]">
          {[
            { id: 'reconstruction', label: 'Database As-Of', icon: Database },
            { id: 'audit', label: 'Key Provenance', icon: Clock },
            { id: 'diff', label: 'Version Diff', icon: GitCompare },
            { id: 'retention', label: 'Retention & Snapshots', icon: Camera },
          ].map((tab) => {
            const Icon = tab.icon;
            const active = subTab === tab.id;
            return (
              <button
                key={tab.id}
                onClick={() => setSubTab(tab.id as any)}
                className={`flex items-center gap-2 px-4 py-2 rounded-lg text-xs font-medium transition-all ${
                  active
                    ? 'bg-indigo-600 text-white shadow-md shadow-indigo-600/30'
                    : 'text-white/50 hover:text-white/80 hover:bg-white/[0.03]'
                }`}
              >
                <Icon className="w-3.5 h-3.5" />
                {tab.label}
              </button>
            );
          })}
        </div>

        <button
          onClick={refreshMetadata}
          className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs bg-white/[0.03] hover:bg-white/[0.08] text-white/60 hover:text-white transition"
          title="Refresh version catalog"
        >
          <RefreshCw className="w-3.5 h-3.5" />
          <span>Refresh Catalog</span>
        </button>
      </div>

      {/* Alerts */}
      <AnimatePresence>
        {historyPruned && (
          <motion.div
            initial={{ opacity: 0, y: -10 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -10 }}
            className="flex items-center gap-3 bg-amber-950/60 border border-amber-500/40 rounded-xl px-4 py-3 text-amber-200 text-xs"
          >
            <ShieldAlert className="w-5 h-5 text-amber-400 shrink-0" />
            <div className="flex-1">
              <span className="font-semibold">HISTORY_UNAVAILABLE (HTTP 410):</span> {errorMsg}
            </div>
            <span className="text-[11px] text-amber-400/80 underline cursor-pointer" onClick={() => setSubTab('retention')}>
              View Retention Bounds
            </span>
          </motion.div>
        )}
        {errorMsg && !historyPruned && (
          <motion.div
            initial={{ opacity: 0, y: -10 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -10 }}
            className="flex items-center gap-3 bg-rose-950/60 border border-rose-500/40 rounded-xl px-4 py-3 text-rose-200 text-xs"
          >
            <AlertTriangle className="w-5 h-5 text-rose-400 shrink-0" />
            <div className="flex-1">
              <span className="font-semibold">Query Error:</span> {errorMsg}
            </div>
            <button onClick={() => setErrorMsg(null)} className="text-white/40 hover:text-white">
              <X className="w-4 h-4" />
            </button>
          </motion.div>
        )}
      </AnimatePresence>

      {/* Main Tab Content */}
      <div className="flex-1 overflow-y-auto space-y-4">
        {/* SUBTAB 1: STATE RECONSTRUCTION */}
        {subTab === 'reconstruction' && (
          <div className="space-y-4">
            {/* Control Panel */}
            <div className="bg-surface-900/60 border border-white/[0.06] rounded-2xl p-5 backdrop-blur-xl space-y-4">
              <div className="flex flex-wrap items-center justify-between gap-4">
                {/* Query Mode Picker */}
                <div>
                  <label className="text-[11px] font-semibold text-white/50 uppercase tracking-wider block mb-1.5">
                    Temporal Mode
                  </label>
                  <div className="flex items-center gap-1 bg-surface-950 p-1 rounded-xl border border-white/[0.06]">
                    {(['CURRENT', 'AS_OF_VERSION', 'AS_OF_TIME', 'AS_OF_SNAPSHOT'] as QueryMode[]).map((mode) => (
                      <button
                        key={mode}
                        onClick={() => setQueryMode(mode)}
                        className={`px-3 py-1.5 rounded-lg text-xs font-medium transition ${
                          queryMode === mode
                            ? 'bg-indigo-600 text-white shadow-sm'
                            : 'text-white/50 hover:text-white/80'
                        }`}
                      >
                        {mode.replace(/_/g, ' ')}
                      </button>
                    ))}
                  </div>
                </div>

                {/* Read Mode Toggle */}
                <div>
                  <label className="text-[11px] font-semibold text-white/50 uppercase tracking-wider block mb-1.5">
                    Read Consistency
                  </label>
                  <div className="flex items-center gap-1 bg-surface-950 p-1 rounded-xl border border-white/[0.06]">
                    {(['LINEARIZABLE', 'SPECULATIVE'] as ReadMode[]).map((mode) => (
                      <button
                        key={mode}
                        onClick={() => setReadMode(mode)}
                        className={`px-3 py-1.5 rounded-lg text-xs font-medium transition ${
                          readMode === mode
                            ? 'bg-cyan-600 text-white shadow-sm'
                            : 'text-white/50 hover:text-white/80'
                        }`}
                      >
                        {mode}
                      </button>
                    ))}
                  </div>
                </div>
              </div>

              {/* Dynamic Parameter Selectors */}
              <div className="grid grid-cols-1 md:grid-cols-3 gap-4 pt-2 border-t border-white/[0.04]">
                {queryMode === 'AS_OF_VERSION' && (
                  <div className="col-span-2">
                    <label className="text-xs text-white/60 block mb-1">Target Version ID</label>
                    <select
                      value={selectedVersion}
                      onChange={(e) => setSelectedVersion(e.target.value)}
                      className="w-full bg-surface-950 border border-white/[0.08] rounded-xl px-3 py-2 text-xs text-white focus:outline-none focus:border-indigo-500 font-mono"
                    >
                      {versions.map((v) => (
                        <option key={v.versionId} value={v.versionId}>
                          {v.versionId} (seq={v.sequence}, branch={v.branchId}, {new Date(v.timestamp).toLocaleTimeString()})
                        </option>
                      ))}
                    </select>
                  </div>
                )}

                {queryMode === 'AS_OF_TIME' && (
                  <div className="col-span-2">
                    <label className="text-xs text-white/60 block mb-1">Historical Timestamp</label>
                    <div className="flex items-center gap-2">
                      <input
                        type="datetime-local"
                        value={asOfTimestamp}
                        onChange={(e) => setAsOfTimestamp(e.target.value)}
                        className="flex-1 bg-surface-950 border border-white/[0.08] rounded-xl px-3 py-2 text-xs text-white focus:outline-none focus:border-indigo-500"
                      />
                      <button
                        onClick={() => setAsOfTimestamp(new Date(Date.now() - 60000).toISOString().slice(0, 16))}
                        className="px-2.5 py-2 text-xs bg-white/[0.04] hover:bg-white/[0.08] rounded-xl text-white/60"
                      >
                        -1m
                      </button>
                      <button
                        onClick={() => setAsOfTimestamp(new Date(Date.now() - 300000).toISOString().slice(0, 16))}
                        className="px-2.5 py-2 text-xs bg-white/[0.04] hover:bg-white/[0.08] rounded-xl text-white/60"
                      >
                        -5m
                      </button>
                    </div>
                  </div>
                )}

                {queryMode === 'AS_OF_SNAPSHOT' && (
                  <div className="col-span-2">
                    <label className="text-xs text-white/60 block mb-1">Snapshot Name</label>
                    <select
                      value={selectedSnapshot}
                      onChange={(e) => setSelectedSnapshot(e.target.value)}
                      className="w-full bg-surface-950 border border-white/[0.08] rounded-xl px-3 py-2 text-xs text-white focus:outline-none focus:border-indigo-500 font-mono"
                    >
                      {snapshots.map((s) => (
                        <option key={s} value={s}>{s}</option>
                      ))}
                    </select>
                  </div>
                )}

                <div className="flex items-end">
                  <button
                    onClick={executeReconstruction}
                    disabled={loading}
                    className="w-full bg-gradient-to-r from-indigo-600 to-purple-600 hover:from-indigo-500 hover:to-purple-500 text-white font-medium text-xs py-2 px-4 rounded-xl flex items-center justify-center gap-2 shadow-lg shadow-indigo-600/30 transition disabled:opacity-50"
                  >
                    {loading ? <Loader2 className="w-4 h-4 animate-spin" /> : <Search className="w-4 h-4" />}
                    <span>Reconstruct State</span>
                  </button>
                </div>
              </div>
            </div>

            {/* Reconstructed Table Results */}
            {dbState && (
              <div className="bg-surface-900/60 border border-white/[0.06] rounded-2xl p-5 backdrop-blur-xl space-y-3">
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-3">
                    <h3 className="text-xs font-bold text-white uppercase tracking-wider">
                      Reconstructed State Table ({dbState.totalKeys} keys)
                    </h3>
                    <span className="px-2 py-0.5 text-[10px] font-mono rounded bg-white/[0.04] text-white/60">
                      Anchor: {dbState.asOfVersionId}
                    </span>
                    <span className="px-2 py-0.5 text-[10px] font-mono rounded bg-emerald-500/10 text-emerald-400 border border-emerald-500/20">
                      {dbState.executionTimeMs?.toFixed(1)} ms
                    </span>
                  </div>
                </div>

                <div className="border border-white/[0.06] rounded-xl overflow-hidden">
                  <table className="w-full text-left text-xs">
                    <thead className="bg-surface-950/80 text-white/50 border-b border-white/[0.06]">
                      <tr>
                        <th className="py-2.5 px-4 font-semibold">Key</th>
                        <th className="py-2.5 px-4 font-semibold">Value</th>
                        <th className="py-2.5 px-4 font-semibold text-right">Actions</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-white/[0.04]">
                      {Object.entries(dbState.keys).map(([k, v]) => (
                        <tr key={k} className="hover:bg-white/[0.02] transition">
                          <td className="py-2 px-4 font-mono text-cyan-300 font-medium">{k}</td>
                          <td className="py-2 px-4 font-mono text-white/80 max-w-md truncate">{v}</td>
                          <td className="py-2 px-4 text-right">
                            <button
                              onClick={() => {
                                setQueryKey(k);
                                setSubTab('audit');
                                executeAudit(k);
                              }}
                              className="text-xs text-indigo-400 hover:text-indigo-300 hover:underline"
                            >
                              Audit History
                            </button>
                          </td>
                        </tr>
                      ))}
                      {Object.keys(dbState.keys).length === 0 && (
                        <tr>
                          <td colSpan={3} className="py-6 text-center text-white/40">
                            No keys existed in database at this historical point.
                          </td>
                        </tr>
                      )}
                    </tbody>
                  </table>
                </div>
              </div>
            )}
          </div>
        )}

        {/* SUBTAB 2: KEY PROVENANCE AUDIT */}
        {subTab === 'audit' && (
          <div className="space-y-4">
            <div className="bg-surface-900/60 border border-white/[0.06] rounded-2xl p-5 backdrop-blur-xl flex items-center gap-3">
              <input
                type="text"
                placeholder="Enter key to trace (e.g. counter, balance:acc1)..."
                value={queryKey}
                onChange={(e) => setQueryKey(e.target.value)}
                onKeyDown={(e) => e.key === 'Enter' && executeAudit()}
                className="flex-1 bg-surface-950 border border-white/[0.08] rounded-xl px-4 py-2 text-xs text-white placeholder-white/30 focus:outline-none focus:border-indigo-500 font-mono"
              />
              <button
                onClick={() => executeAudit()}
                disabled={loading || !queryKey.trim()}
                className="bg-indigo-600 hover:bg-indigo-500 text-white px-5 py-2 rounded-xl text-xs font-medium flex items-center gap-2 transition disabled:opacity-50"
              >
                {loading ? <Loader2 className="w-4 h-4 animate-spin" /> : <Search className="w-4 h-4" />}
                <span>Inspect Lineage</span>
              </button>
            </div>

            {auditTrail && (
              <div className="bg-surface-900/60 border border-white/[0.06] rounded-2xl p-5 backdrop-blur-xl space-y-4">
                <div className="flex items-center justify-between border-b border-white/[0.06] pb-3">
                  <div>
                    <h3 className="text-sm font-bold text-white font-mono">{auditTrail.key}</h3>
                    <p className="text-xs text-white/50">{auditTrail.totalModifications} total historical mutations</p>
                  </div>
                </div>

                <div className="space-y-3 relative before:absolute before:inset-0 before:left-3.5 before:w-0.5 before:bg-white/[0.08]">
                  {auditTrail.history.map((entry, idx) => {
                    const isDelete = entry.opType === 'DELETE';
                    const isDelta = entry.opType === 'DELTA';
                    return (
                      <div key={idx} className="relative flex items-start gap-4 pl-8 group">
                        <div className={`absolute left-2 w-3.5 h-3.5 rounded-full border-2 border-surface-950 ${
                          isDelete ? 'bg-red-500' : isDelta ? 'bg-amber-500' : 'bg-emerald-500'
                        }`} />
                        <div className="flex-1 bg-surface-950 border border-white/[0.06] rounded-xl p-3 text-xs space-y-1">
                          <div className="flex items-center justify-between">
                            <span className="font-mono text-indigo-300 font-semibold">{entry.versionId}</span>
                            <span className="text-[10px] text-white/40">{new Date(entry.timestamp).toLocaleString()}</span>
                          </div>
                          <div className="flex items-center gap-2 text-white/80">
                            <span className={`px-1.5 py-0.5 rounded text-[10px] font-bold ${
                              isDelete ? 'bg-red-500/20 text-red-300' : isDelta ? 'bg-amber-500/20 text-amber-300' : 'bg-emerald-500/20 text-emerald-300'
                            }`}>
                              {entry.opType}
                            </span>
                            <span className="text-white/40">by</span>
                            <span className="font-mono text-white/70">{entry.writerId || 'anonymous'}</span>
                            <span className="text-white/40">on branch</span>
                            <span className="font-mono text-purple-300">{entry.branchId || 'main'}</span>
                          </div>
                          <div className="bg-black/40 rounded p-2 font-mono text-[11px] text-white/90">
                            {entry.oldValue && <span className="text-red-400 line-through mr-2">{entry.oldValue}</span>}
                            {entry.newValue && <span className="text-emerald-300">{entry.newValue}</span>}
                            {isDelete && <span className="text-red-400 italic">&lt;tombstone&gt;</span>}
                          </div>
                        </div>
                      </div>
                    );
                  })}
                </div>
              </div>
            )}
          </div>
        )}

        {/* SUBTAB 3: VERSION DIFF */}
        {subTab === 'diff' && (
          <div className="space-y-4">
            <div className="bg-surface-900/60 border border-white/[0.06] rounded-2xl p-5 backdrop-blur-xl grid grid-cols-1 md:grid-cols-3 gap-4 items-end">
              <div>
                <label className="text-xs text-white/60 block mb-1">Base Version (Ancestor)</label>
                <select
                  value={diffBaseVer}
                  onChange={(e) => setDiffBaseVer(e.target.value)}
                  className="w-full bg-surface-950 border border-white/[0.08] rounded-xl px-3 py-2 text-xs text-white font-mono"
                >
                  {versions.map((v) => (
                    <option key={v.versionId} value={v.versionId}>{v.versionId} (seq={v.sequence})</option>
                  ))}
                </select>
              </div>

              <div>
                <label className="text-xs text-white/60 block mb-1">Target Version (Tip)</label>
                <select
                  value={diffTargetVer}
                  onChange={(e) => setDiffTargetVer(e.target.value)}
                  className="w-full bg-surface-950 border border-white/[0.08] rounded-xl px-3 py-2 text-xs text-white font-mono"
                >
                  {versions.map((v) => (
                    <option key={v.versionId} value={v.versionId}>{v.versionId} (seq={v.sequence})</option>
                  ))}
                </select>
              </div>

              <button
                onClick={executeDiff}
                disabled={loading || !diffBaseVer || !diffTargetVer}
                className="bg-indigo-600 hover:bg-indigo-500 text-white font-medium text-xs py-2 px-4 rounded-xl flex items-center justify-center gap-2 transition disabled:opacity-50"
              >
                {loading ? <Loader2 className="w-4 h-4 animate-spin" /> : <GitCompare className="w-4 h-4" />}
                <span>Compute Diff</span>
              </button>
            </div>

            {diffResult && (
              <div className="bg-surface-900/60 border border-white/[0.06] rounded-2xl p-5 backdrop-blur-xl space-y-3">
                <div className="flex items-center justify-between border-b border-white/[0.06] pb-3">
                  <h3 className="text-xs font-bold text-white uppercase tracking-wider">
                    Diff: {diffResult.baseVersionId} → {diffResult.targetVersionId}
                  </h3>
                  <span className="text-xs text-white/50">{diffResult.keysModified} key(s) modified</span>
                </div>

                <div className="divide-y divide-white/[0.04]">
                  {diffResult.changes.map((change) => (
                    <div key={change.key} className="py-2.5 flex items-center justify-between text-xs font-mono">
                      <div className="flex items-center gap-3">
                        <span className={`px-2 py-0.5 rounded text-[10px] font-bold ${
                          change.changeType === 'ADDED' ? 'bg-emerald-500/20 text-emerald-400' :
                          change.changeType === 'DELETED' ? 'bg-red-500/20 text-red-400' :
                          'bg-amber-500/20 text-amber-400'
                        }`}>
                          {change.changeType}
                        </span>
                        <span className="text-cyan-300 font-semibold">{change.key}</span>
                      </div>
                      <div className="flex items-center gap-2">
                        {change.oldValue && <span className="text-red-400 line-through">{change.oldValue}</span>}
                        {change.oldValue && change.newValue && <ArrowRight className="w-3.5 h-3.5 text-white/30" />}
                        {change.newValue && <span className="text-emerald-300">{change.newValue}</span>}
                      </div>
                    </div>
                  ))}
                  {diffResult.changes.length === 0 && (
                    <div className="py-6 text-center text-white/40 text-xs">
                      No key modifications between versions {diffResult.baseVersionId} and {diffResult.targetVersionId}.
                    </div>
                  )}
                </div>
              </div>
            )}
          </div>
        )}

        {/* SUBTAB 4: RETENTION & SNAPSHOTS */}
        {subTab === 'retention' && (
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {/* Snapshot Creator */}
            <div className="bg-surface-900/60 border border-white/[0.06] rounded-2xl p-5 backdrop-blur-xl space-y-4">
              <h3 className="text-xs font-bold text-white uppercase tracking-wider flex items-center gap-2">
                <Camera className="w-4 h-4 text-indigo-400" />
                <span>Create Named Immutable Snapshot</span>
              </h3>
              <p className="text-xs text-white/50">
                Snapshots pin the current database state for audit compliance and long-term historical anchoring.
              </p>
              <div className="flex items-center gap-2">
                <input
                  type="text"
                  placeholder="Snapshot name (e.g. audit-q3-close)..."
                  value={snapshotName}
                  onChange={(e) => setSnapshotName(e.target.value)}
                  className="flex-1 bg-surface-950 border border-white/[0.08] rounded-xl px-3 py-2 text-xs text-white font-mono focus:outline-none focus:border-indigo-500"
                />
                <button
                  onClick={handleCreateSnapshot}
                  disabled={!snapshotName.trim()}
                  className="bg-indigo-600 hover:bg-indigo-500 text-white px-4 py-2 rounded-xl text-xs font-medium transition disabled:opacity-50"
                >
                  Create
                </button>
              </div>

              {snapshotSuccess && (
                <div className="text-xs text-emerald-400 flex items-center gap-1.5">
                  <CheckCircle2 className="w-4 h-4" />
                  <span>{snapshotSuccess}</span>
                </div>
              )}

              <div className="pt-3 border-t border-white/[0.06] space-y-2">
                <span className="text-[11px] font-semibold text-white/50 uppercase tracking-wider block">
                  Existing Snapshots ({snapshots.length})
                </span>
                <div className="divide-y divide-white/[0.04] max-h-48 overflow-y-auto">
                  {snapshots.map((s) => (
                    <div key={s} className="py-2 flex items-center justify-between text-xs font-mono text-white/80">
                      <span>{s}</span>
                      <button
                        onClick={() => {
                          setSelectedSnapshot(s);
                          setQueryMode('AS_OF_SNAPSHOT');
                          setSubTab('reconstruction');
                        }}
                        className="text-[11px] text-indigo-400 hover:underline"
                      >
                        Query As-Of
                      </button>
                    </div>
                  ))}
                  {snapshots.length === 0 && (
                    <div className="text-xs text-white/30 italic py-2">No snapshots created yet.</div>
                  )}
                </div>
              </div>
            </div>

            {/* Retention Watermark & Policy Info */}
            {watermark && (
              <div className="bg-surface-900/60 border border-white/[0.06] rounded-2xl p-5 backdrop-blur-xl space-y-4">
                <h3 className="text-xs font-bold text-white uppercase tracking-wider flex items-center gap-2">
                  <ShieldCheck className="w-4 h-4 text-emerald-400" />
                  <span>Safe Low-Watermark Retention Policy</span>
                </h3>

                <div className="space-y-2 text-xs">
                  <div className="flex justify-between py-1 border-b border-white/[0.04]">
                    <span className="text-white/50">Active Retention Policy:</span>
                    <span className="font-mono text-indigo-300 font-semibold">{watermark.policy}</span>
                  </div>
                  <div className="flex justify-between py-1 border-b border-white/[0.04]">
                    <span className="text-white/50">Earliest Retained Sequence:</span>
                    <span className="font-mono text-emerald-400">{watermark.watermark?.earliestAvailableSeq}</span>
                  </div>
                  <div className="flex justify-between py-1 border-b border-white/[0.04]">
                    <span className="text-white/50">Earliest Retained Time:</span>
                    <span className="font-mono text-white/80">
                      {new Date(watermark.watermark?.earliestAvailableTimestamp || 0).toLocaleString()}
                    </span>
                  </div>
                  <div className="flex justify-between py-1 border-b border-white/[0.04]">
                    <span className="text-white/50">Active Read Leases:</span>
                    <span className="font-mono text-cyan-300">{watermark.activeReadPins}</span>
                  </div>
                  <div className="flex justify-between py-1 border-b border-white/[0.04]">
                    <span className="text-white/50">Total Versions Purged by GC:</span>
                    <span className="font-mono text-amber-400">{watermark.totalVersionsPurged}</span>
                  </div>
                  <div className="flex justify-between py-1">
                    <span className="text-white/50">Total MVCC Records Purged:</span>
                    <span className="font-mono text-amber-400">{watermark.totalMvccPurged}</span>
                  </div>
                </div>

                <div className="bg-surface-950 p-3 rounded-xl border border-white/[0.06] text-[11px] text-white/50 space-y-1">
                  <span className="font-semibold text-white/70 block">SafeToDelete Invariant:</span>
                  <p>
                    Historical queries targeting sequence numbers or timestamps older than the Low-Watermark cleanly return HTTP 410 (HistoryUnavailable) rather than returning silent wrong answers.
                  </p>
                </div>
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

