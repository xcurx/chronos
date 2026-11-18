import { useState, useEffect, useCallback, useMemo } from 'react';
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
  BitemporalTimelineResponse,
  StateDetailEntry,
} from '../types/chronos';
import {
  Clock, History, Search, GitCompare, Camera, ShieldAlert,
  Calendar, Layers, CheckCircle2, AlertTriangle, ArrowRight,
  Database, RefreshCw, Loader2, Sparkles, Sliders, ShieldCheck, X,
  Play, Hourglass, Info
} from 'lucide-react';

function getApiBase() {
  return useClusterStore.getState().apiBaseUrl || '/api';
}

function formatTimestamp(ts?: number | null) {
  if (ts === null || ts === undefined) return '—';
  if (ts >= 9223372036854775000 || ts >= 253402300799000) return '∞ (Open-ended)';
  return new Date(ts).toLocaleString();
}

function formatValidInterval(from?: number | null, to?: number | null) {
  const formatTimeOrDate = (ts: number) => {
    const d = new Date(ts);
    const hasTime = d.getHours() !== 0 || d.getMinutes() !== 0 || d.getSeconds() !== 0;
    return hasTime ? d.toLocaleString() : d.toLocaleDateString();
  };

  const fromStr = (from !== null && from !== undefined && from > 0)
    ? formatTimeOrDate(from)
    : 'Epoch';
  const toStr = (to !== null && to !== undefined && to < 9223372036854775000 && to < 253402300799000)
    ? formatTimeOrDate(to)
    : '∞';
  return `[${fromStr} → ${toStr})`;
}

export default function TimeTravelPanel() {
  const [subTab, setSubTab] = useState<'reconstruction' | 'audit' | 'diff' | 'retention'>('reconstruction');

  // Query parameters
  const [queryMode, setQueryMode] = useState<QueryMode>('CURRENT');
  const [readMode, setReadMode] = useState<ReadMode>('LINEARIZABLE');
  const [selectedVersion, setSelectedVersion] = useState('');
  const [asOfTimestamp, setAsOfTimestamp] = useState<string>('');
  const [validTime, setValidTime] = useState<string>(() => new Date().toISOString().slice(0, 16));
  const [bitemporalTtMode, setBitemporalTtMode] = useState<'VERSION' | 'TIME'>('VERSION');
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

  // Bitemporal Timeline modal/details
  const [bitemporalTimelineKey, setBitemporalTimelineKey] = useState<string | null>(null);
  const [bitemporalTimelineData, setBitemporalTimelineData] = useState<BitemporalTimelineResponse | null>(null);
  const [timelineLoading, setTimelineLoading] = useState(false);
  const [auditViewMode, setAuditViewMode] = useState<'trail' | 'timeline'>('trail');
  const [timelineSort, setTimelineSort] = useState<'TT' | 'TV'>('TT');

  // UI state
  const [loading, setLoading] = useState(false);
  const [errorMsg, setErrorMsg] = useState<string | null>(null);
  const [historyPruned, setHistoryPruned] = useState(false);
  const [snapshotName, setSnapshotName] = useState('');
  const [snapshotSuccess, setSnapshotSuccess] = useState<string | null>(null);
  const [demoRunning, setDemoRunning] = useState(false);
  const [demoNotice, setDemoNotice] = useState<string | null>(null);

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
      if (queryMode === 'AS_OF_VALID_TIME') {
        const parsedVt = Date.parse(validTime);
        payload.validTime = !isNaN(parsedVt) ? parsedVt : Date.now();
      }
      if (queryMode === 'BITEMPORAL') {
        const parsedVt = Date.parse(validTime);
        payload.validTime = !isNaN(parsedVt) ? parsedVt : Date.now();
        if (bitemporalTtMode === 'VERSION') {
          payload.versionId = selectedVersion;
        } else {
          const parsedTt = Date.parse(asOfTimestamp);
          payload.timestamp = !isNaN(parsedTt) ? parsedTt : Date.now();
        }
      }

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
          details: data.details || [],
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
  }, [queryMode, readMode, selectedVersion, asOfTimestamp, validTime, bitemporalTtMode, selectedSnapshot]);

  // Auto-run reconstruction when query parameters change or on mount
  useEffect(() => {
    executeReconstruction();
  }, [executeReconstruction]);

  // Fetch Bitemporal Timeline for a specific key
  const fetchBitemporalTimeline = async (targetKey: string) => {
    setTimelineLoading(true);
    setBitemporalTimelineKey(targetKey);
    try {
      const res = await fetch(`${getApiBase()}/chronos/bitemporal/timeline/${encodeURIComponent(targetKey.trim())}`);
      if (res.ok) {
        const data = await res.json();
        setBitemporalTimelineData(data);
      } else {
        setErrorMsg(`Failed to fetch bitemporal timeline for ${targetKey}`);
      }
    } catch (err: any) {
      setErrorMsg('Timeline error: ' + err.message);
    } finally {
      setTimelineLoading(false);
    }
  };

  // Execute Key Audit Query
  const executeAudit = async (targetKey?: string) => {
    const k = targetKey || queryKey;
    if (!k.trim()) return;
    setLoading(true);
    setErrorMsg(null);
    try {
      const [auditRes, timelineRes] = await Promise.all([
        fetch(`${getApiBase()}/chronos/audit/${encodeURIComponent(k.trim())}`),
        fetch(`${getApiBase()}/chronos/bitemporal/timeline/${encodeURIComponent(k.trim())}`),
      ]);

      if (auditRes.status === 410) {
        setHistoryPruned(true);
        setErrorMsg('Some audit history for this key was pruned beyond the low watermark');
      } else if (!auditRes.ok) {
        setErrorMsg(`Audit lookup failed (${auditRes.status})`);
      } else {
        const data = await auditRes.json();
        setAuditTrail(data);
      }

      if (timelineRes.ok) {
        const timeData = await timelineRes.json();
        setBitemporalTimelineData(timeData);
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

  // Interactive 1-Click Bitemporal Retroactive Demo Scenario
  const runBitemporalDemo = async () => {
    setDemoRunning(true);
    setDemoNotice('Populating 3-step Retroactive Payroll Compensation scenario...');
    try {
      const baseDate = new Date('2026-01-01T09:00:00Z').getTime();
      const promoDate = new Date('2026-06-01T09:00:00Z').getTime();
      const retroStart = new Date('2026-02-01T09:00:00Z').getTime();
      const retroEnd = new Date('2026-04-01T09:00:00Z').getTime();

      // Step 1: Base compensation contract effective Jan 1, 2026
      await fetch(`${getApiBase()}/chronos/write`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          key: 'employee:alice:salary',
          value: '$80,000/yr (Base Contract)',
          validFrom: baseDate,
          validTo: null,
          writerId: 'hr-onboarding',
        }),
      });

      // Step 2: Mid-year promotion effective Jun 1, 2026
      await fetch(`${getApiBase()}/chronos/write`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          key: 'employee:alice:salary',
          value: '$95,000/yr (Senior Promo)',
          validFrom: promoDate,
          validTo: null,
          writerId: 'hr-promotion-board',
        }),
      });

      // Step 3: Retroactive Audit Correction (Effective Feb 1 - Apr 1 adjusted to $88,000)
      await fetch(`${getApiBase()}/chronos/write`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          key: 'employee:alice:salary',
          value: '$88,000/yr (Retroactive Equity Adjustment)',
          validFrom: retroStart,
          validTo: retroEnd,
          writerId: 'internal-audit',
        }),
      });

      await refreshMetadata();
      setQueryMode('AS_OF_VALID_TIME');
      setValidTime('2026-02-15T12:00');
      setDemoNotice(
        'Scenario active! "employee:alice:salary" populated. Reconstructing state effective Feb 15, 2026 returns retroactive $88,000!'
      );
      setTimeout(() => setDemoNotice(null), 9000);
    } catch (e: any) {
      setErrorMsg('Demo failed: ' + e.message);
    } finally {
      setDemoRunning(false);
    }
  };

  // Map key details for fast table lookup
  const detailsMap = useMemo(() => {
    const map = new Map<string, StateDetailEntry>();
    if (dbState?.details) {
      for (const d of dbState.details) {
        map.set(d.key, d);
      }
    }
    return map;
  }, [dbState]);

  // Chronologically sorted timeline with closed superseded intervals
  const computedTimeline = useMemo(() => {
    if (!bitemporalTimelineData?.timeline) return [];
    const list = bitemporalTimelineData.timeline.map((item) => ({ ...item }));
    // 1. Sort strictly in consensus commit sequence (Tt)
    list.sort((a, b) => (a.sequence ?? 0) - (b.sequence ?? 0) || (a.txnTime ?? 0) - (b.txnTime ?? 0));

    // 2. Compute closed superseded intervals
    for (let i = 0; i < list.length - 1; i++) {
      const curr = list[i];
      const next = list[i + 1];
      if (!curr.branchId || !next.branchId || curr.branchId === next.branchId) {
        const nextFrom = next.validFrom ?? next.txnTime;
        const currFrom = curr.validFrom ?? curr.txnTime;
        if (nextFrom && currFrom && nextFrom >= currFrom) {
          if (curr.validTo === null || curr.validTo === undefined || curr.validTo > nextFrom || curr.validTo >= 253402300799000) {
            curr.validTo = nextFrom;
          }
        }
      }
    }

    // 3. Detect retroactive adjustment mutations (where validFrom targets a date earlier than previously recorded commits)
    let maxValidFromSeen = -1;
    const enriched = list.map((item) => {
      const vFrom = item.validFrom ?? item.txnTime ?? 0;
      const isRetroactive = maxValidFromSeen > 0 && vFrom < maxValidFromSeen;
      if (vFrom > maxValidFromSeen) {
        maxValidFromSeen = vFrom;
      }
      return { ...item, isRetroactive };
    });

    // 4. Re-sort if TV mode selected
    if (timelineSort === 'TV') {
      enriched.sort((a, b) => {
        const aFrom = a.validFrom ?? a.txnTime ?? 0;
        const bFrom = b.validFrom ?? b.txnTime ?? 0;
        return aFrom - bFrom || (a.sequence ?? 0) - (b.sequence ?? 0);
      });
    }

    return enriched;
  }, [bitemporalTimelineData, timelineSort]);

  return (
    <div className="flex-1 flex flex-col h-full overflow-hidden bg-surface-950 p-6 space-y-5">
      {/* Top Banner / Watermark Stats / Demo Trigger */}
      <div className="flex flex-wrap items-center justify-between gap-4 bg-surface-900/70 border border-white/[0.08] rounded-2xl px-6 py-4 backdrop-blur-xl shadow-lg">
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
              <span className="px-2 py-0.5 text-[10px] font-semibold tracking-wider uppercase rounded-full bg-emerald-500/20 text-emerald-300 border border-emerald-500/30">
                2D Bitemporal (Tv / Tt)
              </span>
            </div>
            <p className="text-xs text-white/50 mt-0.5">
              Reconstruct point-in-time database states, evaluate valid-time intervals, and audit version DAG lineages.
            </p>
          </div>
        </div>

        <div className="flex items-center gap-3">
          {/* 1-Click Interactive Bitemporal Demo Button */}
          <button
            onClick={runBitemporalDemo}
            disabled={demoRunning}
            className="flex items-center gap-2 px-3.5 py-2 rounded-xl text-xs font-semibold bg-gradient-to-r from-purple-600 to-indigo-600 hover:from-purple-500 hover:to-indigo-500 text-white shadow-md shadow-indigo-600/20 transition disabled:opacity-50"
            title="Populate a 3-step retroactive payroll contract scenario"
          >
            {demoRunning ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Sparkles className="w-3.5 h-3.5 text-amber-300" />}
            <span>Run Retroactive Adjustment Demo</span>
          </button>

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
      </div>

      {/* Demo Notification Banner */}
      <AnimatePresence>
        {demoNotice && (
          <motion.div
            initial={{ opacity: 0, y: -10 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -10 }}
            className="flex items-center gap-3 bg-indigo-950/70 border border-indigo-500/40 rounded-xl px-4 py-3 text-indigo-200 text-xs shadow-md"
          >
            <Sparkles className="w-4 h-4 text-amber-300 shrink-0" />
            <div className="flex-1 font-medium">{demoNotice}</div>
            <button onClick={() => setDemoNotice(null)} className="text-white/40 hover:text-white">
              <X className="w-4 h-4" />
            </button>
          </motion.div>
        )}
      </AnimatePresence>

      {/* Navigation Sub-tabs */}
      <div className="flex items-center justify-between border-b border-white/[0.08] pb-3">
        <div className="flex items-center gap-1.5 bg-surface-900/60 p-1 rounded-xl border border-white/[0.06]">
          {[
            { id: 'reconstruction', label: 'Database As-Of', icon: Database },
            { id: 'audit', label: 'Key Provenance & Timeline', icon: Clock },
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
                  <div className="flex flex-wrap items-center gap-1 bg-surface-950 p-1 rounded-xl border border-white/[0.06]">
                    {([
                      'CURRENT',
                      'AS_OF_VERSION',
                      'AS_OF_TIME',
                      'AS_OF_SNAPSHOT',
                      'AS_OF_VALID_TIME',
                      'BITEMPORAL',
                    ] as QueryMode[]).map((mode) => (
                      <button
                        key={mode}
                        onClick={() => setQueryMode(mode)}
                        className={`px-3 py-1.5 rounded-lg text-xs font-medium transition ${
                          queryMode === mode
                            ? mode === 'BITEMPORAL'
                              ? 'bg-gradient-to-r from-purple-600 to-indigo-600 text-white shadow-sm'
                              : mode === 'AS_OF_VALID_TIME'
                              ? 'bg-emerald-600 text-white shadow-sm'
                              : 'bg-indigo-600 text-white shadow-sm'
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
                    <label className="text-xs text-white/60 block mb-1">Target Version ID (Transaction Time Tt)</label>
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
                    <label className="text-xs text-white/60 block mb-1">Historical Transaction-Time (Tt)</label>
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
                      <button
                        onClick={() => setAsOfTimestamp(new Date(Date.now() - 3600000).toISOString().slice(0, 16))}
                        className="px-2.5 py-2 text-xs bg-white/[0.04] hover:bg-white/[0.08] rounded-xl text-white/60"
                      >
                        -1h
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

                {queryMode === 'AS_OF_VALID_TIME' && (
                  <div className="col-span-2">
                    <label className="text-xs text-emerald-400 font-medium block mb-1 flex items-center gap-1.5">
                      <Calendar className="w-3.5 h-3.5" />
                      <span>Valid-Time (Tv) — Real-World Effective Point</span>
                    </label>
                    <div className="flex items-center gap-2">
                      <input
                        type="datetime-local"
                        value={validTime}
                        onChange={(e) => setValidTime(e.target.value)}
                        className="flex-1 bg-surface-950 border border-emerald-500/30 rounded-xl px-3 py-2 text-xs text-white focus:outline-none focus:border-emerald-500"
                      />
                      <button
                        onClick={() => setValidTime('2026-02-15T12:00')}
                        className="px-2.5 py-2 text-xs bg-emerald-500/10 hover:bg-emerald-500/20 text-emerald-300 rounded-xl border border-emerald-500/20"
                        title="Jump to retroactive demo date"
                      >
                        Demo Date
                      </button>
                      <button
                        onClick={() => setValidTime(new Date().toISOString().slice(0, 16))}
                        className="px-2.5 py-2 text-xs bg-white/[0.04] hover:bg-white/[0.08] rounded-xl text-white/60"
                      >
                        Now
                      </button>
                    </div>
                    <span className="text-[11px] text-white/40 block mt-1">
                      Evaluates reality effective at Tv using all currently committed transaction knowledge.
                    </span>
                  </div>
                )}

                {queryMode === 'BITEMPORAL' && (
                  <div className="col-span-2 space-y-2">
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                      <div>
                        <label className="text-xs text-emerald-400 font-medium block mb-1 flex items-center gap-1">
                          <Calendar className="w-3.5 h-3.5" />
                          <span>Dimension 1: Valid-Time (Tv)</span>
                        </label>
                        <input
                          type="datetime-local"
                          value={validTime}
                          onChange={(e) => setValidTime(e.target.value)}
                          className="w-full bg-surface-950 border border-emerald-500/30 rounded-xl px-3 py-2 text-xs text-white focus:outline-none focus:border-emerald-500"
                        />
                      </div>
                      <div>
                        <div className="flex items-center justify-between mb-1">
                          <label className="text-xs text-purple-400 font-medium flex items-center gap-1">
                            {bitemporalTtMode === 'VERSION' ? (
                              <Layers className="w-3.5 h-3.5" />
                            ) : (
                              <Clock className="w-3.5 h-3.5" />
                            )}
                            <span>Dimension 2: {bitemporalTtMode === 'VERSION' ? 'System Version (Tt)' : 'System Time (Tt)'}</span>
                          </label>
                          <div className="flex items-center gap-1 text-[10px]">
                            <button
                              type="button"
                              onClick={() => setBitemporalTtMode('VERSION')}
                              className={`px-1.5 py-0.5 rounded transition ${bitemporalTtMode === 'VERSION' ? 'bg-purple-600 text-white font-semibold' : 'text-white/40 hover:text-white/70'}`}
                            >
                              Ver
                            </button>
                            <button
                              type="button"
                              onClick={() => setBitemporalTtMode('TIME')}
                              className={`px-1.5 py-0.5 rounded transition ${bitemporalTtMode === 'TIME' ? 'bg-purple-600 text-white font-semibold' : 'text-white/40 hover:text-white/70'}`}
                            >
                              Time
                            </button>
                          </div>
                        </div>
                        {bitemporalTtMode === 'VERSION' ? (
                          <select
                            value={selectedVersion}
                            onChange={(e) => setSelectedVersion(e.target.value)}
                            className="w-full bg-surface-950 border border-purple-500/30 rounded-xl px-3 py-2 text-xs text-white font-mono focus:outline-none focus:border-purple-500"
                          >
                            {versions.map((v) => (
                              <option key={v.versionId} value={v.versionId}>
                                {v.versionId} (seq={v.sequence})
                              </option>
                            ))}
                          </select>
                        ) : (
                          <input
                            type="datetime-local"
                            value={asOfTimestamp}
                            onChange={(e) => setAsOfTimestamp(e.target.value)}
                            className="w-full bg-surface-950 border border-purple-500/30 rounded-xl px-3 py-2 text-xs text-white font-mono focus:outline-none focus:border-purple-500"
                          />
                        )}
                      </div>
                    </div>
                    <div className="text-[11px] text-white/40 bg-white/[0.02] p-2 rounded-lg border border-white/[0.04]">
                      2D Intersection: Reconstructs state effective at valid-time <strong className="text-emerald-300">Tv</strong> as was recorded up to system commit {bitemporalTtMode === 'VERSION' ? 'version' : 'time'} <strong className="text-purple-300">Tt</strong>.
                    </div>
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
                <div className="flex flex-wrap items-center justify-between gap-2">
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
                        <th className="py-2.5 px-4 font-semibold">Valid Interval (Tv)</th>
                        <th className="py-2.5 px-4 font-semibold">Tx System (Tt)</th>
                        <th className="py-2.5 px-4 font-semibold text-right">Actions</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-white/[0.04]">
                      {Object.entries(dbState.keys).map(([k, v]) => {
                        const detail = detailsMap.get(k);
                        const hasInterval = detail && (detail.validFrom !== null || detail.validTo !== null);
                        return (
                          <tr key={k} className="hover:bg-white/[0.02] transition">
                            <td className="py-2.5 px-4 font-mono text-cyan-300 font-medium">{k}</td>
                            <td className="py-2.5 px-4 font-mono text-white/90 max-w-xs truncate">{v}</td>
                            <td className="py-2.5 px-4 font-mono text-[11px]">
                              {hasInterval ? (
                                <div className="flex items-center gap-1.5 flex-wrap">
                                  <span className="px-2 py-0.5 rounded bg-emerald-500/10 text-emerald-300 border border-emerald-500/20 font-mono">
                                    {formatValidInterval(detail.validFrom, detail.validTo)}
                                  </span>
                                  <span className="text-[10px] px-1.5 py-0.5 rounded bg-emerald-500/20 text-emerald-300 border border-emerald-500/30">
                                    Bitemporal
                                  </span>
                                </div>
                              ) : (
                                <div className="flex items-center gap-1.5 flex-wrap">
                                  <span className="text-white/40 font-mono">[Epoch → ∞)</span>
                                  <span
                                    className="text-[10px] px-1.5 py-0.5 rounded bg-white/[0.04] text-white/40 border border-white/[0.08]"
                                    title="Standard non-temporal key: valid across all business time"
                                  >
                                    Standard KV (Unbounded)
                                  </span>
                                </div>
                              )}
                            </td>
                            <td className="py-2.5 px-4 font-mono text-[11px] text-white/60">
                              {detail ? (
                                <span>{detail.versionId} ({new Date(detail.timestamp).toLocaleTimeString()})</span>
                              ) : (
                                <span>{dbState.asOfVersionId}</span>
                              )}
                            </td>
                            <td className="py-2.5 px-4 text-right space-x-2">
                              <button
                                onClick={() => fetchBitemporalTimeline(k)}
                                className="text-xs text-emerald-400 hover:text-emerald-300 hover:underline inline-flex items-center gap-1"
                                title="View 2D Bitemporal Timeline"
                              >
                                <Calendar className="w-3 h-3" />
                                <span>Timeline</span>
                              </button>
                              <button
                                onClick={() => {
                                  setQueryKey(k);
                                  setSubTab('audit');
                                  executeAudit(k);
                                }}
                                className="text-xs text-indigo-400 hover:text-indigo-300 hover:underline inline-flex items-center gap-1"
                              >
                                <Clock className="w-3 h-3" />
                                <span>Audit</span>
                              </button>
                            </td>
                          </tr>
                        );
                      })}
                      {Object.keys(dbState.keys).length === 0 && (
                        <tr>
                          <td colSpan={5} className="py-6 text-center text-white/40">
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

        {/* SUBTAB 2: KEY PROVENANCE AUDIT & 2D BITEMPORAL TIMELINE */}
        {subTab === 'audit' && (
          <div className="space-y-4">
            <div className="bg-surface-900/60 border border-white/[0.06] rounded-2xl p-5 backdrop-blur-xl flex flex-wrap items-center gap-3">
              <input
                type="text"
                placeholder="Enter key to trace (e.g. employee:alice:salary, counter)..."
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

            {/* Audit / Timeline Toggle */}
            {(auditTrail || bitemporalTimelineData) && (
              <div className="flex items-center gap-2">
                <button
                  onClick={() => setAuditViewMode('trail')}
                  className={`px-3 py-1.5 rounded-lg text-xs font-medium transition ${
                    auditViewMode === 'trail'
                      ? 'bg-indigo-600 text-white shadow-sm'
                      : 'bg-white/[0.04] text-white/50 hover:text-white'
                  }`}
                >
                  Mutation Log Trail ({auditTrail?.totalModifications ?? 0})
                </button>
                <button
                  onClick={() => setAuditViewMode('timeline')}
                  className={`px-3 py-1.5 rounded-lg text-xs font-medium transition flex items-center gap-1.5 ${
                    auditViewMode === 'timeline'
                      ? 'bg-emerald-600 text-white shadow-sm'
                      : 'bg-white/[0.04] text-white/50 hover:text-white'
                  }`}
                >
                  <Calendar className="w-3.5 h-3.5" />
                  <span>2D Bitemporal Intervals ({bitemporalTimelineData?.timeline?.length ?? 0})</span>
                </button>
              </div>
            )}

            {/* VIEW 1: Standard Audit Trail */}
            {auditViewMode === 'trail' && auditTrail && (
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

            {/* VIEW 2: 2D Bitemporal Intervals View */}
            {auditViewMode === 'timeline' && bitemporalTimelineData && (
              <div className="bg-surface-900/60 border border-white/[0.06] rounded-2xl p-5 backdrop-blur-xl space-y-4">
                <div className="flex flex-wrap items-center justify-between gap-3 border-b border-white/[0.06] pb-3">
                  <div>
                    <h3 className="text-sm font-bold text-white font-mono">{bitemporalTimelineData.key}</h3>
                    <p className="text-xs text-white/50">
                      2D Bitemporal Interval Mapping ({computedTimeline.length} recorded versions)
                    </p>
                  </div>

                  {/* Ordering Toggle */}
                  <div className="flex items-center gap-1.5 bg-surface-950 p-1 rounded-xl border border-white/[0.08] text-xs">
                    <span className="text-white/40 text-[11px] px-2">Order by:</span>
                    <button
                      onClick={() => setTimelineSort('TT')}
                      className={`px-3 py-1 rounded-lg font-medium transition text-[11px] ${
                        timelineSort === 'TT'
                          ? 'bg-purple-600 text-white shadow-sm'
                          : 'text-white/50 hover:text-white'
                      }`}
                      title="Sort by Raft consensus commit sequence (physical ledger order)"
                    >
                      Commit Order (Tt)
                    </button>
                    <button
                      onClick={() => setTimelineSort('TV')}
                      className={`px-3 py-1 rounded-lg font-medium transition text-[11px] ${
                        timelineSort === 'TV'
                          ? 'bg-emerald-600 text-white shadow-sm'
                          : 'text-white/50 hover:text-white'
                      }`}
                      title="Sort by Real-World Calendar Validity (business effective dates)"
                    >
                      Calendar Order (Tv)
                    </button>
                  </div>
                </div>

                {/* Explanatory callout based on active sort */}
                <div className="bg-surface-950/70 border border-white/[0.06] rounded-xl px-3.5 py-2.5 text-[11px] text-white/60 flex items-center gap-2">
                  <Info className="w-4 h-4 text-indigo-400 shrink-0" />
                  {timelineSort === 'TT' ? (
                    <span>
                      <strong className="text-purple-300 font-semibold">Consensus Commit Order (Tt):</strong> Showing physical append order in the Raft consensus log. Retroactive adjustments appear at their commit timestamp, preserving an immutable audit trail.
                    </span>
                  ) : (
                    <span>
                      <strong className="text-emerald-300 font-semibold">Calendar Validity Order (Tv):</strong> Showing real-world chronological timeline. Versions are sorted by the business dates when facts were effective in reality.
                    </span>
                  )}
                </div>

                <div className="grid grid-cols-1 gap-3">
                  {computedTimeline.map((item, idx) => (
                    <div
                      key={idx}
                      className="bg-surface-950 border border-white/[0.06] rounded-xl p-4 text-xs space-y-2 hover:border-emerald-500/30 transition"
                    >
                      <div className="flex flex-wrap items-center justify-between gap-2">
                        <div className="flex items-center gap-2">
                          <span className="font-mono font-bold text-purple-300">{item.versionId}</span>
                          <span className="text-white/40">seq={item.sequence}</span>
                          <span className="px-2 py-0.5 rounded text-[10px] font-bold bg-emerald-500/10 text-emerald-300 border border-emerald-500/20">
                            {item.opType}
                          </span>
                          {item.isRetroactive && (
                            <span className="px-2 py-0.5 rounded-full text-[10px] font-bold bg-amber-500/20 text-amber-300 border border-amber-500/30 flex items-center gap-1">
                              <Sparkles className="w-3 h-3 text-amber-400" />
                              <span>Retroactive Adjustment</span>
                            </span>
                          )}
                        </div>
                        <div className="text-[11px] text-white/40">
                          Recorded at Tt: <span className="text-white/70 font-mono">{new Date(item.txnTime).toLocaleString()}</span>
                        </div>
                      </div>

                      <div className="flex flex-wrap items-center gap-3 pt-1 border-t border-white/[0.04]">
                        <div className="flex items-center gap-1.5">
                          <Calendar className="w-3.5 h-3.5 text-emerald-400" />
                          <span className="text-white/50">Valid-Time Window (Tv):</span>
                          <span className="font-mono font-semibold text-emerald-300">
                            {formatValidInterval(item.validFrom, item.validTo)}
                          </span>
                        </div>
                        <div className="text-white/30">•</div>
                        <div className="text-white/50">
                          Writer: <span className="font-mono text-white/80">{item.writerId}</span>
                        </div>
                      </div>

                      <div className="bg-black/40 rounded-lg p-2.5 font-mono text-[11px] text-emerald-300">
                        {item.value || '<deleted>'}
                      </div>
                    </div>
                  ))}
                  {computedTimeline.length === 0 && (
                    <div className="py-6 text-center text-white/40 text-xs">
                      No mutations recorded for key {bitemporalTimelineData.key}.
                    </div>
                  )}
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

      {/* 2D Bitemporal Timeline Modal */}
      <AnimatePresence>
        {bitemporalTimelineKey && (
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/70 backdrop-blur-sm"
          >
            <motion.div
              initial={{ scale: 0.95, opacity: 0 }}
              animate={{ scale: 1, opacity: 1 }}
              exit={{ scale: 0.95, opacity: 0 }}
              className="bg-surface-900 border border-white/[0.1] rounded-2xl max-w-2xl w-full p-6 space-y-4 shadow-2xl max-h-[85vh] flex flex-col"
            >
              <div className="flex flex-wrap items-center justify-between gap-3 border-b border-white/[0.06] pb-3">
                <div className="flex items-center gap-2">
                  <Calendar className="w-5 h-5 text-emerald-400" />
                  <div>
                    <h3 className="text-sm font-bold text-white font-mono">{bitemporalTimelineKey}</h3>
                    <span className="text-xs text-white/50">2D Bitemporal Timeline & Valid Windows</span>
                  </div>
                </div>

                <div className="flex items-center gap-2">
                  {/* Ordering Toggle */}
                  <div className="flex items-center gap-1 bg-surface-950 p-1 rounded-xl border border-white/[0.08] text-xs">
                    <button
                      onClick={() => setTimelineSort('TT')}
                      className={`px-2.5 py-1 rounded-lg font-medium transition text-[11px] ${
                        timelineSort === 'TT'
                          ? 'bg-purple-600 text-white shadow-sm'
                          : 'text-white/50 hover:text-white'
                      }`}
                      title="Sort by Raft consensus commit sequence (physical ledger order)"
                    >
                      Commit Order (Tt)
                    </button>
                    <button
                      onClick={() => setTimelineSort('TV')}
                      className={`px-2.5 py-1 rounded-lg font-medium transition text-[11px] ${
                        timelineSort === 'TV'
                          ? 'bg-emerald-600 text-white shadow-sm'
                          : 'text-white/50 hover:text-white'
                      }`}
                      title="Sort by Real-World Calendar Validity (business effective dates)"
                    >
                      Calendar Order (Tv)
                    </button>
                  </div>

                  <button
                    onClick={() => {
                      setBitemporalTimelineKey(null);
                      setBitemporalTimelineData(null);
                    }}
                    className="p-1 rounded-lg text-white/40 hover:text-white hover:bg-white/[0.05]"
                  >
                    <X className="w-4 h-4" />
                  </button>
                </div>
              </div>

              {/* Explanatory callout */}
              <div className="bg-surface-950/70 border border-white/[0.06] rounded-xl px-3 py-2 text-[11px] text-white/60 flex items-center gap-2">
                <Info className="w-3.5 h-3.5 text-indigo-400 shrink-0" />
                {timelineSort === 'TT' ? (
                  <span>
                    <strong className="text-purple-300 font-semibold">Consensus Commit Order (Tt):</strong> Ledger append order. Retroactive audit adjustments are stored at commit time to guarantee immutability.
                  </span>
                ) : (
                  <span>
                    <strong className="text-emerald-300 font-semibold">Calendar Validity Order (Tv):</strong> Effective chronological business timeline sorted by real-world dates.
                  </span>
                )}
              </div>

              <div className="flex-1 overflow-y-auto space-y-3">
                {timelineLoading ? (
                  <div className="py-12 flex items-center justify-center gap-2 text-white/50 text-xs">
                    <Loader2 className="w-4 h-4 animate-spin text-emerald-400" />
                    <span>Loading timeline...</span>
                  </div>
                ) : computedTimeline.length ? (
                  computedTimeline.map((point, i) => (
                    <div
                      key={i}
                      className="bg-surface-950 border border-white/[0.06] rounded-xl p-3.5 text-xs space-y-2"
                    >
                      <div className="flex items-center justify-between">
                        <div className="flex items-center gap-2">
                          <span className="font-mono font-bold text-purple-300">{point.versionId}</span>
                          <span className="px-1.5 py-0.5 rounded text-[10px] font-bold bg-emerald-500/10 text-emerald-300 border border-emerald-500/20">
                            {point.opType}
                          </span>
                          {point.isRetroactive && (
                            <span className="px-2 py-0.5 rounded-full text-[10px] font-bold bg-amber-500/20 text-amber-300 border border-amber-500/30 flex items-center gap-1">
                              <Sparkles className="w-3 h-3 text-amber-400" />
                              <span>Retroactive Adjustment</span>
                            </span>
                          )}
                          <span className="text-white/40 font-mono text-[11px]">{point.writerId}</span>
                        </div>
                        <span className="text-[10px] text-white/40 font-mono">
                          Recorded: {new Date(point.txnTime).toLocaleString()}
                        </span>
                      </div>

                      <div className="flex items-center gap-2 text-[11px]">
                        <span className="text-white/50">Valid-Time (Tv):</span>
                        <span className="font-mono text-emerald-300 font-semibold">
                          {formatValidInterval(point.validFrom, point.validTo)}
                        </span>
                      </div>

                      <div className="bg-black/50 rounded p-2 font-mono text-emerald-300 text-[11px]">
                        {point.value || '<deleted>'}
                      </div>
                    </div>
                  ))
                ) : (
                  <div className="py-8 text-center text-white/40 text-xs">
                    No timeline records found for {bitemporalTimelineKey}.
                  </div>
                )}
              </div>

              <div className="pt-2 border-t border-white/[0.06] flex justify-end">
                <button
                  onClick={() => {
                    setBitemporalTimelineKey(null);
                    setBitemporalTimelineData(null);
                  }}
                  className="px-4 py-2 rounded-xl bg-white/[0.05] hover:bg-white/[0.08] text-xs text-white"
                >
                  Close
                </button>
              </div>
            </motion.div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}
