import { useState, useEffect, useCallback, Fragment } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { useClusterStore } from '../stores/clusterStore';
import type { KvPair, MvccVersion, TxnOperation } from '../types/events';
import {
  Terminal, Table2, GitBranch, Search, Plus, Trash2,
  ArrowUpCircle, ArrowDownCircle, Play, Loader2, Clock,
  ChevronDown, ChevronRight, X, CheckCircle2, XCircle
} from 'lucide-react';

function getApiBase() {
  return useClusterStore.getState().apiBaseUrl || '/api';
}

export default function KvExplorer() {
  const [activeTab, setActiveTab] = useState<'query' | 'browse' | 'txn'>('query');
  const [key, setKey] = useState('');
  const [value, setValue] = useState('');
  const [scanPrefix, setScanPrefix] = useState('');
  const [results, setResults] = useState<KvPair[]>([]);
  const [versions, setVersions] = useState<MvccVersion[]>([]);
  const [expandedKey, setExpandedKey] = useState<string | null>(null);
  const [versionsLoading, setVersionsLoading] = useState(false);
  const [output, setOutput] = useState('');
  const [outputType, setOutputType] = useState<'success' | 'error' | 'info'>('info');
  const [loading, setLoading] = useState(false);
  const [txnOps, setTxnOps] = useState<TxnOperation[]>([]);
  const [txnResult, setTxnResult] = useState('');
  const [validFrom, setValidFrom] = useState('');
  const [validTo, setValidTo] = useState('');
  const [showTemporalFields, setShowTemporalFields] = useState(false);
  const [branches, setBranches] = useState<{ branchId: string; status: string; headVersionId?: string }[]>([]);
  const [selectedBranch, setSelectedBranch] = useState('main');

  const fetchBranches = useCallback(async () => {
    try {
      const res = await fetch(`${getApiBase()}/chronos/branches`);
      if (res.ok) {
        const data = await res.json();
        if (data?.branches) {
          setBranches(data.branches.filter((b: any) => b.status === 'ACTIVE'));
        }
      }
    } catch {}
  }, []);

  useEffect(() => {
    fetchBranches();
  }, [fetchBranches]);

  const handleBranchChange = (newBranch: string) => {
    setSelectedBranch(newBranch);
    if (activeTab === 'browse') {
      handleScan(newBranch);
    }
  };

  const apiCall = useCallback(async (url: string, body: object) => {
    setLoading(true);
    try {
      const res = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      const data = await res.json();
      setLoading(false);
      return data;
    } catch (e: any) {
      setLoading(false);
      setOutput(`Error: ${e.message}`);
      setOutputType('error');
      return null;
    }
  }, []);

  const handlePut = async () => {
    if (!key || !value) return;
    const body: any = { key, value, branchId: selectedBranch };
    if (validFrom) body.validFrom = validFrom;
    if (validTo) body.validTo = validTo;
    const data = await apiCall(`${getApiBase()}/kv/put`, body);
    if (data) {
      setOutput(`PUT [${selectedBranch}] ${key} = "${value}" ${validFrom ? `(Tv: ${validFrom})` : ''} → ${data.success ? 'OK' : 'FAIL'}`);
      setOutputType(data.success ? 'success' : 'error');
      if (activeTab === 'browse') handleScan();
    }
  };

  const handleGet = async () => {
    if (!key) return;
    const data = await apiCall(`${getApiBase()}/kv/get`, { key, branchId: selectedBranch });
    if (data) {
      if (data.found) {
        setValue(data.value || '');
        setOutput(`GET [${selectedBranch}] ${key} → "${data.value}"`);
        setOutputType('success');
      } else {
        setOutput(`GET [${selectedBranch}] ${key} → NOT FOUND`);
        setOutputType('error');
      }
    }
  };

  const handleDelete = async () => {
    if (!key) return;
    const data = await apiCall(`${getApiBase()}/kv/delete`, { key, branchId: selectedBranch });
    if (data) {
      setOutput(`DELETE [${selectedBranch}] ${key} → ${data.success ? 'OK' : 'FAIL'}`);
      setOutputType(data.success ? 'success' : 'error');
      setValue('');
      if (activeTab === 'browse') handleScan();
    }
  };

  const handleScan = async (branch = selectedBranch) => {
    const data = await apiCall(`${getApiBase()}/kv/scan`, { prefix: scanPrefix || '', limit: 100, branchId: branch });
    if (data && data.pairs) {
      setResults(data.pairs);
      setOutput(`SCAN [${branch}] "${scanPrefix || '*'}" → ${data.pairs.length} results`);
      setOutputType('info');
    }
  };

  const handleVersions = async (k: string) => {
    if (expandedKey === k) {
      setExpandedKey(null);
      setVersions([]);
      return;
    }
    setExpandedKey(k);
    setVersions([]);
    setVersionsLoading(true);
    try {
      const res = await fetch(`${getApiBase()}/kv/versions/${encodeURIComponent(k)}`);
      if (res.ok) {
        const data = await res.json();
        setVersions(Array.isArray(data) ? data : (data.versions || []));
      }
    } catch {
      setVersions([]);
    } finally {
      setVersionsLoading(false);
    }
  };

  const addTxnOp = () => setTxnOps([...txnOps, { type: 'PUT', key: '', value: '' }]);
  const updateTxnOp = (i: number, field: string, val: string) => {
    const ops = [...txnOps]; (ops[i] as any)[field] = val; setTxnOps(ops);
  };
  const removeTxnOp = (i: number) => setTxnOps(txnOps.filter((_, idx) => idx !== i));
  const executeTxn = async () => {
    if (txnOps.length === 0) return;
    const data = await apiCall(`${getApiBase()}/kv/txn`, { operations: txnOps, branchId: selectedBranch });
    if (data) setTxnResult(`TXN [${selectedBranch}] ${data.txnId}: ${data.status} (${data.message || 'done'})`);
  };

  const tabs = [
    { id: 'query' as const, label: 'Query Console', icon: Terminal },
    { id: 'browse' as const, label: 'Data Browser', icon: Table2 },
    { id: 'txn' as const, label: 'Transactions', icon: GitBranch },
  ];

  return (
    <div className="h-full flex flex-col gap-4">
      {/* Header */}
      <div className="flex items-center gap-4">
        <div>
          <h2 className="text-xl font-bold tracking-tight bg-gradient-to-r from-white to-white/70 bg-clip-text text-transparent">KV Store</h2>
          <p className="text-xs text-white/30 mt-0.5">Query, browse, and manage your distributed key-value data</p>
        </div>

        <div className="ml-auto flex items-center gap-3">
          {/* Branch Selector Dropdown */}
          <div className="flex items-center gap-2 bg-surface-900/80 border border-white/[0.08] rounded-xl px-3 py-1.5 text-xs backdrop-blur-md">
            <GitBranch className="w-3.5 h-3.5 text-purple-400" />
            <span className="text-white/40 font-medium">Branch:</span>
            <select
              value={selectedBranch}
              onChange={(e) => handleBranchChange(e.target.value)}
              className="bg-transparent text-purple-300 font-mono font-bold focus:outline-none cursor-pointer"
            >
              {branches.map((b) => (
                <option key={b.branchId} value={b.branchId} className="bg-surface-950 text-white font-mono">
                  {b.branchId} {b.branchId === 'main' ? '(main)' : ''}
                </option>
              ))}
              {!branches.some((b) => b.branchId === 'main') && (
                <option value="main" className="bg-surface-950 text-white font-mono">main (main)</option>
              )}
            </select>
            {selectedBranch !== 'main' && (
              <span className="px-1.5 py-0.5 rounded text-[10px] font-semibold bg-purple-500/20 text-purple-300 border border-purple-500/30">
                Isolated
              </span>
            )}
          </div>

          <div className="flex items-center gap-1 bg-white/[0.025] rounded-2xl p-1 border border-white/[0.05]">
            {tabs.map((tab) => {
              const Icon = tab.icon;
              return (
                <button
                  key={tab.id}
                  onClick={() => setActiveTab(tab.id)}
                  className={`flex items-center gap-2 px-3.5 py-2 rounded-xl text-xs font-medium transition-all duration-200 ${
                    activeTab === tab.id
                      ? 'bg-white/[0.1] text-white ring-1 ring-white/[0.08]'
                      : 'text-white/35 hover:text-white/60 hover:bg-white/[0.03]'
                  }`}
                >
                  <Icon className={`w-3.5 h-3.5 ${activeTab === tab.id ? 'text-emerald-400' : ''}`} />
                  <span>{tab.label}</span>
                </button>
              );
            })}
          </div>
        </div>
      </div>

      <div className="flex-1 flex gap-4 overflow-hidden">
        {/* Left Panel */}
        <div className="w-80 flex flex-col gap-3 flex-shrink-0">
          {activeTab === 'query' && (
            <div className="glass-card p-5 space-y-4">
              <p className="section-title">Key-Value Operations</p>
              <div>
                <label className="block text-[11px] text-white/30 mb-1.5 font-medium">Key</label>
                <input
                  type="text" value={key} onChange={(e) => setKey(e.target.value)}
                  placeholder="e.g. user:1001"
                  className="glass-input w-full font-mono text-xs"
                />
              </div>
              <div>
                <label className="block text-[11px] text-white/30 mb-1.5 font-medium">Value</label>
                <textarea
                  value={value} onChange={(e) => setValue(e.target.value)}
                  placeholder='e.g. {"name": "Alice", "age": 30}'
                  rows={3}
                  className="glass-input w-full font-mono text-xs resize-none"
                />
              </div>
              <div>
                <button
                  type="button"
                  onClick={() => setShowTemporalFields(!showTemporalFields)}
                  className="text-[11px] text-emerald-400 hover:text-emerald-300 flex items-center gap-1 transition"
                >
                  <Clock className="w-3 h-3" />
                  <span>{showTemporalFields ? 'Hide Temporal Validity (Tv)' : '+ Bitemporal Validity Window (Tv)'}</span>
                </button>

                {showTemporalFields && (
                  <div className="mt-2 p-2.5 rounded-xl bg-white/[0.02] border border-white/[0.06] space-y-2">
                    <div>
                      <label className="block text-[10px] text-white/40 mb-1">Valid-Time From (Tv)</label>
                      <input
                        type="datetime-local"
                        value={validFrom}
                        onChange={(e) => setValidFrom(e.target.value)}
                        className="glass-input w-full text-[11px] font-mono"
                      />
                    </div>
                    <div>
                      <label className="block text-[10px] text-white/40 mb-1">Valid-Time To (Tv) (optional)</label>
                      <input
                        type="datetime-local"
                        value={validTo}
                        onChange={(e) => setValidTo(e.target.value)}
                        placeholder="Leave blank for ∞"
                        className="glass-input w-full text-[11px] font-mono"
                      />
                    </div>
                    <p className="text-[10px] text-white/30 italic">
                      Leave empty for standard unbounded non-temporal key [Epoch → ∞).
                    </p>
                  </div>
                )}
              </div>
              <div className="flex gap-2">
                <button onClick={handlePut} disabled={loading}
                  className="flex-1 glass-button-success flex items-center justify-center gap-1.5 text-xs py-2">
                  <ArrowUpCircle className="w-3.5 h-3.5" /> PUT
                </button>
                <button onClick={handleGet} disabled={loading}
                  className="flex-1 glass-button-primary flex items-center justify-center gap-1.5 text-xs py-2">
                  <ArrowDownCircle className="w-3.5 h-3.5" /> GET
                </button>
                <button onClick={handleDelete} disabled={loading}
                  className="flex-1 glass-button-danger flex items-center justify-center gap-1.5 text-xs py-2">
                  <Trash2 className="w-3.5 h-3.5" /> DEL
                </button>
              </div>
            </div>
          )}

          {activeTab === 'browse' && (
            <div className="glass-card p-5 space-y-4">
              <p className="section-title">Scan Keys</p>
              <div>
                <label className="block text-[11px] text-white/30 mb-1.5 font-medium">Prefix Filter</label>
                <input
                  type="text" value={scanPrefix} onChange={(e) => setScanPrefix(e.target.value)}
                  placeholder="e.g. user:"
                  className="glass-input w-full font-mono text-xs"
                />
              </div>
              <button onClick={() => handleScan()} disabled={loading}
                className="w-full glass-button-primary flex items-center justify-center gap-2 text-xs">
                <Search className="w-3.5 h-3.5" /> Scan Database
              </button>
            </div>
          )}

          {activeTab === 'txn' && (
            <div className="glass-card p-5 space-y-3">
              <p className="section-title">2PC Transaction</p>
              {txnOps.map((op, i) => (
                <div key={i} className="flex gap-1.5 items-center">
                  <select value={op.type} onChange={(e) => updateTxnOp(i, 'type', e.target.value)}
                    className="glass-input w-16 px-2 py-1.5 text-[11px]">
                    <option value="PUT">PUT</option>
                    <option value="DELETE">DEL</option>
                  </select>
                  <input value={op.key} onChange={(e) => updateTxnOp(i, 'key', e.target.value)}
                    placeholder="key" className="glass-input flex-1 px-2 py-1.5 text-[11px] font-mono" />
                  {op.type === 'PUT' && (
                    <input value={op.value || ''} onChange={(e) => updateTxnOp(i, 'value', e.target.value)}
                      placeholder="value" className="glass-input flex-1 px-2 py-1.5 text-[11px] font-mono" />
                  )}
                  <button onClick={() => removeTxnOp(i)} className="text-red-400/60 hover:text-red-400 p-1 transition-colors">
                    <X className="w-3 h-3" />
                  </button>
                </div>
              ))}
              <div className="flex gap-2">
                <button onClick={addTxnOp} className="flex-1 glass-button flex items-center justify-center gap-1.5 text-[11px] py-2">
                  <Plus className="w-3 h-3" /> Add Op
                </button>
                <button onClick={executeTxn} disabled={loading || txnOps.length === 0}
                  className="flex-1 glass-button-primary flex items-center justify-center gap-1.5 text-[11px] py-2">
                  <Play className="w-3 h-3" /> Execute
                </button>
              </div>
              {txnResult && (
                <div className="text-[11px] text-white/50 bg-white/[0.03] p-2.5 rounded-xl font-mono border border-white/[0.04]">
                  {txnResult}
                </div>
              )}
            </div>
          )}

          {/* Output Console */}
          <div className="glass-card p-4">
            <p className="section-title mb-2">Output</p>
            <div className={`font-mono text-xs p-3.5 rounded-xl border min-h-[56px] flex items-center gap-2.5 ${
              outputType === 'success' ? 'bg-emerald-500/[0.04] border-emerald-500/10 text-emerald-400' :
              outputType === 'error' ? 'bg-red-500/[0.04] border-red-500/10 text-red-400' :
              'bg-white/[0.02] border-white/[0.04] text-white/50'
            }`}>
              {loading ? (
                <><Loader2 className="w-3.5 h-3.5 animate-spin" /> <span>Running...</span></>
              ) : output ? (
                <>
                  {outputType === 'success' ? <CheckCircle2 className="w-3.5 h-3.5 flex-shrink-0" /> :
                   outputType === 'error' ? <XCircle className="w-3.5 h-3.5 flex-shrink-0" /> :
                   <Terminal className="w-3.5 h-3.5 flex-shrink-0" />}
                  <span className="break-all">{output}</span>
                </>
              ) : (
                <span className="text-white/20">Ready — enter a key and run an operation</span>
              )}
            </div>
          </div>
        </div>

        {/* Right Panel: Results */}
        <div className="flex-1 glass-card overflow-hidden flex flex-col">
          <div className="p-4 border-b border-white/[0.06] flex items-center justify-between">
            <div className="flex items-center gap-2.5">
              <div className="p-1.5 rounded-lg bg-white/[0.04]">
                <Table2 className="w-4 h-4 text-white/40" />
              </div>
              <h3 className="text-sm font-semibold text-white/60 flex items-center gap-2">
                <span>{activeTab === 'browse' ? 'Browse Keys' : 'Data Table'}</span>
                <span className="font-mono text-purple-300 text-xs px-2 py-0.5 rounded-md bg-purple-500/10 border border-purple-500/20">
                  branch: {selectedBranch}
                </span>
              </h3>
              {results.length > 0 && (
                <span className="text-[10px] font-mono text-brand-400 bg-brand-500/10 px-2 py-0.5 rounded-md border border-brand-500/15">
                  {results.length}
                </span>
              )}
            </div>
            {activeTab !== 'browse' && (
              <button onClick={() => handleScan()} className="text-[11px] text-brand-400 hover:text-brand-300 flex items-center gap-1.5 transition-colors px-2.5 py-1 rounded-lg hover:bg-brand-500/[0.06]">
                <Search className="w-3 h-3" /> Load All
              </button>
            )}
          </div>
          <div className="flex-1 overflow-y-auto">
            <table className="w-full table-fixed text-sm">
              <thead className="sticky top-0 bg-surface-800/80 backdrop-blur-sm z-10">
                <tr className="text-left text-[10px] text-white/25 uppercase tracking-wider">
                  <th className="px-4 py-2.5 w-12">#</th>
                  <th className="px-4 py-2.5 w-48">Key</th>
                  <th className="px-4 py-2.5">Value</th>
                  <th className="px-4 py-2.5 w-24 text-center">Version</th>
                  <th className="px-4 py-2.5 w-24 text-right">History</th>
                </tr>
              </thead>
              <tbody>
                {results.map((pair, i) => {
                  const isExpanded = expandedKey === pair.key;
                  return (
                    <Fragment key={pair.key}>
                      <tr
                        className={`border-b transition-colors ${
                          isExpanded
                            ? 'bg-purple-500/[0.08] border-purple-500/30'
                            : 'border-white/[0.03] hover:bg-white/[0.02]'
                        }`}
                      >
                        <td className="px-4 py-2.5 text-white/15 text-xs font-mono">{i + 1}</td>
                        <td className="px-4 py-2.5 font-mono text-brand-400 text-xs font-medium truncate" title={pair.key}>{pair.key}</td>
                        <td className="px-4 py-2.5 font-mono text-white/60 text-xs truncate" title={pair.value}>{pair.value}</td>
                        <td className="px-4 py-2.5 text-white/25 text-xs font-mono text-center">v{pair.version || '?'}</td>
                        <td className="px-4 py-2.5 text-right">
                          <button
                            onClick={() => handleVersions(pair.key)}
                            className={`inline-flex items-center gap-1 text-[11px] px-2 py-0.5 rounded transition-all ${
                              isExpanded
                                ? 'text-purple-300 bg-purple-500/20 font-medium'
                                : 'text-purple-400/60 hover:text-purple-300 hover:bg-purple-500/10'
                            }`}
                          >
                            {isExpanded ? <ChevronDown className="w-3 h-3 text-purple-400" /> : <ChevronRight className="w-3 h-3" />}
                            <Clock className="w-3 h-3" />
                            MVCC
                          </button>
                        </td>
                      </tr>

                      {/* Expand directly under chosen row */}
                      {isExpanded && (
                        <tr className="bg-surface-900/90 border-b border-purple-500/20">
                          <td colSpan={5} className="p-0">
                            <motion.div
                              initial={{ opacity: 0, height: 0 }}
                              animate={{ opacity: 1, height: 'auto' }}
                              exit={{ opacity: 0, height: 0 }}
                              transition={{ duration: 0.15 }}
                              className="overflow-hidden w-full"
                            >
                              <div className="mx-3 my-2.5 bg-purple-500/[0.04] border border-purple-500/15 rounded-xl p-3.5 shadow-inner overflow-hidden">
                                <div className="flex items-center justify-between mb-2.5 min-w-0">
                                  <div className="flex items-center gap-2 min-w-0">
                                    <GitBranch className="w-3.5 h-3.5 text-purple-400 flex-shrink-0" />
                                    <h4 className="text-xs font-semibold text-purple-300 truncate">
                                      Version History — <span className="font-mono text-white/90">{pair.key}</span>
                                    </h4>
                                    {!versionsLoading && versions.length > 0 && (
                                      <span className="text-[10px] font-mono text-purple-300 bg-purple-500/15 px-2 py-0.5 rounded-full border border-purple-500/20 flex-shrink-0">
                                        {versions.length} {versions.length === 1 ? 'version' : 'versions'}
                                      </span>
                                    )}
                                  </div>
                                  <button
                                    onClick={() => { setExpandedKey(null); setVersions([]); }}
                                    className="text-white/30 hover:text-white/70 p-1 rounded hover:bg-white/[0.05] transition-colors flex-shrink-0 ml-2"
                                    title="Close"
                                  >
                                    <X className="w-3 h-3" />
                                  </button>
                                </div>

                                {versionsLoading ? (
                                  <div className="flex items-center gap-2 py-3 px-2 text-xs text-purple-300/60 font-mono">
                                    <Loader2 className="w-3.5 h-3.5 animate-spin text-purple-400" />
                                    <span>Fetching MVCC version chain...</span>
                                  </div>
                                ) : versions.length === 0 ? (
                                  <div className="text-xs text-white/30 py-2.5 px-2 font-mono">
                                    No historical versions found for this key.
                                  </div>
                                ) : (
                                  <div className="space-y-1.5 max-h-64 overflow-y-auto pr-1">
                                    {versions.map((v, idx) => (
                                      <div
                                        key={v.timestamp ? `${v.timestamp}-${idx}` : idx}
                                        className="flex items-center gap-3 text-xs font-mono py-1.5 px-2.5 rounded-lg bg-white/[0.02] border border-white/[0.03] hover:bg-white/[0.04] transition-colors min-w-0"
                                      >
                                        <span className="text-white/20 w-5 flex-shrink-0">{idx + 1}.</span>
                                        <Clock className="w-3 h-3 text-white/30 flex-shrink-0" />
                                        <span className="text-white/40 w-44 flex-shrink-0">
                                          {new Date(v.timestamp).toLocaleString()}
                                        </span>
                                        {v.versionState && (
                                          <span className={`text-[10px] px-1.5 py-0.5 rounded-md border font-semibold flex-shrink-0 ${
                                            v.versionState === 'SPECULATIVE'
                                              ? 'text-amber-400 bg-amber-500/10 border-amber-500/20'
                                              : v.versionState === 'COMMITTED'
                                              ? 'text-emerald-400 bg-emerald-500/10 border-emerald-500/20'
                                              : 'text-red-400 bg-red-500/10 border-red-500/20'
                                          }`}>
                                            {v.versionState}
                                          </span>
                                        )}
                                        <span
                                          className={`truncate min-w-0 flex-1 ${
                                            v.tombstone ? 'text-red-400/70 line-through' : 'text-white/70'
                                          }`}
                                          title={v.value || ''}
                                        >
                                          {v.tombstone ? '(tombstone - deleted)' : v.value}
                                        </span>
                                      </div>
                                    ))}
                                  </div>
                                )}
                              </div>
                            </motion.div>
                          </td>
                        </tr>
                      )}
                    </Fragment>
                  );
                })}
                {results.length === 0 && (
                  <tr>
                    <td colSpan={5} className="text-center py-16">
                      <div className="flex flex-col items-center gap-2">
                        <Table2 className="w-8 h-8 text-white/10" />
                        <p className="text-white/20 text-sm">No data yet</p>
                        <p className="text-white/10 text-xs">Use the controls to Put or Scan keys</p>
                      </div>
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  );
}
