import { useState, useEffect, useCallback, useMemo, useRef } from 'react';
import * as d3 from 'd3';
import { motion, AnimatePresence } from 'framer-motion';
import { useClusterStore } from '../stores/clusterStore';
import type {
  DagNode,
  DagEdge,
  VersionDagResponse,
  ChronosBranch,
  MergePolicy,
  MergePreviewResponse,
  MergeResult,
} from '../types/chronos';
import {
  GitFork, GitMerge, GitBranch, Plus, Play, RefreshCw,
  Loader2, CheckCircle2, AlertTriangle, XCircle, ArrowRight,
  ShieldAlert, Sparkles, Layers, User, Clock, ChevronRight, X,
  ZoomIn, ZoomOut, Maximize2, Crosshair
} from 'lucide-react';

function getApiBase() {
  return useClusterStore.getState().apiBaseUrl || '/api';
}

const BRANCH_COLORS: Record<string, { bg: string; border: string; text: string; glow: string }> = {
  main: { bg: 'bg-emerald-500/20', border: 'border-emerald-500/50', text: 'text-emerald-400', glow: 'shadow-emerald-500/20' },
  default: { bg: 'bg-purple-500/20', border: 'border-purple-500/50', text: 'text-purple-400', glow: 'shadow-purple-500/20' },
};

export default function VersionDagPanel() {
  const [dag, setDag] = useState<VersionDagResponse | null>(null);
  const [branches, setBranches] = useState<ChronosBranch[]>([]);
  const [selectedNode, setSelectedNode] = useState<DagNode | null>(null);
  const [loading, setLoading] = useState(false);
  const [errorMsg, setErrorMsg] = useState<string | null>(null);

  // New Branch Modal
  const [showNewBranchModal, setShowNewBranchModal] = useState(false);
  const [newBranchId, setNewBranchId] = useState('');
  const [newBranchBase, setNewBranchBase] = useState('');
  const [newBranchWriter, setNewBranchWriter] = useState('');

  // Merge Studio
  const [showMergeDrawer, setShowMergeDrawer] = useState(false);
  const [mergeSource, setMergeSource] = useState('');
  const [mergeTarget, setMergeTarget] = useState('main');
  const [mergePolicy, setMergePolicy] = useState<MergePolicy>('COMMUTATIVE_DELTA');
  const [mergePreview, setMergePreview] = useState<MergePreviewResponse | null>(null);
  const [mergeResult, setMergeResult] = useState<MergeResult | null>(null);
  const [previewLoading, setPreviewLoading] = useState(false);

  // Demo Running State
  const [demoRunning, setDemoRunning] = useState(false);
  const [demoNotice, setDemoNotice] = useState<string | null>(null);

  // Graph viewer refs and interactive zoom state
  const svgRef = useRef<SVGSVGElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const contentRef = useRef<SVGGElement>(null);
  const zoomBehaviorRef = useRef<d3.ZoomBehavior<SVGSVGElement, unknown> | null>(null);
  const hasInitialFitRef = useRef(false);
  const [zoomScale, setZoomScale] = useState(1.0);
  const [hoveredNodeId, setHoveredNodeId] = useState<string | null>(null);

  const fetchDagAndBranches = useCallback(async () => {
    setLoading(true);
    try {
      const [dagRes, branchRes] = await Promise.all([
        fetch(`${getApiBase()}/chronos/dag?limit=50`).then((r) => r.ok ? r.json() : null),
        fetch(`${getApiBase()}/chronos/branches`).then((r) => r.ok ? r.json() : null),
      ]);

      if (dagRes) {
        setDag(dagRes);
        if (!selectedNode && dagRes.nodes?.length > 0) {
          setSelectedNode(dagRes.nodes[0]);
        }
      }
      if (branchRes?.branches) {
        setBranches(branchRes.branches);
        const nonMain = branchRes.branches.filter((b: ChronosBranch) => b.branchId !== 'main');
        if (nonMain.length > 0 && !mergeSource) {
          setMergeSource(nonMain[0].branchId);
        }
      }
    } catch (err: any) {
      setErrorMsg('Failed to load DAG: ' + err.message);
    } finally {
      setLoading(false);
    }
  }, [selectedNode, mergeSource]);

  useEffect(() => {
    fetchDagAndBranches();
  }, [fetchDagAndBranches]);

  // Create New Branch
  const handleCreateBranch = async () => {
    if (!newBranchId.trim()) return;
    try {
      const res = await fetch(`${getApiBase()}/chronos/branches`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          branchId: newBranchId.trim(),
          baseVersionId: newBranchBase || undefined,
          writerId: newBranchWriter || 'ui-user',
        }),
      });
      if (res.ok) {
        setShowNewBranchModal(false);
        setNewBranchId('');
        fetchDagAndBranches();
      }
    } catch (e: any) {
      setErrorMsg('Failed to create branch: ' + e.message);
    }
  };

  // Preview Merge
  const handlePreviewMerge = async () => {
    if (!mergeSource || !mergeTarget) return;
    setPreviewLoading(true);
    setMergeResult(null);
    setErrorMsg(null);
    try {
      const res = await fetch(`${getApiBase()}/chronos/merge/preview`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sourceBranch: mergeSource,
          targetBranch: mergeTarget,
          policy: mergePolicy,
        }),
      });
      const data = await res.json();
      if (!res.ok || data.error || data.canMerge === undefined) {
        setErrorMsg(data.error || data.message || `Preview failed with status ${res.status}`);
        setMergePreview(null);
      } else {
        setMergePreview({
          ...data,
          conflicts: data.conflicts || [],
          invariantViolations: data.invariantViolations || [],
          tentativeMergedState: data.tentativeMergedState || {},
        });
      }
    } catch (e: any) {
      setErrorMsg('Preview failed: ' + e.message);
      setMergePreview(null);
    } finally {
      setPreviewLoading(false);
    }
  };

  // Execute Merge
  const handleExecuteMerge = async () => {
    if (!mergeSource || !mergeTarget) return;
    setPreviewLoading(true);
    setErrorMsg(null);
    try {
      const res = await fetch(`${getApiBase()}/chronos/merge`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sourceBranch: mergeSource,
          targetBranch: mergeTarget,
          policy: mergePolicy,
          writerId: 'ui-merge-studio',
        }),
      });
      const data = await res.json();
      if (data.success) {
        setMergeResult(data);
        fetchDagAndBranches();
      } else {
        setErrorMsg(data.error || data.message || 'Merge commit rejected');
      }
    } catch (e: any) {
      setErrorMsg('Merge commit failed: ' + e.message);
    } finally {
      setPreviewLoading(false);
    }
  };

  // One-Click Interactive Demos
  const runCommutativeDemo = async () => {
    setDemoRunning(true);
    setDemoNotice('Running Commutative Delta Reconciliation Demo...');
    try {
      // 1. Base on main: inventory:apples = 100
      await fetch(`${getApiBase()}/chronos/write`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ branchId: 'main', writerId: 'demo-init', operations: [{ opType: 'PUT', key: 'inventory:apples', value: '100' }], ops: [{ opType: 'PUT', key: 'inventory:apples', value: '100' }] }),
      });

      // 2. Fork store-east and store-west
      const b1 = 'store-east-' + Math.floor(Math.random() * 1000);
      const b2 = 'store-west-' + Math.floor(Math.random() * 1000);
      await fetch(`${getApiBase()}/chronos/branches`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ branchId: b1, writerId: 'cashier-east' }),
      });
      await fetch(`${getApiBase()}/chronos/branches`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ branchId: b2, writerId: 'cashier-west' }),
      });

      // 3. East sells 20 (-20), West sells 15 (-15)
      await fetch(`${getApiBase()}/chronos/write`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ branchId: b1, writerId: 'cashier-east', operations: [{ opType: 'DELTA', key: 'inventory:apples', deltaAmount: -20 }], ops: [{ opType: 'DELTA', key: 'inventory:apples', deltaAmount: -20 }] }),
      });
      await fetch(`${getApiBase()}/chronos/write`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ branchId: b2, writerId: 'cashier-west', operations: [{ opType: 'DELTA', key: 'inventory:apples', deltaAmount: -15 }], ops: [{ opType: 'DELTA', key: 'inventory:apples', deltaAmount: -15 }] }),
      });

      // 4. Merge b1 into main first
      await fetch(`${getApiBase()}/chronos/merge`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sourceBranch: b1, targetBranch: 'main', policy: 'COMMUTATIVE_DELTA' }),
      });

      // Setup merge studio for b2 into main
      setMergeSource(b2);
      setMergeTarget('main');
      setMergePolicy('COMMUTATIVE_DELTA');
      setShowMergeDrawer(true);
      await fetchDagAndBranches();
      setDemoNotice(`Demo Ready: Click 'Preview Merge' below to verify 100 - 20 - 15 = 65!`);
    } catch (e: any) {
      setErrorMsg('Demo error: ' + e.message);
    } finally {
      setDemoRunning(false);
    }
  };

  const runInvariantRejectionDemo = async () => {
    setDemoRunning(true);
    setDemoNotice('Running Financial Account Domain Invariant Demo...');
    try {
      // 1. Base on main: balance:acc1 = 50
      await fetch(`${getApiBase()}/chronos/write`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ branchId: 'main', writerId: 'bank-admin', operations: [{ opType: 'PUT', key: 'balance:acc1', value: '50' }], ops: [{ opType: 'PUT', key: 'balance:acc1', value: '50' }] }),
      });

      // 2. Fork atm-1 and atm-2
      const b1 = 'atm-1-' + Math.floor(Math.random() * 1000);
      const b2 = 'atm-2-' + Math.floor(Math.random() * 1000);
      await fetch(`${getApiBase()}/chronos/branches`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ branchId: b1, writerId: 'atm-1' }),
      });
      await fetch(`${getApiBase()}/chronos/branches`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ branchId: b2, writerId: 'atm-2' }),
      });

      // 3. ATM-1 withdraws 40, ATM-2 withdraws 30
      await fetch(`${getApiBase()}/chronos/write`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ branchId: b1, writerId: 'atm-1', operations: [{ opType: 'DELTA', key: 'balance:acc1', deltaAmount: -40 }], ops: [{ opType: 'DELTA', key: 'balance:acc1', deltaAmount: -40 }] }),
      });
      await fetch(`${getApiBase()}/chronos/write`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ branchId: b2, writerId: 'atm-2', operations: [{ opType: 'DELTA', key: 'balance:acc1', deltaAmount: -30 }], ops: [{ opType: 'DELTA', key: 'balance:acc1', deltaAmount: -30 }] }),
      });

      // 4. Merge atm-1 into main
      await fetch(`${getApiBase()}/chronos/merge`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sourceBranch: b1, targetBranch: 'main', policy: 'COMMUTATIVE_DELTA' }),
      });

      // Setup merge studio for atm-2 into main
      setMergeSource(b2);
      setMergeTarget('main');
      setMergePolicy('COMMUTATIVE_DELTA');
      setShowMergeDrawer(true);
      await fetchDagAndBranches();
      setDemoNotice(`Demo Ready: Click 'Preview Merge' to see Invariant Violation (50 - 40 - 30 = -20 < 0)!`);
    } catch (e: any) {
      setErrorMsg('Demo error: ' + e.message);
    } finally {
      setDemoRunning(false);
    }
  };

  // Node positioning for SVG DAG using smart topological interval lane allocation
  const layoutNodes = useMemo(() => {
    if (!dag?.nodes || dag.nodes.length === 0) return [];
    // Sort chronological by sequence
    const sorted = [...dag.nodes].sort((a, b) => a.sequence - b.sequence);

    // Calculate index intervals [start, end] for each branch
    const intervals: Record<string, [number, number]> = {};
    sorted.forEach((n, idx) => {
      const b = n.branchId || 'main';
      if (!intervals[b]) {
        intervals[b] = [idx, idx];
      } else {
        intervals[b][1] = idx;
      }
    });

    // Extend branch lifespan if a merge commit references this branch as parent
    sorted.forEach((n, idx) => {
      if (n.isMerge && n.parentVersionIds) {
        n.parentVersionIds.forEach((pId) => {
          const parentNode = sorted.find((m) => m.versionId === pId);
          if (parentNode && parentNode.branchId && parentNode.branchId !== 'main') {
            if (intervals[parentNode.branchId]) {
              intervals[parentNode.branchId][1] = Math.max(intervals[parentNode.branchId][1], idx);
            }
          }
        });
      }
    });

    // main is always lane 0
    const branchLanes: Record<string, number> = { main: 0 };
    const laneOccupiedUntil: Record<number, number> = { 0: Infinity };

    // Sort non-main branches by their start index
    const otherBranches = Object.keys(intervals)
      .filter((b) => b !== 'main')
      .sort((a, b) => intervals[a][0] - intervals[b][0]);

    otherBranches.forEach((b) => {
      const [start, end] = intervals[b];
      let lane = 1;
      while (laneOccupiedUntil[lane] !== undefined && laneOccupiedUntil[lane] >= start) {
        lane++;
      }
      branchLanes[b] = lane;
      laneOccupiedUntil[lane] = end;
    });

    const LANE_HEIGHT = 70;
    const NODE_SPACING_X = 110;

    return sorted.map((n, idx) => ({
      ...n,
      x: 80 + idx * NODE_SPACING_X,
      y: 60 + (branchLanes[n.branchId] ?? 0) * LANE_HEIGHT,
      lane: branchLanes[n.branchId] ?? 0,
    }));
  }, [dag]);

  // Helper to ensure zoom behavior is initialized and attached
  const ensureZoom = useCallback(() => {
    if (!svgRef.current || !contentRef.current) return null;
    if (zoomBehaviorRef.current) return zoomBehaviorRef.current;

    const svg = d3.select(svgRef.current);
    const content = d3.select(contentRef.current);

    const zoom = d3.zoom<SVGSVGElement, unknown>()
      .scaleExtent([0.15, 3.0])
      .on('zoom', (event) => {
        content.attr('transform', event.transform);
        setZoomScale(event.transform.k);
      });

    zoomBehaviorRef.current = zoom;
    svg.call(zoom);
    svg.on('dblclick.zoom', null);
    return zoom;
  }, []);

  // Setup D3 zoom and pan on mount and whenever canvas mounts
  useEffect(() => {
    ensureZoom();
  }, [ensureZoom, layoutNodes.length]);

  // Fit all nodes comfortably into the screen
  const fitToScreen = useCallback((animate = true) => {
    if (!svgRef.current || !contentRef.current || !containerRef.current || layoutNodes.length === 0) return;
    const zoom = ensureZoom();
    if (!zoom) return;

    const container = containerRef.current;
    const width = container.clientWidth || 800;
    const height = container.clientHeight || 450;
    if (width <= 0 || height <= 0) return;

    let minX = Infinity, maxX = -Infinity, minY = Infinity, maxY = -Infinity;
    for (const node of layoutNodes) {
      minX = Math.min(minX, node.x - 30);
      maxX = Math.max(maxX, node.x + 30);
      minY = Math.min(minY, node.y - 30);
      maxY = Math.max(maxY, node.y + 55);
    }

    const graphWidth = Math.max(100, maxX - minX);
    const graphHeight = Math.max(100, maxY - minY);

    const paddingX = 80;
    const paddingY = 60;
    const scaleX = (width - paddingX * 2) / graphWidth;
    const scaleY = (height - paddingY * 2) / graphHeight;
    const targetScale = Math.min(1.15, Math.max(0.18, Math.min(scaleX, scaleY)));

    const centerX = (minX + maxX) / 2;
    const centerY = (minY + maxY) / 2;

    const tx = width / 2 - centerX * targetScale;
    const ty = height / 2 - centerY * targetScale;

    const transform = d3.zoomIdentity.translate(tx, ty).scale(targetScale);
    const svg = d3.select(svgRef.current);

    if (animate) {
      svg.transition()
        .duration(600)
        .ease(d3.easeCubicOut)
        .call(zoom.transform, transform);
    } else {
      svg.call(zoom.transform, transform);
    }
  }, [layoutNodes, ensureZoom]);

  // Auto-fit on initial node layout load
  useEffect(() => {
    if (layoutNodes.length > 0) {
      const t = setTimeout(() => fitToScreen(false), 80);
      return () => clearTimeout(t);
    }
  }, [layoutNodes.length, fitToScreen]);

  // Center view on a specific node
  const focusNode = useCallback((node: typeof layoutNodes[0]) => {
    if (!svgRef.current || !containerRef.current) return;
    const zoom = ensureZoom();
    if (!zoom) return;

    const cw = containerRef.current.clientWidth || 800;
    const ch = containerRef.current.clientHeight || 450;
    const currentK = zoomScale || 1.0;
    const targetK = Math.max(currentK, 0.85);
    const tx = cw / 2 - node.x * targetK;
    const ty = ch / 2 - node.y * targetK;

    d3.select(svgRef.current)
      .transition()
      .duration(500)
      .ease(d3.easeCubicOut)
      .call(zoom.transform, d3.zoomIdentity.translate(tx, ty).scale(targetK));
  }, [zoomScale, ensureZoom]);

  // Zoom controls
  const handleZoomIn = () => {
    if (!svgRef.current) return;
    const zoom = ensureZoom();
    if (!zoom) return;
    d3.select(svgRef.current)
      .transition()
      .duration(250)
      .ease(d3.easeQuadOut)
      .call(zoom.scaleBy, 1.3);
  };

  const handleZoomOut = () => {
    if (!svgRef.current) return;
    const zoom = ensureZoom();
    if (!zoom) return;
    d3.select(svgRef.current)
      .transition()
      .duration(250)
      .ease(d3.easeQuadOut)
      .call(zoom.scaleBy, 1 / 1.3);
  };

  // Connected edges for hover highlighting
  const connectedEdgeSet = useMemo(() => {
    const activeId = hoveredNodeId || selectedNode?.versionId;
    if (!activeId || !dag?.edges) return new Set<string>();

    const set = new Set<string>();
    dag.edges.forEach((edge) => {
      if (edge.childVersionId === activeId || edge.parentVersionId === activeId) {
        set.add(`${edge.childVersionId}->${edge.parentVersionId}`);
      }
    });
    return set;
  }, [hoveredNodeId, selectedNode, dag]);

  // Edge curve generation
  const getEdgePath = (source: { x: number; y: number }, target: { x: number; y: number }) => {
    const x1 = source.x;
    const y1 = source.y;
    const x2 = target.x;
    const y2 = target.y;

    if (y1 === y2) {
      return `M ${x1} ${y1} L ${x2} ${y2}`;
    }

    const dx = Math.abs(x1 - x2);
    const curvature = Math.min(55, Math.max(25, dx * 0.45));
    const cp1x = x1 - curvature;
    const cp1y = y1;
    const cp2x = x2 + curvature;
    const cp2y = y2;

    return `M ${x1} ${y1} C ${cp1x} ${cp1y}, ${cp2x} ${cp2y}, ${x2} ${y2}`;
  };

  return (
    <div className="flex-1 flex flex-col h-full overflow-hidden bg-surface-950 p-6 space-y-5">
      {/* Top Banner */}
      <div className="flex items-center justify-between bg-surface-900/70 border border-white/[0.08] rounded-2xl px-6 py-4 backdrop-blur-xl shadow-lg">
        <div className="flex items-center gap-4">
          <div className="w-12 h-12 rounded-xl bg-gradient-to-br from-purple-500/20 to-pink-500/20 border border-purple-500/30 flex items-center justify-center text-purple-400">
            <GitFork className="w-6 h-6" />
          </div>
          <div>
            <div className="flex items-center gap-2">
              <h2 className="text-base font-bold text-white tracking-tight">Version DAG & Deterministic Merge Studio</h2>
              <span className="px-2 py-0.5 text-[10px] font-semibold tracking-wider uppercase rounded-full bg-purple-500/20 text-purple-300 border border-purple-500/30">
                Multi-Writer Lineage
              </span>
            </div>
            <p className="text-xs text-white/50 mt-0.5">
              Visualize branch forks, diamond merges, and Lowest Common Ancestors (LCA). Reconcile concurrent changes with commutative deltas or LWW.
            </p>
          </div>
        </div>

        <div className="flex items-center gap-2">
          <button
            onClick={() => setShowNewBranchModal(true)}
            className="flex items-center gap-1.5 px-3.5 py-2 rounded-xl text-xs font-medium bg-white/[0.04] hover:bg-white/[0.08] text-white border border-white/[0.08] transition"
          >
            <Plus className="w-4 h-4 text-purple-400" />
            <span>New Branch</span>
          </button>

          <button
            onClick={() => setShowMergeDrawer(true)}
            className="flex items-center gap-1.5 px-3.5 py-2 rounded-xl text-xs font-medium bg-gradient-to-r from-purple-600 to-indigo-600 hover:from-purple-500 hover:to-indigo-500 text-white shadow-lg shadow-purple-600/30 transition"
          >
            <GitMerge className="w-4 h-4" />
            <span>Merge Studio</span>
          </button>
        </div>
      </div>

      {/* Interactive Walkthrough / Demo Pills */}
      <div className="flex items-center justify-between bg-surface-900/40 border border-white/[0.05] rounded-xl px-4 py-2 text-xs">
        <div className="flex items-center gap-2 text-white/60">
          <Sparkles className="w-4 h-4 text-amber-400" />
          <span className="font-semibold text-white/80">Interactive Guided Scenarios:</span>
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={runCommutativeDemo}
            disabled={demoRunning}
            className="px-3 py-1 rounded-lg bg-emerald-500/10 hover:bg-emerald-500/20 text-emerald-300 border border-emerald-500/30 text-xs font-medium flex items-center gap-1.5 transition disabled:opacity-50"
          >
            <Play className="w-3 h-3" />
            <span>Scenario 1: Commutative Inventory Merge ($100 - 20 - 15 = 65)</span>
          </button>

          <button
            onClick={runInvariantRejectionDemo}
            disabled={demoRunning}
            className="px-3 py-1 rounded-lg bg-red-500/10 hover:bg-red-500/20 text-red-300 border border-red-500/30 text-xs font-medium flex items-center gap-1.5 transition disabled:opacity-50"
          >
            <Play className="w-3 h-3" />
            <span>Scenario 2: Financial Invariant Rejection ($50 - 40 - 30 &lt; 0)</span>
          </button>
        </div>
      </div>

      {demoNotice && (
        <div className="bg-indigo-950/60 border border-indigo-500/40 rounded-xl px-4 py-2.5 text-xs text-indigo-200 flex items-center justify-between">
          <span>{demoNotice}</span>
          <button onClick={() => setDemoNotice(null)} className="text-white/40 hover:text-white">
            <X className="w-4 h-4" />
          </button>
        </div>
      )}

      {errorMsg && (
        <div className="bg-rose-950/60 border border-rose-500/40 rounded-xl px-4 py-2.5 text-xs text-rose-200 flex items-center justify-between">
          <span>{errorMsg}</span>
          <button onClick={() => setErrorMsg(null)} className="text-white/40 hover:text-white">
            <X className="w-4 h-4" />
          </button>
        </div>
      )}

      {/* Main Workspace: Visual DAG + Inspector */}
      <div className="flex-1 flex gap-5 overflow-hidden min-h-0">
        {/* SVG DAG Visualizer */}
        <div className="flex-1 bg-surface-900/60 border border-white/[0.06] rounded-2xl p-4 backdrop-blur-xl flex flex-col overflow-hidden relative">
          <div className="flex items-center justify-between pb-3 border-b border-white/[0.06] z-10">
            <div className="flex items-center gap-2 text-xs">
              <span className="font-bold text-white uppercase tracking-wider">Version DAG Graph</span>
              <span className="text-white/40">
                ({dag?.nodeCount ?? dag?.nodes?.length ?? 0} versions, {dag?.edgeCount ?? dag?.edges?.length ?? 0} edges)
              </span>
            </div>
            <div className="flex items-center gap-2">
              <button
                onClick={() => fitToScreen(true)}
                className="text-white/50 hover:text-white px-2.5 py-1 rounded-lg hover:bg-white/[0.04] border border-white/[0.06] text-[11px] flex items-center gap-1.5 transition"
                title="Fit graph to viewport"
              >
                <Maximize2 className="w-3 h-3 text-purple-400" />
                <span>Fit View</span>
              </button>
              <button
                onClick={fetchDagAndBranches}
                className="text-white/40 hover:text-white p-1 rounded-lg hover:bg-white/[0.04]"
                title="Refresh DAG"
              >
                <RefreshCw className="w-3.5 h-3.5" />
              </button>
            </div>
          </div>

          <div
            ref={containerRef}
            className="flex-1 relative overflow-hidden bg-surface-950/60 rounded-xl mt-3 select-none"
          >
            <svg
              ref={svgRef}
              className="w-full h-full cursor-grab active:cursor-grabbing block"
              style={{ touchAction: 'none' }}
            >
              <defs>
                <pattern id="dag-grid-pattern" width="36" height="36" patternUnits="userSpaceOnUse">
                  <circle cx="18" cy="18" r="1" fill="rgba(255,255,255,0.06)" />
                </pattern>
                <marker id="arrow" viewBox="0 0 10 10" refX="22" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
                  <path d="M 0 1 L 10 5 L 0 9 z" fill="rgba(255,255,255,0.3)" />
                </marker>
                <marker id="arrow-merge" viewBox="0 0 10 10" refX="26" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
                  <path d="M 0 1 L 10 5 L 0 9 z" fill="#c084fc" />
                </marker>
              </defs>

              {/* Background event-capturing plane */}
              <rect width="100%" height="100%" fill="transparent" pointerEvents="all" />

              {/* Zooming / Panning Content Group */}
              <g ref={contentRef}>
                {/* Infinite Pan/Zoom Grid Background */}
                <rect x={-10000} y={-10000} width={20000} height={20000} fill="url(#dag-grid-pattern)" pointerEvents="none" />

                {/* Render Directed Edges */}
                {dag?.edges?.map((edge, idx) => {
                  const source = layoutNodes.find((n) => n.versionId === edge.childVersionId);
                  const target = layoutNodes.find((n) => n.versionId === edge.parentVersionId);
                  if (!source || !target) return null;

                  const edgeKey = `${edge.childVersionId}->${edge.parentVersionId}`;
                  const isHighlighted = connectedEdgeSet.has(edgeKey);
                  const hasActiveHover = Boolean(hoveredNodeId || selectedNode);
                  const isMergeEdge = source.isMerge;

                  const strokeColor = isHighlighted
                    ? isMergeEdge ? '#d8b4fe' : '#34d399'
                    : isMergeEdge ? '#a855f7' : 'rgba(255,255,255,0.2)';

                  const strokeWidth = isHighlighted ? (isMergeEdge ? 3 : 2.5) : (isMergeEdge ? 2 : 1.5);
                  const opacity = hasActiveHover && !isHighlighted ? 0.22 : 1;

                  return (
                    <path
                      key={idx}
                      d={getEdgePath(source, target)}
                      fill="none"
                      stroke={strokeColor}
                      strokeWidth={strokeWidth}
                      strokeDasharray={isMergeEdge ? '5 3' : undefined}
                      markerEnd={isMergeEdge ? 'url(#arrow-merge)' : 'url(#arrow)'}
                      opacity={opacity}
                      className="transition-all duration-200"
                    />
                  );
                })}

                {/* Render DAG Nodes */}
                {layoutNodes.map((node) => {
                  const isSelected = selectedNode?.versionId === node.versionId;
                  const isHovered = hoveredNodeId === node.versionId;
                  const isMain = node.branchId === 'main';
                  const isMerge = node.isMerge;

                  return (
                    <g
                      key={node.versionId}
                      transform={`translate(${node.x}, ${node.y})`}
                      onClick={() => setSelectedNode(node)}
                      onMouseEnter={() => setHoveredNodeId(node.versionId)}
                      onMouseLeave={() => setHoveredNodeId(null)}
                      className="cursor-pointer group"
                    >
                      {/* Node Halo / Selection Ring */}
                      {(isSelected || isHovered) && (
                        <circle
                          r={isMerge ? 26 : 22}
                          fill="none"
                          stroke={isMerge ? '#c084fc' : isMain ? '#34d399' : '#818cf8'}
                          strokeWidth={1.5}
                          strokeDasharray="3 3"
                          opacity={isSelected ? 0.8 : 0.4}
                        />
                      )}

                      {/* Node Circle */}
                      <circle
                        r={isMerge ? 19 : 15}
                        fill={isMerge ? '#581c87' : isMain ? '#064e3b' : '#312e81'}
                        stroke={
                          isSelected ? '#ffffff' : isMerge ? '#c084fc' : isMain ? '#34d399' : '#818cf8'
                        }
                        strokeWidth={isSelected ? 3 : 2}
                        className="transition-all duration-200 group-hover:scale-110"
                      />

                      {/* Icon / Shape inside node */}
                      {isMerge ? (
                        <text textAnchor="middle" dy="4" fill="#f3e8ff" fontSize="10" fontWeight="bold">
                          ⑂
                        </text>
                      ) : (
                        <text textAnchor="middle" dy="4" fill="#ffffff" fontSize="9" fontWeight="bold">
                          {node.sequence}
                        </text>
                      )}

                      {/* Node Label Below */}
                      <text
                        textAnchor="middle"
                        y={isMerge ? 32 : 28}
                        fill="#ffffff"
                        fontSize="11"
                        fontFamily="monospace"
                        fontWeight="600"
                        className="pointer-events-none"
                      >
                        {node.versionId}
                      </text>

                      <text
                        textAnchor="middle"
                        y={isMerge ? 45 : 41}
                        fill="rgba(255,255,255,0.45)"
                        fontSize="9"
                        className="pointer-events-none"
                      >
                        {node.branchId}
                      </text>
                    </g>
                  );
                })}
              </g>
            </svg>

            {/* Empty or loading state overlay */}
            {layoutNodes.length === 0 && (
              <div className="absolute inset-0 flex items-center justify-center text-center text-white/40 text-xs pointer-events-none">
                {loading ? 'Loading version graph...' : 'No versions in DAG yet.'}
              </div>
            )}

            {/* Floating Zoom and Pan Controls */}
            {layoutNodes.length > 0 && (
              <>
                <div className="absolute bottom-4 right-4 z-20 flex items-center gap-1 bg-surface-900/90 border border-white/10 rounded-xl px-2 py-1.5 shadow-2xl backdrop-blur-md">
                  <button
                    onClick={handleZoomIn}
                    className="p-1.5 text-white/70 hover:text-white hover:bg-white/10 rounded-lg transition active:scale-95"
                    title="Zoom In (+)"
                  >
                    <ZoomIn className="w-3.5 h-3.5" />
                  </button>
                  <span className="text-[10px] font-mono text-white/70 min-w-[36px] text-center select-none font-medium">
                    {Math.round(zoomScale * 100)}%
                  </span>
                  <button
                    onClick={handleZoomOut}
                    className="p-1.5 text-white/70 hover:text-white hover:bg-white/10 rounded-lg transition active:scale-95"
                    title="Zoom Out (-)"
                  >
                    <ZoomOut className="w-3.5 h-3.5" />
                  </button>
                  <div className="w-px h-3.5 bg-white/10 mx-0.5" />
                  <button
                    onClick={() => fitToScreen(true)}
                    className="p-1.5 text-purple-300 hover:text-white hover:bg-purple-500/20 rounded-lg transition active:scale-95"
                    title="Fit to Screen"
                  >
                    <Maximize2 className="w-3.5 h-3.5" />
                  </button>
                </div>

                {/* Navigation Hint */}
                <div className="absolute bottom-4 left-4 z-20 pointer-events-none text-[10px] text-white/30 font-medium flex items-center gap-1.5">
                  <span>🖱 Drag to Pan</span>
                  <span>•</span>
                  <span>Scroll to Zoom</span>
                </div>
              </>
            )}
          </div>

          {/* Graph Legend */}
          <div className="flex items-center justify-center gap-6 pt-3 mt-1 border-t border-white/[0.06] text-[11px] text-white/50">
            <div className="flex items-center gap-2">
              <span className="w-3 h-3 rounded-full bg-emerald-500/40 border border-emerald-400" />
              <span>Main Lineage</span>
            </div>
            <div className="flex items-center gap-2">
              <span className="w-3 h-3 rounded-full bg-indigo-500/40 border border-indigo-400" />
              <span>Feature Branch</span>
            </div>
            <div className="flex items-center gap-2">
              <span className="w-3.5 h-3.5 rounded-full bg-purple-500/40 border-2 border-purple-400" />
              <span>2-Parent Merge Commit</span>
            </div>
          </div>
        </div>

        {/* Selected Node Inspector Sidebar */}
        <div className="w-80 bg-surface-900/60 border border-white/[0.06] rounded-2xl p-5 backdrop-blur-xl flex flex-col justify-between overflow-y-auto">
          {selectedNode ? (
            <div className="space-y-4 text-xs">
              <div className="flex items-center justify-between border-b border-white/[0.06] pb-3">
                <div>
                  <span className="text-[10px] text-white/40 uppercase tracking-wider block">Selected Version</span>
                  <h3 className="text-sm font-bold text-white font-mono">{selectedNode.versionId}</h3>
                </div>
                <div className="flex items-center gap-1.5">
                  <button
                    onClick={() => {
                      const found = layoutNodes.find((n) => n.versionId === selectedNode.versionId);
                      if (found) focusNode(found);
                    }}
                    className="p-1 rounded-lg bg-white/[0.04] hover:bg-white/[0.08] text-white/60 hover:text-white transition"
                    title="Center on this node in graph"
                  >
                    <Crosshair className="w-3.5 h-3.5 text-purple-400" />
                  </button>
                  <span className={`px-2 py-0.5 rounded text-[10px] font-bold ${
                    selectedNode.isMerge ? 'bg-purple-500/20 text-purple-300 border border-purple-500/30' : 'bg-emerald-500/20 text-emerald-300'
                  }`}>
                    {selectedNode.isMerge ? 'MERGE COMMIT' : 'COMMIT'}
                  </span>
                </div>
              </div>

              <div className="space-y-2">
                <div className="flex justify-between py-1 border-b border-white/[0.04]">
                  <span className="text-white/40">Sequence:</span>
                  <span className="font-mono text-white/80">{selectedNode.sequence}</span>
                </div>
                <div className="flex justify-between py-1 border-b border-white/[0.04]">
                  <span className="text-white/40">Branch:</span>
                  <span className="font-mono text-purple-300">{selectedNode.branchId}</span>
                </div>
                <div className="flex justify-between py-1 border-b border-white/[0.04]">
                  <span className="text-white/40">Writer:</span>
                  <span className="font-mono text-cyan-300">{selectedNode.writerId || 'system'}</span>
                </div>
                <div className="flex justify-between py-1 border-b border-white/[0.04]">
                  <span className="text-white/40">Committed At:</span>
                  <span className="font-mono text-white/70">{new Date(selectedNode.timestamp).toLocaleTimeString()}</span>
                </div>
                <div className="flex justify-between py-1 border-b border-white/[0.04]">
                  <span className="text-white/40">Parent Versions:</span>
                  <span className="font-mono text-indigo-300">{selectedNode.parentVersionIds?.join(', ') || 'root'}</span>
                </div>
                {selectedNode.mergePolicy && (
                  <div className="flex justify-between py-1 border-b border-white/[0.04]">
                    <span className="text-white/40">Merge Policy:</span>
                    <span className="font-mono text-amber-300 font-semibold">{selectedNode.mergePolicy}</span>
                  </div>
                )}
                <div className="flex justify-between py-1">
                  <span className="text-white/40">Mutations Count:</span>
                  <span className="font-mono text-white/90">
                    {selectedNode.changeCount ?? (selectedNode as any).changeSet?.length ?? 0}
                  </span>
                </div>
              </div>
            </div>
          ) : (
            <div className="flex-1 flex items-center justify-center text-center text-white/30 text-xs">
              Click any node in the graph to inspect its version metadata.
            </div>
          )}

          {/* Branches Summary */}
          <div className="pt-4 border-t border-white/[0.06] space-y-2">
            <span className="text-[10px] uppercase font-bold tracking-wider text-white/40 block">
              Active Branches ({branches.length})
            </span>
            <div className="space-y-1.5 max-h-40 overflow-y-auto">
              {branches.map((b) => (
                <div key={b.branchId} className="flex items-center justify-between bg-surface-950 p-2 rounded-lg text-xs font-mono">
                  <span className="text-purple-300">{b.branchId}</span>
                  <span className={`text-[10px] px-1.5 py-0.5 rounded ${
                    b.status === 'MERGED' ? 'bg-white/10 text-white/40' : 'bg-emerald-500/20 text-emerald-400'
                  }`}>
                    {b.status}
                  </span>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>

      {/* CREATE NEW BRANCH MODAL */}
      <AnimatePresence>
        {showNewBranchModal && (
          <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 backdrop-blur-sm p-4">
            <motion.div
              initial={{ scale: 0.95, opacity: 0 }}
              animate={{ scale: 1, opacity: 1 }}
              exit={{ scale: 0.95, opacity: 0 }}
              className="bg-surface-900 border border-white/[0.08] rounded-2xl p-6 w-full max-w-md shadow-2xl space-y-4"
            >
              <div className="flex items-center justify-between border-b border-white/[0.06] pb-3">
                <h3 className="text-sm font-bold text-white flex items-center gap-2">
                  <GitBranch className="w-4 h-4 text-purple-400" />
                  <span>Fork Isolated Branch</span>
                </h3>
                <button onClick={() => setShowNewBranchModal(false)} className="text-white/40 hover:text-white">
                  <X className="w-4 h-4" />
                </button>
              </div>

              <div className="space-y-3 text-xs">
                <div>
                  <label className="text-white/60 block mb-1">Branch ID</label>
                  <input
                    type="text"
                    placeholder="feature-x..."
                    value={newBranchId}
                    onChange={(e) => setNewBranchId(e.target.value)}
                    className="w-full bg-surface-950 border border-white/[0.08] rounded-xl px-3 py-2 text-white font-mono focus:outline-none focus:border-purple-500"
                  />
                </div>

                <div>
                  <label className="text-white/60 block mb-1">Base Version ID (Optional, defaults to head)</label>
                  <select
                    value={newBranchBase}
                    onChange={(e) => setNewBranchBase(e.target.value)}
                    className="w-full bg-surface-950 border border-white/[0.08] rounded-xl px-3 py-2 text-white font-mono"
                  >
                    <option value="">Head of main (latest)</option>
                    {dag?.nodes?.map((n) => (
                      <option key={n.versionId} value={n.versionId}>{n.versionId} ({n.branchId})</option>
                    ))}
                  </select>
                </div>

                <div>
                  <label className="text-white/60 block mb-1">Writer / Author ID</label>
                  <input
                    type="text"
                    placeholder="developer-1..."
                    value={newBranchWriter}
                    onChange={(e) => setNewBranchWriter(e.target.value)}
                    className="w-full bg-surface-950 border border-white/[0.08] rounded-xl px-3 py-2 text-white font-mono focus:outline-none focus:border-purple-500"
                  />
                </div>
              </div>

              <div className="flex justify-end gap-2 pt-2">
                <button
                  onClick={() => setShowNewBranchModal(false)}
                  className="px-4 py-2 rounded-xl text-xs text-white/60 hover:text-white hover:bg-white/[0.04]"
                >
                  Cancel
                </button>
                <button
                  onClick={handleCreateBranch}
                  disabled={!newBranchId.trim()}
                  className="bg-purple-600 hover:bg-purple-500 text-white font-medium px-4 py-2 rounded-xl text-xs transition disabled:opacity-50"
                >
                  Create Branch
                </button>
              </div>
            </motion.div>
          </div>
        )}
      </AnimatePresence>

      {/* MERGE STUDIO DRAWER */}
      <AnimatePresence>
        {showMergeDrawer && (
          <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 backdrop-blur-sm p-4">
            <motion.div
              initial={{ scale: 0.95, opacity: 0 }}
              animate={{ scale: 1, opacity: 1 }}
              exit={{ scale: 0.95, opacity: 0 }}
              className="bg-surface-900 border border-white/[0.08] rounded-2xl p-6 w-full max-w-2xl max-h-[85vh] flex flex-col shadow-2xl space-y-4 overflow-hidden"
            >
              <div className="flex items-center justify-between border-b border-white/[0.06] pb-3 shrink-0">
                <div className="flex items-center gap-2">
                  <GitMerge className="w-5 h-5 text-purple-400" />
                  <h3 className="text-sm font-bold text-white">Deterministic Branch Merge Studio</h3>
                </div>
                <button onClick={() => setShowMergeDrawer(false)} className="text-white/40 hover:text-white">
                  <X className="w-4 h-4" />
                </button>
              </div>

              <div className="space-y-4 overflow-y-auto pr-1">
                {errorMsg && (
                  <div className="bg-red-500/10 border border-red-500/30 rounded-xl p-3 text-xs text-red-200 flex items-center justify-between">
                    <span>{errorMsg}</span>
                    <button onClick={() => setErrorMsg(null)} className="text-white/40 hover:text-white">
                      <X className="w-3.5 h-3.5" />
                    </button>
                  </div>
                )}
                {/* Branch and Policy Pickers */}
                <div className="grid grid-cols-1 md:grid-cols-3 gap-3 text-xs">
                  <div>
                    <label className="text-white/60 block mb-1">Source Branch (to merge)</label>
                    <select
                      value={mergeSource}
                      onChange={(e) => setMergeSource(e.target.value)}
                      className="w-full bg-surface-950 border border-white/[0.08] rounded-xl px-3 py-2 text-white font-mono"
                    >
                      {branches.map((b) => (
                        <option key={b.branchId} value={b.branchId}>{b.branchId} ({b.status})</option>
                      ))}
                    </select>
                  </div>

                  <div>
                    <label className="text-white/60 block mb-1">Target Branch (dest)</label>
                    <select
                      value={mergeTarget}
                      onChange={(e) => setMergeTarget(e.target.value)}
                      className="w-full bg-surface-950 border border-white/[0.08] rounded-xl px-3 py-2 text-white font-mono"
                    >
                      {branches.map((b) => (
                        <option key={b.branchId} value={b.branchId}>{b.branchId}</option>
                      ))}
                    </select>
                  </div>

                  <div>
                    <label className="text-white/60 block mb-1">Reconciliation Policy</label>
                    <select
                      value={mergePolicy}
                      onChange={(e) => setMergePolicy(e.target.value as any)}
                      className="w-full bg-surface-950 border border-white/[0.08] rounded-xl px-3 py-2 text-white font-mono"
                    >
                      <option value="COMMUTATIVE_DELTA">COMMUTATIVE_DELTA</option>
                      <option value="LAST_WRITE_WINS">LAST_WRITE_WINS</option>
                      <option value="FAIL_ON_CONFLICT">FAIL_ON_CONFLICT</option>
                      <option value="CUSTOM_RESOLUTIONS">CUSTOM_RESOLUTIONS</option>
                    </select>
                  </div>
                </div>

                <div className="flex justify-end gap-2">
                  <button
                    onClick={handlePreviewMerge}
                    disabled={previewLoading || !mergeSource || !mergeTarget}
                    className="bg-white/[0.06] hover:bg-white/[0.1] text-white font-medium px-4 py-2 rounded-xl text-xs flex items-center gap-1.5 transition disabled:opacity-50"
                  >
                    {previewLoading ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Layers className="w-3.5 h-3.5" />}
                    <span>Preview 3-Way Merge</span>
                  </button>

                  <button
                    onClick={handleExecuteMerge}
                    disabled={Boolean(previewLoading || !mergeSource || !mergeTarget || (mergePreview && !mergePreview.canMerge))}
                    className="bg-gradient-to-r from-purple-600 to-indigo-600 hover:from-purple-500 hover:to-indigo-500 text-white font-medium px-4 py-2 rounded-xl text-xs flex items-center gap-1.5 transition disabled:opacity-50"
                  >
                    <GitMerge className="w-3.5 h-3.5" />
                    <span>Commit Merge to Raft</span>
                  </button>
                </div>

                {/* Preview Results Display */}
                {mergePreview && (
                  <div className="bg-surface-950 border border-white/[0.08] rounded-xl p-4 text-xs space-y-3">
                    <div className="flex items-center justify-between">
                      <div className="flex items-center gap-2">
                        {mergePreview.canMerge ? (
                          <CheckCircle2 className="w-4 h-4 text-emerald-400" />
                        ) : (
                          <XCircle className="w-4 h-4 text-red-400" />
                        )}
                        <span className={`font-bold ${mergePreview.canMerge ? 'text-emerald-300' : 'text-red-300'}`}>
                          {mergePreview.canMerge ? 'MERGE APPROVED' : 'MERGE REJECTED'}
                        </span>
                      </div>
                      <span className="text-white/40 font-mono">LCA: {mergePreview.lcaVersionId || 'None'}</span>
                    </div>

                    {mergePreview.reason && (
                      <div className="p-2 rounded bg-white/[0.03] text-white/70 italic text-[11px]">
                        {mergePreview.reason}
                      </div>
                    )}

                    {/* Detected Conflicts */}
                    {(mergePreview.conflicts?.length || 0) > 0 && (
                      <div className="space-y-1">
                        <span className="font-bold text-amber-300 text-[11px] block">
                          Detected Conflicts ({mergePreview.conflicts?.length})
                        </span>
                        <div className="space-y-1">
                          {mergePreview.conflicts?.map((c, i) => (
                            <div key={i} className="flex items-center justify-between bg-white/[0.02] p-2 rounded text-[11px] font-mono">
                              <span className="text-cyan-300 font-semibold">{c.key}</span>
                              <span className="text-amber-400">{c.conflictType}</span>
                              <span className="text-white/50">{c.description}</span>
                            </div>
                          ))}
                        </div>
                      </div>
                    )}

                    {/* Invariant Violations */}
                    {(mergePreview.invariantViolations?.length || 0) > 0 && (
                      <div className="space-y-1">
                        <span className="font-bold text-red-400 text-[11px] block">
                          Domain Invariant Failures ({mergePreview.invariantViolations?.length})
                        </span>
                        <div className="space-y-1">
                          {mergePreview.invariantViolations?.map((v, i) => (
                            <div key={i} className="bg-red-500/10 border border-red-500/30 p-2 rounded text-[11px] text-red-200">
                              <span className="font-bold">{v.ruleName}:</span> {v.message} (actual={v.actualValue}, expected {v.expectedConstraint})
                            </div>
                          ))}
                        </div>
                      </div>
                    )}

                    {/* Tentative Merged State Preview */}
                    <div className="space-y-1 pt-2 border-t border-white/[0.04]">
                      <span className="font-bold text-white/50 text-[11px] block">Tentative Merged State Preview</span>
                      <div className="grid grid-cols-2 gap-1 max-h-32 overflow-y-auto">
                        {Object.entries(mergePreview.tentativeMergedState || {}).map(([k, v]) => (
                          <div key={k} className="flex justify-between bg-white/[0.02] px-2 py-1 rounded font-mono text-[11px]">
                            <span className="text-cyan-300">{k}:</span>
                            <span className="text-white/90">{v}</span>
                          </div>
                        ))}
                      </div>
                    </div>
                  </div>
                )}

                {/* Commit Result Success Display */}
                {mergeResult && mergeResult.success && (
                  <div className="bg-emerald-950/60 border border-emerald-500/40 rounded-xl p-4 text-xs text-emerald-200 space-y-1">
                    <div className="flex items-center gap-2 font-bold text-emerald-300">
                      <CheckCircle2 className="w-4 h-4" />
                      <span>Merge Committed to Raft!</span>
                    </div>
                    <p className="font-mono text-[11px]">Version ID: {mergeResult.versionId} (seq={mergeResult.sequence})</p>
                    <p className="font-mono text-[11px]">Parents: {mergeResult.parentVersionIds?.join(', ')}</p>
                  </div>
                )}
              </div>
            </motion.div>
          </div>
        )}
      </AnimatePresence>
    </div>
  );
}
