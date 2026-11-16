export type QueryMode = 'CURRENT' | 'AS_OF_VERSION' | 'AS_OF_TIME' | 'AS_OF_SNAPSHOT';
export type ReadMode = 'LINEARIZABLE' | 'SPECULATIVE';
export type BranchStatus = 'ACTIVE' | 'MERGED' | 'ABANDONED';
export type ConflictType =
  | 'NONE'
  | 'COMPATIBLE_DELTA'
  | 'IDENTICAL_WRITE'
  | 'WRITE_WRITE'
  | 'UPDATE_DELETE'
  | 'DELETE_UPDATE'
  | 'CAUSAL_DIVERGENCE'
  | 'INVARIANT_VIOLATION';

export type MergePolicy = 'COMMUTATIVE_DELTA' | 'LAST_WRITE_WINS' | 'FAIL_ON_CONFLICT' | 'CUSTOM_RESOLUTIONS';

export interface ChronosVersionSummary {
  versionId: string;
  sequence: number;
  timestamp: number;
  parentVersionIds: string[];
  branchId: string;
  status: 'COMMITTED' | 'SPECULATIVE' | 'ROLLED_BACK';
  writerId: string;
  isMerge: boolean;
  mergePolicy?: string;
  changeCount: number;
}

export interface ChronosBranch {
  branchId: string;
  baseVersionId: string;
  headVersionId: string;
  writerId: string;
  status: BranchStatus;
  createdAt: number;
  updatedAt: number;
  description?: string;
}

export interface TemporalQueryResult {
  key: string;
  value: string | null;
  found: boolean;
  versionId?: string;
  timestamp?: number;
  queryMode: QueryMode;
  readMode: ReadMode;
  tombstone: boolean;
  executionTimeMs: number;
}

export interface DatabaseStateResult {
  keys: Record<string, string>;
  totalKeys: number;
  asOfVersionId: string;
  asOfTimestamp: number;
  queryMode: QueryMode;
  readMode: ReadMode;
  executionTimeMs: number;
}

export interface KeyAuditEntry {
  versionId: string;
  sequence: number;
  timestamp: number;
  opType: 'PUT' | 'DELETE' | 'DELTA';
  oldValue: string | null;
  newValue: string | null;
  deltaAmount: number | null;
  writerId: string;
  branchId: string;
}

export interface KeyAuditTrail {
  key: string;
  history: KeyAuditEntry[];
  totalModifications: number;
}

export interface KeyDiffEntry {
  key: string;
  oldValue: string | null;
  newValue: string | null;
  changeType: 'ADDED' | 'MODIFIED' | 'DELETED' | 'UNCHANGED';
}

export interface VersionDiffResult {
  baseVersionId: string;
  targetVersionId: string;
  totalChanges: number;
  keysModified: number;
  changes: KeyDiffEntry[];
}

export interface DagNode {
  versionId: string;
  sequence: number;
  timestamp: number;
  branchId: string;
  writerId: string;
  isMerge: boolean;
  mergePolicy?: string;
  status: string;
  parentVersionIds: string[];
  changeCount: number;
}

export interface DagEdge {
  parentVersionId: string;
  childVersionId: string;
  branchId: string;
}

export interface VersionDagResponse {
  nodes: DagNode[];
  edges: DagEdge[];
  totalNodes?: number;
  totalEdges?: number;
  nodeCount?: number;
  edgeCount?: number;
  lcaCandidate?: string;
}

export interface ConflictRecord {
  key: string;
  conflictType: ConflictType;
  leftBranchId: string;
  rightBranchId: string;
  baseVersionId: string;
  leftValue?: string;
  rightValue?: string;
  resolutionStrategy?: string;
  resolvedValue?: string;
  winningWriterId?: string;
  notes?: string;
}

export interface DetectedConflict {
  key: string;
  conflictType: ConflictType;
  leftBranchId: string;
  rightBranchId: string;
  baseValue: string | null;
  leftValue: string | null;
  rightValue: string | null;
  description: string;
  resolvable: boolean;
}

export interface InvariantViolation {
  ruleName: string;
  key: string;
  actualValue: string;
  expectedConstraint: string;
  message: string;
}

export interface MergePreviewResponse {
  canMerge: boolean;
  sourceBranch: string;
  targetBranch: string;
  sourceTipId: string;
  targetTipId: string;
  lcaVersionId?: string;
  policy: MergePolicy;
  conflicts: DetectedConflict[];
  invariantViolations: InvariantViolation[];
  tentativeMergedState: Record<string, string>;
  finalResolutions: Record<string, string>;
  totalConflicts: number;
  totalViolations: number;
  reason?: string;
}

export interface MergeResult {
  success: boolean;
  versionId: string;
  sequence: number;
  timestamp: number;
  targetBranch: string;
  sourceBranch: string;
  parentVersionIds: string[];
  policy: MergePolicy;
  resolvedConflictsCount: number;
  conflictRecords: ConflictRecord[];
  message: string;
}

export interface InvariantRule {
  name: string;
  type: 'NON_NEGATIVE_NUMERIC' | 'CUSTOM_BOUNDS' | 'CONSERVATION_SUM';
  prefix: string;
  min?: number;
  max?: number;
}

export interface ChronosMetricsSummary {
  versionsTotal: number;
  conflictsTotal: number;
  mergesCommitted: number;
  mergesRejected: number;
  activeBranches: number;
  highestSequence: number;
  latencies: {
    versionCreationAvgMs: number;
    historicalQueryAvgMs: number;
    reconstructionAvgMs: number;
    conflictDetectionAvgMs: number;
    mergeAvgMs: number;
  };
}

export interface RetentionWatermark {
  policy: string;
  maxVersions: number;
  windowMs: number;
  pinnedVersions: string[];
  activeReadPins: number;
  activeLowWatermarkSeq: number;
  activeLowWatermarkTimestamp: number;
  watermark: {
    earliestAvailableSeq: number;
    earliestAvailableTimestamp: number;
  };
  totalVersionsPurged: number;
  totalMvccPurged: number;
}

