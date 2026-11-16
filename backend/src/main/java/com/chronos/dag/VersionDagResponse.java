package com.chronos.dag;

import com.chronos.model.ChronosVersion;

import java.util.List;
import java.util.Map;

/**
 * Structural DAG representation containing version nodes, directed parent-to-child edges,
 * and current branch heads.
 */
public record VersionDagResponse(
        List<ChronosVersion> nodes,
        List<DagEdge> edges,
        Map<String, String> branchHeads,
        int nodeCount,
        int edgeCount
) {}

