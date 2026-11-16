package com.chronos.dag;

/**
 * Directed parent-to-child lineage edge in the Chronos Version DAG.
 */
public record DagEdge(
        String parentVersionId,
        String childVersionId
) {}

