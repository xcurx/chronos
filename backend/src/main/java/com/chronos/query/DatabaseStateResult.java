package com.chronos.query;

import java.util.List;
import java.util.Map;

/**
 * Result of reconstructing the entire database state (or key prefix) at a point in history.
 */
public record DatabaseStateResult(
        String versionId,
        long versionSequence,
        long timestamp,
        String branchId,
        QueryMode queryMode,
        Map<String, String> entries,
        List<StateEntry> details,
        int count,
        long latencyMs
) {
    /**
     * Detailed provenance for a single key present in the reconstructed state.
     */
    public record StateEntry(
            String key,
            String value,
            String versionId,
            long versionSequence,
            long timestamp,
            String branchId,
            String writerId,
            String transactionId,
            Long validFrom,
            Long validTo
    ) {
        public StateEntry(
                String key,
                String value,
                String versionId,
                long versionSequence,
                long timestamp,
                String branchId,
                String writerId,
                String transactionId) {
            this(key, value, versionId, versionSequence, timestamp, branchId, writerId, transactionId, null, null);
        }
    }
}

