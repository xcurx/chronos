package com.chronos.query;

import com.minidb.mvcc.ReadMode;

/**
 * Structured request for single-key temporal queries or full-state reconstruction.
 */
public record TemporalQueryRequest(
        QueryMode mode,
        String key,
        String versionId,
        String timestamp,
        String snapshotId,
        String branchId,
        ReadMode readMode,
        boolean consistent,
        String prefix,
        int limit,
        String validTime,
        Long validFrom,
        Long validTo
) {
    public TemporalQueryRequest(
            QueryMode mode,
            String key,
            String versionId,
            String timestamp,
            String snapshotId,
            String branchId,
            ReadMode readMode,
            boolean consistent,
            String prefix,
            int limit) {
        this(mode, key, versionId, timestamp, snapshotId, branchId, readMode, consistent, prefix, limit, null, null, null);
    }

    public static TemporalQueryRequest current(String key, String branchId, ReadMode readMode, boolean consistent) {
        return new TemporalQueryRequest(QueryMode.CURRENT, key, null, null, null, branchId, readMode, consistent, null, 0);
    }

    public static TemporalQueryRequest asOfVersion(String key, String versionId, boolean consistent) {
        return new TemporalQueryRequest(QueryMode.AS_OF_VERSION, key, versionId, null, null, null, ReadMode.LINEARIZABLE, consistent, null, 0);
    }

    public static TemporalQueryRequest asOfTime(String key, String timestamp, String branchId, boolean consistent) {
        return new TemporalQueryRequest(QueryMode.AS_OF_TIME, key, null, timestamp, null, branchId, ReadMode.LINEARIZABLE, consistent, null, 0);
    }

    public static TemporalQueryRequest asOfSnapshot(String key, String snapshotId, boolean consistent) {
        return new TemporalQueryRequest(QueryMode.AS_OF_SNAPSHOT, key, null, null, snapshotId, null, ReadMode.LINEARIZABLE, consistent, null, 0);
    }

    public static TemporalQueryRequest asOfValidTime(String key, String validTime, String branchId, boolean consistent) {
        return new TemporalQueryRequest(QueryMode.AS_OF_VALID_TIME, key, null, null, null, branchId, ReadMode.LINEARIZABLE, consistent, null, 0, validTime, null, null);
    }

    public static TemporalQueryRequest bitemporal(String key, String validTime, String txnTimestamp, String branchId, boolean consistent) {
        return new TemporalQueryRequest(QueryMode.BITEMPORAL, key, null, txnTimestamp, null, branchId, ReadMode.LINEARIZABLE, consistent, null, 0, validTime, null, null);
    }
}

