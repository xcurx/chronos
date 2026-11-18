package com.chronos.query;

import java.nio.charset.StandardCharsets;

/**
 * Result of a single-key temporal lookup at CURRENT, AS OF VERSION, AS OF TIME, or AS OF SNAPSHOT.
 */
public record TemporalQueryResult(
        boolean found,
        String key,
        byte[] valueBytes,
        String value,
        boolean tombstone,
        boolean speculative,
        String versionId,
        long versionSequence,
        long timestamp,
        String branchId,
        String writerId,
        String transactionId,
        QueryMode queryMode,
        long latencyMs,
        Long validFrom,
        Long validTo
) {
    public TemporalQueryResult(
            boolean found,
            String key,
            byte[] valueBytes,
            String value,
            boolean tombstone,
            boolean speculative,
            String versionId,
            long versionSequence,
            long timestamp,
            String branchId,
            String writerId,
            String transactionId,
            QueryMode queryMode,
            long latencyMs) {
        this(found, key, valueBytes, value, tombstone, speculative,
                versionId, versionSequence, timestamp, branchId, writerId, transactionId, queryMode, latencyMs, null, null);
    }

    public static TemporalQueryResult ofFound(String key,
                                              byte[] valueBytes,
                                              boolean speculative,
                                              String versionId,
                                              long versionSequence,
                                              long timestamp,
                                              String branchId,
                                              String writerId,
                                              String transactionId,
                                              QueryMode queryMode,
                                              long latencyMs) {
        return ofFound(key, valueBytes, speculative, versionId, versionSequence, timestamp,
                branchId, writerId, transactionId, queryMode, latencyMs, null, null);
    }

    public static TemporalQueryResult ofFound(String key,
                                              byte[] valueBytes,
                                              boolean speculative,
                                              String versionId,
                                              long versionSequence,
                                              long timestamp,
                                              String branchId,
                                              String writerId,
                                              String transactionId,
                                              QueryMode queryMode,
                                              long latencyMs,
                                              Long validFrom,
                                              Long validTo) {
        String text = valueBytes != null ? new String(valueBytes, StandardCharsets.UTF_8) : null;
        return new TemporalQueryResult(true, key, valueBytes, text, false, speculative,
                versionId, versionSequence, timestamp, branchId, writerId, transactionId, queryMode, latencyMs, validFrom, validTo);
    }

    public static TemporalQueryResult ofNotFound(String key,
                                                 boolean tombstone,
                                                 String versionId,
                                                 long versionSequence,
                                                 long timestamp,
                                                 String branchId,
                                                 String writerId,
                                                 String transactionId,
                                                 QueryMode queryMode,
                                                 long latencyMs) {
        return ofNotFound(key, tombstone, versionId, versionSequence, timestamp,
                branchId, writerId, transactionId, queryMode, latencyMs, null, null);
    }

    public static TemporalQueryResult ofNotFound(String key,
                                                 boolean tombstone,
                                                 String versionId,
                                                 long versionSequence,
                                                 long timestamp,
                                                 String branchId,
                                                 String writerId,
                                                 String transactionId,
                                                 QueryMode queryMode,
                                                 long latencyMs,
                                                 Long validFrom,
                                                 Long validTo) {
        return new TemporalQueryResult(false, key, null, null, tombstone, false,
                versionId, versionSequence, timestamp, branchId, writerId, transactionId, queryMode, latencyMs, validFrom, validTo);
    }
}

