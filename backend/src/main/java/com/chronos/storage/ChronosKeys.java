package com.chronos.storage;

import java.nio.charset.StandardCharsets;

/**
 * RocksDB key layout for Chronos version metadata, indexes, heads, snapshots, and watermarks.
 *
 * <p>All keys use a {@code __chronos_} prefix so MVCC user scans and MVCC record GC
 * skip them automatically.</p>
 */
public final class ChronosKeys {

    public static final String VER_PREFIX = "__chronos_ver__:";
    public static final String SEQ_PREFIX = "__chronos_seq__:";
    public static final String SEQ_END = "__chronos_seq__;";
    public static final String TIME_PREFIX = "__chronos_time__:";
    public static final String TIME_END = "__chronos_time__;";
    public static final String HEAD_PREFIX = "__chronos_head__:";
    public static final String HEAD_END = "__chronos_head__;";
    public static final String SNAPSHOT_PREFIX = "__chronos_snap__:";
    public static final String SNAPSHOT_END = "__chronos_snap__;";
    public static final String BRANCH_PREFIX = "__chronos_branch__:";
    public static final String BRANCH_END = "__chronos_branch__;";
    public static final String WATERMARK_KEY = "__chronos_meta__:watermark";
    public static final String LAST_SEQ_KEY = "__chronos_meta__:last_seq";
    public static final String ROLLED_BACK_PREFIX = "__chronos_rolledback__:";
    public static final String BITEMP_PREFIX = "__chronos_bitemp__:";
    public static final String BITEMP_END = "__chronos_bitemp__;";

    public static final byte[] VER_PREFIX_BYTES = utf8(VER_PREFIX);
    public static final byte[] SEQ_PREFIX_BYTES = utf8(SEQ_PREFIX);
    public static final byte[] SEQ_END_BYTES = utf8(SEQ_END);
    public static final byte[] TIME_PREFIX_BYTES = utf8(TIME_PREFIX);
    public static final byte[] TIME_END_BYTES = utf8(TIME_END);
    public static final byte[] HEAD_PREFIX_BYTES = utf8(HEAD_PREFIX);
    public static final byte[] HEAD_END_BYTES = utf8(HEAD_END);
    public static final byte[] SNAPSHOT_PREFIX_BYTES = utf8(SNAPSHOT_PREFIX);
    public static final byte[] SNAPSHOT_END_BYTES = utf8(SNAPSHOT_END);
    public static final byte[] BRANCH_PREFIX_BYTES = utf8(BRANCH_PREFIX);
    public static final byte[] BRANCH_END_BYTES = utf8(BRANCH_END);
    public static final byte[] WATERMARK_KEY_BYTES = utf8(WATERMARK_KEY);
    public static final byte[] LAST_SEQ_KEY_BYTES = utf8(LAST_SEQ_KEY);
    public static final byte[] BITEMP_PREFIX_BYTES = utf8(BITEMP_PREFIX);
    public static final byte[] BITEMP_END_BYTES = utf8(BITEMP_END);

    private ChronosKeys() {}

    public static byte[] versionKey(String versionId) {
        return utf8(VER_PREFIX + versionId);
    }

    public static byte[] seqKey(long sequence) {
        return utf8(SEQ_PREFIX + formatPadded(sequence));
    }

    public static byte[] timeKey(long timestamp, long sequence, String versionId) {
        return utf8(TIME_PREFIX + formatPadded(timestamp) + ":" + formatPadded(sequence) + ":" + versionId);
    }

    public static byte[] timeLowerBoundForTimestamp(long timestamp) {
        long safeTs = Math.max(0L, timestamp);
        return utf8(TIME_PREFIX + formatPadded(safeTs) + ":");
    }

    /**
     * Exclusive upper bound covering every time-index entry with {@code entryTimestamp <= timestamp}.
     * Uses ';' (ASCII ':' + 1) so {@code timestamp == Long.MAX_VALUE} does not overflow.
     */
    public static byte[] timeUpperBoundForTimestamp(long timestamp) {
        long safeTs = Math.max(0L, timestamp);
        return utf8(TIME_PREFIX + formatPadded(safeTs) + ";");
    }

    public static byte[] headKey(String branchId) {
        return utf8(HEAD_PREFIX + branchId);
    }

    public static byte[] snapshotKey(String snapshotId) {
        return utf8(SNAPSHOT_PREFIX + snapshotId);
    }

    public static byte[] branchKey(String branchId) {
        return utf8(BRANCH_PREFIX + branchId);
    }

    public static byte[] rolledBackArchiveKey(String versionId, long raftLogIndex) {
        return utf8(ROLLED_BACK_PREFIX + versionId + ":" + raftLogIndex);
    }

    public static byte[] bitempKey(String key, long validFrom, long validTo, long sequence) {
        long safeFrom = Math.max(0L, validFrom);
        long safeTo = Math.max(safeFrom, validTo);
        return utf8(BITEMP_PREFIX + key + ":" + formatPadded(safeFrom) + ":" + formatPadded(safeTo) + ":" + formatPadded(sequence));
    }

    public static byte[] bitempPrefixForKey(String key) {
        return utf8(BITEMP_PREFIX + key + ":");
    }

    public static byte[] bitempEndForKey(String key) {
        return utf8(BITEMP_PREFIX + key + ";");
    }

    public static String formatPadded(long value) {
        if (value < 0) {
            throw new IllegalArgumentException("Padded index value must be non-negative: " + value);
        }
        return String.format("%020d", value);
    }

    public static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}

