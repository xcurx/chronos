package com.chronos.storage;

import com.chronos.model.*;
import com.minidb.mvcc.MvccStore;
import com.minidb.mvcc.ReadMode;
import com.minidb.storage.RocksDbEngine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link VersionCatalog}, Chronos domain models, immutability invariants,
 * reverse time-index seeks, persistence across RocksDB reopen, and MvccStore coexistence.
 */
class VersionCatalogTest {

    @TempDir
    Path tempDir;

    private String dbPath;
    private RocksDbEngine storage;
    private VersionCatalog catalog;

    @BeforeEach
    void setUp() {
        dbPath = tempDir.resolve("chronos-db").toString();
        storage = new RocksDbEngine(dbPath);
        catalog = new VersionCatalog(storage);
    }

    @AfterEach
    void tearDown() {
        if (storage != null) {
            storage.close();
        }
    }

    @Test
    void testSaveRetrieveAndSerializationRoundTrip() {
        ChronosVersion v1 = ChronosVersion.builder()
                .versionId("V1")
                .sequence(catalog.nextSequence())
                .branchId("main")
                .timestamp(1000L)
                .raftLogIndex(5L)
                .writerId("writer-1")
                .transactionId("txn-100")
                .status(VersionStatus.COMMITTED)
                .changeSet(List.of(
                        ChangeEntry.put("Alice", null, bytes("1000"), "writer-1"),
                        ChangeEntry.delta("Bob", bytes("1500"), bytes("2000"), 500L, "writer-1"),
                        ChangeEntry.delete("TempKey", bytes("old"), "writer-1")
                ))
                .build();

        catalog.saveVersion(v1);

        Optional<ChronosVersion> loaded = catalog.getVersion("V1");
        assertTrue(loaded.isPresent());
        assertEquals(v1, loaded.get());
        assertEquals(1L, loaded.get().getSequence());
        assertEquals(3, loaded.get().getChangeSet().size());
        assertEquals(OperationType.DELTA, loaded.get().getChangeSet().get(1).getOpType());
        assertEquals(500L, loaded.get().getChangeSet().get(1).getDeltaAmount());
        assertEquals(OperationType.DELETE, loaded.get().getChangeSet().get(2).getOpType());
        assertFalse(loaded.get().getChangeSet().get(2).isNewPresent());
        assertEquals("V1", catalog.getBranchHead("main").orElse(null));

        assertEquals(v1, catalog.getVersionBySequence(1L).orElse(null));
    }

    @Test
    void testMergeVersionWithConflictMetadataRoundTrip() {
        ChronosVersion merge = ChronosVersion.builder()
                .versionId("V3")
                .sequence(catalog.nextSequence())
                .parentVersionIds(List.of("V2A", "V2B"))
                .branchId("main")
                .timestamp(3000L)
                .raftLogIndex(9L)
                .writerId("merge-coordinator")
                .status(VersionStatus.COMMITTED)
                .isMerge(true)
                .mergePolicy("OPERATION_AWARE")
                .conflictMetadata(List.of(
                        ConflictRecord.builder()
                                .key("Alice")
                                .conflictType(ConflictType.WRITE_WRITE)
                                .leftBranchId("branchA")
                                .rightBranchId("branchB")
                                .baseVersionId("V1")
                                .leftValue(bytes("1500"))
                                .rightValue(bytes("1200"))
                                .resolutionStrategy("DELTA_COMBINE")
                                .resolvedValue(bytes("1700"))
                                .winningWriterId("branchA+branchB")
                                .notes("Combined +500 and +200")
                                .build()
                ))
                .changeSet(List.of(ChangeEntry.delta("Alice", bytes("1000"), bytes("1700"), 700L, "merge")))
                .build();

        catalog.saveVersion(merge);

        ChronosVersion loaded = catalog.getVersion("V3").orElseThrow();
        assertTrue(loaded.isMerge());
        assertEquals("OPERATION_AWARE", loaded.getMergePolicy());
        assertEquals(List.of("V2A", "V2B"), loaded.getParentVersionIds());
        assertEquals(1, loaded.getConflictMetadata().size());
        assertEquals("1700", loaded.getConflictMetadata().get(0).getResolvedValueAsString());
    }

    @Test
    void testTimeIndexLookupsAndSameMillisecondTieBreaking() {
        ChronosVersion v1 = committed("V1", catalog.nextSequence(), List.of(), 1000L, "Alice", "1000");
        ChronosVersion v2 = committed("V2", catalog.nextSequence(), List.of("V1"), 2000L, "Alice", "1500");
        ChronosVersion v3 = committed("V3", catalog.nextSequence(), List.of("V2"), 2000L, "Bob", "3000");
        ChronosVersion v4 = committed("V4", catalog.nextSequence(), List.of("V3"), 5000L, "Bob", "4000");

        catalog.saveVersion(v1);
        catalog.saveVersion(v2);
        catalog.saveVersion(v3);
        catalog.saveVersion(v4);

        assertTrue(catalog.findVersionAtOrBeforeTimestamp(999L).isEmpty());
        assertEquals("V1", catalog.findVersionAtOrBeforeTimestamp(1000L).orElseThrow().getVersionId());
        assertEquals("V1", catalog.findVersionAtOrBeforeTimestamp(1500L).orElseThrow().getVersionId());
        // Same timestamp (2000L) breaks tie cleanly toward higher sequence (V3)
        assertEquals("V3", catalog.findVersionAtOrBeforeTimestamp(2000L).orElseThrow().getVersionId());
        assertEquals("V3", catalog.findVersionAtOrBeforeTimestamp(4999L).orElseThrow().getVersionId());
        assertEquals("V4", catalog.findVersionAtOrBeforeTimestamp(Long.MAX_VALUE).orElseThrow().getVersionId());

        List<ChronosVersion> range = catalog.listVersionsByTimeRange(2000L, 4999L, 10);
        assertEquals(List.of("V2", "V3"), range.stream().map(ChronosVersion::getVersionId).toList());
        assertEquals(List.of("V4", "V3", "V2", "V1"),
                catalog.listVersionsDescending(10).stream().map(ChronosVersion::getVersionId).toList());
    }

    @Test
    void testSpeculativeLifecycleAndPagedReverseTimeSeek() {
        ChronosVersion v1 = committed("V1", catalog.nextSequence(), List.of(), 1000L, "Alice", "1000");
        catalog.saveVersion(v1);

        // Insert > 16 rolled-back versions after V1 to exercise reverse-seek paging
        for (int i = 0; i < 20; i++) {
            ChronosVersion spec = ChronosVersion.builder()
                    .versionId("VSPEC-" + i)
                    .sequence(catalog.nextSequence())
                    .parentVersionIds(List.of("V1"))
                    .branchId("main")
                    .timestamp(2000L + i)
                    .raftLogIndex(10L + i)
                    .status(VersionStatus.SPECULATIVE)
                    .changeSet(List.of(ChangeEntry.put("k", null, bytes("v" + i), "w")))
                    .build();
            catalog.saveVersion(spec, true);
            catalog.rollbackSpeculative("VSPEC-" + i);
        }

        // Branch head must still point to V1; time query must page back to V1
        assertEquals("V1", catalog.getBranchHead("main").orElse(null));
        assertEquals("V1", catalog.findVersionAtOrBeforeTimestamp(2500L).orElseThrow().getVersionId());

        // New speculative write is invisible to committed time query until promoted
        long seq = catalog.nextSequence();
        ChronosVersion v2Spec = ChronosVersion.builder()
                .versionId("V2")
                .sequence(seq)
                .parentVersionIds(List.of("V1"))
                .branchId("main")
                .timestamp(3000L)
                .raftLogIndex(50L)
                .status(VersionStatus.SPECULATIVE)
                .changeSet(List.of(ChangeEntry.put("Alice", bytes("1000"), bytes("1500"), "w1")))
                .build();
        catalog.saveVersion(v2Spec, true);

        assertEquals("V1", catalog.getBranchHead("main").orElse(null));
        assertEquals("V1", catalog.findVersionAtOrBeforeTimestamp(3500L).orElseThrow().getVersionId());
        assertEquals("V2", catalog.findVersionAtOrBeforeTimestamp(3500L, "main", true).orElseThrow().getVersionId());

        ChronosVersion promoted = catalog.promoteSpeculative("V2");
        assertEquals(VersionStatus.COMMITTED, promoted.getStatus());
        assertEquals("V2", catalog.getBranchHead("main").orElse(null));
        assertEquals("V2", catalog.findVersionAtOrBeforeTimestamp(3500L).orElseThrow().getVersionId());
        // Promoting speculative version must not double-count totalVersions
        assertEquals(22L, catalog.getWatermark().getTotalVersions());
    }

    @Test
    void testImmutabilityEnforcement() {
        ChronosVersion v1 = committed("V1", catalog.nextSequence(), List.of(), 1000L, "Alice", "1000");
        catalog.saveVersion(v1);

        // Reject overwriting already committed version (even with identical data)
        assertThrows(VersionImmutabilityException.class, () -> catalog.saveVersion(v1));

        // Reject mutating changeSet or parents on existing committed version
        ChronosVersion tamperedV1 = v1.toBuilder()
                .changeSet(List.of(ChangeEntry.put("Alice", null, bytes("9999"), "evil")))
                .build();
        assertThrows(VersionImmutabilityException.class, () -> catalog.saveVersion(tamperedV1));

        // Reject assigning an already used sequence number to a different versionId
        ChronosVersion duplicateSeq = ChronosVersion.builder()
                .versionId("V1-CLONE")
                .sequence(1L)
                .branchId("main")
                .timestamp(1500L)
                .status(VersionStatus.COMMITTED)
                .build();
        assertThrows(VersionImmutabilityException.class, () -> catalog.saveVersion(duplicateSeq));

        // Reject mutating payload while promoting SPECULATIVE -> COMMITTED
        ChronosVersion v2Spec = ChronosVersion.builder()
                .versionId("V2")
                .sequence(catalog.nextSequence())
                .parentVersionIds(List.of("V1"))
                .branchId("main")
                .timestamp(2000L)
                .status(VersionStatus.SPECULATIVE)
                .changeSet(List.of(ChangeEntry.put("Bob", null, bytes("2000"), "w")))
                .build();
        catalog.saveVersion(v2Spec, true);

        ChronosVersion tamperedPromotion = v2Spec.toBuilder()
                .status(VersionStatus.COMMITTED)
                .changeSet(List.of(ChangeEntry.put("Bob", null, bytes("9999"), "w")))
                .build();
        assertThrows(VersionImmutabilityException.class, () -> catalog.saveVersion(tamperedPromotion));

        // Reject overwriting ROLLED_BACK version
        catalog.rollbackSpeculative("V2");
        assertThrows(VersionImmutabilityException.class, () -> catalog.promoteSpeculative("V2"));
    }

    @Test
    void testSnapshotsWatermarkAndPersistenceAcrossRocksDbReopen() {
        ChronosVersion v1 = committed("V1", catalog.nextSequence(), List.of(), 1000L, "Alice", "1000");
        ChronosVersion v2 = committed("V2", catalog.nextSequence(), List.of("V1"), 2000L, "Bob", "2000");
        catalog.saveVersion(v1);
        catalog.saveVersion(v2);

        ChronosSnapshot snap = new ChronosSnapshot(
                "before-audit", "V1", "main", 1000L, 4L, 1005L, "Pre-audit baseline");
        catalog.saveSnapshot(snap);

        assertThrows(VersionImmutabilityException.class, () -> catalog.saveSnapshot(snap));
        assertThrows(IllegalArgumentException.class, () -> catalog.saveSnapshot(
                new ChronosSnapshot("bad", "V999", "main", 1000L, 0L, 1000L, null)));

        // Close and reopen RocksDB + VersionCatalog
        storage.close();
        storage = new RocksDbEngine(dbPath);
        VersionCatalog reopened = new VersionCatalog(storage);

        assertEquals(2L, reopened.getHighestSequence());
        assertEquals(3L, reopened.nextSequence());
        assertEquals(v1, reopened.getVersion("V1").orElseThrow());
        assertEquals(v2, reopened.getVersion("V2").orElseThrow());
        assertEquals("V2", reopened.getBranchHead("main").orElse(null));
        assertEquals("V1", reopened.findVersionAtOrBeforeTimestamp(1500L).orElseThrow().getVersionId());
        assertEquals(List.of(snap), reopened.listSnapshots());

        CatalogWatermark watermark = reopened.getWatermark();
        assertEquals(1L, watermark.getEarliestAvailableSeq());
        assertEquals(1000L, watermark.getEarliestAvailableTimestamp());
        assertEquals(2L, watermark.getTotalVersions());
    }

    @Test
    void testCoexistenceWithMvccStoreAndScanReverseBounds() {
        MvccStore mvcc = new MvccStore(storage, null);
        mvcc.put("Alice", bytes("1000"), 100L);
        mvcc.put("Alice", bytes("1500"), 200L);

        catalog.saveVersion(committed("V1", catalog.nextSequence(), List.of(), 100L, "Alice", "1000"));
        catalog.saveVersion(committed("V2", catalog.nextSequence(), List.of("V1"), 200L, "Alice", "1500"));

        // MVCC user scan must never leak __chronos_* internal keys
        List<MvccStore.MvccResult> scanResults = mvcc.scan("", null, 100, 0L, ReadMode.LINEARIZABLE);
        assertEquals(1, scanResults.size());
        assertEquals("Alice", scanResults.get(0).getKey());

        // MVCC GC must not delete Chronos version catalog or time-index entries
        int purged = mvcc.garbageCollect(-60_000L);
        assertEquals(1, purged);
        assertTrue(catalog.getVersion("V1").isPresent());
        assertTrue(catalog.getVersion("V2").isPresent());
        assertEquals("V1", catalog.findVersionAtOrBeforeTimestamp(150L).orElseThrow().getVersionId());

        // Verify scanReverse exclusive upper bound edge case
        List<Map.Entry<byte[], byte[]>> reverse = storage.scanReverse(
                ChronosKeys.SEQ_PREFIX_BYTES, ChronosKeys.seqKey(2L), 10);
        assertEquals(1, reverse.size());
        assertEquals("V1", new String(reverse.get(0).getValue(), StandardCharsets.UTF_8));
    }

    private ChronosVersion committed(String id, long seq, List<String> parents, long ts, String key, String val) {
        return ChronosVersion.builder()
                .versionId(id)
                .sequence(seq)
                .parentVersionIds(parents)
                .branchId("main")
                .timestamp(ts)
                .raftLogIndex(seq * 10)
                .writerId("writer-main")
                .status(VersionStatus.COMMITTED)
                .changeSet(List.of(ChangeEntry.put(key, null, bytes(val), "writer-main")))
                .build();
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
