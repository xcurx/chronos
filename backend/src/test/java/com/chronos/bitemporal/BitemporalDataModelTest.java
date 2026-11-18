package com.chronos.bitemporal;

import com.chronos.model.ChangeEntry;
import com.chronos.model.ChronosVersion;
import com.chronos.model.VersionStatus;
import com.chronos.query.DatabaseStateResult;
import com.chronos.query.QueryMode;
import com.chronos.query.TemporalQueryRequest;
import com.chronos.query.TemporalQueryResult;
import com.chronos.query.TemporalQueryService;
import com.chronos.storage.VersionCatalog;
import com.minidb.mvcc.MvccStore;
import com.minidb.storage.RocksDbEngine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verification of the Full Bitemporal Data Model ($T_t$ vs. $T_v$).
 * Tests:
 * 1. Valid-time interval evaluation $[v_{from}, v_{to})$
 * 2. Point queries with AS_OF_VALID_TIME (business effective date)
 * 3. 2D Bitemporal queries (Transaction Time $T_t$ + Valid Time $T_v$)
 * 4. Retroactive corrections and audit reproducibility
 * 5. 2D timeline inspection
 */
public class BitemporalDataModelTest {

    @TempDir
    Path tempDir;

    private RocksDbEngine storage;
    private VersionCatalog catalog;
    private MvccStore mvccStore;
    private TemporalQueryService queryService;

    @BeforeEach
    void setUp() {
        storage = new RocksDbEngine(tempDir.toString());
        catalog = new VersionCatalog(storage);
        mvccStore = new MvccStore(storage, new com.minidb.events.EventBus());
        queryService = new TemporalQueryService(null, catalog, mvccStore);
    }

    @AfterEach
    void tearDown() {
        if (storage != null) {
            storage.close();
        }
    }

    @Test
    void testChangeEntryValidTimeInterval() {
        // Valid from epoch 100 to 500
        ChangeEntry entry = ChangeEntry.put(
                "salary:emp1",
                null,
                "50000".getBytes(StandardCharsets.UTF_8),
                "writer-1",
                100L,
                500L
        );

        assertTrue(entry.isValidAt(100L));  // inclusive start
        assertTrue(entry.isValidAt(300L));  // inside interval
        assertTrue(entry.isValidAt(499L));  // inside interval
        assertFalse(entry.isValidAt(99L));  // before interval
        assertFalse(entry.isValidAt(500L)); // exclusive end
        assertFalse(entry.isValidAt(1000L));
    }

    @Test
    void testAsOfValidTimePointQuery() {
        // Version 1 committed at Tt = 1000:
        // salary:alice = 50000 for valid period [1000, 2000)
        ChronosVersion v1 = ChronosVersion.builder()
                .versionId("V1")
                .sequence(catalog.nextSequence())
                .timestamp(1000L)
                .status(VersionStatus.COMMITTED)
                .branchId("main")
                .changeSet(List.of(
                        ChangeEntry.put("salary:alice", null, "50000".getBytes(StandardCharsets.UTF_8), "hr", 1000L, 2000L)
                ))
                .build();
        catalog.saveVersion(v1);

        // Version 2 committed at Tt = 2000:
        // salary:alice = 65000 for valid period [2000, infinity)
        ChronosVersion v2 = ChronosVersion.builder()
                .versionId("V2")
                .sequence(catalog.nextSequence())
                .parentVersionIds(List.of("V1"))
                .timestamp(2000L)
                .status(VersionStatus.COMMITTED)
                .branchId("main")
                .changeSet(List.of(
                        ChangeEntry.put("salary:alice", "50000".getBytes(StandardCharsets.UTF_8), "65000".getBytes(StandardCharsets.UTF_8), "hr", 2000L, null)
                ))
                .build();
        catalog.saveVersion(v2);

        // Query AS_OF_VALID_TIME at Tv = 1500 (within first contract period)
        TemporalQueryResult r1500 = queryService.queryKey(
                TemporalQueryRequest.asOfValidTime("salary:alice", "1500", "main", false));
        assertTrue(r1500.found());
        assertEquals("50000", r1500.value());
        assertEquals(1000L, r1500.validFrom());
        assertEquals(2000L, r1500.validTo());

        // Query AS_OF_VALID_TIME at Tv = 2500 (within raise period)
        TemporalQueryResult r2500 = queryService.queryKey(
                TemporalQueryRequest.asOfValidTime("salary:alice", "2500", "main", false));
        assertTrue(r2500.found());
        assertEquals("65000", r2500.value());
        assertEquals(2000L, r2500.validFrom());
        assertNull(r2500.validTo());

        // Query AS_OF_VALID_TIME at Tv = 500 (before employment began)
        TemporalQueryResult r500 = queryService.queryKey(
                TemporalQueryRequest.asOfValidTime("salary:alice", "500", "main", false));
        assertFalse(r500.found());
    }

    @Test
    void test2DBitemporalRetroactiveCorrectionAndAuditability() {
        // Timeline scenario:
        // System Day 1 (Tt = 1000): Record employee insurance plan as 'silver' effective Tv = 100.
        ChronosVersion v1 = ChronosVersion.builder()
                .versionId("V1")
                .sequence(catalog.nextSequence())
                .timestamp(1000L)
                .status(VersionStatus.COMMITTED)
                .branchId("main")
                .changeSet(List.of(
                        ChangeEntry.put("plan:bob", null, "silver".getBytes(StandardCharsets.UTF_8), "admin", 100L, null)
                ))
                .build();
        catalog.saveVersion(v1);

        // System Day 10 (Tt = 2000): Retroactive correction discovered!
        // Bob was supposed to be 'gold' starting at Tv = 100 all along!
        ChronosVersion v2 = ChronosVersion.builder()
                .versionId("V2")
                .sequence(catalog.nextSequence())
                .parentVersionIds(List.of("V1"))
                .timestamp(2000L)
                .status(VersionStatus.COMMITTED)
                .branchId("main")
                .changeSet(List.of(
                        ChangeEntry.put("plan:bob", "silver".getBytes(StandardCharsets.UTF_8), "gold".getBytes(StandardCharsets.UTF_8), "auditor", 100L, null)
                ))
                .build();
        catalog.saveVersion(v2);

        // 1. Audit slice: "What did the database report on Day 5 (Tt = 1500) about Bob's plan on Tv = 100?"
        // Must return 'silver' for regulatory audit reproducibility!
        TemporalQueryResult auditSlice = queryService.queryKey(
                TemporalQueryRequest.bitemporal("plan:bob", "100", "1500", "main", false));
        assertTrue(auditSlice.found());
        assertEquals("silver", auditSlice.value());
        assertEquals("V1", auditSlice.versionId());

        // 2. Current knowledge slice: "According to the database today (Tt = 2000+), what was Bob's plan on Tv = 100?"
        // Must reflect retroactive correction 'gold'!
        TemporalQueryResult currentSlice = queryService.queryKey(
                TemporalQueryRequest.asOfValidTime("plan:bob", "100", "main", false));
        assertTrue(currentSlice.found());
        assertEquals("gold", currentSlice.value());
        assertEquals("V2", currentSlice.versionId());
    }

    @Test
    void testBitemporalStateReconstruction() {
        ChronosVersion v1 = ChronosVersion.builder()
                .versionId("V1")
                .sequence(catalog.nextSequence())
                .timestamp(1000L)
                .status(VersionStatus.COMMITTED)
                .branchId("main")
                .changeSet(List.of(
                        ChangeEntry.put("k1", null, "v1-early".getBytes(StandardCharsets.UTF_8), "w", 100L, 500L),
                        ChangeEntry.put("k2", null, "v2-always".getBytes(StandardCharsets.UTF_8), "w", 100L, null)
                ))
                .build();
        catalog.saveVersion(v1);

        ChronosVersion v2 = ChronosVersion.builder()
                .versionId("V2")
                .sequence(catalog.nextSequence())
                .parentVersionIds(List.of("V1"))
                .timestamp(2000L)
                .status(VersionStatus.COMMITTED)
                .branchId("main")
                .changeSet(List.of(
                        ChangeEntry.put("k1", "v1-early".getBytes(StandardCharsets.UTF_8), "v1-late".getBytes(StandardCharsets.UTF_8), "w", 500L, null)
                ))
                .build();
        catalog.saveVersion(v2);

        // State at Tv = 300: k1 should be "v1-early", k2 should be "v2-always"
        DatabaseStateResult stateEarly = queryService.reconstructState(
                TemporalQueryRequest.asOfValidTime(null, "300", "main", false));
        assertEquals("v1-early", stateEarly.entries().get("k1"));
        assertEquals("v2-always", stateEarly.entries().get("k2"));

        // State at Tv = 800: k1 should be "v1-late", k2 should be "v2-always"
        DatabaseStateResult stateLate = queryService.reconstructState(
                TemporalQueryRequest.asOfValidTime(null, "800", "main", false));
        assertEquals("v1-late", stateLate.entries().get("k1"));
        assertEquals("v2-always", stateLate.entries().get("k2"));
    }

    @Test
    void testKeyBitemporalTimeline() {
        ChronosVersion v1 = ChronosVersion.builder()
                .versionId("V1")
                .sequence(catalog.nextSequence())
                .timestamp(1000L)
                .status(VersionStatus.COMMITTED)
                .branchId("main")
                .changeSet(List.of(
                        ChangeEntry.put("contract:c1", null, "draft".getBytes(StandardCharsets.UTF_8), "alice", 100L, 200L)
                ))
                .build();
        catalog.saveVersion(v1);

        ChronosVersion v2 = ChronosVersion.builder()
                .versionId("V2")
                .sequence(catalog.nextSequence())
                .parentVersionIds(List.of("V1"))
                .timestamp(2000L)
                .status(VersionStatus.COMMITTED)
                .branchId("main")
                .changeSet(List.of(
                        ChangeEntry.put("contract:c1", "draft".getBytes(StandardCharsets.UTF_8), "signed".getBytes(StandardCharsets.UTF_8), "bob", 200L, null)
                ))
                .build();
        catalog.saveVersion(v2);

        List<Map<String, Object>> timeline = queryService.getKeyBitemporalTimeline("contract:c1");
        assertEquals(2, timeline.size());
        assertEquals("draft", timeline.get(0).get("value"));
        assertEquals(100L, timeline.get(0).get("validFrom"));
        assertEquals(200L, timeline.get(0).get("validTo"));

        assertEquals("signed", timeline.get(1).get("value"));
        assertEquals(200L, timeline.get(1).get("validFrom"));
        assertNull(timeline.get(1).get("validTo"));
    }
}
