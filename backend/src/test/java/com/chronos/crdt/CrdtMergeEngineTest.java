package com.chronos.crdt;

import com.chronos.branch.BranchManager;
import com.chronos.dag.VersionDagService;
import com.chronos.merge.*;
import com.chronos.model.*;
import com.chronos.query.TemporalQueryService;
import com.chronos.storage.VersionCatalog;
import com.minidb.events.EventBus;
import com.minidb.mvcc.MvccStore;
import com.minidb.storage.RocksDbEngine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class CrdtMergeEngineTest {

    @TempDir
    File tempDir;

    private RocksDbEngine storage;
    private VersionCatalog versionCatalog;
    private BranchManager branchManager;
    private VersionDagService versionDagService;
    private TemporalQueryService temporalQueryService;
    private ConflictDetector conflictDetector;
    private InvariantValidator invariantValidator;
    private MergeEngine mergeEngine;
    private CrdtMergeResolver crdtMergeResolver;

    @BeforeEach
    void setUp() throws Exception {
        storage = new RocksDbEngine(new File(tempDir, "chronos-crdt-test").getAbsolutePath());
        versionCatalog = new VersionCatalog(storage);
        branchManager = new BranchManager(storage, versionCatalog);
        temporalQueryService = new TemporalQueryService(null, versionCatalog, new MvccStore(storage, new EventBus()));
        versionDagService = new VersionDagService(versionCatalog, temporalQueryService);
        crdtMergeResolver = new CrdtMergeResolver();
        conflictDetector = new ConflictDetector(versionCatalog, versionDagService, temporalQueryService, crdtMergeResolver);
        invariantValidator = new InvariantValidator(new InvariantRegistry());
        mergeEngine = new MergeEngine(versionCatalog, branchManager, versionDagService, temporalQueryService, conflictDetector, invariantValidator, null, null, null, crdtMergeResolver);
    }

    @AfterEach
    void tearDown() {
        if (storage != null) {
            storage.close();
        }
    }

    @Test
    void testPnCounterConcurrentIncrementsAndDecrements() {
        // Base counter at 100
        PnCounter base = new PnCounter();
        base.increment("base", 100);
        assertEquals(100, base.value());

        // Branch A adds 30, subtracts 5 (net +25 -> 125)
        PnCounter branchA = new PnCounter(base.getP(), base.getN());
        branchA.increment("branch-A", 30);
        branchA.decrement("branch-A", 5);
        assertEquals(125, branchA.value());

        // Branch B adds 50, subtracts 10 (net +40 -> 140)
        PnCounter branchB = new PnCounter(base.getP(), base.getN());
        branchB.increment("branch-B", 50);
        branchB.decrement("branch-B", 10);
        assertEquals(140, branchB.value());

        // Merge: should be 100 + 25 + 40 = 165
        PnCounter merged = branchA.merge(branchB);
        assertEquals(165, merged.value());

        // Commutativity: B merge A equals A merge B
        PnCounter reverseMerged = branchB.merge(branchA);
        assertEquals(165, reverseMerged.value());
        assertEquals(merged.getP(), reverseMerged.getP());
        assertEquals(merged.getN(), reverseMerged.getN());
    }

    @Test
    void testPnCounter3WayRelativeNumericResolution() {
        CrdtResolutionResult res = crdtMergeResolver.resolve(
                "counter:site_visits",
                "100",  // Base
                "125",  // Left (+25)
                "140",  // Right (+40)
                "feature-a", "feature-b",
                1000L, 1005L,
                1L, 2L,
                "writer-a", "writer-b",
                MergePolicy.CRDT
        );

        assertEquals("165", res.resolvedValue());
        assertEquals(CrdtType.PN_COUNTER, res.crdtType());
    }

    @Test
    void testLwwRegisterDeterministicTimestampTieBreak() {
        // Left write at t=1000
        LwwRegister regLeft = LwwRegister.of("left-value", 1000L, 10L, "writer-a");
        // Right write at t=2000
        LwwRegister regRight = LwwRegister.of("right-value", 2000L, 11L, "writer-b");

        // Higher timestamp wins
        LwwRegister merged = regLeft.merge(regRight);
        assertEquals("right-value", merged.getValue());
        assertEquals("writer-b", merged.getWriterId());

        // Tie-breaker when timestamps identical: sequence wins
        LwwRegister tie1 = LwwRegister.of("v1", 1000L, 5L, "writer-a");
        LwwRegister tie2 = LwwRegister.of("v2", 1000L, 8L, "writer-b");
        assertEquals("v2", tie1.merge(tie2).getValue());

        // Tie-breaker when timestamp and sequence identical: writerId wins lexicographically
        LwwRegister tieW1 = LwwRegister.of("val-alpha", 1000L, 5L, "alice");
        LwwRegister tieW2 = LwwRegister.of("val-bob", 1000L, 5L, "bob");
        assertEquals("val-bob", tieW1.merge(tieW2).getValue());
    }

    @Test
    void testOrSetAddWinsSemantics() {
        OrSet set = new OrSet();
        String tagApple = set.add("apple");
        set.add("banana");

        assertEquals(Set.of("apple", "banana"), set.readElements());

        // Branch A removes "apple" (observing tagApple)
        OrSet branchA = new OrSet(set.getElements(), set.getTombstones());
        branchA.remove("apple");
        assertFalse(branchA.contains("apple"));

        // Branch B concurrently re-adds "apple" (generates new unobserved tag) and adds "cherry"
        OrSet branchB = new OrSet(set.getElements(), set.getTombstones());
        branchB.add("apple");
        branchB.add("cherry");

        // Merge: Add-Wins ensures "apple" is kept because branch B's addition was concurrent and unobserved
        OrSet merged = branchA.merge(branchB);
        assertTrue(merged.contains("apple"), "Add-Wins: concurrent addition must win over removal");
        assertTrue(merged.contains("banana"));
        assertTrue(merged.contains("cherry"));
        assertEquals(Set.of("apple", "banana", "cherry"), merged.readElements());

        // Commutative
        OrSet reverse = branchB.merge(branchA);
        assertEquals(merged.readElements(), reverse.readElements());
    }

    @Test
    void testOrMapDisjointFieldUpdatesMergeWithZeroConflict() {
        String baseJson = "{\"name\":\"Alice\",\"city\":\"NYC\",\"tier\":\"Bronze\"}";
        String leftJson = "{\"name\":\"Alice\",\"city\":\"San Francisco\",\"tier\":\"Bronze\"}"; // changed city
        String rightJson = "{\"name\":\"Alice\",\"city\":\"NYC\",\"tier\":\"Gold\"}";           // changed tier

        CrdtResolutionResult result = crdtMergeResolver.resolve(
                "user:profile:100",
                baseJson,
                leftJson,
                rightJson,
                "branch-city",
                "branch-tier",
                1000L, 1005L,
                1L, 2L,
                "writer-city", "writer-tier",
                MergePolicy.CRDT
        );

        assertEquals(CrdtType.OR_MAP, result.crdtType());
        Map<String, String> plain = OrMap.parseJsonMap(result.resolvedValue());
        assertEquals("Alice", plain.get("name"));
        assertEquals("San Francisco", plain.get("city"));
        assertEquals("Gold", plain.get("tier"));
    }

    @Test
    void testThreeWayConflictDetectionWithCrdtPolicy() {
        // Commit base version V1 on main
        ChronosVersion v1 = ChronosVersion.builder()
                .versionId("V1")
                .sequence(1L)
                .timestamp(1000L)
                .branchId("main")
                .writerId("writer-0")
                .status(VersionStatus.COMMITTED)
                .changeSet(List.of(
                        ChangeEntry.put("counter:inventory", null, "100".getBytes(StandardCharsets.UTF_8), "writer-0"),
                        ChangeEntry.put("set:tags", null, "[\"fast\", \"stable\"]".getBytes(StandardCharsets.UTF_8), "writer-0")
                ))
                .build();
        versionCatalog.saveVersion(v1);
        branchManager.createBranch("branch-A", "V1", "writer-A", "Branch A");
        branchManager.createBranch("branch-B", "V1", "writer-B", "Branch B");

        // Branch A updates: counter +20 -> 120, tags adds "experimental"
        ChronosVersion vA = ChronosVersion.builder()
                .versionId("V2")
                .sequence(2L)
                .timestamp(1010L)
                .parentVersionIds(List.of("V1"))
                .branchId("branch-A")
                .writerId("writer-A")
                .status(VersionStatus.COMMITTED)
                .changeSet(List.of(
                        ChangeEntry.put("counter:inventory", "100".getBytes(StandardCharsets.UTF_8), "120".getBytes(StandardCharsets.UTF_8), "writer-A"),
                        ChangeEntry.put("set:tags", "[\"fast\", \"stable\"]".getBytes(StandardCharsets.UTF_8), "[\"fast\", \"stable\", \"experimental\"]".getBytes(StandardCharsets.UTF_8), "writer-A")
                ))
                .build();
        versionCatalog.saveVersion(vA);
        branchManager.recordCommitToBranch("branch-A", "V2", "writer-A");

        // Branch B updates: counter -10 -> 90, tags removes "fast" and adds "secure"
        ChronosVersion vB = ChronosVersion.builder()
                .versionId("V3")
                .sequence(3L)
                .timestamp(1020L)
                .parentVersionIds(List.of("V1"))
                .branchId("branch-B")
                .writerId("writer-B")
                .status(VersionStatus.COMMITTED)
                .changeSet(List.of(
                        ChangeEntry.put("counter:inventory", "100".getBytes(StandardCharsets.UTF_8), "90".getBytes(StandardCharsets.UTF_8), "writer-B"),
                        ChangeEntry.put("set:tags", "[\"fast\", \"stable\"]".getBytes(StandardCharsets.UTF_8), "[\"stable\", \"secure\"]".getBytes(StandardCharsets.UTF_8), "writer-B")
                ))
                .build();
        versionCatalog.saveVersion(vB);
        branchManager.recordCommitToBranch("branch-B", "V3", "writer-B");

        // Preview merge between branch-A and branch-B under CRDT policy
        MergeRequest req = new MergeRequest("branch-B", "branch-A", MergePolicy.CRDT, Collections.emptyMap(), "test-merger", "Merge CRDT branches");
        MergePreviewResponse preview = mergeEngine.preview(req);

        assertTrue(preview.canMerge(), "Merge must be accepted under CRDT policy");
        Map<String, String> mergedState = preview.tentativeMergedState();

        // 100 + 20 - 10 = 110
        assertEquals("110", mergedState.get("counter:inventory"));

        // Tags merged: stable + experimental + secure (fast removed)
        String tagStr = mergedState.get("set:tags");
        assertTrue(tagStr.contains("stable"));
        assertTrue(tagStr.contains("experimental"));
        assertTrue(tagStr.contains("secure"));
    }
}
