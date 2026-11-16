package com.chronos.gc;

import com.chronos.model.CatalogWatermark;
import com.chronos.model.ChronosVersion;
import com.chronos.model.VersionStatus;
import com.chronos.storage.VersionCatalog;
import com.minidb.events.ClusterEvent;
import com.minidb.events.EventBus;
import com.minidb.mvcc.MvccStore;
import com.minidb.raft.RaftNode;
import com.minidb.raft.SnapshotManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Chronos Safe Low-Watermark Garbage Collection service implementing {@code SafeToDelete(V)}.
 *
 * <p>Protects versions within configured retention bounds, pinned audit versions, active
 * branch heads/bases/LCAs, active temporal query read pins, and per-key watermark boundary
 * values so retained historical reconstruction never produces silent wrong answers.</p>
 */
@Service
public class ChronosGcService {

    private static final Logger log = LoggerFactory.getLogger(ChronosGcService.class);

    public record GcRunResult(
            int versionsPurged,
            int mvccEntriesPurged,
            long earliestAvailableSeq,
            long earliestAvailableTimestamp,
            boolean blockedByActiveReads,
            int activeReadPins,
            RetentionPolicy policy
    ) {}

    private final VersionCatalog versionCatalog;
    private final ChronosRetentionConfig retentionConfig;
    private final ReadWatermarkRegistry watermarkRegistry;
    private final Supplier<MvccStore> mvccStoreSupplier;
    private final Supplier<SnapshotManager> snapshotManagerSupplier;
    private final EventBus eventBus;
    private final Object gcLock = new Object();
    private final AtomicLong totalVersionsPurged = new AtomicLong(0);
    private final AtomicLong totalMvccPurged = new AtomicLong(0);

    @Autowired
    public ChronosGcService(@Lazy RaftNode raftNode,
                            VersionCatalog versionCatalog,
                            ChronosRetentionConfig retentionConfig,
                            ReadWatermarkRegistry watermarkRegistry,
                            @Autowired(required = false) EventBus eventBus) {
        this.versionCatalog = versionCatalog;
        this.retentionConfig = retentionConfig;
        this.watermarkRegistry = watermarkRegistry;
        this.eventBus = eventBus;
        this.mvccStoreSupplier = () -> raftNode != null ? raftNode.getMvccStore() : null;
        this.snapshotManagerSupplier = () -> raftNode != null ? raftNode.getSnapshotManager() : null;
    }

    public ChronosGcService(VersionCatalog versionCatalog,
                            MvccStore mvccStore,
                            SnapshotManager snapshotManager,
                            ChronosRetentionConfig retentionConfig,
                            ReadWatermarkRegistry watermarkRegistry) {
        this(versionCatalog, mvccStore, snapshotManager, retentionConfig, watermarkRegistry, null);
    }

    public ChronosGcService(VersionCatalog versionCatalog,
                            MvccStore mvccStore,
                            SnapshotManager snapshotManager,
                            ChronosRetentionConfig retentionConfig,
                            ReadWatermarkRegistry watermarkRegistry,
                            EventBus eventBus) {
        this.versionCatalog = versionCatalog;
        this.retentionConfig = retentionConfig;
        this.watermarkRegistry = watermarkRegistry;
        this.eventBus = eventBus;
        this.mvccStoreSupplier = () -> mvccStore;
        this.snapshotManagerSupplier = () -> snapshotManager;
    }

    public GcRunResult runGc() {
        return runGcAt(System.currentTimeMillis());
    }

    /**
     * Execute one deterministic Safe GC cycle using the supplied wall-clock reference.
     */
    public GcRunResult runGcAt(long nowMs) {
        synchronized (gcLock) {
            RetentionPolicy policy = retentionConfig.getPolicy();
            MvccStore mvccStore = mvccStoreSupplier.get();
            SnapshotManager snapshotManager = snapshotManagerSupplier.get();

            long activeMinSeq = watermarkRegistry.getActiveLowWatermarkSeq();
            long activeMinTs = watermarkRegistry.getActiveLowWatermarkTimestamp();
            int activePinCount = watermarkRegistry.getActivePinCount();

            List<ChronosVersion> allVersions = versionCatalog.listVersions(0);
            List<ChronosVersion> committed = new ArrayList<>();
            for (ChronosVersion v : allVersions) {
                if (v.getStatus() == VersionStatus.COMMITTED) {
                    committed.add(v);
                }
            }

            if (committed.isEmpty() || policy == RetentionPolicy.UNBOUNDED) {
                int mvccPurged = mvccStore != null ? mvccStore.garbageCollectAtCutoff(0L, false) : 0;
                totalMvccPurged.addAndGet(mvccPurged);
                CatalogWatermark current = versionCatalog.getWatermark();
                return new GcRunResult(0, mvccPurged, current.getEarliestAvailableSeq(),
                        current.getEarliestAvailableTimestamp(), false, activePinCount, policy);
            }

            Set<String> protectedIds = versionCatalog.getProtectedVersionIds();
            Set<String> pinnedIds = retentionConfig.getPinnedVersions();
            ChronosVersion latestCommitted = committed.get(committed.size() - 1);

            long policyFloorSeq = latestCommitted.getSequence();
            for (int i = 0; i < committed.size(); i++) {
                ChronosVersion v = committed.get(i);
                int rankFromEnd = committed.size() - 1 - i;
                if (isRetainedByPolicy(v, rankFromEnd, nowMs, policy)
                        || protectedIds.contains(v.getVersionId())
                        || pinnedIds.contains(v.getVersionId())) {
                    policyFloorSeq = v.getSequence();
                    break;
                }
            }

            long safeBarrierSeq = policyFloorSeq;
            boolean blockedByActiveReads = false;

            for (ChronosVersion v : committed) {
                if (v.getSequence() >= safeBarrierSeq) {
                    break;
                }
                if (v.getSequence() >= activeMinSeq || v.getTimestamp() >= activeMinTs) {
                    safeBarrierSeq = v.getSequence();
                    blockedByActiveReads = true;
                    break;
                }
            }

            ChronosVersion firstRetained = latestCommitted;
            for (ChronosVersion v : committed) {
                if (v.getSequence() >= safeBarrierSeq) {
                    firstRetained = v;
                    break;
                }
            }

            final long cutoffSeq = safeBarrierSeq;
            List<ChronosVersion> toPurge = new ArrayList<>();
            int retainedCount = 0;
            for (ChronosVersion v : allVersions) {
                if (v.getSequence() < cutoffSeq) {
                    toPurge.add(v);
                } else {
                    retainedCount++;
                }
            }

            int versionsPurged = 0;
            int mvccPurged = 0;
            CatalogWatermark resultingWatermark = versionCatalog.getWatermark();

            if (!toPurge.isEmpty()) {
                resultingWatermark = new CatalogWatermark(
                        firstRetained.getSequence(),
                        firstRetained.getTimestamp(),
                        retainedCount,
                        nowMs
                );
                versionsPurged = versionCatalog.purgeVersionsWithWatermark(toPurge, resultingWatermark);
                totalVersionsPurged.addAndGet(versionsPurged);

                if (mvccStore != null) {
                    mvccPurged = mvccStore.garbageCollectAtCutoff(firstRetained.getTimestamp(), true);
                }
                if (snapshotManager != null) {
                    snapshotManager.updateRetainedRaftLogBound(firstRetained.getRaftLogIndex());
                }
                log.info("Chronos Safe GC purged {} catalog versions and {} MVCC entries; watermark seq={}, ts={}",
                        versionsPurged, mvccPurged, resultingWatermark.getEarliestAvailableSeq(),
                        resultingWatermark.getEarliestAvailableTimestamp());
                if (eventBus != null && (versionsPurged > 0 || mvccPurged > 0)) {
                    eventBus.publish(ClusterEvent.chronosWatermarkGc(
                            "cluster", versionsPurged, mvccPurged, resultingWatermark.getEarliestAvailableSeq()));
                }
            } else if (mvccStore != null) {
                // Still sweep any index-0 ROLLED_BACK MVCC records even when no committed versions expire
                mvccPurged = mvccStore.garbageCollectAtCutoff(0L, false);
            }

            totalMvccPurged.addAndGet(mvccPurged);
            return new GcRunResult(
                    versionsPurged,
                    mvccPurged,
                    resultingWatermark.getEarliestAvailableSeq(),
                    resultingWatermark.getEarliestAvailableTimestamp(),
                    blockedByActiveReads,
                    activePinCount,
                    policy
            );
        }
    }

    private boolean isRetainedByPolicy(ChronosVersion v, int rankFromEnd, long nowMs, RetentionPolicy policy) {
        return switch (policy) {
            case UNBOUNDED -> true;
            case LAST_N_VERSIONS -> rankFromEnd < retentionConfig.getMaxVersions();
            case TIME_WINDOW -> (nowMs - v.getTimestamp()) <= retentionConfig.getWindowMs();
            case HYBRID -> rankFromEnd < retentionConfig.getMaxVersions()
                    && (nowMs - v.getTimestamp()) <= retentionConfig.getWindowMs();
        };
    }

    public long getTotalVersionsPurged() {
        return totalVersionsPurged.get();
    }

    public long getTotalMvccPurged() {
        return totalMvccPurged.get();
    }
}

