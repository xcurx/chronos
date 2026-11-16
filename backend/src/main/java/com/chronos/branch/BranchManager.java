package com.chronos.branch;

import com.chronos.metrics.ChronosMetrics;
import com.chronos.model.ChronosVersion;
import com.chronos.storage.ChronosKeys;
import com.chronos.storage.VersionCatalog;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.minidb.events.ClusterEvent;
import com.minidb.events.EventBus;
import com.minidb.storage.StorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages explicit and optimistic stale-base auto-branches in Chronos,
 * persisting metadata in RocksDB and protecting active branch base versions from GC.
 */
@Service
public class BranchManager {

    private static final Logger log = LoggerFactory.getLogger(BranchManager.class);
    public static final String MAIN_BRANCH = "main";

    private final StorageEngine storage;
    private final VersionCatalog versionCatalog;
    private final EventBus eventBus;
    private final ChronosMetrics chronosMetrics;
    private final ObjectMapper mapper;
    private final Object writeLock = new Object();
    private final Map<String, BranchInfo> branchCache = new ConcurrentHashMap<>();

    public BranchManager(StorageEngine storage, VersionCatalog versionCatalog) {
        this(storage, versionCatalog, null, null);
    }

    @Autowired
    public BranchManager(StorageEngine storage,
                         VersionCatalog versionCatalog,
                         @Autowired(required = false) EventBus eventBus,
                         @Autowired(required = false) ChronosMetrics chronosMetrics) {
        this.storage = Objects.requireNonNull(storage, "storage must not be null");
        this.versionCatalog = Objects.requireNonNull(versionCatalog, "versionCatalog must not be null");
        this.eventBus = eventBus;
        this.chronosMetrics = chronosMetrics;
        this.mapper = new ObjectMapper();
        initializeBranches();
        this.versionCatalog.registerProtectedVersionsSupplier(this::getProtectedBranchVersionIds);
        if (this.chronosMetrics != null) {
            this.chronosMetrics.setActiveBranches(getActiveBranchCount());
        }
    }

    private void initializeBranches() {
        List<Map.Entry<byte[], byte[]>> entries = storage.scan(
                ChronosKeys.BRANCH_PREFIX_BYTES, ChronosKeys.BRANCH_END_BYTES, 0);
        for (Map.Entry<byte[], byte[]> entry : entries) {
            try {
                BranchInfo info = deserialize(entry.getValue());
                branchCache.put(info.getBranchId(), info);
            } catch (Exception e) {
                log.warn("Failed to deserialize branch record: {}", e.getMessage());
            }
        }

        // Ensure main branch always exists
        if (!branchCache.containsKey(MAIN_BRANCH)) {
            String head = versionCatalog.getBranchHead(MAIN_BRANCH).orElse(null);
            BranchInfo main = new BranchInfo(MAIN_BRANCH, head, head, "system", BranchStatus.ACTIVE,
                    System.currentTimeMillis(), System.currentTimeMillis(), "Default main branch");
            saveBranchDirect(main);
        }
    }

    /**
     * Create an explicit branch anchored at {@code baseVersionId}.
     */
    public BranchInfo createBranch(String branchId, String baseVersionId, String writerId, String description) {
        if (branchId == null || branchId.isBlank()) {
            throw new IllegalArgumentException("branchId must be non-blank");
        }
        String normalizedId = branchId.trim();
        synchronized (writeLock) {
            if (branchCache.containsKey(normalizedId)) {
                throw new IllegalStateException("Branch '" + normalizedId + "' already exists");
            }

            String effectiveBase = baseVersionId;
            if (effectiveBase == null || effectiveBase.isBlank()) {
                effectiveBase = versionCatalog.getBranchHead(MAIN_BRANCH)
                        .orElseThrow(() -> new IllegalStateException("No committed versions to branch from"));
            } else {
                Optional<ChronosVersion> baseVer = versionCatalog.getVersion(effectiveBase);
                if (baseVer.isEmpty()) {
                    throw new IllegalArgumentException("Base version '" + effectiveBase + "' not found");
                }
            }

            BranchInfo branch = BranchInfo.create(normalizedId, effectiveBase, writerId, description);
            saveBranchDirect(branch);
            if (eventBus != null) {
                eventBus.publish(ClusterEvent.chronosBranchCreated(
                        "cluster", normalizedId, effectiveBase, writerId));
            }
            log.info("Created branch '{}' at baseVersion='{}' by writer='{}'",
                    normalizedId, effectiveBase, writerId);
            return branch;
        }
    }

    /**
     * Auto-create an optimistic branch when a concurrent writer detects a stale parent on main.
     */
    public BranchInfo autoCreateStaleBranch(String writerId, String staleBaseVersionId, long sequence) {
        String base = (staleBaseVersionId != null && !staleBaseVersionId.isBlank())
                ? staleBaseVersionId
                : versionCatalog.getBranchHead(MAIN_BRANCH).orElse("root");
        String prefix = (writerId != null && !writerId.isBlank()) ? writerId : "writer";
        String branchId = "auto-" + prefix + "-" + (sequence > 0 ? sequence : System.currentTimeMillis());

        synchronized (writeLock) {
            BranchInfo branch = BranchInfo.create(
                    branchId,
                    base,
                    writerId,
                    "Auto-created branch due to stale base " + base
            );
            saveBranchDirect(branch);
            log.info("Auto-created branch '{}' from stale base '{}'", branchId, base);
            return branch;
        }
    }

    public Optional<BranchInfo> getBranch(String branchId) {
        if (branchId == null || branchId.isBlank()) {
            return Optional.empty();
        }
        String id = branchId.trim();
        BranchInfo info = branchCache.get(id);
        if (info == null) {
            byte[] bytes = storage.get(ChronosKeys.branchKey(id));
            if (bytes != null) {
                info = deserialize(bytes);
                branchCache.put(info.getBranchId(), info);
            }
        }
        if (info != null) {
            if (info.getHeadVersionId() == null && versionCatalog != null) {
                String head = versionCatalog.getBranchHead(info.getBranchId()).orElse(null);
                if (head != null) {
                    info = info.withHeadVersionId(head);
                    branchCache.put(info.getBranchId(), info);
                }
            }
            return Optional.of(info);
        }
        return Optional.empty();
    }

    public List<BranchInfo> listBranches() {
        List<BranchInfo> list = new ArrayList<>(branchCache.size());
        for (BranchInfo info : branchCache.values()) {
            if (info.getHeadVersionId() == null && versionCatalog != null) {
                String head = versionCatalog.getBranchHead(info.getBranchId()).orElse(null);
                if (head != null) {
                    info = info.withHeadVersionId(head);
                    branchCache.put(info.getBranchId(), info);
                }
            }
            list.add(info);
        }
        return list;
    }

    /**
     * Update head version of a branch when a new ChronosVersion commits to it.
     */
    public void recordCommitToBranch(String branchId, String versionId, String writerId) {
        if (branchId == null || branchId.isBlank() || versionId == null || versionId.isBlank()) {
            return;
        }
        String id = branchId.trim();
        synchronized (writeLock) {
            BranchInfo existing = branchCache.get(id);
            if (existing != null) {
                BranchInfo updated = existing.withHeadVersionId(versionId);
                saveBranchDirect(updated);
            } else {
                // Auto-register branch if not yet known
                BranchInfo newBranch = new BranchInfo(
                        id,
                        versionId,
                        versionId,
                        writerId,
                        BranchStatus.ACTIVE,
                        System.currentTimeMillis(),
                        System.currentTimeMillis(),
                        "Auto-registered on commit"
                );
                saveBranchDirect(newBranch);
            }
        }
    }

    public void updateBranchStatus(String branchId, BranchStatus newStatus) {
        if (branchId == null || branchId.isBlank()) {
            throw new IllegalArgumentException("branchId must be non-blank");
        }
        synchronized (writeLock) {
            BranchInfo existing = getBranch(branchId)
                    .orElseThrow(() -> new IllegalArgumentException("Branch '" + branchId + "' not found"));
            BranchInfo updated = existing.withStatus(newStatus);
            saveBranchDirect(updated);
        }
    }

    /**
     * Returns all baseVersionIds and active headVersionIds across all non-abandoned branches.
     * Registered with VersionCatalog so GC never purges branch divergence roots!
     */
    public Set<String> getProtectedBranchVersionIds() {
        Set<String> protectedIds = new HashSet<>();
        for (BranchInfo b : branchCache.values()) {
            if (b.getStatus() != BranchStatus.ABANDONED) {
                if (b.getBaseVersionId() != null && !b.getBaseVersionId().isBlank()) {
                    protectedIds.add(b.getBaseVersionId());
                }
                if (b.getHeadVersionId() != null && !b.getHeadVersionId().isBlank()) {
                    protectedIds.add(b.getHeadVersionId());
                }
            }
        }
        return protectedIds;
    }

    private void saveBranchDirect(BranchInfo branch) {
        branchCache.put(branch.getBranchId(), branch);
        storage.put(ChronosKeys.branchKey(branch.getBranchId()), serialize(branch));
        if (chronosMetrics != null) {
            chronosMetrics.setActiveBranches(getActiveBranchCount());
        }
    }

    private int getActiveBranchCount() {
        int count = 0;
        for (BranchInfo b : branchCache.values()) {
            if (b.getStatus() == BranchStatus.ACTIVE) count++;
        }
        return count;
    }

    private byte[] serialize(BranchInfo branch) {
        try {
            return mapper.writeValueAsBytes(branch);
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize BranchInfo", e);
        }
    }

    private BranchInfo deserialize(byte[] bytes) {
        try {
            return mapper.readValue(bytes, BranchInfo.class);
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize BranchInfo", e);
        }
    }
}

