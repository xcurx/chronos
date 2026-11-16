package com.chronos.storage;

import com.chronos.model.CatalogWatermark;
import com.chronos.model.ChronosSnapshot;
import com.chronos.model.ChronosVersion;
import com.chronos.model.VersionStatus;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.minidb.storage.StorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Persistent RocksDB-backed catalog for immutable {@link ChronosVersion} records,
 * sequence/time indexes, branch head pointers, snapshots, and GC low watermarks.
 */
@Service
public class VersionCatalog {

    private static final Logger log = LoggerFactory.getLogger(VersionCatalog.class);
    private static final int TIME_SEEK_PAGE_SIZE = 16;

    private final StorageEngine storage;
    private final ObjectMapper mapper;
    private final Object writeLock = new Object();
    private final AtomicLong sequenceCounter = new AtomicLong(0);
    private final List<Supplier<Set<String>>> protectedVersionSuppliers = new CopyOnWriteArrayList<>();

    public VersionCatalog(StorageEngine storage) {
        this.storage = Objects.requireNonNull(storage, "storage must not be null");
        this.mapper = new ObjectMapper();
        initializeSequenceCounter();
    }

    private void initializeSequenceCounter() {
        long maxSeq = 0L;

        byte[] persistedLastSeq = storage.get(ChronosKeys.LAST_SEQ_KEY_BYTES);
        if (persistedLastSeq != null) {
            try {
                maxSeq = Math.max(maxSeq, Long.parseLong(new String(persistedLastSeq, StandardCharsets.UTF_8)));
            } catch (NumberFormatException ignored) {
                // Fall back to sequence index scan below
            }
        }

        List<Map.Entry<byte[], byte[]>> latestSeqEntries = storage.scanReverse(
                ChronosKeys.SEQ_PREFIX_BYTES, ChronosKeys.SEQ_END_BYTES, 1);
        if (!latestSeqEntries.isEmpty()) {
            String keyStr = new String(latestSeqEntries.get(0).getKey(), StandardCharsets.UTF_8);
            String seqPart = keyStr.substring(ChronosKeys.SEQ_PREFIX.length());
            try {
                maxSeq = Math.max(maxSeq, Long.parseLong(seqPart));
            } catch (NumberFormatException ignored) {
                // Ignore malformed key
            }
        }

        sequenceCounter.set(maxSeq);
        log.debug("Chronos VersionCatalog initialized with highest sequence={}", maxSeq);
    }

    /**
     * Reinitialize in-memory sequence state after restoring storage from a Raft snapshot.
     */
    public void reloadFromStorage() {
        synchronized (writeLock) {
            sequenceCounter.set(0);
            initializeSequenceCounter();
        }
    }

    /**
     * Highest timestamp persisted in the version time index, or {@code 0L} if empty.
     */
    public long getLatestTimestamp() {
        List<Map.Entry<byte[], byte[]>> entries = storage.scanReverse(
                ChronosKeys.TIME_PREFIX_BYTES, ChronosKeys.TIME_END_BYTES, 1);
        if (entries.isEmpty()) {
            return 0L;
        }
        String versionId = new String(entries.get(0).getValue(), StandardCharsets.UTF_8);
        return getVersion(versionId).map(ChronosVersion::getTimestamp).orElse(0L);
    }

    /**
     * Allocate the next monotonic version sequence number.
     */
    public long nextSequence() {
        return sequenceCounter.incrementAndGet();
    }

    /**
     * Highest persisted version sequence observed by this catalog (0 if empty).
     */
    public long getHighestSequence() {
        return sequenceCounter.get();
    }

    /**
     * Persist a version and advance its branch head if {@code version.getStatus() == COMMITTED}.
     */
    public void saveVersion(ChronosVersion version) {
        saveVersion(version, true);
    }

    /**
     * Persist a new {@link ChronosVersion} or perform a valid speculative lifecycle transition
     * ({@code SPECULATIVE -> COMMITTED} or {@code SPECULATIVE -> ROLLED_BACK}).
     *
     * <p>Enforces immutability: already committed, rolled-back, or rejected versions cannot be
     * overwritten, and speculative transitions cannot modify immutable payload or lineage fields.</p>
     */
    public void saveVersion(ChronosVersion version, boolean advanceBranchHead) {
        Objects.requireNonNull(version, "version must not be null");

        synchronized (writeLock) {
            ChronosVersion existing = getVersion(version.getVersionId()).orElse(null);
            if (existing != null) {
                validateTransition(existing, version);
            } else {
                byte[] existingSeqOwner = storage.get(ChronosKeys.seqKey(version.getSequence()));
                if (existingSeqOwner != null) {
                    String ownerVersionId = new String(existingSeqOwner, StandardCharsets.UTF_8);
                    if (!ownerVersionId.equals(version.getVersionId())) {
                        throw new VersionImmutabilityException(
                                "Sequence " + version.getSequence() + " is already assigned to version " + ownerVersionId);
                    }
                }
            }
            persistVersionBatch(version, existing == null, Collections.emptyList(), advanceBranchHead);
        }
    }

    /**
     * Apply a Raft-committed {@link ChronosVersion} deterministically.
     *
     * <p>Provides idempotent replay if the identical committed version was already persisted
     * before restart, and archives any uncommitted speculative/rolled-back attempt from a
     * superseded term before installing the canonical Raft-committed version.</p>
     */
    public void saveCommittedFromRaft(ChronosVersion version) {
        Objects.requireNonNull(version, "version must not be null");
        if (version.getStatus() != VersionStatus.COMMITTED) {
            throw new IllegalArgumentException("saveCommittedFromRaft requires COMMITTED status");
        }

        synchronized (writeLock) {
            ChronosVersion existing = getVersion(version.getVersionId()).orElse(null);
            List<ChronosVersion> supersededToArchive = new ArrayList<>();
            boolean countAsNew = (existing == null);

            if (existing != null) {
                if (existing.getStatus() == VersionStatus.COMMITTED) {
                    if (!existing.sameImmutablePayload(version)) {
                        throw new VersionImmutabilityException(
                                "Conflicting Raft commit for immutable version " + version.getVersionId());
                    }
                    if (getBranchHead(version.getBranchId()).isEmpty()) {
                        storage.put(ChronosKeys.headKey(version.getBranchId()),
                                ChronosKeys.utf8(version.getVersionId()));
                    }
                    sequenceCounter.accumulateAndGet(version.getSequence(), Math::max);
                    return;
                }
                if (existing.getStatus() == VersionStatus.SPECULATIVE && existing.sameImmutablePayload(version)) {
                    persistVersionBatch(version, false, Collections.emptyList(), true);
                    return;
                }
                if (existing.getStatus() == VersionStatus.SPECULATIVE
                        || existing.getStatus() == VersionStatus.ROLLED_BACK) {
                    supersededToArchive.add(existing);
                } else {
                    throw new VersionImmutabilityException(
                            "Version " + existing.getVersionId() + " is immutable in status " + existing.getStatus());
                }
            }

            byte[] existingSeqOwner = storage.get(ChronosKeys.seqKey(version.getSequence()));
            if (existingSeqOwner != null) {
                String ownerVersionId = new String(existingSeqOwner, StandardCharsets.UTF_8);
                if (!ownerVersionId.equals(version.getVersionId())) {
                    ChronosVersion owner = getVersion(ownerVersionId).orElse(null);
                    if (owner != null && (owner.getStatus() == VersionStatus.SPECULATIVE
                            || owner.getStatus() == VersionStatus.ROLLED_BACK)) {
                        supersededToArchive.add(owner);
                    } else {
                        throw new VersionImmutabilityException(
                                "Sequence " + version.getSequence() + " is already assigned to committed version "
                                        + ownerVersionId);
                    }
                }
            }

            persistVersionBatch(version, countAsNew, supersededToArchive, true);
        }
    }

    private void persistVersionBatch(ChronosVersion version,
                                     boolean countAsNewVersion,
                                     List<ChronosVersion> supersededToArchive,
                                     boolean advanceBranchHead) {
        List<Map.Entry<byte[], byte[]>> batch = new ArrayList<>();

        for (ChronosVersion superseded : supersededToArchive) {
            ChronosVersion archived = superseded.getStatus() == VersionStatus.ROLLED_BACK
                    ? superseded
                    : superseded.withStatus(VersionStatus.ROLLED_BACK);
            batch.add(Map.entry(
                    ChronosKeys.rolledBackArchiveKey(archived.getVersionId(), archived.getRaftLogIndex()),
                    serialize(archived)));
            if (!archived.getVersionId().equals(version.getVersionId())) {
                batch.add(new AbstractMap.SimpleEntry<>(ChronosKeys.versionKey(archived.getVersionId()), null));
            }
            if (archived.getSequence() != version.getSequence()) {
                batch.add(new AbstractMap.SimpleEntry<>(ChronosKeys.seqKey(archived.getSequence()), null));
            }
            batch.add(new AbstractMap.SimpleEntry<>(
                    ChronosKeys.timeKey(archived.getTimestamp(), archived.getSequence(), archived.getVersionId()),
                    null));
        }

        batch.add(Map.entry(ChronosKeys.versionKey(version.getVersionId()), serialize(version)));
        byte[] versionIdBytes = ChronosKeys.utf8(version.getVersionId());
        batch.add(Map.entry(ChronosKeys.seqKey(version.getSequence()), versionIdBytes));
        batch.add(Map.entry(
                ChronosKeys.timeKey(version.getTimestamp(), version.getSequence(), version.getVersionId()),
                versionIdBytes));

        if (version.getSequence() > sequenceCounter.get()) {
            batch.add(Map.entry(ChronosKeys.LAST_SEQ_KEY_BYTES,
                    ChronosKeys.utf8(Long.toString(version.getSequence()))));
        }

        if (countAsNewVersion && supersededToArchive.isEmpty()) {
            CatalogWatermark current = getWatermark();
            long newTotal = current.getTotalVersions() + 1;
            long newEarliestSeq = current.getEarliestAvailableSeq() == 0
                    ? version.getSequence()
                    : Math.min(current.getEarliestAvailableSeq(), version.getSequence());
            long newEarliestTs = current.getTotalVersions() == 0
                    ? version.getTimestamp()
                    : Math.min(current.getEarliestAvailableTimestamp(), version.getTimestamp());
            CatalogWatermark updatedWatermark = new CatalogWatermark(
                    newEarliestSeq, newEarliestTs, newTotal, current.getLastPurgedAt());
            batch.add(Map.entry(ChronosKeys.WATERMARK_KEY_BYTES, serialize(updatedWatermark)));
        }

        if (advanceBranchHead && version.getStatus() == VersionStatus.COMMITTED) {
            batch.add(Map.entry(ChronosKeys.headKey(version.getBranchId()), versionIdBytes));
        }

        storage.writeBatch(batch);
        sequenceCounter.accumulateAndGet(version.getSequence(), Math::max);
    }

    /**
     * Roll back all {@code SPECULATIVE} versions whose {@code raftLogIndex >= fromRaftIndex}.
     * Used during Raft leader step-down or log truncation.
     *
     * @return number of speculative versions transitioned to {@code ROLLED_BACK}
     */
    public int rollbackSpeculativeByRaftIndex(long fromRaftIndex) {
        synchronized (writeLock) {
            int rolledBack = 0;
            List<Map.Entry<byte[], byte[]>> entries = storage.scanReverse(
                    ChronosKeys.SEQ_PREFIX_BYTES, ChronosKeys.SEQ_END_BYTES, 0);
            for (Map.Entry<byte[], byte[]> entry : entries) {
                String versionId = new String(entry.getValue(), StandardCharsets.UTF_8);
                ChronosVersion version = getVersion(versionId).orElse(null);
                if (version == null) continue;
                if (version.getRaftLogIndex() >= fromRaftIndex) {
                    if (version.getStatus() == VersionStatus.SPECULATIVE) {
                        saveVersion(version.withStatus(VersionStatus.ROLLED_BACK), false);
                        rolledBack++;
                    }
                } else if (version.getStatus() != VersionStatus.SPECULATIVE) {
                    break;
                }
            }
            return rolledBack;
        }
    }

    private void validateTransition(ChronosVersion existing, ChronosVersion incoming) {
        if (!existing.sameImmutablePayload(incoming)) {
            throw new VersionImmutabilityException(
                    "Cannot modify immutable payload or lineage of existing version " + existing.getVersionId());
        }
        if (existing.getStatus() != VersionStatus.SPECULATIVE) {
            throw new VersionImmutabilityException(
                    "Version " + existing.getVersionId() + " is immutable in status " + existing.getStatus());
        }
        if (incoming.getStatus() != VersionStatus.COMMITTED && incoming.getStatus() != VersionStatus.ROLLED_BACK) {
            throw new VersionImmutabilityException(
                    "Invalid speculative transition for " + existing.getVersionId()
                            + ": " + existing.getStatus() + " -> " + incoming.getStatus());
        }
    }

    /**
     * Promote an existing {@code SPECULATIVE} version to {@code COMMITTED} and advance its branch head.
     */
    public ChronosVersion promoteSpeculative(String versionId) {
        synchronized (writeLock) {
            ChronosVersion existing = getVersion(versionId)
                    .orElseThrow(() -> new IllegalArgumentException("Unknown version: " + versionId));
            ChronosVersion committed = existing.withStatus(VersionStatus.COMMITTED);
            saveVersion(committed, true);
            return committed;
        }
    }

    /**
     * Mark an existing {@code SPECULATIVE} version as {@code ROLLED_BACK} without advancing branch head.
     */
    public ChronosVersion rollbackSpeculative(String versionId) {
        synchronized (writeLock) {
            ChronosVersion existing = getVersion(versionId)
                    .orElseThrow(() -> new IllegalArgumentException("Unknown version: " + versionId));
            ChronosVersion rolledBack = existing.withStatus(VersionStatus.ROLLED_BACK);
            saveVersion(rolledBack, false);
            return rolledBack;
        }
    }

    public Optional<ChronosVersion> getVersion(String versionId) {
        if (versionId == null || versionId.isBlank()) {
            return Optional.empty();
        }
        byte[] data = storage.get(ChronosKeys.versionKey(versionId));
        if (data == null) {
            return Optional.empty();
        }
        return Optional.of(deserialize(data, ChronosVersion.class));
    }

    public Optional<ChronosVersion> getVersionBySequence(long sequence) {
        if (sequence < 1) {
            return Optional.empty();
        }
        byte[] versionIdBytes = storage.get(ChronosKeys.seqKey(sequence));
        if (versionIdBytes == null) {
            return Optional.empty();
        }
        return getVersion(new String(versionIdBytes, StandardCharsets.UTF_8));
    }

    public Optional<String> getBranchHead(String branchId) {
        if (branchId == null || branchId.isBlank()) {
            return Optional.empty();
        }
        byte[] headBytes = storage.get(ChronosKeys.headKey(branchId));
        return headBytes != null
                ? Optional.of(new String(headBytes, StandardCharsets.UTF_8))
                : Optional.empty();
    }

    public Optional<String> getBranchBase(String branchId) {
        if (branchId == null || branchId.isBlank()) {
            return Optional.empty();
        }
        byte[] bytes = storage.get(ChronosKeys.branchKey(branchId.trim()));
        if (bytes == null) {
            return Optional.empty();
        }
        try {
            com.chronos.branch.BranchInfo b = deserialize(bytes, com.chronos.branch.BranchInfo.class);
            return Optional.ofNullable(b.getBaseVersionId());
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    public Optional<ChronosVersion> getBranchHeadVersion(String branchId) {
        return getBranchHead(branchId).flatMap(this::getVersion);
    }

    /**
     * Find the latest committed version across any branch with {@code timestamp <= targetTimestamp}
     * in O(log N) time via reverse RocksDB seek.
     */
    public Optional<ChronosVersion> findVersionAtOrBeforeTimestamp(long targetTimestamp) {
        return findVersionAtOrBeforeTimestamp(targetTimestamp, null, false);
    }

    /**
     * Find the latest version matching the optional branch and status filter with
     * {@code timestamp <= targetTimestamp} via paged reverse seek.
     */
    public Optional<ChronosVersion> findVersionAtOrBeforeTimestamp(
            long targetTimestamp, String branchId, boolean includeSpeculative) {
        if (targetTimestamp < 0) {
            return Optional.empty();
        }
        byte[] upperBound = ChronosKeys.timeUpperBoundForTimestamp(targetTimestamp);
        while (true) {
            List<Map.Entry<byte[], byte[]>> page = storage.scanReverse(
                    ChronosKeys.TIME_PREFIX_BYTES, upperBound, TIME_SEEK_PAGE_SIZE);
            if (page.isEmpty()) {
                return Optional.empty();
            }
            for (Map.Entry<byte[], byte[]> entry : page) {
                String versionId = new String(entry.getValue(), StandardCharsets.UTF_8);
                Optional<ChronosVersion> candidate = getVersion(versionId);
                if (candidate.isPresent() && matchesQueryFilter(candidate.get(), branchId, includeSpeculative)) {
                    return candidate;
                }
            }
            if (page.size() < TIME_SEEK_PAGE_SIZE) {
                return Optional.empty();
            }
            // Page backwards using the oldest entry key from this batch as the new exclusive upper bound
            upperBound = page.get(page.size() - 1).getKey();
        }
    }

    private boolean matchesQueryFilter(ChronosVersion v, String branchId, boolean includeSpeculative) {
        if (branchId != null && !branchId.isBlank() && !branchId.equals(v.getBranchId())) {
            return false;
        }
        if (v.getStatus() == VersionStatus.COMMITTED) {
            return true;
        }
        return includeSpeculative && v.getStatus() == VersionStatus.SPECULATIVE;
    }

    /**
     * List versions ordered ascending by sequence.
     */
    public List<ChronosVersion> listVersions(int limit) {
        List<Map.Entry<byte[], byte[]>> entries = storage.scan(
                ChronosKeys.SEQ_PREFIX_BYTES, ChronosKeys.SEQ_END_BYTES, limit);
        return resolveVersionEntries(entries);
    }

    /**
     * List versions ordered descending by sequence (newest first).
     */
    public List<ChronosVersion> listVersionsDescending(int limit) {
        List<Map.Entry<byte[], byte[]>> entries = storage.scanReverse(
                ChronosKeys.SEQ_PREFIX_BYTES, ChronosKeys.SEQ_END_BYTES, limit);
        return resolveVersionEntries(entries);
    }

    /**
     * List versions in {@code [fromTimestamp, toTimestamp]} inclusive ordered by time and sequence.
     */
    public List<ChronosVersion> listVersionsByTimeRange(long fromTimestamp, long toTimestamp, int limit) {
        if (toTimestamp < fromTimestamp || toTimestamp < 0) {
            return Collections.emptyList();
        }
        List<Map.Entry<byte[], byte[]>> entries = storage.scan(
                ChronosKeys.timeLowerBoundForTimestamp(fromTimestamp),
                ChronosKeys.timeUpperBoundForTimestamp(toTimestamp),
                limit);
        return resolveVersionEntries(entries);
    }

    /**
     * List versions belonging to a specific branch ordered ascending by sequence.
     */
    public List<ChronosVersion> listVersionsByBranch(String branchId, int limit) {
        if (branchId == null || branchId.isBlank()) {
            return Collections.emptyList();
        }
        List<Map.Entry<byte[], byte[]>> entries = storage.scan(
                ChronosKeys.SEQ_PREFIX_BYTES, ChronosKeys.SEQ_END_BYTES, 0);
        List<ChronosVersion> filtered = new ArrayList<>();
        for (Map.Entry<byte[], byte[]> entry : entries) {
            String versionId = new String(entry.getValue(), StandardCharsets.UTF_8);
            getVersion(versionId).ifPresent(v -> {
                if (branchId.equals(v.getBranchId())) {
                    filtered.add(v);
                }
            });
            if (limit > 0 && filtered.size() >= limit) {
                break;
            }
        }
        return filtered;
    }

    /**
     * List all branch heads currently tracked in the catalog.
     */
    public Map<String, String> listBranchHeads() {
        List<Map.Entry<byte[], byte[]>> entries = storage.scan(
                ChronosKeys.HEAD_PREFIX_BYTES, ChronosKeys.HEAD_END_BYTES, 0);
        Map<String, String> heads = new LinkedHashMap<>();
        for (Map.Entry<byte[], byte[]> entry : entries) {
            String fullKey = new String(entry.getKey(), StandardCharsets.UTF_8);
            String branchId = fullKey.substring(ChronosKeys.HEAD_PREFIX.length());
            heads.put(branchId, new String(entry.getValue(), StandardCharsets.UTF_8));
        }
        return heads;
    }

    /**
     * Persist an immutable named snapshot referencing an existing version.
     */
    public void saveSnapshot(ChronosSnapshot snapshot) {
        Objects.requireNonNull(snapshot, "snapshot must not be null");
        synchronized (writeLock) {
            byte[] key = ChronosKeys.snapshotKey(snapshot.getSnapshotId());
            if (storage.get(key) != null) {
                throw new VersionImmutabilityException(
                        "Snapshot already exists and is immutable: " + snapshot.getSnapshotId());
            }
            if (getVersion(snapshot.getVersionId()).isEmpty()) {
                throw new IllegalArgumentException(
                        "Cannot create snapshot for unknown version: " + snapshot.getVersionId());
            }
            storage.put(key, serialize(snapshot));
        }
    }

    public Optional<ChronosSnapshot> getSnapshot(String snapshotId) {
        if (snapshotId == null || snapshotId.isBlank()) {
            return Optional.empty();
        }
        byte[] data = storage.get(ChronosKeys.snapshotKey(snapshotId));
        if (data == null) {
            return Optional.empty();
        }
        return Optional.of(deserialize(data, ChronosSnapshot.class));
    }

    public List<ChronosSnapshot> listSnapshots() {
        List<Map.Entry<byte[], byte[]>> entries = storage.scan(
                ChronosKeys.SNAPSHOT_PREFIX_BYTES, ChronosKeys.SNAPSHOT_END_BYTES, 0);
        List<ChronosSnapshot> snapshots = new ArrayList<>(entries.size());
        for (Map.Entry<byte[], byte[]> entry : entries) {
            snapshots.add(deserialize(entry.getValue(), ChronosSnapshot.class));
        }
        return snapshots;
    }

    public CatalogWatermark getWatermark() {
        byte[] data = storage.get(ChronosKeys.WATERMARK_KEY_BYTES);
        if (data == null) {
            return CatalogWatermark.empty();
        }
        return deserialize(data, CatalogWatermark.class);
    }

    public void updateWatermark(CatalogWatermark watermark) {
        Objects.requireNonNull(watermark, "watermark must not be null");
        synchronized (writeLock) {
            storage.put(ChronosKeys.WATERMARK_KEY_BYTES, serialize(watermark));
        }
    }

    /**
     * Register a supplier of version IDs that must remain protected from garbage collection
     * (e.g. active branch bases and unresolved LCAs).
     */
    public void registerProtectedVersionsSupplier(Supplier<Set<String>> supplier) {
        if (supplier != null) {
            protectedVersionSuppliers.add(supplier);
        }
    }

    /**
     * Return all version IDs protected by branch heads and registered branch/LCA suppliers.
     */
    public Set<String> getProtectedVersionIds() {
        Set<String> protectedIds = new HashSet<>(listBranchHeads().values());
        for (Supplier<Set<String>> supplier : protectedVersionSuppliers) {
            Set<String> extra = supplier.get();
            if (extra != null) {
                protectedIds.addAll(extra);
            }
        }
        return protectedIds;
    }

    /**
     * Atomically advance the safe GC watermark FIRST, then delete the purged
     * {@link ChronosVersion} records and secondary indexes from RocksDB.
     */
    public int purgeVersionsWithWatermark(List<ChronosVersion> versionsToPurge, CatalogWatermark newWatermark) {
        Objects.requireNonNull(newWatermark, "newWatermark must not be null");
        if (versionsToPurge == null || versionsToPurge.isEmpty()) {
            return 0;
        }
        synchronized (writeLock) {
            // Advance watermark before deleting version entries so concurrent readers fail fast cleanly.
            storage.put(ChronosKeys.WATERMARK_KEY_BYTES, serialize(newWatermark));

            List<Map.Entry<byte[], byte[]>> deleteBatch = new ArrayList<>(versionsToPurge.size() * 3);
            for (ChronosVersion v : versionsToPurge) {
                deleteBatch.add(new AbstractMap.SimpleEntry<>(ChronosKeys.versionKey(v.getVersionId()), null));
                deleteBatch.add(new AbstractMap.SimpleEntry<>(ChronosKeys.seqKey(v.getSequence()), null));
                deleteBatch.add(new AbstractMap.SimpleEntry<>(
                        ChronosKeys.timeKey(v.getTimestamp(), v.getSequence(), v.getVersionId()), null));
            }
            storage.writeBatch(deleteBatch);
            return versionsToPurge.size();
        }
    }

    private List<ChronosVersion> resolveVersionEntries(List<Map.Entry<byte[], byte[]>> entries) {
        List<ChronosVersion> result = new ArrayList<>(entries.size());
        for (Map.Entry<byte[], byte[]> entry : entries) {
            String versionId = new String(entry.getValue(), StandardCharsets.UTF_8);
            getVersion(versionId).ifPresent(result::add);
        }
        return result;
    }

    private byte[] serialize(Object value) {
        try {
            return mapper.writeValueAsBytes(value);
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize Chronos catalog object", e);
        }
    }

    private <T> T deserialize(byte[] bytes, Class<T> type) {
        try {
            return mapper.readValue(bytes, type);
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize Chronos catalog object as " + type.getSimpleName(), e);
        }
    }
}

