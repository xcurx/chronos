package com.chronos.gc;

import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Tracks active historical read pins ({@code AS OF VERSION}, {@code AS OF TIME},
 * {@code AS OF SNAPSHOT}, diffs) so background garbage collection never deletes
 * versions or timestamps actively being inspected by concurrent readers.
 */
@Service
public class ReadWatermarkRegistry {

    public static final class ReadPin {
        private final String pinId;
        private volatile long minSeq;
        private volatile long minTimestamp;
        private final long acquiredAt;

        public ReadPin(String pinId, long minSeq, long minTimestamp, long acquiredAt) {
            this.pinId = pinId;
            this.minSeq = minSeq;
            this.minTimestamp = minTimestamp;
            this.acquiredAt = acquiredAt;
        }

        public String getPinId() { return pinId; }
        public long getMinSeq() { return minSeq; }
        public long getMinTimestamp() { return minTimestamp; }
        public long getAcquiredAt() { return acquiredAt; }

        void update(long newMinSeq, long newMinTimestamp) {
            this.minSeq = newMinSeq;
            this.minTimestamp = newMinTimestamp;
        }
    }

    /**
     * Scoped handle released in try-with-resources when a temporal query completes.
     */
    public final class ReadLease implements AutoCloseable {
        private final ReadPin pin;
        private final AtomicBoolean closed = new AtomicBoolean(false);

        private ReadLease(ReadPin pin) {
            this.pin = pin;
        }

        public String getPinId() {
            return pin.getPinId();
        }

        public void updateBounds(long minSeq, long minTimestamp) {
            if (!closed.get()) {
                pin.update(minSeq, minTimestamp);
            }
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                activePins.remove(pin.getPinId());
            }
        }
    }

    private final ConcurrentHashMap<String, ReadPin> activePins = new ConcurrentHashMap<>();

    /**
     * Acquire a read pin protecting all versions with {@code seq >= minSeq} or
     * {@code timestamp >= minTimestamp}.
     */
    public ReadLease acquirePin(long minSeq, long minTimestamp) {
        String id = UUID.randomUUID().toString();
        ReadPin pin = new ReadPin(id, minSeq, minTimestamp, System.currentTimeMillis());
        activePins.put(id, pin);
        return new ReadLease(pin);
    }

    /**
     * Lowest sequence number pinned by any active reader, or {@link Long#MAX_VALUE} if idle.
     */
    public long getActiveLowWatermarkSeq() {
        long min = Long.MAX_VALUE;
        for (ReadPin pin : activePins.values()) {
            min = Math.min(min, pin.getMinSeq());
        }
        return min;
    }

    /**
     * Lowest timestamp pinned by any active reader, or {@link Long#MAX_VALUE} if idle.
     */
    public long getActiveLowWatermarkTimestamp() {
        long min = Long.MAX_VALUE;
        for (ReadPin pin : activePins.values()) {
            min = Math.min(min, pin.getMinTimestamp());
        }
        return min;
    }

    public int getActivePinCount() {
        return activePins.size();
    }

    public List<ReadPin> getActivePinsSnapshot() {
        return new ArrayList<>(activePins.values());
    }
}

