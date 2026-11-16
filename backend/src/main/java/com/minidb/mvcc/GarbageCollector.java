package com.minidb.mvcc;

import com.chronos.gc.ChronosGcService;
import com.minidb.config.RaftConfig;
import com.minidb.raft.RaftNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Background garbage collector that delegates to Chronos Safe Low-Watermark GC when
 * available, falling back to MVCC cleanup otherwise.
 */
@Component
public class GarbageCollector {

    private static final Logger log = LoggerFactory.getLogger(GarbageCollector.class);

    private final RaftNode raftNode;
    private final RaftConfig raftConfig;
    private final ChronosGcService chronosGcService;
    private final AtomicLong totalPurged = new AtomicLong(0);

    public GarbageCollector(@Lazy RaftNode raftNode, RaftConfig raftConfig) {
        this(raftNode, raftConfig, null);
    }

    @Autowired
    public GarbageCollector(@Lazy RaftNode raftNode,
                            RaftConfig raftConfig,
                            @Lazy ChronosGcService chronosGcService) {
        this.raftNode = raftNode;
        this.raftConfig = raftConfig;
        this.chronosGcService = chronosGcService;
    }

    @Scheduled(fixedDelayString = "${minidb.mvcc.gc-interval-ms:60000}")
    public void runGc() {
        try {
            if (chronosGcService != null) {
                ChronosGcService.GcRunResult res = chronosGcService.runGc();
                int purged = res.versionsPurged() + res.mvccEntriesPurged();
                totalPurged.addAndGet(purged);
                if (purged > 0) {
                    log.info("GC cycle complete: purged {} catalog versions + {} MVCC entries (total={})",
                            res.versionsPurged(), res.mvccEntriesPurged(), totalPurged.get());
                }
                return;
            }
            MvccStore mvccStore = raftNode.getMvccStore();
            if (mvccStore == null) return;
            int purged = mvccStore.garbageCollect(raftConfig.getMvccRetentionMs());
            totalPurged.addAndGet(purged);
            if (purged > 0) {
                log.info("GC cycle complete: purged {} versions, total purged: {}",
                        purged, totalPurged.get());
            }
        } catch (Exception e) {
            log.error("GC cycle failed", e);
        }
    }

    public long getTotalPurged() {
        return totalPurged.get();
    }
}
