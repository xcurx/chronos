package com.chronos.gc;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Runtime-configurable retention policy and audit-pin configuration for Chronos Safe GC.
 */
@Configuration
public class ChronosRetentionConfig {

    private volatile RetentionPolicy policy = RetentionPolicy.HYBRID;
    private volatile int maxVersions = 10_000;
    private volatile long windowMs = 3_600_000L;
    private final Set<String> pinnedVersions = ConcurrentHashMap.newKeySet();

    public ChronosRetentionConfig() {
    }

    public ChronosRetentionConfig(RetentionPolicy policy, int maxVersions, long windowMs, List<String> initialPins) {
        if (policy != null) this.policy = policy;
        if (maxVersions >= 0) this.maxVersions = maxVersions;
        if (windowMs >= 0) this.windowMs = windowMs;
        if (initialPins != null) {
            initialPins.stream().filter(s -> s != null && !s.isBlank()).forEach(pinnedVersions::add);
        }
    }

    @Value("${chronos.retention.policy:HYBRID}")
    public void setPolicyFromConfig(String rawPolicy) {
        if (rawPolicy != null && !rawPolicy.isBlank()) {
            this.policy = RetentionPolicy.valueOf(rawPolicy.trim().toUpperCase());
        }
    }

    @Value("${chronos.retention.max-versions:10000}")
    public void setMaxVersions(int maxVersions) {
        if (maxVersions < 1) {
            throw new IllegalArgumentException("maxVersions must be >= 1");
        }
        this.maxVersions = maxVersions;
    }

    @Value("${chronos.retention.window-ms:3600000}")
    public void setWindowMs(long windowMs) {
        if (windowMs < 0) {
            throw new IllegalArgumentException("windowMs must be >= 0");
        }
        this.windowMs = windowMs;
    }

    @Value("${chronos.retention.pinned-versions:}")
    public void setInitialPinnedVersions(List<String> initialPins) {
        if (initialPins != null) {
            initialPins.stream().filter(s -> s != null && !s.isBlank()).forEach(pinnedVersions::add);
        }
    }

    public RetentionPolicy getPolicy() {
        return policy;
    }

    public void setPolicy(RetentionPolicy policy) {
        if (policy != null) {
            this.policy = policy;
        }
    }

    public int getMaxVersions() {
        return maxVersions;
    }

    public long getWindowMs() {
        return windowMs;
    }

    public Set<String> getPinnedVersions() {
        return Collections.unmodifiableSet(pinnedVersions);
    }

    public boolean pinVersion(String versionId) {
        return versionId != null && !versionId.isBlank() && pinnedVersions.add(versionId.trim());
    }

    public boolean unpinVersion(String versionId) {
        return versionId != null && pinnedVersions.remove(versionId.trim());
    }
}

