package com.chronos.merge;

import java.util.Collections;
import java.util.Map;

/**
 * Request payload for evaluating a merge preview or committing a branch merge.
 */
public record MergeRequest(
        String sourceBranch,
        String targetBranch,
        MergePolicy policy,
        Map<String, String> customResolutions,
        String writerId,
        String commitMessage
) {
    public MergeRequest {
        if (sourceBranch == null || sourceBranch.isBlank()) {
            throw new IllegalArgumentException("sourceBranch must be non-blank");
        }
        if (targetBranch == null || targetBranch.isBlank()) {
            targetBranch = "main";
        }
        if (policy == null) {
            policy = MergePolicy.COMMUTATIVE_DELTA;
        }
        if (customResolutions == null) {
            customResolutions = Collections.emptyMap();
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String sourceBranch;
        private String targetBranch = "main";
        private MergePolicy policy = MergePolicy.COMMUTATIVE_DELTA;
        private Map<String, String> customResolutions = Collections.emptyMap();
        private String writerId;
        private String commitMessage;

        public Builder sourceBranch(String sourceBranch) { this.sourceBranch = sourceBranch; return this; }
        public Builder targetBranch(String targetBranch) { this.targetBranch = targetBranch; return this; }
        public Builder policy(MergePolicy policy) { this.policy = policy; return this; }
        public Builder customResolutions(Map<String, String> customResolutions) { this.customResolutions = customResolutions; return this; }
        public Builder writerId(String writerId) { this.writerId = writerId; return this; }
        public Builder commitMessage(String commitMessage) { this.commitMessage = commitMessage; return this; }

        public MergeRequest build() {
            return new MergeRequest(sourceBranch, targetBranch, policy, customResolutions, writerId, commitMessage);
        }
    }
}

