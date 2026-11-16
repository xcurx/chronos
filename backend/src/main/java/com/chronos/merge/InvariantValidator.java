package com.chronos.merge;

import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Validates tentative merged key-value states against registered domain safety invariant rules.
 */
@Service
public class InvariantValidator {

    private final InvariantRegistry registry;

    public InvariantValidator(InvariantRegistry registry) {
        this.registry = Objects.requireNonNull(registry, "registry must not be null");
    }

    /**
     * Validate tentative merged state against all registered invariant rules.
     */
    public List<InvariantViolation> validate(Map<String, String> tentativeState, Map<String, String> baseState) {
        return validate(tentativeState, baseState, registry.getRules());
    }

    /**
     * Validate tentative merged state against a specific collection of rules.
     */
    public List<InvariantViolation> validate(
            Map<String, String> tentativeState,
            Map<String, String> baseState,
            Collection<InvariantRule> rules) {
        if (tentativeState == null || rules == null || rules.isEmpty()) {
            return Collections.emptyList();
        }

        List<InvariantViolation> violations = new ArrayList<>();
        for (InvariantRule rule : rules) {
            violations.addAll(rule.validate(tentativeState, baseState));
        }
        return violations;
    }
}

