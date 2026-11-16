package com.chronos.merge;

import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe registry for domain invariants validated during merge attempts.
 */
@Service
public class InvariantRegistry {

    private final Map<String, InvariantRule> rules = new ConcurrentHashMap<>();

    public InvariantRegistry() {
        resetDefaults();
    }

    /**
     * Resets the registry to default built-in domain invariants.
     */
    public void resetDefaults() {
        rules.clear();
        registerRule(InvariantRule.nonNegative("NonNegativeBalance", "balance:"));
        registerRule(InvariantRule.nonNegative("NonNegativeAccount", "account:"));
        registerRule(InvariantRule.nonNegative("NonNegativeInventory", "inventory:"));
        registerRule(InvariantRule.nonNegative("NonNegativeCredits", "credits:"));
    }

    public void registerRule(InvariantRule rule) {
        Objects.requireNonNull(rule, "rule must not be null");
        rules.put(rule.getName(), rule);
    }

    public boolean unregisterRule(String ruleName) {
        if (ruleName == null) return false;
        return rules.remove(ruleName) != null;
    }

    public Optional<InvariantRule> getRule(String ruleName) {
        if (ruleName == null) return Optional.empty();
        return Optional.ofNullable(rules.get(ruleName));
    }

    public List<InvariantRule> getRules() {
        return List.copyOf(rules.values());
    }

    public void clear() {
        rules.clear();
    }
}

