package com.chronos.merge;

/**
 * Details of an invariant rule failure during tentative merge validation.
 */
public record InvariantViolation(
        String ruleName,
        String key,
        String actualValue,
        String expectedConstraint,
        String message
) {}

